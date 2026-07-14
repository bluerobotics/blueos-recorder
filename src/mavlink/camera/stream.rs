use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use mavlink::{
    MavHeader,
    ardupilotmega::{CAMERA_CAPTURE_STATUS_DATA, COMMAND_ACK_DATA, MavCmd, MavMessage, MavResult},
};
use tracing::{Instrument, *};
use zenoh::pubsub::Publisher;

use super::super::encode;
use crate::{mavlink::worker::VideoRecordingGate, service::SystemAndComponent};

pub struct VideoStream {
    #[allow(dead_code)]
    pub topic: String,
    pub camera: SystemAndComponent,
    pub is_recording: bool,
    recording_gate: Arc<VideoRecordingGate>,
    recording_start: Option<Instant>,
    sequence: u8,
    status_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for VideoStream {
    fn drop(&mut self) {
        self.abort_status_task();
    }
}

impl VideoStream {
    #[instrument(skip_all, level = "debug", fields(stream_topic = %topic))]
    pub(super) fn new(
        topic: String,
        camera: SystemAndComponent,
        recording_gate: Arc<VideoRecordingGate>,
    ) -> Self {
        Self {
            topic,
            camera,
            is_recording: false,
            recording_gate,
            recording_start: None,
            sequence: 0,
            status_task: None,
        }
    }

    fn next_sequence(&mut self) -> u8 {
        let sequence = self.sequence;
        self.sequence = self.sequence.wrapping_add(1);
        sequence
    }

    #[instrument(skip(self, publisher), fields(interval_ms))]
    fn spawn_status_task(&mut self, publisher: Arc<Publisher<'static>>, interval_ms: u64) {
        self.abort_status_task();

        let Some(recording_start) = self.recording_start else {
            return;
        };

        self.status_task = Some(tokio::spawn({
            let system_id = self.camera.system_id;
            let component_id = self.camera.component_id;
            let mut sequence = self.sequence;
            async move {
                let mut interval =
                    tokio::time::interval(Duration::from_millis(interval_ms.max(100)));
                loop {
                    interval.tick().await;

                    let recording_time_ms = recording_start.elapsed().as_millis() as u32;
                    let bytes = build_camera_capture_status(
                        SystemAndComponent {
                            system_id,
                            component_id,
                        },
                        sequence,
                        1,
                        recording_time_ms,
                    );
                    sequence = sequence.wrapping_add(1);

                    let span = debug_span!("capture_status_tick", system_id, component_id,);
                    async {
                        if let Err(error) = publisher.put(bytes).await {
                            warn!(%error, "Failed to publish capture status");
                        }
                    }
                    .instrument(span)
                    .await;
                }
            }
        }));
    }

    fn abort_status_task(&mut self) {
        if let Some(task) = self.status_task.take() {
            task.abort();
        }
    }

    #[instrument(
        skip(self, publisher),
        fields(command = ?command, stream_topic = %self.topic),
    )]
    #[allow(deprecated)]
    pub(super) async fn handle_command(
        &mut self,
        command: MavCmd,
        params: [f32; 7],
        publisher: &Arc<Publisher<'static>>,
    ) {
        match command {
            MavCmd::MAV_CMD_VIDEO_START_CAPTURE => {
                self.is_recording = true;
                self.recording_gate.set_recording(&self.topic, true);
                self.recording_start = Some(Instant::now());

                let status_hz = params[1].clamp(1.0, 10.0);
                info!(status_hz = status_hz, "Video recording started");

                let ack = build_command_ack(
                    self.camera,
                    self.next_sequence(),
                    command,
                    MavResult::MAV_RESULT_ACCEPTED,
                );
                publish_mavlink(publisher, ack, "COMMAND_ACK").await;

                let interval_ms = (1000.0 / status_hz) as u64;
                debug!(interval_ms, "Starting periodic CAMERA_CAPTURE_STATUS task");
                self.spawn_status_task(publisher.clone(), interval_ms);
            }
            MavCmd::MAV_CMD_VIDEO_STOP_CAPTURE => {
                self.is_recording = false;
                self.recording_gate.set_recording(&self.topic, false);
                self.recording_start = None;
                self.abort_status_task();

                info!("Video recording stopped");

                let ack = build_command_ack(
                    self.camera,
                    self.next_sequence(),
                    command,
                    MavResult::MAV_RESULT_ACCEPTED,
                );
                publish_mavlink(publisher, ack, "COMMAND_ACK").await;
            }
            MavCmd::MAV_CMD_REQUEST_CAMERA_CAPTURE_STATUS => {
                let (video_status, recording_time_ms) = self.capture_status_fields();
                debug!(
                    is_recording = self.is_recording,
                    video_status, recording_time_ms, "Camera capture status requested"
                );

                let ack = build_command_ack(
                    self.camera,
                    self.next_sequence(),
                    command,
                    MavResult::MAV_RESULT_ACCEPTED,
                );
                publish_mavlink(publisher, ack, "COMMAND_ACK").await;

                let status = build_camera_capture_status(
                    self.camera,
                    self.next_sequence(),
                    video_status,
                    recording_time_ms,
                );
                publish_mavlink(publisher, status, "CAMERA_CAPTURE_STATUS").await;
            }
            _ => trace!("Unhandled recording command"),
        }
    }

    fn capture_status_fields(&self) -> (u8, u32) {
        if self.is_recording {
            (
                1,
                self.recording_start
                    .map(|start| start.elapsed().as_millis() as u32)
                    .unwrap_or(0),
            )
        } else {
            (0, 0)
        }
    }
}

#[instrument(skip(publisher, bytes), fields(message = message, bytes = bytes.len()))]
async fn publish_mavlink(publisher: &Arc<Publisher<'static>>, bytes: Vec<u8>, message: &str) {
    debug!("Publishing MAVLink reply");
    if let Err(error) = publisher.put(bytes).await {
        warn!(%error, "Failed to publish MAVLink message");
    }
}

pub(super) fn video_topic_from_name(name: &str) -> String {
    let sanitized_stream_name = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();
    format!("video/{sanitized_stream_name}/stream")
}

#[instrument(skip_all, level = "trace")]
fn build_command_ack(
    camera: SystemAndComponent,
    sequence: u8,
    command: MavCmd,
    result: MavResult,
) -> Vec<u8> {
    encode(
        camera_header(camera, sequence),
        &MavMessage::COMMAND_ACK(COMMAND_ACK_DATA {
            command,
            result,
            progress: u8::MAX,
            result_param2: 0,
            target_system: 0,
            target_component: 0,
        }),
    )
}

#[instrument(skip_all, level = "trace")]
fn build_camera_capture_status(
    camera: SystemAndComponent,
    sequence: u8,
    video_status: u8,
    recording_time_ms: u32,
) -> Vec<u8> {
    encode(
        camera_header(camera, sequence),
        &MavMessage::CAMERA_CAPTURE_STATUS(CAMERA_CAPTURE_STATUS_DATA {
            time_boot_ms: time_boot_ms(),
            image_interval: 0.0,
            recording_time_ms,
            available_capacity: 0.0,
            image_status: 0,
            video_status,
            image_count: 0,
        }),
    )
}

#[instrument(skip_all, level = "trace")]
fn camera_header(camera: SystemAndComponent, sequence: u8) -> MavHeader {
    MavHeader {
        system_id: camera.system_id,
        component_id: camera.component_id,
        sequence,
    }
}

fn time_boot_ms() -> u32 {
    static RECORDER_BOOT: LazyLock<Instant> = LazyLock::new(Instant::now);
    RECORDER_BOOT.elapsed().as_millis() as u32
}
