use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use mavlink::{
    MavHeader,
    dialects::ardupilotmega::{
        CAMERA_CAPTURE_STATUS_DATA, COMMAND_ACK_DATA, MavCmd, MavMessage, MavResult,
    },
};
use tracing::*;
use zenoh::pubsub::Publisher;

use super::super::encode;
use crate::service::SystemAndComponent;

#[allow(dead_code)]
pub struct VideoStream {
    pub topic: String,
    pub camera: SystemAndComponent,
    pub is_recording: bool,
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
    pub(super) fn new(topic: String, camera: SystemAndComponent) -> Self {
        Self {
            topic,
            camera,
            is_recording: false,
            recording_start: None,
            sequence: 0,
            status_task: None,
        }
    }

    /// Applies a recording command and returns the MAVLink frames to publish in
    /// reply. Kept synchronous so the caller can release the video-stream lock
    /// before awaiting the publishes; the periodic status task is spawned here.
    #[allow(deprecated)]
    pub(super) fn handle_command(
        &mut self,
        command: MavCmd,
        params: [f32; 7],
        publisher: &Arc<Publisher<'static>>,
    ) -> Vec<Vec<u8>> {
        match command {
            MavCmd::MAV_CMD_VIDEO_START_CAPTURE => {
                self.is_recording = true;
                self.recording_start = Some(Instant::now());

                let status_hz = params[1].clamp(1.0, 10.0);
                info!(status_hz, "Video recording started");

                let ack = build_command_ack(
                    self.camera,
                    self.next_sequence(),
                    command,
                    MavResult::MAV_RESULT_ACCEPTED,
                );

                let interval_ms = (1000.0 / status_hz) as u64;
                self.spawn_status_task(publisher.clone(), interval_ms);

                vec![ack]
            }
            MavCmd::MAV_CMD_VIDEO_STOP_CAPTURE => {
                self.is_recording = false;
                self.recording_start = None;
                self.abort_status_task();

                info!("Video recording stopped");

                vec![build_command_ack(
                    self.camera,
                    self.next_sequence(),
                    command,
                    MavResult::MAV_RESULT_ACCEPTED,
                )]
            }
            MavCmd::MAV_CMD_REQUEST_CAMERA_CAPTURE_STATUS => {
                let (video_status, recording_time_ms) = self.capture_status_fields();

                let ack = build_command_ack(
                    self.camera,
                    self.next_sequence(),
                    command,
                    MavResult::MAV_RESULT_ACCEPTED,
                );
                let status = build_camera_capture_status(
                    self.camera,
                    self.next_sequence(),
                    video_status,
                    recording_time_ms,
                );

                vec![ack, status]
            }
            _ => {
                trace!("Unhandled recording command");
                vec![]
            }
        }
    }

    fn next_sequence(&mut self) -> u8 {
        let sequence = self.sequence;
        self.sequence = self.sequence.wrapping_add(1);
        sequence
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

    fn spawn_status_task(&mut self, publisher: Arc<Publisher<'static>>, interval_ms: u64) {
        self.abort_status_task();

        let Some(recording_start) = self.recording_start else {
            return;
        };

        let camera = self.camera;
        let mut sequence = self.sequence;
        self.status_task = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms.max(100)));
            loop {
                interval.tick().await;

                let recording_time_ms = recording_start.elapsed().as_millis() as u32;
                let bytes = build_camera_capture_status(camera, sequence, 1, recording_time_ms);
                sequence = sequence.wrapping_add(1);

                if let Err(error) = publisher.put(bytes).await {
                    warn!(%error, "Failed to publish capture status");
                }
            }
        }));
    }

    fn abort_status_task(&mut self) {
        if let Some(task) = self.status_task.take() {
            task.abort();
        }
    }
}

pub(super) fn video_topic_from_name(name: &str) -> String {
    let sanitized_stream_name = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();
    format!("video/{sanitized_stream_name}/stream")
}

fn build_command_ack(
    camera: SystemAndComponent,
    sequence: u8,
    command: MavCmd,
    result: MavResult,
) -> Vec<u8> {
    encode(
        camera_header(camera, sequence),
        &MavMessage::COMMAND_ACK(COMMAND_ACK_DATA { command, result }),
    )
}

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
        }),
    )
}

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
