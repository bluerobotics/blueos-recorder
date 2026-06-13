use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use mavlink::{
    MessageData,
    ardupilotmega::{
        CAMERA_INFORMATION_DATA, COMMAND_LONG_DATA, CameraCapFlags, MavCmd, MavComponent,
        VIDEO_STREAM_INFORMATION_DATA,
    },
};
use tracing::*;
use zenoh::pubsub::Publisher;

use super::encode_command_long;
use crate::service::SystemAndComponent;

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);

#[allow(dead_code)]
pub struct VideoStream {
    pub topic: String,
    camera: SystemAndComponent,
    pub is_recording: bool,
}

impl VideoStream {
    fn new(topic: String, camera: SystemAndComponent) -> Self {
        Self {
            topic,
            camera,
            is_recording: false,
        }
    }

    fn handle_command(
        &mut self,
        command: MavCmd,
        _params: [f32; 7],
        _publisher: &Arc<Publisher<'static>>,
    ) {
        trace!(?command, camera = ?self.camera, "Recording command received");
    }
}

pub struct CameraDiscoverer {
    state: Arc<Mutex<CameraDiscovererState>>,
    publisher: Arc<Publisher<'static>>,
}

struct CameraDiscovererState {
    cameras: HashSet<SystemAndComponent>,
    source: SystemAndComponent,
    sequence: u8,
}

impl CameraDiscovererState {
    fn new() -> Self {
        Self {
            cameras: HashSet::new(),
            source: SystemAndComponent {
                system_id: 255,
                component_id: MavComponent::MAV_COMP_ID_MISSIONPLANNER as u8,
            },
            sequence: 0,
        }
    }

    fn encode_discovery_requests(&mut self, camera: SystemAndComponent) -> Vec<Vec<u8>> {
        vec![
            encode_command_long(
                self.source,
                &mut self.sequence,
                camera,
                MavCmd::MAV_CMD_REQUEST_MESSAGE,
                [
                    CAMERA_INFORMATION_DATA::ID as f32,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ],
            ),
            encode_command_long(
                self.source,
                &mut self.sequence,
                camera,
                MavCmd::MAV_CMD_REQUEST_MESSAGE,
                [
                    VIDEO_STREAM_INFORMATION_DATA::ID as f32,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ],
            ),
        ]
    }
}

impl CameraDiscoverer {
    pub fn new(publisher: Publisher<'static>) -> Self {
        let state = Arc::new(Mutex::new(CameraDiscovererState::new()));
        let publisher = Arc::new(publisher);

        tokio::spawn({
            let state = state.clone();
            let publisher = publisher.clone();

            async move {
                let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
                loop {
                    interval.tick().await;

                    let cameras = {
                        let state = state.lock().expect("camera discoverer state poisoned");
                        state.cameras.clone()
                    };

                    for camera in cameras {
                        let messages = {
                            let mut state = state.lock().expect("camera discoverer state poisoned");
                            state.encode_discovery_requests(camera)
                        };

                        for bytes in messages {
                            if let Err(error) = publisher.put(bytes).await {
                                warn!(%error, ?camera, "Failed to publish MAVLink discovery command");
                            }
                        }
                    }
                }
            }
        });

        Self { state, publisher }
    }

    pub async fn request_for_camera(&self, camera: SystemAndComponent) {
        let messages = {
            let mut state = self.state.lock().expect("camera discoverer state poisoned");
            state.encode_discovery_requests(camera)
        };

        for bytes in messages {
            if let Err(error) = self.publisher.put(bytes).await {
                warn!(%error, ?camera, "Failed to publish MAVLink discovery command");
            }
        }
    }
}

#[instrument(skip(video_streams, data, publisher))]
#[allow(deprecated)]
pub fn on_command_long(
    data: &COMMAND_LONG_DATA,
    video_streams: &mut HashMap<String, VideoStream>,
    publisher: &Arc<Publisher<'static>>,
) {
    let target = SystemAndComponent {
        system_id: data.target_system,
        component_id: data.target_component,
    };

    let Some(stream) = video_stream_for_camera(video_streams, target) else {
        return;
    };

    let params = [
        data.param1,
        data.param2,
        data.param3,
        data.param4,
        data.param5,
        data.param6,
        data.param7,
    ];

    match data.command {
        MavCmd::MAV_CMD_VIDEO_START_CAPTURE
        | MavCmd::MAV_CMD_VIDEO_STOP_CAPTURE
        | MavCmd::MAV_CMD_REQUEST_CAMERA_CAPTURE_STATUS => {
            stream.handle_command(data.command, params, publisher);
        }
        _ => {}
    }
}

fn video_stream_for_camera(
    video_streams: &mut HashMap<String, VideoStream>,
    camera: SystemAndComponent,
) -> Option<&mut VideoStream> {
    video_streams
        .values_mut()
        .find(|stream| stream.camera == camera)
}

#[instrument(skip(discoverer))]
pub(crate) fn on_heartbeat(
    discoverer: &CameraDiscoverer,
    camera: SystemAndComponent,
) -> Option<SystemAndComponent> {
    let newly_discovered = {
        let mut state = discoverer
            .state
            .lock()
            .expect("camera discoverer state poisoned");
        state.cameras.insert(camera)
    };

    if newly_discovered {
        info!("Discovered camera component");
        Some(camera)
    } else {
        None
    }
}

#[instrument(skip(recording_capable, data))]
pub(crate) fn on_camera_information(
    camera: SystemAndComponent,
    data: &CAMERA_INFORMATION_DATA,
    recording_capable: &mut HashSet<SystemAndComponent>,
) {
    let has_capture_video = data
        .flags
        .contains(CameraCapFlags::CAMERA_CAP_FLAGS_CAPTURE_VIDEO);
    let already_recording_capable = recording_capable.contains(&camera);

    match (has_capture_video, already_recording_capable) {
        (true, false) => {
            recording_capable.insert(camera);
            debug!("Added camera to recording capable set");
        }
        (false, true) => {
            recording_capable.remove(&camera);
            debug!("Removed camera from recording capable set");
        }
        _ => {}
    }
}

#[instrument(skip(recording_capable, video_streams, data))]
pub(crate) fn on_video_stream_information(
    camera: SystemAndComponent,
    data: &VIDEO_STREAM_INFORMATION_DATA,
    recording_capable: &HashSet<SystemAndComponent>,
    video_streams: &mut HashMap<String, VideoStream>,
) {
    if !recording_capable.contains(&camera) {
        return;
    }

    let name = match data.name.to_str() {
        Ok(name) => name,
        Err(error) => {
            warn!(%error, "Invalid video stream name");
            return;
        }
    };

    let topic = video_topic_from_name(name);

    if video_streams.contains_key(&topic) {
        return; // Already registered
    }

    info!(stream_topic = %topic, "Registering video stream");
    video_streams.insert(topic.clone(), VideoStream::new(topic, camera));
}

fn video_topic_from_name(name: &str) -> String {
    format!("video/{}/stream", sanitize_stream_name(name))
}

fn sanitize_stream_name(name: &str) -> String {
    name.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}
