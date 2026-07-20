use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use mavlink::{
    MessageData,
    dialects::ardupilotmega::{
        CAMERA_INFORMATION_DATA, MavCmd, MavComponent, VIDEO_STREAM_INFORMATION_DATA,
    },
};
use tracing::*;
use zenoh::pubsub::Publisher;

use super::super::encode_command_long;
use crate::service::SystemAndComponent;

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct CameraDiscoverer {
    state: Arc<Mutex<CameraDiscovererState>>,
    publisher: Arc<Publisher<'static>>,
}

struct CameraDiscovererState {
    cameras: HashSet<SystemAndComponent>,
    source: SystemAndComponent,
    sequence: u8,
}

impl CameraDiscoverer {
    #[instrument(skip(publisher))]
    pub fn new(publisher: Arc<Publisher<'static>>) -> Self {
        let discoverer = Self {
            state: Arc::new(Mutex::new(CameraDiscovererState::new())),
            publisher,
        };

        tokio::spawn({
            let discoverer = discoverer.clone();

            async move {
                let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
                loop {
                    interval.tick().await;

                    let cameras = discoverer
                        .state
                        .lock()
                        .expect("camera discoverer state poisoned")
                        .cameras
                        .clone();

                    for camera in cameras {
                        discoverer.request_for_camera(camera).await;
                    }
                }
            }
        });

        discoverer
    }

    #[instrument(skip(self))]
    pub async fn request_for_camera(&self, camera: SystemAndComponent) {
        let messages = {
            let mut state = self.state.lock().expect("camera discoverer state poisoned");
            state.encode_discovery_requests(camera)
        };

        for bytes in messages {
            if let Err(error) = self.publisher.put(bytes).await {
                warn!(%error, "Failed to publish MAVLink discovery command");
            }
        }
    }

    #[instrument(skip(self, camera))]
    pub(super) fn register_camera(&self, camera: SystemAndComponent) -> bool {
        let mut state = self.state.lock().expect("camera discoverer state poisoned");
        state.cameras.insert(camera)
    }
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
