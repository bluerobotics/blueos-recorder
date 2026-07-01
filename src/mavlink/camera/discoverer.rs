use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use mavlink::{
    MessageData,
    ardupilotmega::{CAMERA_INFORMATION_DATA, MavCmd, MavComponent, VIDEO_STREAM_INFORMATION_DATA},
};
use tracing::{Instrument, *};
use zenoh::pubsub::Publisher;

use super::super::encode_command_long;
use crate::service::SystemAndComponent;

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);

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
        let state = Arc::new(Mutex::new(CameraDiscovererState::new()));

        tokio::spawn({
            let state = state.clone();
            let publisher = publisher.clone();

            async move {
                let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
                loop {
                    interval.tick().await;

                    let cameras = {
                        let _tick_span = info_span!("camera_discovery_tick").entered();
                        let state = state.lock().expect("camera discoverer state poisoned");
                        state.cameras.clone()
                    };

                    for camera in cameras {
                        let system_id = camera.system_id;
                        let component_id = camera.component_id;
                        let messages = {
                            let mut state = state.lock().expect("camera discoverer state poisoned");
                            state.encode_discovery_requests(camera)
                        };

                        for bytes in messages {
                            let span = debug_span!("discovery_request", system_id, component_id);
                            async {
                                if let Err(error) = publisher.put(bytes).await {
                                    warn!(%error, "Failed to publish MAVLink discovery command");
                                }
                            }
                            .instrument(span)
                            .await;
                        }
                    }
                }
            }
        });

        Self { state, publisher }
    }

    #[instrument(skip(self, camera))]
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
