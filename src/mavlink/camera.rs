use std::sync::Arc;

use mavlink::ardupilotmega::MavCmd;
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::service::SystemAndComponent;

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
