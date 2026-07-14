use std::sync::Arc;

use mavlink::ardupilotmega::MavCmd;
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::service::SystemAndComponent;

#[allow(dead_code)]
pub struct VideoStream {
    pub topic: String,
    pub camera: SystemAndComponent,
    pub is_recording: bool,
}

impl VideoStream {
    pub(super) fn new(topic: String, camera: SystemAndComponent) -> Self {
        Self {
            topic,
            camera,
            is_recording: false,
        }
    }

    pub(super) fn handle_command(
        &mut self,
        command: MavCmd,
        _params: [f32; 7],
        _publisher: &Arc<Publisher<'static>>,
    ) {
        trace!(?command, camera = ?self.camera, "Recording command received");
    }
}

pub(super) fn video_topic_from_name(name: &str) -> String {
    let sanitized_stream_name = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();
    format!("video/{sanitized_stream_name}/stream")
}
