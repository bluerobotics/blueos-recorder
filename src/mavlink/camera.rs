use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use mavlink::ardupilotmega::{
    CAMERA_INFORMATION_DATA, CameraCapFlags, MavCmd, MavMessage, VIDEO_STREAM_INFORMATION_DATA,
};
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::{mavlink::decode, service::SystemAndComponent};

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

#[instrument(skip_all, fields(topic = %topic), level = "trace")]
pub fn discover<R: std::io::Read>(
    topic: &str,
    bytes: R,
    recording_capable: &mut HashSet<SystemAndComponent>,
    video_streams: &mut HashMap<String, VideoStream>,
) {
    let (header, message) = match decode(bytes) {
        Ok(packet) => packet,
        Err(error) => {
            warn!("Failed decoding mavlink raw message: {error:?}");
            return;
        }
    };

    match message {
        MavMessage::CAMERA_INFORMATION(data) => {
            trace!("Message decoded: {header:?}, {data:?}");

            on_camera_information(
                SystemAndComponent {
                    system_id: header.system_id,
                    component_id: header.component_id,
                },
                &data,
                recording_capable,
            );
        }
        MavMessage::VIDEO_STREAM_INFORMATION(data) => {
            trace!("Message decoded: {header:?}, {data:?}");

            on_video_stream_information(
                SystemAndComponent {
                    system_id: header.system_id,
                    component_id: header.component_id,
                },
                &data,
                recording_capable,
                video_streams,
            );
        }
        _ => {}
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
