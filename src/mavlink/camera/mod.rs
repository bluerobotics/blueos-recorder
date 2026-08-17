pub mod discoverer;
pub mod stream;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use mavlink::dialects::ardupilotmega::{
    CAMERA_INFORMATION_DATA, COMMAND_LONG_DATA, CameraCapFlags, MavCmd, MavComponent,
    VIDEO_STREAM_INFORMATION_DATA,
};
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::service::SystemAndComponent;
use discoverer::CameraDiscoverer;
use stream::VideoStream;

#[instrument(skip(discoverer, camera))]
pub(crate) fn on_heartbeat(
    discoverer: &CameraDiscoverer,
    camera: SystemAndComponent,
) -> Option<SystemAndComponent> {
    discoverer.register_camera(camera).then(|| {
        info!("Discovered camera component");
        camera
    })
}

#[instrument(skip(recording_capable, data, camera))]
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

    let name = data.name.to_str().unwrap_or("");
    if name.is_empty() {
        warn!("Invalid video stream name");
        return;
    }

    let topic = stream::video_topic_from_name(name);

    if video_streams.contains_key(&topic) {
        return; // Already registered
    }

    info!(stream_topic = %topic, "Registering video stream");
    video_streams.insert(topic.clone(), VideoStream::new(topic, camera));
}

/// Applies a recording command to every addressed stream and returns the
/// MAVLink frames to publish in reply. `target_system`/`target_component` of 0
/// broadcast to all matching cameras (MAVLink all-systems / MAV_COMP_ID_ALL).
/// Synchronous so the caller can drop the video-stream lock before awaiting
/// the publishes.
#[instrument(skip(video_streams, data, publisher))]
#[allow(deprecated)]
pub fn on_command_long(
    data: &COMMAND_LONG_DATA,
    video_streams: &mut HashMap<String, VideoStream>,
    publisher: &Arc<Publisher<'static>>,
) -> Vec<Vec<u8>> {
    match data.command {
        MavCmd::MAV_CMD_VIDEO_START_CAPTURE
        | MavCmd::MAV_CMD_VIDEO_STOP_CAPTURE
        | MavCmd::MAV_CMD_REQUEST_CAMERA_CAPTURE_STATUS => {}
        _ => return Vec::new(),
    }

    let params = [
        data.param1,
        data.param2,
        data.param3,
        data.param4,
        data.param5,
        data.param6,
        data.param7,
    ];

    let mut replies = Vec::new();
    for stream in video_streams.values_mut() {
        if !is_addressed_to(
            data.target_system,
            data.target_component,
            stream.camera.system_id,
            stream.camera.component_id,
        ) {
            continue;
        }
        replies.extend(stream.handle_command(data.command, params, publisher));
    }
    replies
}

fn is_addressed_to(
    target_system: u8,
    target_component: u8,
    our_system_id: u8,
    our_component_id: u8,
) -> bool {
    let system_matches = target_system == 0 || target_system == our_system_id;
    let component_matches = target_component == MavComponent::MAV_COMP_ID_ALL as u8
        || target_component == our_component_id;
    system_matches && component_matches
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_addressed_to_honors_broadcast_zero() {
        assert!(is_addressed_to(1, 106, 1, 106));
        assert!(is_addressed_to(1, 0, 1, 106));
        assert!(is_addressed_to(0, 106, 1, 106));
        assert!(is_addressed_to(0, 0, 1, 106));
        assert!(!is_addressed_to(1, 107, 1, 106));
        assert!(!is_addressed_to(2, 106, 1, 106));
        assert!(!is_addressed_to(2, 0, 1, 106));
        assert!(!is_addressed_to(0, 107, 1, 106));
    }
}
