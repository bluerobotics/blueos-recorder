pub mod discoverer;
pub mod stream;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use mavlink::dialects::ardupilotmega::{
    CAMERA_INFORMATION_DATA, COMMAND_LONG_DATA, CameraCapFlags, MavCmd,
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

    let Some(stream) = video_streams
        .values_mut()
        .find(|stream| stream.camera == target)
    else {
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
