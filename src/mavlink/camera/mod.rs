pub mod discoverer;
pub mod stream;

use std::collections::{HashMap, HashSet};

use discoverer::CameraDiscoverer;
use mavlink::ardupilotmega::{
    CAMERA_INFORMATION_DATA, COMMAND_LONG_DATA, CameraCapFlags, MavCmd,
    VIDEO_STREAM_INFORMATION_DATA,
};
use stream::VideoStream;
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::{
    mavlink::{mavlink_string, worker::VideoRecordingGate},
    service::SystemAndComponent,
};

#[instrument(
    skip(video_streams, data, publisher),
    fields(
        target_system = tracing::field::Empty,
        target_component = tracing::field::Empty,
        command = tracing::field::Empty,
    ),
)]
#[allow(deprecated)]
pub(crate) async fn on_command_long(
    data: &COMMAND_LONG_DATA,
    video_streams: &mut HashMap<String, VideoStream>,
    publisher: &std::sync::Arc<Publisher<'static>>,
) {
    let span = Span::current();
    span.record("target_system", data.target_system);
    span.record("target_component", data.target_component);
    span.record("command", tracing::field::debug(&data.command));

    let target = SystemAndComponent {
        system_id: data.target_system,
        component_id: data.target_component,
    };

    let is_recording_command = matches!(
        data.command,
        MavCmd::MAV_CMD_VIDEO_START_CAPTURE
            | MavCmd::MAV_CMD_VIDEO_STOP_CAPTURE
            | MavCmd::MAV_CMD_REQUEST_CAMERA_CAPTURE_STATUS
    );

    if !is_recording_command {
        return;
    }

    let Some(stream) = video_stream_for_camera(video_streams, target) else {
        let registered: Vec<_> = video_streams.keys().cloned().collect();
        debug!(
            ?registered,
            "Recording command ignored: no registered video stream for target camera"
        );
        return;
    };

    debug!(
        stream_topic = %stream.topic,
        is_recording = stream.is_recording,
        "Dispatching camera recording command"
    );

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
            stream.handle_command(data.command, params, publisher).await;
        }
        _ => {}
    }
}

#[instrument(skip_all, level = "trace")]
fn video_stream_for_camera(
    video_streams: &mut HashMap<String, VideoStream>,
    camera: SystemAndComponent,
) -> Option<&mut VideoStream> {
    video_streams
        .values_mut()
        .find(|stream| stream.camera == camera)
}

#[instrument(skip(discoverer, camera))]
pub(crate) fn on_heartbeat(
    discoverer: &CameraDiscoverer,
    camera: SystemAndComponent,
) -> Option<SystemAndComponent> {
    if discoverer.register_camera(camera) {
        info!("Discovered camera component");
        Some(camera)
    } else {
        None
    }
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

#[instrument(
    skip(recording_capable, video_streams, data, camera, video_recording_gate),
    fields(
        stream_name = tracing::field::Empty,
        stream_topic = tracing::field::Empty,
    ),
)]
pub(crate) fn on_video_stream_information(
    camera: SystemAndComponent,
    data: &VIDEO_STREAM_INFORMATION_DATA,
    recording_capable: &HashSet<SystemAndComponent>,
    video_streams: &mut HashMap<String, VideoStream>,
    video_recording_gate: &std::sync::Arc<VideoRecordingGate>,
) {
    if !recording_capable.contains(&camera) {
        debug!(
            stream_name = %mavlink_string(&data.name),
            "Ignoring VIDEO_STREAM_INFORMATION: camera not recording capable"
        );
        return;
    }

    let name = mavlink_string(&data.name);
    if name.is_empty() {
        warn!("Invalid video stream name");
        return;
    }

    let span = Span::current();
    span.record("stream_name", name);

    let topic = stream::video_topic_from_name(name);
    span.record("stream_topic", topic.as_str());

    if video_streams.contains_key(&topic) {
        trace!("Video stream already registered");
        return;
    }

    info!("Registering video stream");
    video_recording_gate.register(&topic);
    video_streams.insert(
        topic.clone(),
        VideoStream::new(topic, camera, video_recording_gate.clone()),
    );
}
