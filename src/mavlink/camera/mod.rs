pub mod discoverer;

use std::collections::HashSet;

use mavlink::ardupilotmega::{CAMERA_INFORMATION_DATA, CameraCapFlags};
use tracing::*;

use crate::service::SystemAndComponent;
use discoverer::CameraDiscoverer;

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
