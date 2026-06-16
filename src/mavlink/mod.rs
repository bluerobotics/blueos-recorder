pub mod camera;
pub mod vehicle;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use ::mavlink::{
    MavHeader,
    ardupilotmega::{COMMAND_LONG_DATA, MavCmd, MavComponent, MavMessage, MavType},
    peek_reader::PeekReader,
};
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::service::SystemAndComponent;

use self::{
    camera::{CameraDiscoverer, VideoStream},
    vehicle::VehicleArmGate,
};

pub const RAW_MAVLINK_OUT_TOPIC: &str = "mavlink_raw/out";
pub const RAW_MAVLINK_IN_TOPIC: &str = "mavlink_raw/in";

#[instrument(skip_all, level = "trace")]
pub fn decode<R>(bytes: R) -> Result<(MavHeader, MavMessage), mavlink::error::MessageReadError>
where
    R: std::io::Read,
{
    let mut reader = PeekReader::new(bytes);
    mavlink::read_any_msg(&mut reader)
}

#[instrument(skip(message), level = "debug")]
pub fn encode(header: MavHeader, message: &MavMessage) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Err(error) = mavlink::write_v2_msg(&mut bytes, header, message) {
        warn!(%error, "Failed to encode MAVLink message");
    }
    bytes
}

#[instrument(skip(params))]
pub fn encode_command_long(
    source: SystemAndComponent,
    sequence: &mut u8,
    target: SystemAndComponent,
    command: MavCmd,
    params: [f32; 7],
) -> Vec<u8> {
    let header = MavHeader {
        system_id: source.system_id,
        component_id: source.component_id,
        sequence: {
            let value = *sequence;
            *sequence = sequence.wrapping_add(1);
            value
        },
    };
    let message = MavMessage::COMMAND_LONG(COMMAND_LONG_DATA {
        target_system: target.system_id,
        target_component: target.component_id,
        command,
        confirmation: 0,
        param1: params[0],
        param2: params[1],
        param3: params[2],
        param4: params[3],
        param5: params[4],
        param6: params[5],
        param7: params[6],
    });

    encode(header, &message)
}

#[instrument(skip_all, level = "trace")]
pub async fn handle_mavlink_message(
    bytes: &[u8],
    vehicle_arm: &mut VehicleArmGate,
    discoverer: &CameraDiscoverer,
    recording_capable: &mut HashSet<SystemAndComponent>,
    video_streams: &mut HashMap<String, VideoStream>,
    publisher: &Arc<Publisher<'static>>,
) {
    let (header, message) = match decode(bytes) {
        Ok(packet) => packet,
        Err(error) => {
            warn!("Failed decoding mavlink raw message: {error:?}");
            return;
        }
    };

    match message {
        MavMessage::HEARTBEAT(data)
            if header.component_id == MavComponent::MAV_COMP_ID_AUTOPILOT1 as u8 =>
        {
            trace!("Message decoded: {header:?}, {data:?}");

            let _state = vehicle::on_heartbeat(vehicle_arm, &data);
        }
        MavMessage::HEARTBEAT(data) if data.mavtype == MavType::MAV_TYPE_CAMERA => {
            trace!("Message decoded: {header:?}, {data:?}");

            if let Some(camera) = camera::on_heartbeat(
                discoverer,
                SystemAndComponent {
                    system_id: header.system_id,
                    component_id: header.component_id,
                },
            ) {
                discoverer.request_for_camera(camera).await;
            }
        }
        MavMessage::CAMERA_INFORMATION(data) => {
            trace!("Message decoded: {header:?}, {data:?}");

            camera::on_camera_information(
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

            camera::on_video_stream_information(
                SystemAndComponent {
                    system_id: header.system_id,
                    component_id: header.component_id,
                },
                &data,
                recording_capable,
                video_streams,
            );
        }
        MavMessage::COMMAND_LONG(data) => {
            trace!("Message decoded: {header:?}, {data:?}");

            camera::on_command_long(&data, video_streams, publisher);
        }
        _ => trace!("Message skipped"),
    }
}
