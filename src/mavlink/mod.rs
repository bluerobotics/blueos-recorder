pub mod camera;
pub mod vehicle;
pub mod worker;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use ::mavlink::{
    MavHeader, MavlinkVersion, MessageData,
    dialects::ardupilotmega::{
        CAMERA_INFORMATION_DATA, COMMAND_LONG_DATA, HEARTBEAT_DATA, MavCmd, MavComponent,
        MavMessage, MavType, VIDEO_STREAM_INFORMATION_DATA,
    },
};
use mavlink_codec::PacketRef;
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::service::SystemAndComponent;

use self::{
    camera::discoverer::CameraDiscoverer, camera::stream::VideoStream, vehicle::VehicleArmGate,
};

pub const RAW_MAVLINK_OUT_TOPIC: &str = "mavlink_raw/out";
pub const RAW_MAVLINK_IN_TOPIC: &str = "mavlink_raw/in";

/// CRC-validates a borrowed frame and parses its payload straight into the concrete
/// message type `D`, skipping the large `MavMessage` enum.
fn decode<D: MessageData>(packet: &PacketRef) -> Option<D> {
    if packet.try_validate::<MavMessage>().is_err() {
        warn!(msg_id = packet.message_id(), "Frame failed CRC validation");
        return None;
    }
    let version = match packet {
        PacketRef::V1(_) => MavlinkVersion::V1,
        PacketRef::V2(_) => MavlinkVersion::V2,
    };
    match D::deser(version, packet.payload()) {
        Ok(data) => Some(data),
        Err(error) => {
            warn!(%error, msg_id = packet.message_id(), "Failed decoding frame");
            None
        }
    }
}

#[allow(unused)]
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

/// Message ids the worker acts on; mirrors `handle_mavlink_message`'s arms.
/// Read cheaply from the frame header so the recorder can skip the channel
/// handoff for the `mavlink_raw/out` traffic the worker ignores.
pub fn is_handled(msg_id: u32) -> bool {
    msg_id == HEARTBEAT_DATA::ID
        || msg_id == CAMERA_INFORMATION_DATA::ID
        || msg_id == VIDEO_STREAM_INFORMATION_DATA::ID
        || msg_id == COMMAND_LONG_DATA::ID
}

#[instrument(skip_all, level = "trace")]
pub async fn handle_mavlink_message(
    bytes: &[u8],
    vehicle_arm: &mut VehicleArmGate,
    discoverer: &CameraDiscoverer,
    recording_capable: &mut HashSet<SystemAndComponent>,
    video_streams: &RwLock<HashMap<String, VideoStream>>,
    publisher: &Arc<Publisher<'static>>,
) {
    let Some(packet) = PacketRef::new(bytes) else {
        trace!("Not a MAVLink frame");
        return;
    };

    match packet.message_id() {
        id if id == HEARTBEAT_DATA::ID => {
            let Some(data) = decode::<HEARTBEAT_DATA>(&packet) else {
                return;
            };
            let source = SystemAndComponent {
                system_id: *packet.system_id(),
                component_id: *packet.component_id(),
            };
            if source.component_id == MavComponent::MAV_COMP_ID_AUTOPILOT1 as u8 {
                let _state = vehicle::on_heartbeat(vehicle_arm, &data);
            } else if data.mavtype == MavType::MAV_TYPE_CAMERA
                && let Some(camera) = camera::on_heartbeat(discoverer, source)
            {
                discoverer.request_for_camera(camera).await;
            }
        }
        id if id == CAMERA_INFORMATION_DATA::ID => {
            let Some(data) = decode::<CAMERA_INFORMATION_DATA>(&packet) else {
                return;
            };
            let source = SystemAndComponent {
                system_id: *packet.system_id(),
                component_id: *packet.component_id(),
            };
            camera::on_camera_information(source, &data, recording_capable);
        }
        id if id == VIDEO_STREAM_INFORMATION_DATA::ID => {
            let Some(data) = decode::<VIDEO_STREAM_INFORMATION_DATA>(&packet) else {
                return;
            };
            let source = SystemAndComponent {
                system_id: *packet.system_id(),
                component_id: *packet.component_id(),
            };
            camera::on_video_stream_information(
                source,
                &data,
                recording_capable,
                &mut video_streams.write().unwrap(),
            );
        }
        id if id == COMMAND_LONG_DATA::ID => {
            let Some(data) = decode::<COMMAND_LONG_DATA>(&packet) else {
                return;
            };
            let replies = {
                let mut streams = video_streams.write().unwrap();
                camera::on_command_long(&data, &mut streams, publisher)
            };
            for bytes in replies {
                if let Err(error) = publisher.put(bytes).await {
                    warn!(%error, "Failed to publish MAVLink message");
                }
            }
        }
        _ => trace!("Message skipped"),
    }
}
