pub mod camera;
pub mod frame;
pub mod vehicle;
pub mod worker;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use ::mavlink::{
    MavHeader, MessageData,
    ardupilotmega::{
        CAMERA_INFORMATION_DATA, COMMAND_LONG_DATA, HEARTBEAT_DATA, MavCmd, MavComponent,
        MavMessage, MavType, VIDEO_STREAM_INFORMATION_DATA,
    },
};
use mavlink_codec::Packet;
use tracing::*;
use zenoh::pubsub::Publisher;

use crate::service::SystemAndComponent;

use self::{
    camera::discoverer::CameraDiscoverer, camera::stream::VideoStream, frame::decode_mav_message,
    vehicle::VehicleArmGate, worker::VideoRecordingGate,
};

pub const RAW_MAVLINK_OUT_TOPIC: &str = "mavlink_raw/out";
pub const RAW_MAVLINK_IN_TOPIC: &str = "mavlink_raw/in";

#[instrument(skip(message), level = "trace")]
pub fn encode(header: MavHeader, message: &MavMessage) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Err(error) = mavlink::write_v2_msg(&mut bytes, header, message) {
        warn!(%error, "Failed to encode MAVLink message");
    }
    bytes
}

#[instrument(
    skip(params),
    fields(
        target_system = target.system_id,
        target_component = target.component_id,
        command = ?command,
    ),
)]
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

pub(crate) fn mavlink_string(bytes: &[u8]) -> &str {
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

#[instrument(
    skip_all,
    level = "trace",
    fields(
        system_id = tracing::field::Empty,
        component_id = tracing::field::Empty,
        message_id = tracing::field::Empty,
    ),
)]
pub async fn handle_mavlink_packet(
    packet: Packet,
    vehicle_arm: &mut VehicleArmGate,
    discoverer: &CameraDiscoverer,
    recording_capable: &mut HashSet<SystemAndComponent>,
    video_streams: &mut HashMap<String, VideoStream>,
    publisher: &Arc<Publisher<'static>>,
    video_recording_gate: &Arc<VideoRecordingGate>,
) {
    let msg_id = packet.message_id();
    if !vehicle_arm.is_armed() && !frame::needed_while_disarmed(msg_id) {
        return;
    }

    let span = Span::current();
    span.record("system_id", *packet.system_id());
    span.record("component_id", *packet.component_id());
    span.record("message_id", msg_id);

    match msg_id {
        id if id == HEARTBEAT_DATA::ID => {
            let Ok((header, message)) = decode_mav_message(&packet) else {
                warn!("Failed decoding HEARTBEAT");
                return;
            };
            let MavMessage::HEARTBEAT(data) = message else {
                return;
            };
            if header.component_id == MavComponent::MAV_COMP_ID_AUTOPILOT1 as u8 {
                let _state = vehicle::on_heartbeat(vehicle_arm, &data);
            } else if data.mavtype == MavType::MAV_TYPE_CAMERA {
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
        }
        id if id == CAMERA_INFORMATION_DATA::ID => {
            let Ok((header, message)) = decode_mav_message(&packet) else {
                warn!("Failed decoding CAMERA_INFORMATION");
                return;
            };
            let MavMessage::CAMERA_INFORMATION(data) = message else {
                return;
            };
            camera::on_camera_information(
                SystemAndComponent {
                    system_id: header.system_id,
                    component_id: header.component_id,
                },
                &data,
                recording_capable,
            );
        }
        id if id == VIDEO_STREAM_INFORMATION_DATA::ID => {
            let Ok((header, message)) = decode_mav_message(&packet) else {
                warn!("Failed decoding VIDEO_STREAM_INFORMATION");
                return;
            };
            let MavMessage::VIDEO_STREAM_INFORMATION(data) = message else {
                return;
            };
            camera::on_video_stream_information(
                SystemAndComponent {
                    system_id: header.system_id,
                    component_id: header.component_id,
                },
                &data,
                recording_capable,
                video_streams,
                video_recording_gate,
            );
        }
        id if id == COMMAND_LONG_DATA::ID => {
            let Ok((_, message)) = decode_mav_message(&packet) else {
                warn!("Failed decoding COMMAND_LONG");
                return;
            };
            let MavMessage::COMMAND_LONG(data) = message else {
                return;
            };
            camera::on_command_long(&data, video_streams, publisher).await;
        }
        _ => trace!("Message skipped"),
    }
}
