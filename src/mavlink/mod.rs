pub mod frame;
pub mod vehicle;

use ::mavlink::{
    MavHeader, MessageData,
    ardupilotmega::{HEARTBEAT_DATA, MavComponent, MavMessage},
};
use tracing::*;

use self::{frame::decode_mav_message, vehicle::VehicleArmGate};

pub const RAW_MAVLINK_OUT_TOPIC: &str = "mavlink_raw/out";
#[allow(unused)]
pub const RAW_MAVLINK_IN_TOPIC: &str = "mavlink_raw/in";

#[allow(unused)]
#[instrument(skip(message), level = "debug")]
pub fn encode(header: MavHeader, message: &MavMessage) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Err(error) = mavlink::write_v2_msg(&mut bytes, header, message) {
        warn!(%error, "Failed to encode MAVLink message");
    }
    bytes
}

#[instrument(skip_all, level = "trace")]
pub async fn handle_mavlink_message(bytes: &[u8], vehicle_arm: &mut VehicleArmGate) {
    let mut decoder = frame::FrameDecoder::default();
    let Some(packet) = decoder.decode(bytes) else {
        return;
    };

    if packet.message_id() != HEARTBEAT_DATA::ID {
        trace!("Message skipped");
        return;
    }

    let Ok((header, message)) = decode_mav_message(&packet) else {
        warn!("Failed decoding HEARTBEAT");
        return;
    };
    let MavMessage::HEARTBEAT(data) = message else {
        return;
    };
    if header.component_id == MavComponent::MAV_COMP_ID_AUTOPILOT1 as u8 {
        trace!("Message decoded: {header:?}, {data:?}");

        let _state = vehicle::on_heartbeat(vehicle_arm, &data);
    }
}
