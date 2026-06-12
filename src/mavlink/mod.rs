pub mod camera;
pub mod vehicle;

use ::mavlink::{
    MavHeader,
    ardupilotmega::{MavComponent, MavMessage},
    peek_reader::PeekReader,
};
use tracing::*;

use self::vehicle::VehicleArmGate;

pub const RAW_MAVLINK_OUT_TOPIC: &str = "mavlink_raw/out";
#[allow(unused)]
pub const RAW_MAVLINK_IN_TOPIC: &str = "mavlink_raw/in";

#[instrument(skip_all, level = "trace")]
pub fn decode<R>(bytes: R) -> Result<(MavHeader, MavMessage), mavlink::error::MessageReadError>
where
    R: std::io::Read,
{
    let mut reader = PeekReader::new(bytes);
    mavlink::read_any_msg(&mut reader)
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

#[instrument(skip_all, level = "trace")]
pub async fn handle_mavlink_message(bytes: &[u8], vehicle_arm: &mut VehicleArmGate) {
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
        _ => trace!("Message skipped"),
    }
}
