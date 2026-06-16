use mavlink::ardupilotmega::{HEARTBEAT_DATA, MavModeFlag};
use tracing::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmState {
    Armed,
    Disarmed,
}

pub struct VehicleArmGate {
    is_armed: bool,
}

impl VehicleArmGate {
    pub fn new() -> Self {
        Self { is_armed: false }
    }

    pub fn is_armed(&self) -> bool {
        self.is_armed
    }
}

#[instrument(skip(gate, data))]
pub(crate) fn on_heartbeat(gate: &mut VehicleArmGate, data: &HEARTBEAT_DATA) -> Option<ArmState> {
    let armed = data
        .base_mode
        .contains(MavModeFlag::MAV_MODE_FLAG_SAFETY_ARMED);

    match (armed, gate.is_armed) {
        (true, false) => {
            info!("Vehicle changed to armed");
            gate.is_armed = true;
            Some(ArmState::Armed)
        }
        (false, true) => {
            info!("Vehicle changed to disarmed");
            gate.is_armed = false;
            Some(ArmState::Disarmed)
        }
        _ => None,
    }
}
