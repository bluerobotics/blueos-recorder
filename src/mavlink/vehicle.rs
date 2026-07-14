use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use mavlink::ardupilotmega::{HEARTBEAT_DATA, MavModeFlag};
use tracing::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmState {
    Armed,
    Disarmed,
}

pub struct VehicleArmGate {
    is_armed: Arc<AtomicBool>,
}

impl VehicleArmGate {
    pub fn new(is_armed: Arc<AtomicBool>) -> Self {
        Self { is_armed }
    }

    pub fn is_armed(&self) -> bool {
        self.is_armed.load(Ordering::Relaxed)
    }
}

#[instrument(skip(gate, data), level = "trace")]
pub(crate) fn on_heartbeat(gate: &mut VehicleArmGate, data: &HEARTBEAT_DATA) -> Option<ArmState> {
    let armed = data
        .base_mode
        .contains(MavModeFlag::MAV_MODE_FLAG_SAFETY_ARMED);

    match (armed, gate.is_armed()) {
        (true, false) => {
            info!("Vehicle changed to armed");
            gate.is_armed.store(true, Ordering::Relaxed);
            Some(ArmState::Armed)
        }
        (false, true) => {
            info!("Vehicle changed to disarmed");
            gate.is_armed.store(false, Ordering::Relaxed);
            Some(ArmState::Disarmed)
        }
        _ => None,
    }
}
