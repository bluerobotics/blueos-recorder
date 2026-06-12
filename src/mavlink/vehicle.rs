use tracing::*;
use zenoh::bytes::ZBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmState {
    NotApplicable,
    Armed,
    Disarmed,
}

pub struct VehicleArmGate {
    is_armed: bool,
    base_mode_regex: regex::Regex,
}

impl VehicleArmGate {
    pub fn new() -> Self {
        Self {
            is_armed: false,
            base_mode_regex: regex::Regex::new(r"mavlink/\d+/1/HEARTBEAT/base_mode")
                .expect("valid heartbeat base_mode regex"),
        }
    }

    pub fn is_armed(&self) -> bool {
        self.is_armed
    }

    /// Updates arm state from a Zenoh sample. Returns `Some` only when the armed state changes.
    #[instrument(skip_all)]
    pub fn update(&mut self, topic: &str, payload: &ZBytes) -> Option<ArmState> {
        match self.check(topic, payload) {
            ArmState::Armed if !self.is_armed => {
                self.is_armed = true;
                Some(ArmState::Armed)
            }
            ArmState::Disarmed if self.is_armed => {
                self.is_armed = false;
                Some(ArmState::Disarmed)
            }
            _ => None,
        }
    }

    #[instrument(skip_all, level = "trace")]
    fn check(&self, topic: &str, payload: &ZBytes) -> ArmState {
        if !self.base_mode_regex.is_match(topic) {
            return ArmState::NotApplicable;
        }

        let Ok(payload) = payload.try_to_string() else {
            return ArmState::NotApplicable;
        };

        if payload.contains("MAV_MODE_FLAG_SAFETY_ARMED") {
            // https://mavlink.io/en/messages/common.html#MAV_MODE_FLAG_SAFETY_ARMED
            ArmState::Armed
        } else {
            ArmState::Disarmed
        }
    }
}
