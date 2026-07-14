use bytes::BytesMut;
use mavlink::{
    MavlinkVersion, Message, MessageData,
    ardupilotmega::{
        CAMERA_INFORMATION_DATA, COMMAND_LONG_DATA, HEARTBEAT_DATA, MavMessage,
        VIDEO_STREAM_INFORMATION_DATA,
    },
};
use mavlink_codec::{Packet, codec::MavlinkCodec};
use tokio_util::codec::Decoder;
use tracing::*;

pub type RecorderCodec = MavlinkCodec<true, true, false, false, false, false, false, false>;

pub struct FrameDecoder {
    codec: RecorderCodec,
    buffer: BytesMut,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self {
            codec: RecorderCodec::default(),
            buffer: BytesMut::with_capacity(280),
        }
    }
}

impl FrameDecoder {
    pub fn decode(&mut self, data: &[u8]) -> Option<Packet> {
        self.buffer.clear();
        self.buffer.extend_from_slice(data);
        match self.codec.decode(&mut self.buffer) {
            Ok(Some(Ok(packet))) => Some(packet),
            Ok(Some(Err(error))) => {
                trace!(%error, "Rejected MAVLink frame");
                None
            }
            Ok(None) => {
                trace!("Incomplete MAVLink frame");
                None
            }
            Err(error) => {
                trace!(%error, "MAVLink frame decode I/O error");
                None
            }
        }
    }
}

/// Extract the MAVLink message id from a single raw frame without running the
/// codec (no CRC validation, no payload parse). Each `mavlink_raw/out` sample
/// carries exactly one frame, so a header read is enough to decide whether the
/// packet needs full decoding. Returns `None` if the buffer is too short or the
/// magic byte is not a recognized MAVLink version.
pub fn frame_msg_id(data: &[u8]) -> Option<u32> {
    match data.first()? {
        // MAVLink v2: [magic, len, incompat, compat, seq, sysid, compid, msgid(3, LE), ...]
        0xFD => {
            if data.len() < 10 {
                return None;
            }
            Some(u32::from(data[7]) | (u32::from(data[8]) << 8) | (u32::from(data[9]) << 16))
        }
        // MAVLink v1: [magic, len, seq, sysid, compid, msgid, ...]
        0xFE => {
            if data.len() < 6 {
                return None;
            }
            Some(u32::from(data[5]))
        }
        _ => None,
    }
}

pub fn needed_while_disarmed(msg_id: u32) -> bool {
    matches!(
        msg_id,
        HEARTBEAT_DATA::ID
            | COMMAND_LONG_DATA::ID
            | CAMERA_INFORMATION_DATA::ID
            | VIDEO_STREAM_INFORMATION_DATA::ID
    )
}

pub fn decode_mav_message(
    packet: &Packet,
) -> Result<(mavlink::MavHeader, MavMessage), mavlink::error::ParserError> {
    let (version, msg_id, system_id, component_id, sequence, payload) = match packet {
        Packet::V1(v1) => (
            MavlinkVersion::V1,
            u32::from(*v1.message_id()),
            *v1.system_id(),
            *v1.component_id(),
            *v1.sequence(),
            v1.payload(),
        ),
        Packet::V2(v2) => (
            MavlinkVersion::V2,
            v2.message_id(),
            *v2.system_id(),
            *v2.component_id(),
            *v2.sequence(),
            v2.payload(),
        ),
    };

    let header = mavlink::MavHeader {
        system_id,
        component_id,
        sequence,
    };
    let message = MavMessage::parse(version, msg_id, payload)?;
    Ok((header, message))
}
