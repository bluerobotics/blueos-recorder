use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tracing::*;
use zenoh::pubsub::Publisher;

use mavlink_codec::Packet;

use super::{
    camera::discoverer::CameraDiscoverer, camera::stream::VideoStream, handle_mavlink_packet,
    vehicle::VehicleArmGate,
};

const MAVLINK_QUEUE_CAPACITY: usize = 512;

#[derive(Debug)]
pub struct VideoRecordingGate {
    registered: std::sync::RwLock<HashSet<Arc<str>>>,
    recording: std::sync::RwLock<HashSet<Arc<str>>>,
}

impl VideoRecordingGate {
    pub fn new() -> Self {
        Self {
            registered: std::sync::RwLock::new(HashSet::new()),
            recording: std::sync::RwLock::new(HashSet::new()),
        }
    }

    pub fn register(&self, topic: &str) {
        self.registered
            .write()
            .expect("video recording gate lock")
            .insert(Arc::from(topic));
    }

    pub fn set_recording(&self, topic: &str, recording: bool) {
        let topic: Arc<str> = Arc::from(topic);
        let mut active = self.recording.write().expect("video recording gate lock");
        if recording {
            active.insert(topic);
        } else {
            active.remove(&topic);
        }
    }

    pub fn is_recording(&self, topic: &str) -> bool {
        self.recording
            .read()
            .expect("video recording gate lock")
            .contains(topic)
    }

    pub fn is_registered(&self, topic: &str) -> bool {
        self.registered
            .read()
            .expect("video recording gate lock")
            .contains(topic)
    }
}

pub struct MavlinkWorker {
    tx: mpsc::Sender<Packet>,
    armed: Arc<AtomicBool>,
    #[allow(dead_code)]
    video_streams: Arc<RwLock<HashMap<String, VideoStream>>>,
    video_recording_gate: Arc<VideoRecordingGate>,
    #[allow(dead_code)]
    task: JoinHandle<()>,
}

impl MavlinkWorker {
    pub fn new(publisher: Arc<Publisher<'static>>) -> Self {
        let (tx, rx) = mpsc::channel(MAVLINK_QUEUE_CAPACITY);
        let armed = Arc::new(AtomicBool::new(false));
        let video_streams = Arc::new(RwLock::new(HashMap::new()));
        let video_recording_gate = Arc::new(VideoRecordingGate::new());

        let task = tokio::spawn(worker_loop(
            rx,
            publisher,
            armed.clone(),
            video_streams.clone(),
            video_recording_gate.clone(),
        ));

        Self {
            tx,
            armed,
            video_streams,
            video_recording_gate,
            task,
        }
    }

    pub fn try_enqueue(&self, packet: Packet) {
        match self.tx.try_send(packet) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("MAVLink worker queue full, dropping message");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("MAVLink worker channel closed");
            }
        }
    }

    pub fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }

    pub fn video_recording_gate(&self) -> &Arc<VideoRecordingGate> {
        &self.video_recording_gate
    }
}

async fn worker_loop(
    mut rx: mpsc::Receiver<Packet>,
    publisher: Arc<Publisher<'static>>,
    armed: Arc<AtomicBool>,
    video_streams: Arc<RwLock<HashMap<String, VideoStream>>>,
    video_recording_gate: Arc<VideoRecordingGate>,
) {
    let mut vehicle_arm = VehicleArmGate::new(armed);
    let discoverer = CameraDiscoverer::new(publisher.clone());
    let mut recording_capable = HashSet::new();

    while let Some(packet) = rx.recv().await {
        let mut streams = video_streams.write().await;
        handle_mavlink_packet(
            packet,
            &mut vehicle_arm,
            &discoverer,
            &mut recording_capable,
            &mut streams,
            &publisher,
            &video_recording_gate,
        )
        .await;
    }
}
