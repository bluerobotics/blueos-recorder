use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use mavlink_codec::PacketRef;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::*;
use zenoh::{bytes::ZBytes, pubsub::Publisher};

use super::{
    camera::discoverer::CameraDiscoverer, camera::stream::VideoStream, handle_mavlink_message,
    is_handled, vehicle::VehicleArmGate,
};

const MAVLINK_QUEUE_CAPACITY: usize = 512;

pub struct MavlinkWorker {
    tx: mpsc::Sender<ZBytes>,
    armed: Arc<AtomicBool>,
    video_streams: Arc<RwLock<HashMap<String, VideoStream>>>,
    #[allow(dead_code)]
    task: JoinHandle<()>,
}

impl MavlinkWorker {
    pub fn new(publisher: Arc<Publisher<'static>>) -> Self {
        let (tx, rx) = mpsc::channel(MAVLINK_QUEUE_CAPACITY);
        let armed = Arc::new(AtomicBool::new(false));
        let video_streams = Arc::new(RwLock::new(HashMap::new()));

        let task = tokio::spawn(worker_loop(
            rx,
            publisher,
            armed.clone(),
            video_streams.clone(),
        ));

        Self {
            tx,
            armed,
            video_streams,
            task,
        }
    }

    pub fn try_enqueue(&self, payload: &ZBytes) {
        let bytes = payload.to_bytes();
        let Some(packet) = PacketRef::new(bytes.as_ref()) else {
            return;
        };
        if !is_handled(packet.message_id()) {
            return;
        }
        match self.tx.try_send(payload.clone()) {
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

    pub fn is_video_recording(&self, topic: &str) -> bool {
        self.video_streams
            .read()
            .expect("video_streams poisoned")
            .get(topic)
            .is_some_and(|stream| stream.is_recording)
    }
}

async fn worker_loop(
    mut rx: mpsc::Receiver<ZBytes>,
    publisher: Arc<Publisher<'static>>,
    armed: Arc<AtomicBool>,
    video_streams: Arc<RwLock<HashMap<String, VideoStream>>>,
) {
    let mut vehicle_arm = VehicleArmGate::new(armed);
    let discoverer = CameraDiscoverer::new(publisher.clone());
    let mut recording_capable = HashSet::new();

    while let Some(payload) = rx.recv().await {
        let bytes = payload.to_bytes();
        handle_mavlink_message(
            bytes.as_ref(),
            &mut vehicle_arm,
            &discoverer,
            &mut recording_capable,
            &video_streams,
            &publisher,
        )
        .await;
    }
}
