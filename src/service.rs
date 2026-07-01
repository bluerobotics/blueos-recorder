use std::{
    borrow::Cow,
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::*;
use zenoh::{Config, Session, pubsub::Publisher, sample::Sample};

use crate::{
    channel_descriptor::ChannelDescriptor,
    mavlink::{self, RAW_MAVLINK_OUT_TOPIC, frame::FrameDecoder, worker::MavlinkWorker},
    mcap::{Mcap, McapWriteConfig, McapWriter},
    zenoh_subscriptions::{self, SubscriberRegistry},
};

const FLUSH_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct Service {
    #[allow(dead_code)]
    session: Session,
    #[allow(dead_code)]
    mavlink_publisher: Arc<Publisher<'static>>,
    subscribers: Option<SubscriberRegistry>,
    mcap: Mcap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SystemAndComponent {
    pub system_id: u8,
    pub component_id: u8,
}

fn generate_filename() -> String {
    let now = SystemTime::now();
    let datetime = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Time went backwards");
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(
        datetime.as_secs() as i64,
        datetime.subsec_nanos(),
    )
    .expect("Invalid timestamp");
    format!("recorder_{}.mcap", datetime.format("%Y%m%d_%H%M%S"))
}

impl Service {
    #[instrument()]
    pub async fn new(
        config: Config,
        recorder_path: PathBuf,
        schema_path: Option<PathBuf>,
        mcap_config: McapWriteConfig,
    ) -> Self {
        let session = zenoh::open(config)
            .await
            .expect("Failed to open zenoh session");
        let mavlink_publisher = Arc::new(
            session
                .declare_publisher(mavlink::RAW_MAVLINK_IN_TOPIC)
                .encoding(zenoh::bytes::Encoding::APPLICATION_OCTET_STREAM.with_schema("mavlink"))
                .congestion_control(zenoh::qos::CongestionControl::Block)
                .priority(zenoh::qos::Priority::RealTime)
                .await
                .expect("Failed to declare mavlink raw publisher"),
        );

        let path = recorder_path.join(generate_filename());
        info!("Opening recording session");

        let mcap = Mcap::try_new(&path, mcap_config)
            .await
            .expect("Failed to open MCAP file");
        let mavlink_worker = MavlinkWorker::new(mavlink_publisher.clone());

        let processor = Arc::new(SampleProcessor {
            mcap: mcap.writer(),
            mavlink_worker,
            decoder: Mutex::new(FrameDecoder::default()),
            video_gate_skip_logged: Mutex::new(HashSet::new()),
            schema_path,
        });

        let subscribe_patterns = crate::cli::zenoh_subscribe_patterns();
        let subscribers = zenoh_subscriptions::open_callback_subscribers(
            &session,
            &subscribe_patterns,
            processor,
        )
        .await;

        Self {
            session,
            mavlink_publisher,
            subscribers: Some(subscribers),
            mcap,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&mut self, subsystem: &mut SubsystemHandle) -> anyhow::Result<()> {
        info!("Waiting for vehicle to be armed");
        let mut flush_ticker = tokio::time::interval(FLUSH_POLL_INTERVAL);
        flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = flush_ticker.tick() => {
                    if let Err(error) = self.mcap.maybe_flush().await {
                        error!(%error, "Failed to flush MCAP writer");
                    }
                }
                () = subsystem.on_shutdown_requested() => {
                    break;
                }
            }
        }

        // Drop subscribers first so no more callbacks fire while we finalize.
        self.subscribers.take();

        if let Err(error) = self.mcap.finish().await {
            error!(%error, "Failed to finish MCAP writer");
        }

        Ok(())
    }
}

pub struct SampleProcessor {
    mcap: Arc<McapWriter>,
    mavlink_worker: MavlinkWorker,
    decoder: Mutex<FrameDecoder>,
    video_gate_skip_logged: Mutex<HashSet<String>>,
    schema_path: Option<PathBuf>,
}

impl SampleProcessor {
    pub fn handle(&self, sample: Sample) {
        let topic = sample.key_expr().as_str();

        if topic.starts_with(RAW_MAVLINK_OUT_TOPIC) {
            self.process_raw_mavlink(topic, &sample);
            return;
        }

        if !self.should_record_sample(topic) {
            return;
        }

        let payload = sample.payload().to_bytes();
        let payload = match payload {
            Cow::Borrowed(bytes) => Bytes::copy_from_slice(bytes),
            Cow::Owned(bytes) => Bytes::from(bytes),
        };
        self.write_recording(topic, &sample, payload);
    }

    fn process_raw_mavlink(&self, topic: &str, sample: &Sample) {
        let payload = sample.payload().to_bytes();
        let mut decoder = self.decoder.lock().expect("mavlink decoder poisoned");
        let Some(packet) = decoder.decode(payload.as_ref()) else {
            return;
        };

        if self.mavlink_worker.is_armed() {
            let wire = packet.bytes().clone();
            self.mavlink_worker.try_enqueue(packet);
            drop(decoder);
            self.write_recording(topic, sample, wire);
        } else if mavlink::frame::needed_while_disarmed(packet.message_id()) {
            self.mavlink_worker.try_enqueue(packet);
        }
    }

    fn write_recording(&self, topic: &str, sample: &Sample, payload: Bytes) {
        let publish_time = sample
            .timestamp()
            .map(|ts| ts.get_time().as_nanos())
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_nanos() as u64
            });

        if let Err(error) =
            self.mcap
                .write_message(topic, publish_time, publish_time, payload, || {
                    ChannelDescriptor::new(
                        topic,
                        sample.encoding(),
                        sample.payload(),
                        self.schema_path.as_ref(),
                    )
                })
        {
            error!(%error, "Failed to write MCAP message");
        }
    }

    fn should_record_sample(&self, topic: &str) -> bool {
        if topic.starts_with("mavlink/") {
            if self.mavlink_worker.is_armed() {
                true
            } else {
                trace!("Skipping MAVLink sample: vehicle disarmed");
                false
            }
        } else if topic.starts_with("video/") {
            let gate = self.mavlink_worker.video_recording_gate();
            if gate.is_recording(topic) {
                self.video_gate_skip_logged
                    .lock()
                    .expect("video gate skip log poisoned")
                    .remove(topic);
                true
            } else if gate.is_registered(topic) {
                if self
                    .video_gate_skip_logged
                    .lock()
                    .expect("video gate skip log poisoned")
                    .insert(format!("idle:{topic}"))
                {
                    debug!("Skipping video sample: stream registered but not recording");
                }
                false
            } else if self
                .video_gate_skip_logged
                .lock()
                .expect("video gate skip log poisoned")
                .insert(format!("unknown:{topic}"))
            {
                debug!("Skipping video sample: topic not registered");
                false
            } else {
                false
            }
        } else {
            true
        }
    }
}
