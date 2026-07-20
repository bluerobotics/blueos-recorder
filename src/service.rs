use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio_graceful_shutdown::SubsystemHandle;
use tracing::*;
use zenoh::{Config, Session, pubsub::Subscriber, sample::Sample};

use crate::{
    channel_descriptor::ChannelDescriptor,
    mavlink::{self, RAW_MAVLINK_OUT_TOPIC, worker::MavlinkWorker},
    mcap::{Mcap, McapWriteConfig, McapWriter, RecordPayload},
};

const FLUSH_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct Service {
    #[allow(dead_code)]
    session: Session,
    #[allow(dead_code)]
    _subscriber: Subscriber<()>,
    mcap: Mcap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SystemAndComponent {
    pub system_id: u8,
    pub component_id: u8,
}

impl RecordPayload for zenoh::bytes::ZBytes {
    fn bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        self.to_bytes()
    }
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
        let mavlink_worker = MavlinkWorker::new(mavlink_publisher);

        let processor = Arc::new(SampleProcessor {
            mcap: mcap.writer(),
            mavlink_worker,
            schema_path,
        });

        let _subscriber = session
            .declare_subscriber("**")
            .callback(move |sample| processor.handle(sample))
            .await
            .expect("Failed to declare global zenoh subscriber");

        Self {
            session,
            _subscriber,
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
                    if let Err(error) = self.mcap.flush().await {
                        error!(%error, "Failed to flush MCAP writer");
                    }
                }
                () = subsystem.on_shutdown_requested() => {
                    break;
                }
            }
        }

        if let Err(error) = self.mcap.finish().await {
            error!(%error, "Failed to finish MCAP writer");
        }

        Ok(())
    }
}

struct SampleProcessor {
    mcap: Arc<McapWriter>,
    mavlink_worker: MavlinkWorker,
    schema_path: Option<PathBuf>,
}

impl SampleProcessor {
    fn handle(&self, sample: Sample) {
        let topic = sample.key_expr().as_str();

        if topic.starts_with(RAW_MAVLINK_OUT_TOPIC) {
            self.mavlink_worker.try_enqueue(sample.payload());
        }

        if !should_record_sample(&self.mavlink_worker, topic) {
            return;
        }

        let now = SystemTime::now();
        let log_time = now.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let publish_time = sample
            .timestamp()
            .map(|ts| ts.get_time().as_nanos())
            .unwrap_or(log_time);
        if let Err(error) = self.mcap.write_message(
            topic,
            log_time,
            publish_time,
            sample.payload().clone(),
            || {
                ChannelDescriptor::new(
                    topic,
                    sample.encoding(),
                    sample.payload(),
                    self.schema_path.as_ref(),
                )
            },
        ) {
            error!(%error, "Failed to write MCAP message");
        }
    }
}

fn should_record_sample(worker: &MavlinkWorker, topic: &str) -> bool {
    if topic.starts_with("mavlink/") || topic.starts_with("mavlink_raw/") {
        worker.is_armed()
    } else if topic.starts_with("video/") {
        worker.is_video_recording(topic)
    } else {
        true
    }
}
