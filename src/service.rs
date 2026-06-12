use std::time::{SystemTime, UNIX_EPOCH};

use tokio_graceful_shutdown::SubsystemHandle;
use tracing::*;
use zenoh::{Config, Session, handlers::FifoChannelHandler, pubsub::Subscriber, sample::Sample};

use crate::{
    channel_descriptor::ChannelDescriptor,
    mavlink::{RAW_MAVLINK_OUT_TOPIC, vehicle::VehicleArmGate},
    mcap::Mcap,
};

pub struct Service {
    #[allow(dead_code)]
    session: Session,
    subscriber: Subscriber<FifoChannelHandler<Sample>>,
    mcap: Mcap,
    vehicle_arm: VehicleArmGate,
    schema_path: Option<std::path::PathBuf>,
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
        recorder_path: std::path::PathBuf,
        schema_path: Option<std::path::PathBuf>,
    ) -> Self {
        let session = zenoh::open(config)
            .await
            .expect("Failed to open zenoh session");
        let subscriber = session
            .declare_subscriber("**")
            .await
            .expect("Failed to declare global zenoh subscriber");

        let path = recorder_path.join(generate_filename());
        info!("Opening recording session");

        let mcap = Mcap::try_new(&path).unwrap();
        Self {
            session,
            subscriber,
            mcap,
            vehicle_arm: VehicleArmGate::new(),
            schema_path,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&mut self, subsystem: &mut SubsystemHandle) -> anyhow::Result<()> {
        let mut last_flush = SystemTime::now();
        info!("Waiting for vehicle to be armed");
        loop {
            let sample = tokio::select! {
                sample = self.subscriber.recv_async() => {
                    let Ok(sample) = sample else {
                        break;
                    };

                    sample
                },
                () = subsystem.on_shutdown_requested() => {
                    break;
                },
            };

            let topic = sample.key_expr().as_str();
            let payload = sample.payload();
            let encoding = sample.encoding();
            let span = info_span!("sample", topic = %topic, encoding = %encoding);
            let _sample_span = span.enter();

            if topic.starts_with(RAW_MAVLINK_OUT_TOPIC) {
                crate::mavlink::handle_mavlink_message(&payload.to_bytes(), &mut self.vehicle_arm)
                    .await;
            }

            if !self.should_record_sample(topic) {
                continue;
            }

            let new_channel = if self.mcap.has_channel(topic) {
                None
            } else {
                let Some(channel_descriptor) =
                    ChannelDescriptor::new(topic, encoding, payload, self.schema_path.as_ref())
                else {
                    warn!("Failed creating a channel descriptor");
                    continue;
                };

                info!(schema_name = %channel_descriptor.schema_name, "Adding schema");
                Some(channel_descriptor)
            };

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
                &payload.to_bytes(),
                new_channel,
            ) {
                error!(%error, "Failed to write MCAP message");
                continue;
            }

            if now.duration_since(last_flush).unwrap() > std::time::Duration::from_secs(30) {
                if let Err(error) = self.mcap.flush() {
                    error!(%error, "Failed to flush MCAP writer");
                }
                last_flush = now;
            }
        }

        if let Err(error) = self.mcap.finish() {
            error!(%error, "Failed to finish MCAP writer");
        }

        Ok(())
    }

    fn should_record_sample(&self, topic: &str) -> bool {
        if topic.starts_with("mavlink/")
            || topic.starts_with("mavlink_raw/")
            || topic.starts_with("video/")
        {
            self.vehicle_arm.is_armed()
        } else {
            true
        }
    }
}
