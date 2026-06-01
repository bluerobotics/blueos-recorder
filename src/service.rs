use std::time::{SystemTime, UNIX_EPOCH};

use tracing::*;
use zenoh::{Config, Session, handlers::FifoChannelHandler, pubsub::Subscriber, sample::Sample};

use crate::{
    channel_descriptor::ChannelDescriptor,
    mavlink::{ArmState, VehicleArmGate},
    mcap::Mcap,
};

pub struct Service {
    #[allow(dead_code)]
    session: Session,
    subscriber: Subscriber<FifoChannelHandler<Sample>>,
    mcap: Mcap,
    vehicle_arm: VehicleArmGate,
    recorder_path: std::path::PathBuf,
    schema_path: Option<std::path::PathBuf>,
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

        Self {
            session,
            subscriber,
            mcap: Mcap::inactive(),
            vehicle_arm: VehicleArmGate::new(),
            recorder_path,
            schema_path,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&mut self) {
        let mut last_flush = SystemTime::now();
        info!("Waiting for vehicle to be armed");
        while let Ok(sample) = self.subscriber.recv_async().await {
            let topic = sample.key_expr().as_str();
            let encoding = sample.encoding();
            let payload = sample.payload();
            let span = info_span!("sample", topic = %topic, encoding = %encoding);
            let _sample_span = span.enter();

            match self.vehicle_arm.update(topic, payload) {
                Some(ArmState::Armed) if !self.mcap.is_recording() => {
                    info!("Vehicle is armed, starting recording");
                    let filename = generate_filename();
                    let path = self.recorder_path.join(filename);
                    match Mcap::try_new(&path) {
                        Ok(mcap) => self.mcap = mcap,
                        Err(error) => {
                            error!(path = %path.display(), %error, "Failed to start MCAP recording");
                        }
                    }
                }
                Some(ArmState::Disarmed) if self.mcap.is_recording() => {
                    info!("Vehicle is disarmed, stopping recording");
                    if let Err(error) = self.mcap.finish() {
                        error!(%error, "Failed to stop MCAP recording");
                    }
                }
                _ => {}
            }

            if !self.mcap.is_recording() {
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
    }
}
