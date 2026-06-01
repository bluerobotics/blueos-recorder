use std::{
    collections::{BTreeMap, HashMap},
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::*;
use zenoh::{Config, Session, handlers::FifoChannelHandler, pubsub::Subscriber, sample::Sample};

use crate::{
    channel_descriptor::ChannelDescriptor,
    mcap::{Channel, Mcap},
};

pub struct Service {
    #[allow(dead_code)]
    session: Session,
    subscriber: Subscriber<FifoChannelHandler<Sample>>,
    mcap: Mcap,
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
            mcap: Mcap {
                writer: None,
                channel: HashMap::new(),
            },
            recorder_path,
            schema_path,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&mut self) {
        let mut last_flush = SystemTime::now();
        let base_mode_regex = regex::Regex::new(r"mavlink/\d+/1/HEARTBEAT/base_mode").unwrap();
        log::info!("Waiting for vehicle to be armed");
        while let Ok(sample) = self.subscriber.recv_async().await {
            let topic = sample.key_expr().to_string();
            let payload = sample.payload();
            let encoding = sample.encoding();
            let span = info_span!("sample", topic = %topic, encoding = %encoding);
            let _sample_span = span.enter();
            let encoding_string = encoding.to_string();

            if base_mode_regex.is_match(&topic)
                && let Ok(string) = payload.try_to_string()
            {
                if string.contains("MAV_MODE_FLAG_SAFETY_ARMED") {
                    // https://mavlink.io/en/messages/common.html#MAV_MODE_FLAG_SAFETY_ARMED
                    if self.mcap.writer.is_none() {
                        log::info!("Vehicle is armed, starting recording");
                        let filename = generate_filename();
                        let path = self.recorder_path.join(filename);
                        self.mcap = Mcap::new(std::path::Path::new(&path));
                    }
                } else if self.mcap.writer.is_some() {
                    log::info!("Vehicle is disarmed, stopping recording");
                    self.mcap.finish();
                }
            }

            if self.mcap.writer.is_none() {
                continue;
            }
            let writer = self
                .mcap
                .writer
                .as_mut()
                .expect("Failed to get mcap writer");

            if !self.mcap.channel.contains_key(&topic) {
                let Some(channel_descriptor) =
                    ChannelDescriptor::new(&topic, encoding, payload, self.schema_path.as_ref())
                else {
                    warn!("Failed creating a channel descriptor");
                    continue;
                };

                log::info!("{topic}: Adding schema: {encoding_string}");
                log::info!("Schema description: {}", channel_descriptor.schema_content);
                let Ok(schema_id) = writer.add_schema(
                    &channel_descriptor.schema_name,
                    channel_descriptor.schema_encoding.as_str(),
                    channel_descriptor.schema_content.as_bytes(),
                ) else {
                    log::warn!("{topic}: Failed to add schema: {encoding_string:?}");
                    continue;
                };

                let Ok(channel_id) = writer.add_channel(
                    schema_id,
                    &topic,
                    channel_descriptor.message_encoding.as_str(),
                    &BTreeMap::new(),
                ) else {
                    log::warn!("{topic}: Failed to add channel: {encoding_string:?}");
                    continue;
                };
                self.mcap
                    .channel
                    .insert(topic.clone(), Channel::new(channel_id));
            }

            let channel = self
                .mcap
                .channel
                .get_mut(&topic)
                .expect("Failed to get mcap channel");
            let now = SystemTime::now();
            let log_time = now.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
            let publish_time = sample
                .timestamp()
                .map(|ts| ts.get_time().as_nanos())
                .unwrap_or(log_time);
            if let Err(e) = writer.write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id: channel.channel_id,
                    sequence: channel.sequence,
                    log_time,
                    publish_time,
                },
                &payload.to_bytes(),
            ) {
                log::error!("{topic}: Failed to write message: {e}");
                continue;
            }
            channel.sequence += 1;

            if now.duration_since(last_flush).unwrap() > std::time::Duration::from_secs(30) {
                self.mcap.flush();
                last_flush = now;
            }
        }
    }
}
