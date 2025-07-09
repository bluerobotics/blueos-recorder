use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::BufWriter,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use mcap::Writer;
use serde_json::{Value, json};
use zenoh::{Config, Session, handlers::FifoChannelHandler, pubsub::Subscriber, sample::Sample};

struct Channel {
    channel_id: u16,
    sequence: u32,
}

impl Channel {
    pub fn new(channel_id: u16) -> Self {
        Self {
            channel_id,
            sequence: 0,
        }
    }
}

struct Mcap {
    writer: Option<Writer<BufWriter<File>>>,
    channel: HashMap<String, Channel>,
}

impl Mcap {
    fn new(path: &std::path::Path) -> Self {
        let writer = Writer::new(BufWriter::new(
            std::fs::File::create(path).expect("Failed to create file"),
        ))
        .expect("Failed to create writer");
        Self {
            writer: Some(writer),
            channel: HashMap::new(),
        }
    }

    fn finish(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            writer.finish().expect("Failed to finish writer");
        }
    }

    fn flush(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            log::info!("Flushing writer");
            writer.flush().expect("Failed to flush writer");
        }
    }
}

impl Drop for Mcap {
    fn drop(&mut self) {
        log::info!("Finishing writer");
        self.finish();
    }
}

pub struct Service {
    #[allow(dead_code)]
    session: Session,
    subscriber: Subscriber<FifoChannelHandler<Sample>>,
    mcap: Mcap,
    recorder_path: std::path::PathBuf,
}

fn load_cdr_schema(schema: &str) -> Result<String> {
    let mut schema_splitted = schema.split(".");
    let schema_package = schema_splitted.next().ok_or(anyhow::anyhow!(
        "Failed to get schema package from {schema}"
    ))?;
    let schema_name = schema_splitted
        .next()
        .ok_or(anyhow::anyhow!("Failed to get schema name from {schema}"))?;
    let current_dir = std::env::current_dir()
        .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?;
    let current_dir_string = current_dir.display().to_string();
    let schema_path = format!(
        "{current_dir_string}/src/external/zBlueberry/msgs/{schema_package}/{schema_name}.msg"
    );
    std::fs::read_to_string(&schema_path)
        .map_err(|e| anyhow::anyhow!("Failed to read schema: {e}, ({schema_path})"))
}

fn create_schema(value: &Value) -> Value {
    match value {
        Value::Null => json!({ "type": "null" }),
        Value::Bool(_) => json!({ "type": "boolean" }),
        Value::Number(n) if n.is_i64() => json!({ "type": "integer" }),
        Value::Number(_) => json!({ "type": "number" }),
        Value::String(_) => json!({ "type": "string" }),
        Value::Array(arr) => {
            let items = if let Some(first) = arr.first() {
                create_schema(first)
            } else {
                json!({})
            };
            json!({ "type": "array", "items": items })
        }
        Value::Object(map) => {
            let properties: BTreeMap<_, _> = map
                .iter()
                .map(|(k, v)| (k.clone(), create_schema(v)))
                .collect();
            json!({ "type": "object", "properties": properties })
        }
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
    pub async fn new(config: Config, recorder_path: std::path::PathBuf) -> Self {
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
        }
    }

    pub async fn run(&mut self) {
        let mut last_flush = SystemTime::now();
        let base_mode_regex = regex::Regex::new(r"mavlink/\d+/1/HEARTBEAT/base_mode").unwrap();
        while let Ok(sample) = self.subscriber.recv_async().await {
            let topic = sample.key_expr().to_string();
            let payload = sample.payload();
            let encoding = sample.encoding();
            let encoding_string = encoding.to_string();
            let mut encoding_string_splitted = encoding_string.split(";");
            let encoding_string_0 = encoding_string_splitted.next().unwrap();
            let encoding_string_1 = encoding_string_splitted.next();

            if base_mode_regex.is_match(&topic) {
                if let Ok(string) = payload.try_to_string() {
                    if let Ok(value) = serde_json5::from_str::<Value>(&string) {
                        if let Some(base_mode) = value.get("bits") {
                            if let Some(base_mode_value) = base_mode.as_u64() {
                                // https://mavlink.io/en/messages/common.html#MAV_MODE_FLAG_SAFETY_ARMED
                                if base_mode_value & 0b10000000 != 0 {
                                    if self.mcap.writer.is_none() {
                                        let filename = generate_filename();
                                        let path = self.recorder_path.join(filename);
                                        self.mcap = Mcap::new(std::path::Path::new(&path));
                                    }
                                } else {
                                    self.mcap.finish();
                                }
                            }
                        }
                    }
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

            // For more information: https://mcap.dev/spec/registry#well-known-schema-encodings
            if !self.mcap.channel.contains_key(&topic) {
                let (encoding, schema_description, msg_encoding) =
                    match (encoding_string_0, encoding_string_1) {
                        ("application/cdr", Some(schema)) => {
                            let schema = match load_cdr_schema(schema) {
                                Ok(schema) => schema,
                                Err(e) => {
                                    log::error!("{topic}: Failed to load schema: {e}");
                                    continue;
                                }
                            };
                            ("ros2msg", schema, "cdr")
                        }
                        ("application/json", _) => {
                            let Ok(string) = payload.try_to_string() else {
                                log::warn!("{topic}: Failed to decode payload as UTF-8 string");
                                continue;
                            };
                            let Ok(value) = serde_json5::from_str::<Value>(&string) else {
                                log::warn!("{topic}: Failed to parse payload as JSON5: {string}");
                                continue;
                            };
                            // Foxglove does not support non-object messages
                            if !value.is_object() {
                                continue;
                            }
                            let schema = create_schema(&value).to_string();
                            ("jsonschema", schema, "json")
                        }
                        _ => {
                            log::warn!("{topic}: Received unknown encoding: {encoding_string:?}");
                            continue;
                        }
                    };

                log::info!("{topic}: Adding schema: {encoding_string}");
                log::info!("Schema description: {schema_description}");
                let name_backup = topic.replace("/", ".").to_string();
                let name = encoding_string_1.unwrap_or(&name_backup);
                let Ok(schema_id) =
                    writer.add_schema(name, encoding, schema_description.as_bytes())
                else {
                    log::warn!("{topic}: Failed to add schema: {encoding_string:?}");
                    continue;
                };

                let Ok(channel_id) =
                    writer.add_channel(schema_id, &topic, msg_encoding, &BTreeMap::new())
                else {
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
            let duration = now.duration_since(UNIX_EPOCH).unwrap();
            let timestamp = duration.as_nanos() as u64;
            if let Err(e) = writer.write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id: channel.channel_id,
                    sequence: channel.sequence,
                    log_time: timestamp,
                    publish_time: timestamp,
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
