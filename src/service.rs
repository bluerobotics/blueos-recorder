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
    writer: Writer<BufWriter<File>>,
    channel: HashMap<String, Channel>,
}

pub struct Service {
    #[allow(dead_code)]
    session: Session,
    subscriber: Subscriber<FifoChannelHandler<Sample>>,
    mcap: Mcap,
}

fn load_cdr_schema(schema: &str) -> Result<String> {
    let mut schema_splitted = schema.split(".");
    let schema_package = schema_splitted.nth(0).ok_or(anyhow::anyhow!(
        "Failed to get schema package from {schema}"
    ))?;
    let schema_name = schema_splitted
        .nth(0)
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

impl Service {
    pub async fn new(config: Config) -> Self {
        let session = zenoh::open(config).await.unwrap();
        let subscriber = session.declare_subscriber("**").await.unwrap();

        let filename = "recorder.mcap";
        if std::path::Path::new(filename).exists() {
            std::fs::remove_file(filename).unwrap();
        }

        let writer = Writer::new(BufWriter::new(
            std::fs::File::create(filename).expect("Failed to create file"),
        ))
        .expect("Failed to create writer");

        Self {
            session,
            subscriber,
            mcap: Mcap {
                writer,
                channel: HashMap::new(),
            },
        }
    }

    pub async fn run(&mut self) {
        let mut last_flush = SystemTime::now();
        while let Ok(sample) = self.subscriber.recv_async().await {
            let topic = sample.key_expr().to_string();
            let payload = sample.payload();
            let encoding = sample.encoding();
            let encoding_string = encoding.to_string();
            let mut encoding_string_splitted = encoding_string.split(";");
            let encoding_string_0 = encoding_string_splitted.nth(0).unwrap();
            let encoding_string_1 = encoding_string_splitted.nth(0);

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
                            log::warn!("{topic}: Received unknown encoding: {:?}", encoding_string);
                            continue;
                        }
                    };

                log::info!("{topic}: Adding schema: {encoding_string}");
                log::info!("Schema description: {schema_description}");
                let name_backup = format!("{}", topic.replace("/", "."));
                let name = encoding_string_1.unwrap_or(&name_backup);
                let Ok(schema_id) =
                    self.mcap
                        .writer
                        .add_schema(name, encoding, schema_description.as_bytes())
                else {
                    log::warn!("{topic}: Failed to add schema: {:?}", encoding_string);
                    continue;
                };

                let Ok(channel_id) =
                    self.mcap
                        .writer
                        .add_channel(schema_id, &topic, msg_encoding, &BTreeMap::new())
                else {
                    log::warn!("{topic}: Failed to add channel: {:?}", encoding_string);
                    continue;
                };
                self.mcap
                    .channel
                    .insert(topic.clone(), Channel::new(channel_id));
            }

            let channel = self.mcap.channel.get_mut(&topic).unwrap();
            let now = SystemTime::now();
            let duration = now.duration_since(UNIX_EPOCH).unwrap();
            let timestamp = duration.as_nanos() as u64;
            if let Err(e) = self.mcap.writer.write_to_known_channel(
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
                log::info!("Flushing writer");
                self.mcap.writer.flush().expect("Failed to flush writer");
                last_flush = now;
            }
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        log::info!("Finishing writer");
        self.mcap.writer.finish().expect("Failed to finish writer");
    }
}
