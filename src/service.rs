use std::{any::Any, collections::{BTreeMap, HashMap}, fs::File, io::BufWriter, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use anyhow::Result;
use foxglove::{Context, McapWriterHandle};
use mcap::Writer;
use zenoh::{
    Config, Session, bytes::Encoding, handlers::FifoChannelHandler, pubsub::Subscriber,
    sample::Sample,
};

struct Channel {
    channel_id: u16,
    sequence: u32,
}

impl Channel {
    pub fn new(channel_id: u16) -> Self {
        Self { channel_id, sequence: 0 }
    }
}

struct Mcap {
    context: Arc<Context>,
    writer: Writer<BufWriter<File>>,
    channel: HashMap<String, Channel>,
}

pub struct Service {
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
    let schema_path =
        format!("{current_dir_string}/src/external/zBlueberry/msgs/{schema_package}/{schema_name}.msg");
    std::fs::read_to_string(&schema_path).map_err(|e| anyhow::anyhow!("Failed to read schema: {e}, ({schema_path})"))
}

impl Service {
    pub async fn new(config: Config) -> Self {
        let session = zenoh::open(config).await.unwrap();
        let subscriber = session.declare_subscriber("**").await.unwrap();

        let filename = "recorder.mcap";
        if std::path::Path::new(filename).exists() {
            std::fs::remove_file(filename).unwrap();
        }

        let context = Context::new();
        let writer = Writer::new(BufWriter::new(
            std::fs::File::create(filename).expect("Failed to create file"),
        ))
        .expect("Failed to create writer");

        Self {
            session,
            subscriber,
            mcap: Mcap {
                context,
                writer,
                channel: HashMap::new(),
            },
        }
    }

    pub async fn run(&mut self) {
        while let Ok(sample) = self.subscriber.recv_async().await {
            let topic = sample.key_expr().to_string();
            let payload = sample.payload();
            let encoding = sample.encoding();
            let encoding_string = sample.encoding().to_string();
            let mut encoding_string_splitted = encoding_string.split(";");
            let encoding_string_0 = encoding_string_splitted.nth(0).unwrap();
            let encoding_string_1 = encoding_string_splitted.nth(0);

            if !self.mcap.channel.contains_key(&topic) {
                let (encoding, schema_description, msg_encoding) = match (encoding_string_0, encoding_string_1) {
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
                    ("application/json", schema) => {
                        continue;
                    },
                    _ => {
                        log::warn!("{topic}: Received unknown encoding: {:?}", encoding_string);
                        continue;
                    }
                };

                /*
                let schema = match encoding_string_1 {
                    Some(schema) => schema.to_string(),
                    None => {
                        log::warn!("{topic}: Received unknown encoding: {:?}", encoding_string);
                        continue;
                    }
                };*/
                let Ok(schema_id) = self.mcap.writer.add_schema(&encoding_string_1.unwrap(), encoding, schema_description.as_bytes()) else {
                    log::warn!("{topic}: Failed to add schema: {:?}", encoding_string);
                    continue;
                };

                let Ok(channel_id) = self.mcap.writer.add_channel(schema_id, &topic, msg_encoding, &BTreeMap::new()) else {
                    log::warn!("{topic}: Failed to add channel: {:?}", encoding_string);
                    continue;
                };
                self.mcap.channel.insert(topic.clone(), Channel::new(channel_id));
            }

            let mut channel = self.mcap.channel.get_mut(&topic).unwrap();
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
                continue
            }
            channel.sequence += 1;

            if channel.sequence % 100 == 0 {
                log::info!("{topic}: Received {} messages", channel.sequence);
            }

            if channel.sequence > 400 {
                break;
            }


            /*
            match *encoding {
                Encoding::ZENOH_BYTES => {
                    // Don't know what to do with it, let just store
                    println!("Received zenoh/bytes from: {:?}", sample.key_expr());
                }
                Encoding::APPLICATION_JSON => {
                    // Cool and nice json messages
                    let Ok(payload_string) = String::from_utf8(payload.to_bytes().into()) else {
                        log::warn!("{topic}: {encoding} message is not valid string to decode json: {:?}", payload.to_bytes());
                        continue;
                    };

                    let Ok(json) = serde_json::from_str::<serde_json::Value>(&payload_string) else {
                        log::warn!("{topic}: {encoding} message is not valid json: {:?}", payload.to_bytes());
                        continue;
                    };

                    // record json here
                    //println!("Received json: {json:?}");
                    continue;
                }
                Encoding::APPLICATION_CDR => {
                    // Time for hardcode binary messages man
                    println!("{topic}: Received encoding: {:?}", sample.encoding());
                    println!("{topic}: Received encoding id: {:?}", sample.encoding().id());
                    println!("{topic}: Received from utf-8: {:?}", String::from_utf8(sample.encoding().schema().unwrap().to_vec()));
                    println!("{topic}: Received to_string: {:?}", sample.encoding().to_string());
                    //let encoding_string = encoding.to_string();
                    //let schema = encoding_string.split(";").nth(1);
                    //println!("Received: {:?}", encoding_string);
                }
                _ => {
                    match encoding_string_0 {
                        "application/json" => {
                            println!("{topic}: Received json");
                        }
                        _ => {
                            println!("{topic}: Received unknown encoding: {:?}", encoding_string);
                        }
                    }
                }
            }*/
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        log::info!("Finishing writer");
        self.mcap.writer.finish().expect("Failed to finish writer");
    }
}
