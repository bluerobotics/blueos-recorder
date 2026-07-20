use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::BufWriter,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::{
        Arc, Mutex,
        mpsc::{self, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use clap::ValueEnum;
use mcap::{Compression, Writer, write::WriteOptions};
use tracing::*;

use crate::channel_descriptor::ChannelDescriptor;

const NO_SCHEMA_ID: u16 = 0; // "A schema_id of 0 indicates there is no schema for this channel." (https://mcap.dev/spec#channel-op0x04)
const IO_BUFFER_BYTES: NonZeroUsize = NonZeroUsize::new(4 * 1024 * 1024).unwrap();
pub const DEFAULT_CHUNK_BYTES: NonZeroU64 = NonZeroU64::new(10 * 1024 * 1024).unwrap();
pub const DEFAULT_FLUSH_INTERVAL_SECS: NonZeroU64 = NonZeroU64::new(30).unwrap();
const WRITER_QUEUE_CAPACITY: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

#[derive(Clone, Copy, Debug)]
pub struct McapWriteConfig {
    pub compression: McapCompression,
    pub chunk_size: NonZeroU64,
    pub flush_interval_secs: NonZeroU64,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum McapCompression {
    None,
    #[default]
    Lz4,
    Zstd,
}

pub struct Mcap {
    writer: Arc<McapWriter>,
    writer_thread: Option<JoinHandle<Result<()>>>,
    flush_interval: Duration,
    last_flush: Instant,
}

pub struct McapWriter {
    tx: SyncSender<WriterCommand>,
    known_topics: Mutex<HashSet<Arc<str>>>,
}

enum WriterCommand {
    Write {
        topic: Arc<str>,
        log_time: u64,
        publish_time: u64,
        payload: Box<dyn RecordPayload>,
        new_channel: Option<ChannelDescriptor>,
    },
    Flush,
    Finish,
}

/// A payload to record, moved into the background writer.
///
/// The contiguous byte view is materialized on the writer thread via
/// [`RecordPayload::bytes`], not on the receive loop. This lets cheap,
/// `Arc`-backed buffers (such as zenoh's `ZBytes`) be handed to the writer
/// without copying the payload on the hot path.
pub trait RecordPayload: Send {
    fn bytes(&self) -> Cow<'_, [u8]>;
}

struct Channel {
    channel_id: u16,
    sequence: u32,
}

impl Default for McapWriteConfig {
    fn default() -> Self {
        Self {
            compression: McapCompression::Lz4,
            chunk_size: DEFAULT_CHUNK_BYTES,
            flush_interval_secs: DEFAULT_FLUSH_INTERVAL_SECS,
        }
    }
}

impl McapWriteConfig {
    fn open_writer(path: &Path, config: Self) -> Result<Writer<BufWriter<File>>> {
        let file = std::fs::File::create(path).context("Failed to create MCAP file")?;
        let io_buffer_bytes = IO_BUFFER_BYTES.get().max(config.chunk_size.get() as usize);
        Writer::with_options(
            BufWriter::with_capacity(io_buffer_bytes, file),
            config.into(),
        )
        .context("Failed to create MCAP writer")
    }
}

impl From<McapWriteConfig> for WriteOptions {
    fn from(val: McapWriteConfig) -> Self {
        let compression = match val.compression {
            McapCompression::None => None,
            McapCompression::Lz4 => Some(Compression::Lz4),
            McapCompression::Zstd => Some(Compression::Zstd),
        };

        WriteOptions::new()
            .compression(compression)
            .chunk_size(Some(val.chunk_size.into()))
            .compression_threads(if matches!(val.compression, McapCompression::None) {
                0
            } else {
                2
            })
            .emit_message_indexes(true) // unindexing limits the files to 1GB
    }
}

impl Mcap {
    #[instrument(skip_all, fields(path = %path.display()))]
    pub async fn try_new(path: &Path, config: McapWriteConfig) -> Result<Self> {
        info!(
            compression = ?config.compression,
            chunk_bytes = config.chunk_size,
            flush_interval_secs = config.flush_interval_secs,
            io_buffer_bytes = IO_BUFFER_BYTES.get().max(config.chunk_size.get() as usize),
            "Opening MCAP file"
        );
        let writer = McapWriteConfig::open_writer(path, config)?;
        let (tx, rx) = mpsc::sync_channel(WRITER_QUEUE_CAPACITY.into());
        let writer_thread = thread::Builder::new()
            .name("mcap-writer".into())
            .spawn(move || writer_loop(rx, writer))
            .context("Failed to spawn MCAP writer thread")?;

        Ok(Self {
            writer: Arc::new(McapWriter {
                tx,
                known_topics: Mutex::new(HashSet::new()),
            }),
            writer_thread: Some(writer_thread),
            flush_interval: Duration::from_secs(config.flush_interval_secs.get()),
            last_flush: Instant::now(),
        })
    }

    pub fn writer(&self) -> Arc<McapWriter> {
        self.writer.clone()
    }

    pub async fn flush(&mut self) -> Result<()> {
        if self.last_flush.elapsed() < self.flush_interval {
            return Ok(());
        }
        self.writer.flush()?;
        self.last_flush = Instant::now();
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.send_finish()?;
        if let Some(handle) = self.writer_thread.take() {
            let join_result = tokio::task::spawn_blocking(move || handle.join())
                .await
                .context("MCAP writer join task failed")?;
            match join_result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => return Err(anyhow!("MCAP writer thread panicked")),
            }
        }
        Ok(())
    }
}

impl McapWriter {
    pub fn write_message<P, F>(
        &self,
        topic: &str,
        log_time: u64,
        publish_time: u64,
        payload: P,
        new_channel: F,
    ) -> Result<()>
    where
        P: RecordPayload + 'static,
        F: FnOnce() -> Option<ChannelDescriptor>,
    {
        let known = self
            .known_topics
            .lock()
            .expect("mcap known_topics poisoned");
        let (topic, new_channel) = if let Some(existing) = known.get(topic) {
            (existing.clone(), None)
        } else {
            drop(known);
            let Some(descriptor) = new_channel() else {
                return Ok(());
            };
            if descriptor.topic != topic {
                return Err(anyhow!(
                    "Channel descriptor topic mismatch: {}",
                    descriptor.topic
                ));
            }
            let topic_arc: Arc<str> = Arc::from(topic);
            self.known_topics
                .lock()
                .expect("mcap known_topics poisoned")
                .insert(topic_arc.clone());
            (topic_arc, Some(descriptor))
        };

        let command = WriterCommand::Write {
            topic,
            log_time,
            publish_time,
            payload: Box::new(payload),
            new_channel,
        };
        self.tx
            .send(command)
            .map_err(|_| anyhow!("MCAP writer thread stopped"))
    }

    pub fn flush(&self) -> Result<()> {
        self.tx
            .send(WriterCommand::Flush)
            .map_err(|_| anyhow!("MCAP writer thread stopped"))
    }

    fn send_finish(&self) -> Result<()> {
        self.tx
            .send(WriterCommand::Finish)
            .map_err(|_| anyhow!("MCAP writer thread stopped"))
    }
}

impl Channel {
    fn new(channel_id: u16) -> Self {
        Self {
            channel_id,
            sequence: 0,
        }
    }
}

fn writer_loop(
    rx: mpsc::Receiver<WriterCommand>,
    mut writer: Writer<BufWriter<File>>,
) -> Result<()> {
    let mut channels = HashMap::<Arc<str>, Channel>::new();

    for command in rx {
        match command {
            WriterCommand::Write {
                topic,
                log_time,
                publish_time,
                payload,
                new_channel,
            } => {
                if let Some(descriptor) = new_channel {
                    register_channel(&mut writer, &mut channels, descriptor)?;
                }

                let channel = channels
                    .get_mut(&topic)
                    .ok_or_else(|| anyhow!("Channel not registered for topic {topic}"))?;

                let header = mcap::records::MessageHeader {
                    channel_id: channel.channel_id,
                    sequence: channel.sequence,
                    log_time,
                    publish_time,
                };

                let data = payload.bytes();
                if let Err(error) = writer.write_to_known_channel(&header, data.as_ref()) {
                    error!(%error, topic = %topic, "Failed to write message to MCAP channel");
                } else {
                    channel.sequence += 1;
                }
            }
            WriterCommand::Flush => {
                if let Err(error) = writer.flush() {
                    error!(%error, "Failed to flush MCAP writer");
                }
            }
            WriterCommand::Finish => {
                writer.finish().context("Failed to finish MCAP writer")?;
                break;
            }
        }
    }

    Ok(())
}

fn register_channel(
    writer: &mut Writer<BufWriter<File>>,
    channels: &mut HashMap<Arc<str>, Channel>,
    desc: ChannelDescriptor,
) -> Result<()> {
    if channels.contains_key(desc.topic.as_str()) {
        return Err(anyhow!("Channel already registered"));
    }

    let schema_id = match &desc.schema {
        Some(schema) => match &schema.content {
            Some(content) => writer
                .add_schema(
                    &content.name,
                    schema.encoding.as_str(),
                    content.data.as_bytes(),
                )
                .context("Failed to add MCAP schema")?,
            None => NO_SCHEMA_ID,
        },
        None => NO_SCHEMA_ID,
    };

    let channel_id = writer
        .add_channel(
            schema_id,
            &desc.topic,
            desc.message_encoding.as_str(),
            &BTreeMap::new(),
        )
        .context("Failed to add MCAP channel")?;

    info!(topic = %desc.topic, "Adding channel");
    channels.insert(Arc::from(desc.topic), Channel::new(channel_id));
    Ok(())
}
