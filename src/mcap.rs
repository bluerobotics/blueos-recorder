use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::BufWriter,
    path::Path,
    sync::{
        Arc, Mutex,
        mpsc::{self, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use clap::ValueEnum;
use mcap::{Compression, Writer, write::WriteOptions};
use tracing::*;

use crate::channel_descriptor::ChannelDescriptor;

const NO_SCHEMA_ID: u16 = 0; // "A schema_id of 0 indicates there is no schema for this channel." (https://mcap.dev/spec#channel-op0x04)
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(30);
const IO_BUFFER_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_CHUNK_BYTES: u64 = 10 * 1024 * 1024;
const WRITER_QUEUE_CAPACITY: usize = 4096;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum McapCompression {
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Copy, Debug)]
pub struct McapWriteConfig {
    pub compression: McapCompression,
    pub crc: bool,
    pub chunk_size: u64,
    pub async_io: bool,
}

impl Default for McapWriteConfig {
    fn default() -> Self {
        Self {
            compression: McapCompression::Lz4,
            crc: true,
            chunk_size: DEFAULT_CHUNK_BYTES,
            async_io: false,
        }
    }
}

impl Default for McapCompression {
    fn default() -> Self {
        Self::Lz4
    }
}

impl McapWriteConfig {
    pub fn write_options(self) -> WriteOptions {
        let compression = match self.compression {
            McapCompression::None => None,
            McapCompression::Lz4 => Some(Compression::Lz4),
            McapCompression::Zstd => Some(Compression::Zstd),
        };

        WriteOptions::new()
            .compression(compression)
            .chunk_size(Some(self.chunk_size))
            .compression_threads(if matches!(self.compression, McapCompression::None) {
                0
            } else {
                2
            })
            .calculate_chunk_crcs(self.crc)
            .calculate_data_section_crc(self.crc)
            .calculate_summary_section_crc(self.crc)
            .calculate_attachment_crcs(self.crc)
            .emit_message_indexes(false)
    }

    fn open_writer(path: &Path, config: Self) -> Result<Writer<BufWriter<File>>> {
        let file = std::fs::File::create(path).context("Failed to create MCAP file")?;
        open_writer_on_file(file, config)
    }

    async fn open_writer_async(path: &Path, config: Self) -> Result<Writer<BufWriter<File>>> {
        let file = tokio::fs::File::create(path)
            .await
            .context("Failed to create MCAP file")?;
        let file = file.into_std().await;
        open_writer_on_file(file, config)
    }
}

fn open_writer_on_file(file: File, config: McapWriteConfig) -> Result<Writer<BufWriter<File>>> {
    let io_buffer_bytes = IO_BUFFER_BYTES.max(config.chunk_size as usize);
    Writer::with_options(
        BufWriter::with_capacity(io_buffer_bytes, file),
        config.write_options(),
    )
    .context("Failed to create MCAP writer")
}

enum WriterCommand {
    Write {
        topic: Arc<str>,
        log_time: u64,
        publish_time: u64,
        payload: Bytes,
        new_channel: Option<ChannelDescriptor>,
    },
    Flush,
    Finish,
}

pub struct Mcap {
    writer: Arc<McapWriter>,
    writer_thread: Option<JoinHandle<Result<()>>>,
    last_flush: Instant,
}

pub struct McapWriter {
    tx: SyncSender<WriterCommand>,
    known_topics: Mutex<HashSet<Arc<str>>>,
}

struct Channel {
    channel_id: u16,
    sequence: u32,
}

impl Mcap {
    #[instrument(skip_all, fields(path = %path.display()))]
    pub async fn try_new(path: &Path, config: McapWriteConfig) -> Result<Self> {
        info!(
            compression = ?config.compression,
            crc = config.crc,
            chunk_bytes = config.chunk_size,
            async_io = config.async_io,
            io_buffer_bytes = IO_BUFFER_BYTES.max(config.chunk_size as usize),
            "Opening MCAP file"
        );
        let writer = if config.async_io {
            McapWriteConfig::open_writer_async(path, config).await?
        } else {
            McapWriteConfig::open_writer(path, config)?
        };
        let (tx, rx) = mpsc::sync_channel(WRITER_QUEUE_CAPACITY);
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
            last_flush: Instant::now(),
        })
    }

    pub fn writer(&self) -> Arc<McapWriter> {
        self.writer.clone()
    }

    pub async fn maybe_flush(&mut self) -> Result<()> {
        if self.last_flush.elapsed() < FLUSH_INTERVAL {
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
    pub fn write_message<F>(
        &self,
        topic: &str,
        log_time: u64,
        publish_time: u64,
        payload: Bytes,
        new_channel: F,
    ) -> Result<()>
    where
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
            payload,
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

                if let Err(error) = writer.write_to_known_channel(&header, payload.as_ref()) {
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
        Some(schema) => writer
            .add_schema(
                &schema.name,
                schema.encoding.as_str(),
                schema.content.as_bytes(),
            )
            .context("Failed to add MCAP schema")?,
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

impl Channel {
    fn new(channel_id: u16) -> Self {
        Self {
            channel_id,
            sequence: 0,
        }
    }
}
