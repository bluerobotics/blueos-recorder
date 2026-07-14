use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::BufWriter,
    path::Path,
};

use anyhow::{Context, Result, anyhow};
use clap::ValueEnum;
use mcap::{Compression, Writer, write::WriteOptions};
use tracing::*;

use crate::channel_descriptor::ChannelDescriptor;

const NO_SCHEMA_ID: u16 = 0; // "A schema_id of 0 indicates there is no schema for this channel." (https://mcap.dev/spec#channel-op0x04)
const IO_BUFFER_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_CHUNK_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum McapCompression {
    None,
    #[default]
    Lz4,
    Zstd,
}

#[derive(Clone, Copy, Debug)]
pub struct McapWriteConfig {
    pub compression: McapCompression,
    pub crc: bool,
    pub chunk_size: u64,
}

impl Default for McapWriteConfig {
    fn default() -> Self {
        Self {
            compression: McapCompression::Lz4,
            crc: true,
            chunk_size: DEFAULT_CHUNK_BYTES,
        }
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
            .emit_message_indexes(true) // unindexing limits the files to 1GB
    }

    fn open_writer(path: &Path, config: Self) -> Result<Writer<BufWriter<File>>> {
        let file = std::fs::File::create(path).context("Failed to create MCAP file")?;
        let io_buffer_bytes = IO_BUFFER_BYTES.max(config.chunk_size as usize);
        Writer::with_options(
            BufWriter::with_capacity(io_buffer_bytes, file),
            config.write_options(),
        )
        .context("Failed to create MCAP writer")
    }
}

pub struct Mcap {
    writer: Option<Writer<BufWriter<File>>>,
    channel: HashMap<String, Channel>,
}

pub struct Channel {
    channel_id: u16,
    sequence: u32,
}

impl Mcap {
    #[instrument(skip_all, fields(path = %path.display()))]
    pub fn try_new(path: &Path, config: McapWriteConfig) -> Result<Self> {
        info!(
            compression = ?config.compression,
            crc = config.crc,
            chunk_bytes = config.chunk_size,
            io_buffer_bytes = IO_BUFFER_BYTES.max(config.chunk_size as usize),
            "Opening MCAP file"
        );
        let writer = McapWriteConfig::open_writer(path, config)?;
        Ok(Self {
            writer: Some(writer),
            channel: HashMap::new(),
        })
    }

    #[instrument(skip_all)]
    pub fn finish(&mut self) -> Result<()> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };
        writer.finish().context("Failed to finish MCAP writer")?;
        Ok(())
    }

    #[instrument(skip_all, level = "info")]
    pub fn flush(&mut self) -> Result<()> {
        let Some(writer) = self.writer.as_mut() else {
            warn!("Writer not available");
            return Ok(()); // Nothing to flush since the writer is not available
        };
        writer.flush().context("Failed to flush MCAP writer")?;
        Ok(())
    }

    #[inline]
    pub fn has_channel(&self, topic: &str) -> bool {
        self.channel.contains_key(topic)
    }

    #[instrument(skip_all)]
    fn register_channel(&mut self, desc: ChannelDescriptor) -> Result<()> {
        if self.channel.contains_key(&desc.topic) {
            return Err(anyhow!("Channel already registered"));
        }

        let Some(writer) = self.writer.as_mut() else {
            return Err(anyhow!("Writer not available"));
        };

        let schema_id = match &desc.schema {
            Some(schema) => match &schema.content {
                Some(content) => writer.add_schema(
                    &content.name,
                    schema.encoding.as_str(),
                    content.data.as_bytes(),
                )?,
                None => NO_SCHEMA_ID, // encoding known, but no definition to register
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
        self.channel.insert(desc.topic, Channel::new(channel_id));
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn write_message(
        &mut self,
        topic: &str,
        log_time: u64,
        publish_time: u64,
        payload: &[u8],
        new_channel: Option<ChannelDescriptor>,
    ) -> Result<()> {
        if let Some(desc) = new_channel {
            if desc.topic != topic {
                return Err(anyhow!("Channel descriptor topic mismatch: {}", desc.topic));
            }
            self.register_channel(desc)?;
        }

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("Writer not available"))?;

        let channel = self
            .channel
            .get_mut(topic)
            .ok_or_else(|| anyhow!("Channel not registered"))?;

        let header = mcap::records::MessageHeader {
            channel_id: channel.channel_id,
            sequence: channel.sequence,
            log_time,
            publish_time,
        };

        writer
            .write_to_known_channel(&header, payload)
            .context("Failed to write message to MCAP channel")?;
        channel.sequence += 1;
        Ok(())
    }
}

impl Drop for Mcap {
    fn drop(&mut self) {
        info!("Finishing MCAP writer");
        if let Err(error) = self.finish() {
            error!(%error, "Failed to finish MCAP writer on drop");
        }
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
