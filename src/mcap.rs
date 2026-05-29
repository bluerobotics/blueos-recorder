use std::{collections::HashMap, fs::File, io::BufWriter};

use mcap::Writer;

pub struct Mcap {
    pub writer: Option<Writer<BufWriter<File>>>,
    pub channel: HashMap<String, Channel>,
}

pub struct Channel {
    pub channel_id: u16,
    pub sequence: u32,
}

impl Mcap {
    pub fn new(path: &std::path::Path) -> Self {
        log::info!("Creating mcap file: {path:?}");
        let writer = Writer::new(BufWriter::new(
            std::fs::File::create(path).expect("Failed to create file"),
        ))
        .expect("Failed to create writer");
        Self {
            writer: Some(writer),
            channel: HashMap::new(),
        }
    }

    pub fn finish(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            writer.finish().expect("Failed to finish writer");
        }
    }

    pub fn flush(&mut self) {
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

impl Channel {
    pub fn new(channel_id: u16) -> Self {
        Self {
            channel_id,
            sequence: 0,
        }
    }
}
