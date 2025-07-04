use foxglove::Context;
use foxglove::bytes::Bytes;
use foxglove::schemas::CompressedVideo;
use std::time::{SystemTime, UNIX_EPOCH};

mod service;
use service::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut config = zenoh::Config::default();
    config
        .insert_json5("mode", r#""client""#)
        .expect("Failed to insert client mode");
    config
        .insert_json5("connect/endpoints", r#"["tcp/192.168.31.177:7447"]"#)
        .expect("Failed to insert connection endpoint");
    config
        .insert_json5("adminspace", r#"{"enabled": true}"#)
        .expect("Failed to insert adminspace");
    config
        .insert_json5("metadata", r#"{"name": "blueos-recorder"}"#)
        .expect("Failed to insert metadata");

    /*
    let session = zenoh::open(config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Zenoh session: {e}"))?;
    let subscriber = session
        .declare_subscriber("**")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to declare subscriber: {e}"))?;

    let filename = "recorder.mcap";
    if std::path::Path::new(filename).exists() {
        std::fs::remove_file(filename).unwrap();
    }

    let ctx = Context::new();
    let mcap = ctx
        .mcap_writer()
        .create_new_buffered_file(filename)
        .expect("Failed to create MCAP file");
    */

    let mut service = Service::new(config).await;
    service.run().await;

    /*
    // Create channel for H.264 video data
    let channel = ctx.channel_builder("camera_h264").build();

    // Handle incoming messages
    let mut sequence = 0;

    while let Ok(sample) = subscriber.recv_async().await {
        let data = sample.payload().to_bytes();

        // Debug print the raw data size
        println!("Received H.264 frame size: {}", data.len());

        // Validate frame
        if data.is_empty() || data.len() >= 1024 * 1024 * 10 {
            // 10MB limit
            eprintln!("Skipping invalid frame: size={}", data.len());
            continue;
        }

        sequence += 1;
        println!("Sequence: {}", sequence);
        if sequence > 1500 {
            break;
        }

        let now = SystemTime::now();

        // Create CompressedVideo message
        let compressed_video = CompressedVideo {
            timestamp: Some(foxglove::schemas::Timestamp::new(
                now.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32,
                now.duration_since(UNIX_EPOCH).unwrap().subsec_nanos() as u32,
            )),
            frame_id: "camera".to_string(),
            data: Bytes::from(data.to_vec()),
            format: "h264".to_string(),
        };

        // Write message to MCAP file
        channel.log(&compressed_video);
    }

    // Finish writing and close the MCAP file
    mcap.close().expect("Failed to close MCAP file");

    // Close the session
    session
        .close()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to close session: {}", e))?;

    */
    Ok(())
}
