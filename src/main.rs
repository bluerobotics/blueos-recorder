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

    let mut service = Service::new(config).await;
    service.run().await;

    Ok(())
}
