mod channel_descriptor;
mod cli;
mod mcap;
mod service;
use service::Service;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::init();
    let default_level = if cli::is_verbose() { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level)),
        )
        .init();

    let mut config = zenoh::Config::default();
    config
        .insert_json5("mode", r#""client""#)
        .expect("Failed to insert client mode");
    config
        .insert_json5("connect/endpoints", r#"["tcp/127.0.0.1:7447"]"#)
        .expect("Failed to insert connection endpoint");
    config
        .insert_json5("adminspace", r#"{"enabled": true}"#)
        .expect("Failed to insert adminspace");
    config
        .insert_json5("metadata", r#"{"name": "blueos-recorder"}"#)
        .expect("Failed to insert metadata");

    for (key, value) in cli::zkey_config() {
        config
            .insert_json5(
                &key,
                &serde_json5::to_string(&value).unwrap_or_else(|error| {
                    panic!("Failed to convert key value to json {key}: {error}")
                }),
            )
            .unwrap_or_else(|error| panic!("Failed to insert {key}: {error}"));
    }

    let mut service = Service::new(config, cli::recorder_path(), cli::schema_path()).await;
    service.run().await;

    Ok(())
}
