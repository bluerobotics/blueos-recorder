mod cli;
mod service;
use service::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::init();
    if cli::is_verbose() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    } else {
        env_logger::init();
    }

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

    let mut service = Service::new(config, cli::recorder_path()).await;
    service.run().await;

    Ok(())
}
