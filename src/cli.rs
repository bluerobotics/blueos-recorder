use clap::Parser;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use tracing::*;

use crate::mcap::{DEFAULT_CHUNK_BYTES, McapCompression, McapWriteConfig};

static MANAGER: OnceCell<Manager> = OnceCell::new();

struct Manager {
    clap_matches: Args,
}

#[derive(Debug, Parser)]
#[command(
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
    about = env!("CARGO_PKG_DESCRIPTION")
)]
pub struct Args {
    /// Turns all log categories up to Debug, for more information check RUST_LOG env variable.
    #[arg(short, long)]
    verbose: bool,

    /// Sets the path where recordings will be stored.
    #[arg(long, default_value = "/tmp")]
    recorder_path: String,

    /// Sets the path for message schemas. E.g: src/external/zBlueberry/msgs
    #[arg(long)]
    schema_path: Option<String>,

    /// Override Zenoh subscription key expressions (default: `**`, all topics).
    #[arg(long, value_name = "KEYEXPR")]
    zenoh_subscribe: Vec<String>,

    /// Zenoh configuration key-value pairs. Can be used multiple times.
    /// Format: --zkey key=value
    #[arg(long, value_name = "KEY=VALUE", num_args = 1..)]
    zkey: Vec<String>,

    /// MCAP chunk compression: lz4 (default), none, or zstd.
    #[arg(long, value_enum, default_value_t = McapCompression::Lz4)]
    mcap_compression: McapCompression,

    /// Calculate MCAP chunk, data-section, summary, and attachment CRCs.
    #[arg(long, default_value_t = true)]
    mcap_crc: bool,

    /// Target uncompressed MCAP chunk size in bytes before sealing.
    #[arg(long, default_value_t = DEFAULT_CHUNK_BYTES, value_parser = parse_chunk_size)]
    mcap_chunk_size: u64,

    /// Open/flush/finish MCAP via tokio fs and the blocking thread pool.
    #[arg(long, default_value_t = false)]
    mcap_async_io: bool,
}

/// Constructs our manager, Should be done inside main
pub fn init() {
    let expanded_args = std::env::args()
        .map(|arg| {
            // Fallback to the original if it fails to expand
            shellexpand::env(&arg.clone())
                .inspect_err(
                    |_| warn!(arg = ?arg, "Failed expanding arg, using the non-expanded instead"),
                )
                .unwrap_or_else(|_| arg.into())
                .into_owned()
        })
        .collect::<Vec<String>>();

    let reparsed_expanded_args = Args::parse_from(expanded_args);

    init_with(reparsed_expanded_args);
}

/// Constructs our manager, Should be done inside main
/// Note: differently from init(), this doesn't expand env variables
pub fn init_with(args: Args) {
    MANAGER.get_or_init(|| Manager { clap_matches: args });
}

/// Local accessor to the parsed Args
fn args() -> &'static Args {
    &MANAGER.get().unwrap().clap_matches
}

/// Checks if the verbosity parameter was used
pub fn is_verbose() -> bool {
    args().verbose
}

pub fn path_dir_from_arg(arg: &str, create_if_not_exists: bool) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(arg);

    let pathbuf = std::fs::canonicalize(&path)
        .inspect_err(
            |_| warn!(path = ?path, "Failed canonicalizing path, using the non-canonized instead"),
        )
        .unwrap_or_else(|_| std::path::PathBuf::from(&path));

    if !pathbuf.exists() {
        warn!(path = ?pathbuf, "Path does not exist");
        if create_if_not_exists {
            info!(path = ?pathbuf, "Creating directory");
            if let Err(error) = std::fs::create_dir_all(&pathbuf) {
                error!(path = ?pathbuf, %error, "Failed to create directory");
                std::process::exit(1);
            }
        } else {
            std::process::exit(1);
        }
    } else if !pathbuf.is_dir() {
        error!(path = ?pathbuf, "Path is not a directory");
        std::process::exit(1);
    }

    pathbuf
}

pub fn recorder_path() -> std::path::PathBuf {
    path_dir_from_arg(&args().recorder_path, true)
}

pub fn schema_path() -> Option<std::path::PathBuf> {
    args()
        .schema_path
        .as_ref()
        .map(|schema_path| path_dir_from_arg(schema_path, false))
}

pub fn mcap_write_config() -> McapWriteConfig {
    McapWriteConfig {
        compression: args().mcap_compression,
        crc: args().mcap_crc,
        chunk_size: args().mcap_chunk_size,
        async_io: args().mcap_async_io,
    }
}

fn parse_chunk_size(raw: &str) -> Result<u64, String> {
    let size = raw
        .parse::<u64>()
        .map_err(|error| format!("invalid mcap chunk size {raw:?}: {error}"))?;
    if size == 0 {
        return Err("mcap chunk size must be greater than zero".into());
    }
    Ok(size)
}

pub fn zenoh_subscribe_patterns() -> Vec<String> {
    let patterns = &args().zenoh_subscribe;
    if patterns.is_empty() {
        return vec![crate::zenoh_subscriptions::DEFAULT_RECORDING_KEYEXPR.to_owned()];
    }
    patterns
        .iter()
        .map(|pattern| crate::zenoh_subscriptions::normalize_keyexpr(pattern))
        .collect()
}

/// Returns the zenoh configuration key-value pairs as a HashMap
pub fn zkey_config() -> HashMap<String, String> {
    let mut config = HashMap::new();

    for zkey_arg in &args().zkey {
        if let Some((key, value)) = zkey_arg.split_once('=') {
            config.insert(key.to_string(), value.to_string());
        } else {
            warn!(zkey = %zkey_arg, "Invalid zkey format, expected KEY=VALUE format");
        }
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing() {
        // Test with both arguments
        let args = Args::parse_from(vec![
            "program_name",
            "--verbose",
            "--recorder-path",
            "/custom/path",
        ]);
        assert_eq!(args.verbose, true);
        assert_eq!(args.recorder_path, "/custom/path");

        // Test with zkey arguments
        let args = Args::parse_from(vec![
            "program_name",
            "--zkey",
            "potato=elefante",
            "--zkey",
            "potato.coiso=fifi",
        ]);
        assert_eq!(args.zkey, vec!["potato=elefante", "potato.coiso=fifi"]);

        // Test with all arguments
        let args = Args::parse_from(vec![
            "program_name",
            "--verbose",
            "--recorder-path",
            "/custom/path",
            "--zkey",
            "eita",
            "--zkey",
            "potato=elefante",
            "--zkey",
            "potato.coiso=fifi",
        ]);
        assert_eq!(args.verbose, true);
        assert_eq!(args.recorder_path, "/custom/path");
        assert_eq!(
            args.zkey,
            vec!["eita", "potato=elefante", "potato.coiso=fifi"]
        );

        init_with(args);

        let config = zkey_config();
        assert_eq!(config.get("eita"), None);
        assert_eq!(config.get("potato"), Some(&"elefante".to_string()));
        assert_eq!(config.get("potato.coiso"), Some(&"fifi".to_string()));
        assert_eq!(config.len(), 2);
    }
}
