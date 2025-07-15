use clap::Parser;
use once_cell::sync::OnceCell;
use std::collections::HashMap;

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

    /// Zenoh configuration key-value pairs. Can be used multiple times.
    /// Format: --zkey key=value
    #[arg(long, value_name = "KEY=VALUE", num_args = 1..)]
    zkey: Vec<String>,
}

/// Constructs our manager, Should be done inside main
pub fn init() {
    let expanded_args = std::env::args()
        .map(|arg| {
            // Fallback to the original if it fails to expand
            shellexpand::env(&arg.clone())
                .inspect_err(|_| {
                    log::warn!("Failed expanding arg: {arg:?}, using the non-expanded instead.")
                })
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

pub fn path_dir_from_arg(arg: &str) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(arg);

    let pathbuf = std::fs::canonicalize(&path)
        .inspect_err(|_| {
            log::warn!("Failed canonicalizing path: {path:?}, using the non-canonized instead.")
        })
        .unwrap_or_else(|_| std::path::PathBuf::from(&path));

    if !pathbuf.exists() {
        log::error!("Path does not exist: {pathbuf:?}");
        std::process::exit(1);
    } else if !pathbuf.is_dir() {
        log::error!("Path is not a directory: {pathbuf:?}");
        std::process::exit(1);
    }

    pathbuf
}

pub fn recorder_path() -> std::path::PathBuf {
    path_dir_from_arg(&args().recorder_path)
}

pub fn schema_path() -> Option<std::path::PathBuf> {
    args()
        .schema_path
        .as_ref()
        .map(|schema_path| path_dir_from_arg(schema_path))
}

/// Returns the zenoh configuration key-value pairs as a HashMap
pub fn zkey_config() -> HashMap<String, String> {
    let mut config = HashMap::new();

    for zkey_arg in &args().zkey {
        if let Some((key, value)) = zkey_arg.split_once('=') {
            config.insert(key.to_string(), value.to_string());
        } else {
            log::warn!("Invalid zkey format: {zkey_arg}, expected KEY=VALUE format");
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
