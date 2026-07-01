use tracing::*;
use zenoh::Config;

pub fn apply_recorder_defaults(config: &mut Config) {
    const DEFAULTS: &[(&str, &str)] = &[
        // Lowlatency mode bypasses priority queues; the recorder is RX-heavy and
        // does not need QoS so we trade per-message overhead for less routing work.
        ("transport/unicast/lowlatency", "true"),
        ("transport/unicast/qos/enabled", "false"),
        // Batch small messages under back-pressure to reduce syscall/wakeup churn.
        ("transport/link/tx/batching/enabled", "true"),
        ("transport/link/tx/batching/time_limit", "4"),
        // Larger RX buffer for sustained high-bandwidth video + mavlink fan-in.
        ("transport/link/rx/buffer_size", "4194304"),
        // Initialize shared memory at session open (recorder and zenohd are co-located).
        ("transport/shared_memory/enabled", "true"),
        ("transport/shared_memory/mode", r#""init""#),
        (
            "transport/shared_memory/transport_optimization/enabled",
            "true",
        ),
        // Deeper TX queues (max allowed is 16) reduce drops under burst load.
        ("transport/link/tx/queue/size/data", "16"),
        ("transport/link/tx/queue/size/data_high", "16"),
        ("transport/link/tx/queue/size/data_low", "16"),
        ("transport/link/tx/queue/size/background", "8"),
        ("transport/link/tx/queue/size/real_time", "8"),
    ];

    for (key, value) in DEFAULTS {
        if let Err(error) = config.insert_json5(key, value) {
            warn!(%key, %error, "Failed to apply zenoh recorder default");
        }
    }
}
