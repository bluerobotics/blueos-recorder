use std::sync::Arc;

use tracing::*;
use zenoh::{Session, pubsub::Subscriber};

use crate::service::SampleProcessor;

/// Catch-all subscription: records every topic, including parsed `mavlink/**`
/// and wire `mavlink_raw/**`.
pub const DEFAULT_RECORDING_KEYEXPR: &str = "**";

pub fn normalize_keyexpr(pattern: &str) -> String {
    if pattern.contains('*') {
        pattern.to_owned()
    } else {
        format!("{pattern}/**")
    }
}

pub struct SubscriberRegistry {
    #[allow(dead_code)]
    subscribers: Vec<Subscriber<()>>,
}

pub async fn open_callback_subscribers(
    session: &Session,
    patterns: &[String],
    processor: Arc<SampleProcessor>,
) -> SubscriberRegistry {
    let mut subscribers = Vec::with_capacity(patterns.len());

    for pattern in patterns {
        let key_expr = session
            .declare_keyexpr(pattern.as_str())
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to declare zenoh key expression {pattern}: {error}");
            });
        let processor = processor.clone();
        let subscriber = session
            .declare_subscriber(&key_expr)
            .callback(move |sample| processor.handle(sample))
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to declare zenoh subscriber for {pattern}: {error}");
            });
        info!(%pattern, "Declared recording subscriber");
        subscribers.push(subscriber);
    }

    SubscriberRegistry { subscribers }
}
