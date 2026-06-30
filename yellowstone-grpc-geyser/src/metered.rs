use {
    crate::{auth::SubscriptionInfo, metrics},
    http::request::Parts,
    std::{
        collections::HashMap,
        sync::{LazyLock, Mutex},
    },
    yellowstone_grpc_tools::server::tonic::metered::{
        MeteredBandwidthHooks, MeteredBandwidthManager,
    },
};

pub const X_SUBSCRIPTION_ID_HEADER: &str = "x-subscription-id";

pub const UNKNOWN_SUBSCRIBER_ID: &str = "unknown";

static ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID: LazyLock<
    Mutex<HashMap<(String /* subscriber_id */, String /* uri_path */), u64 /* active bodies */>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn increment_active_metered_bodies_for_subscriber_and_path(subscriber_id: &str, uri_path: &str) {
    let mut active_by_subscriber = ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID
        .lock()
        .expect("ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID mutex poisoned");
    active_by_subscriber
        .entry((subscriber_id.to_owned(), uri_path.to_owned()))
        .and_modify(|count| *count += 1)
        .or_insert(1);
}

fn decrement_active_metered_bodies_for_subscriber_and_path(
    subscriber_id: &str,
    uri_path: &str,
) -> bool {
    let mut active_by_subscriber = ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID
        .lock()
        .expect("ACTIVE_METERED_BODIES_PER_SUBSCRIBER_ID mutex poisoned");
    let key = (subscriber_id.to_owned(), uri_path.to_owned());
    if let Some(count) = active_by_subscriber.get_mut(&key) {
        if *count > 1 {
            *count -= 1;
            false
        } else {
            active_by_subscriber.remove(&key);
            metrics::reset_grpc_service_outbound_bytes(subscriber_id, uri_path);
            true
        }
    } else {
        true
    }
}

#[derive(Debug)]
pub struct PrometheusMeteredHooks {
    subscriber_id: String,
    uri_path: String,
}

impl Drop for PrometheusMeteredHooks {
    fn drop(&mut self) {
        if self.subscriber_id.is_empty() && self.uri_path.is_empty() {
            return;
        }
        decrement_active_metered_bodies_for_subscriber_and_path(
            &self.subscriber_id,
            &self.uri_path,
        );
    }
}

impl MeteredBandwidthHooks for PrometheusMeteredHooks {
    fn on_emit_bytes(
        &mut self,
        byte_count: u64,
        _now: std::time::Instant,
        _system_now: std::time::SystemTime,
    ) {
        if self.subscriber_id.is_empty() && self.uri_path.is_empty() {
            return;
        }
        metrics::add_total_traffic_sent(byte_count);
        metrics::add_grpc_service_outbound_bytes(&self.subscriber_id, &self.uri_path, byte_count);
    }
}

#[derive(Clone, Debug)]
pub struct PrometheusMeteredManager;

impl MeteredBandwidthManager for PrometheusMeteredManager {
    type Hooks = PrometheusMeteredHooks;

    fn build_hooks(&self, parts: &Parts) -> Self::Hooks {
        let subscriber_id = parts
            .extensions
            .get::<SubscriptionInfo>()
            .map(|info| info.subscription_id.clone())
            .unwrap_or_else(|| {
                parts
                    .headers
                    .get(X_SUBSCRIPTION_ID_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or(UNKNOWN_SUBSCRIBER_ID)
                    .to_owned()
            });
        let uri_path = parts.uri.to_string();

        increment_active_metered_bodies_for_subscriber_and_path(&subscriber_id, &uri_path);
        PrometheusMeteredHooks {
            subscriber_id,
            uri_path,
        }
    }
}
