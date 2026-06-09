use {
    serde::{Deserialize, Serialize},
    std::collections::BTreeMap,
};

/// Tier-keyed integer limits used by HAProxy Lua limiters.
///
/// Values are usually requests/connections per fixed window, and `-1` is
/// commonly interpreted as unlimited in the example configuration.
pub type TierLimitMap = BTreeMap<String, i64>;

/// Tier-keyed per-method rate limits.
///
/// The key is the RPC method path (for example `/geyser.Geyser/Ping`) or a
/// catch-all token such as `grpc`.
pub type MethodLimitMap = BTreeMap<String, u64>;

/// Top-level HAProxy rate-limit configuration parsed from `ratelimit-example.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HaproxyRateLimitConfig {
    /// Maximum HAProxy frontend connections for RPC node traffic.
    pub haproxy_rpcnode_maxconn: u64,
    /// Maximum HAProxy frontend connections reserved for pubsub traffic.
    pub haproxy_rpcnode_maxconn_pubsub: u64,

    /// Ordered list of headers checked to resolve client identity for limiting.
    #[serde(default)]
    pub haproxy_ratelimit_by_header: Vec<String>,

    /// Maximum total requests per 10 seconds per resolved client identity.
    #[serde(default)]
    pub haproxy_ip_maxrps: TierLimitMap,
    /// Maximum requests per 10 seconds per single RPC method.
    #[serde(default)]
    pub haproxy_ip_maxrps_single_rpc: TierLimitMap,
    /// Maximum concurrent active connections per client identity.
    #[serde(default)]
    pub haproxy_ip_conncur: TierLimitMap,
    /// Maximum new connections per 10 seconds per client identity.
    #[serde(default)]
    pub haproxy_ip_connrate: TierLimitMap,
    /// Maximum bytes transferred per 30 seconds per client identity.
    #[serde(default)]
    pub haproxy_ip_datacap: TierLimitMap,
    /// Maximum concurrent pubsub connections per client identity.
    #[serde(default)]
    pub haproxy_ip_pubsub_connections: TierLimitMap,

    /// Mapping from tier name to members/principals belonging to that tier.
    #[serde(default)]
    pub ratelimit_tiers: BTreeMap<String, Vec<String>>,
    /// Mapping from tier name to per-method request caps.
    #[serde(default)]
    pub ratelimit_by_method: BTreeMap<String, MethodLimitMap>,
}
