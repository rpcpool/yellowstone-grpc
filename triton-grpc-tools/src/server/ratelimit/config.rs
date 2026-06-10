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
pub type MethodLimitMap = BTreeMap<String, i64>;

/// Top-level HAProxy rate-limit configuration parsed from `ratelimit-example.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HAproxyRateLimitConfig {
    /// Maximum HAProxy frontend connections for RPC node traffic.
    #[serde(default = "HAproxyRateLimitConfig::default_haproxy_rpcnode_maxconn")]
    pub haproxy_rpcnode_maxconn: u64,
    /// Maximum requests per 10 seconds per single RPC method.
    #[serde(default = "HAproxyRateLimitConfig::default_haproxy_ip_conncur")]
    pub haproxy_ip_conncur: u64,
    /// Mapping from tier name to per-method request caps.
    #[serde(default)]
    pub ratelimit_by_method: BTreeMap<String, MethodLimitMap>,
}

impl HAproxyRateLimitConfig {
    pub const fn default_haproxy_rpcnode_maxconn() -> u64 {
        1000
    }

    pub const fn default_haproxy_ip_conncur() -> u64 {
        250
    }
}
