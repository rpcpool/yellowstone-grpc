use {
    crate::{
        billing::REPORT_INTERVAL,
        plugin::filter::{
            limits::FilterLimits,
            message::{
                WeightScales, DEFAULT_ACCOUNT_DATA_UNIT, DEFAULT_FILTER_NAME_DATA_UNIT,
                DEFAULT_TRANSACTION_DATA_UNIT,
            },
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    bytesize::ByteSize,
    serde::{
        de::{self, DeserializeOwned},
        Deserialize, Deserializer,
    },
    std::{
        collections::HashSet,
        fmt::{self, Write as _},
        fs::read_to_string,
        net::SocketAddr,
        num::NonZeroUsize,
        ops::RangeInclusive,
        path::{Path, PathBuf},
        str::FromStr,
        time::Duration,
    },
    tokio::sync::Semaphore,
    tonic::codec::CompressionEncoding,
    url::Url,
};

pub const DEFAULT_TRITON_AUTH_MAX_CONCURRENT_REQUESTS: NonZeroUsize =
    NonZeroUsize::new(1000).unwrap();

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    #[serde(default)]
    pub log: ConfigLog,
    #[serde(default)]
    pub tokio: ConfigTokio,
    pub grpc: ConfigGrpc,
    #[serde(default)]
    pub prometheus: Option<ConfigPrometheus>,
    /// Collect client filters, processed slot and make it available on prometheus port `/debug_clients`
    #[serde(default)]
    #[deprecated(note = "This option is deprecated and will be removed in future versions.")]
    pub debug_clients_http: bool,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        serde_json::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLog {
    /// Log level.
    #[serde(default = "ConfigLog::default_level")]
    pub level: String,
}

impl Default for ConfigLog {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
        }
    }
}

impl ConfigLog {
    fn default_level() -> String {
        "info".to_owned()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigTokio {
    /// Number of worker threads in Tokio runtime
    #[serde(default)]
    pub worker_threads: Option<usize>,
    /// Threads affinity
    #[serde(default, deserialize_with = "ConfigTokio::deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
}

impl ConfigTokio {
    fn deserialize_affinity<'de, D>(deserializer: D) -> Result<Option<Vec<usize>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<&str>::deserialize(deserializer)? {
            Some(taskset) => parse_taskset(taskset).map(Some).map_err(de::Error::custom),
            None => Ok(None),
        }
    }
}

fn parse_taskset(taskset: &str) -> Result<Vec<usize>, String> {
    let mut set = HashSet::new();
    for taskset2 in taskset.split(',') {
        match taskset2.split_once('-') {
            Some((start, end)) => {
                let start: usize = start
                    .parse()
                    .map_err(|_error| format!("failed to parse {start:?} from {taskset:?}"))?;
                let end: usize = end
                    .parse()
                    .map_err(|_error| format!("failed to parse {end:?} from {taskset:?}"))?;
                if start > end {
                    return Err(format!("invalid interval {taskset2:?} in {taskset:?}"));
                }
                for idx in start..=end {
                    set.insert(idx);
                }
            }
            None => {
                set.insert(
                    taskset2.parse().map_err(|_error| {
                        format!("failed to parse {taskset2:?} from {taskset:?}")
                    })?,
                );
            }
        }
    }

    let mut vec = set.into_iter().collect::<Vec<usize>>();
    vec.sort();

    if let Some(set_max_index) = vec.last().copied() {
        let max_index = crate::util::cpu_core_affinity::get_thread_affinity()
            .map_err(|_err| "failed to get affinity".to_owned())?
            .into_iter()
            .max()
            .unwrap_or(0);

        if set_max_index > max_index {
            return Err(format!("core index must be in the range [0, {max_index}]"));
        }
    }

    Ok(vec)
}

// Default mode: 0o660 (owner+group read/write)
const DEFAULT_UNIX_SOCKET_MODE: u32 = 0o660;

#[derive(Debug, Clone)]
pub enum GrpcAddress {
    Tcp(SocketAddr),
    Unix { path: PathBuf, mode: u32 },
}

#[derive(Debug, Clone)]
pub struct GrpcAddresses {
    pub inner: Vec<GrpcAddress>,
}

impl IntoIterator for GrpcAddresses {
    type Item = GrpcAddress;
    type IntoIter = std::vec::IntoIter<GrpcAddress>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'de> Deserialize<'de> for GrpcAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Support both string: "unix:///path" and object: {"path": "unix:///path", "mode": 504}
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawAddress {
            String(String),
            Object {
                path: String,
                #[serde(default)]
                mode: Option<u32>,
            },
        }

        match RawAddress::deserialize(deserializer)? {
            RawAddress::String(s) => s.parse::<GrpcAddress>().map_err(de::Error::custom),
            RawAddress::Object { path, mode } => {
                match path.parse::<GrpcAddress>().map_err(de::Error::custom)? {
                    GrpcAddress::Unix { path, .. } => Ok(GrpcAddress::Unix {
                        path,
                        mode: mode.unwrap_or(DEFAULT_UNIX_SOCKET_MODE),
                    }),
                    GrpcAddress::Tcp(_) => Err(de::Error::custom(
                        "mode is only valid for unix:// addresses",
                    )),
                }
            }
        }
    }
}

impl FromStr for GrpcAddress {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(path) = s.strip_prefix("unix://") {
            Ok(GrpcAddress::Unix {
                path: PathBuf::from(path),
                mode: DEFAULT_UNIX_SOCKET_MODE,
            })
        } else {
            s.parse::<SocketAddr>()
                .map(GrpcAddress::Tcp)
                .map_err(|e| e.to_string())
        }
    }
}

impl std::fmt::Display for GrpcAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrpcAddress::Tcp(addr) => write!(f, "{addr}"),
            GrpcAddress::Unix { path, .. } => write!(f, "unix://{}", path.display()),
        }
    }
}

impl<'de> Deserialize<'de> for GrpcAddresses {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, SeqAccess, Visitor};

        struct AddressesVisitor;

        impl<'de> Visitor<'de> for AddressesVisitor {
            type Value = GrpcAddresses;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a string, object, or array of addresses")
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                s.parse::<GrpcAddress>()
                    .map(|addr| GrpcAddresses { inner: vec![addr] })
                    .map_err(de::Error::custom)
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                let addr = GrpcAddress::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(GrpcAddresses { inner: vec![addr] })
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut addrs = Vec::new();
                while let Some(addr) = seq.next_element::<GrpcAddress>()? {
                    addrs.push(addr);
                }
                if addrs.is_empty() {
                    return Err(de::Error::custom("at least one address is required"));
                }
                Ok(GrpcAddresses { inner: addrs })
            }
        }

        deserializer.deserialize_any(AddressesVisitor)
    }
}

/// The gRPC server's settings, the `grpc` section of the plugin config. A missing key takes its
/// default, and an unknown key fails to load.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpc {
    /// Multiple addresses of Grpc service.
    pub address: Option<GrpcAddresses>,
    /// TLS config
    pub tls_config: Option<TlsIdentityPair>,
    /// Possible compression options
    #[serde(default)]
    pub compression: ConfigGrpcCompression,
    /// Limits the maximum size of a decoded message, default is 4MiB
    #[serde(
        default = "ConfigGrpc::max_decoding_message_size_default",
        deserialize_with = "deserialize_int_str"
    )]
    pub max_decoding_message_size: usize,
    /// Capacity of the channel used for accounts from snapshot,
    /// on reaching the limit Sender block validator startup.
    #[serde(
        default = "ConfigGrpc::snapshot_plugin_channel_capacity_default",
        deserialize_with = "deserialize_int_str_maybe"
    )]
    pub snapshot_plugin_channel_capacity: Option<usize>,
    /// Capacity of the client channel, applicable only with snapshot
    #[serde(
        default = "ConfigGrpc::snapshot_client_channel_capacity_default",
        deserialize_with = "deserialize_int_str"
    )]
    pub snapshot_client_channel_capacity: usize,
    /// Capacity of the channel per connection
    #[serde(
        default = "ConfigGrpc::channel_capacity_default",
        deserialize_with = "deserialize_int_str"
    )]
    pub channel_capacity: usize,
    /// Capacity of the processed broadcast ring, defaults to `channel_capacity`
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub processed_broadcast_capacity: Option<usize>,
    /// Capacity of the confirmed broadcast ring, defaults to `channel_capacity`
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub confirmed_broadcast_capacity: Option<usize>,
    /// Capacity of the finalized broadcast ring, defaults to `channel_capacity`
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub finalized_broadcast_capacity: Option<usize>,
    #[serde(default = "ConfigGrpc::default_contact_info_channel_capacity")]
    pub contact_info_channel_capacity: usize,
    /// Per-queue capacities and update weights. Unset fields fall back to the settings above.
    #[serde(default)]
    pub capacities: ConfigGrpcCapacities,
    /// How long a `from_slot` replay waits for the client to free queue room before it is
    /// disconnected. `null` waits forever.
    #[serde(
        default = "ConfigGrpc::default_client_unresponsive_timeout",
        with = "humantime_serde"
    )]
    pub client_unresponsive_timeout: Option<Duration>,
    /// Concurrency limit for unary requests
    #[serde(
        default = "ConfigGrpc::unary_concurrency_limit_default",
        deserialize_with = "deserialize_int_str"
    )]
    pub unary_concurrency_limit: usize,
    /// Maximum concurrent subscriptions allowed per subscriber ID.
    /// Subscribers are identified by the `x-subscription-id` header,
    /// falling back to the remote IP address.
    #[serde(
        default = "ConfigGrpc::subscription_limit_default",
        deserialize_with = "deserialize_int_str"
    )]
    pub subscription_limit: NonZeroUsize,
    /// When false (default), exceeding the subscription limit only logs
    /// and emits metrics without rejecting the connection. Set to true
    /// to start rejecting with RESOURCE_EX2AUSTED.
    #[serde(default)]
    pub subscription_limit_enforce: bool,
    /// Enable/disable unary methods
    #[serde(default)]
    pub unary_disabled: bool,
    /// Limits for possible filters
    #[serde(default, alias = "filters")]
    pub filter_limits: FilterLimits,
    /// x_token to enforce on connections
    pub x_token: Option<String>,
    /// Filter name size limit
    #[serde(default = "ConfigGrpc::default_filter_name_size_limit")]
    pub filter_name_size_limit: usize,
    /// Number of cached filter names before doing cleanup
    #[serde(default = "ConfigGrpc::default_filter_names_size_limit")]
    pub filter_names_size_limit: usize,
    /// Cleanup interval once filter names reached `filter_names_size_limit`
    #[serde(
        default = "ConfigGrpc::default_filter_names_cleanup_interval",
        with = "humantime_serde"
    )]
    pub filter_names_cleanup_interval: Duration,
    /// The shortest time between two filter changes applied for one client. A later change sent
    /// sooner waits, and the newest waiting one applies. A change sent during a `from_slot`
    /// replay or a startup snapshot also waits until it ends. The first request that asks for
    /// something, and pings, apply at once. `0s` applies every change at once, or at the end of
    /// the replay or snapshot it was sent during.
    #[serde(
        default = "ConfigGrpc::default_filter_apply_interval",
        with = "humantime_serde"
    )]
    pub filter_apply_interval: Duration,
    /// The shortest time between two slices of an owner's index work, which claims and releases
    /// index keys for index changes. `0s` lets a slice run in every pass.
    #[serde(
        default = "ConfigGrpc::default_route_index_interval",
        with = "humantime_serde"
    )]
    pub route_index_interval: Duration,
    /// The longest one slice of index work runs, plus one step of 64 index keys. `0s` runs one
    /// step per slice.
    #[serde(
        default = "ConfigGrpc::default_route_index_budget",
        with = "humantime_serde"
    )]
    pub route_index_budget: Duration,
    /// Number of slots stored for re-broadcast (replay)
    #[serde(
        default = "ConfigGrpc::default_replay_stored_slots",
        deserialize_with = "deserialize_int_str"
    )]
    pub replay_stored_slots: u64,
    #[serde(default)]
    pub server_http2_adaptive_window: Option<bool>,
    #[serde(default, with = "humantime_serde")]
    pub server_http2_keepalive_interval: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub server_http2_keepalive_timeout: Option<Duration>,
    #[serde(default)]
    pub server_initial_connection_window_size: Option<u32>,
    #[serde(default)]
    pub server_initial_stream_window_size: Option<u32>,

    ///
    /// After how many bytes a TCP connection should update its traffic metrics for prometheus.
    /// If not set, it uses the default 64KiB threshold.
    ///
    #[serde(default)]
    pub traffic_reporting_byte_threhsold: Option<ByteSize>,

    ///
    /// Optional directory to load TLS certificates for gRPC server. If set, the server will start in TLS mode and load certificates from the provided directory.
    ///
    /// This option is mutually exclusive with `tls_config`. If both are set, the server will prioritize `cert_dir`.
    #[serde(default)]
    pub cert_dir: Option<PathBuf>,

    ///
    /// Maximum concurrent connections allowed per remote IP address. If not set, there is no limit enforced at the transport layer,
    /// but the server may still enforce limits at the application layer (e.g. subscription_limit).
    ///
    /// Defaults to 2^64 - 1 (effectively no limit).
    ///
    #[serde(
        default = "ConfigGrpc::default_max_ip_conncur",
        deserialize_with = "deserialize_int_str"
    )]
    pub ip_conncur_rate_limit: u64,

    #[serde(default)]
    pub listen: Option<Vec<ListenConfig>>,

    /// The data plane settings, parsed leniently: a bad entry warns and keeps its default.
    #[serde(default)]
    pub data_plane: ConfigGrpcDataPlane,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum GrpcTlsConfig {
    /// A pair of private-key and cert file paths
    IdentityPair {
        identity: TlsIdentityPair,
        #[serde(default)]
        watch_file: bool,
    },
    /// HAPROXY-like cert directory with `*.pem` files containing the certs and private keys.
    CertDir {
        cert_dir: PathBuf,
        #[serde(default)]
        watch_file: bool,
    },
}

///
/// Configuration for HTTP-backed authentication for subscription requests.
///
/// Query a remote HTTP service to validate incoming subscription requests and retrieve associated metadata.
///
#[derive(Debug, Clone, Deserialize)]
pub struct HttpBackedAuthConfig {
    ///
    /// The URL of the subscription resolver service.
    /// The gRPC server will call this service to validate incoming subscription requests and retrieve associated metadata.
    ///
    pub subscription_resolver_url: Url,

    ///
    /// The TTL for caching subscription resolution results.
    ///
    /// By defaults, there is no caching and the gRPC server will call the subscription resolver for every incoming subscription request. Setting a TTL enables caching of resolver results, which can help reduce latency and load on the resolver service for frequently seen tokens and hosts.
    #[serde(default, with = "humantime_serde")]
    pub subscription_resolution_cache_ttl: Option<Duration>,

    ///
    /// The maximum number of concurrent authentication requests allowed to the subscription resolver service.
    /// By default, it is set to 1000,
    #[serde(
        default = "HttpBackedAuthConfig::default_max_concurrent_auth_requests",
        deserialize_with = "deserialize_int_str"
    )]
    pub max_concurrent_auth_requests: NonZeroUsize,

    #[serde(default = "HttpBackedAuthConfig::default_forwarded_headers")]
    pub forwarded_headers: Vec<String>,

    #[serde(default)]
    pub ratelimit: Option<RatelimitConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RatelimitConfig {
    ///
    /// The maximum number of hits allowed within the specified time window.
    /// A negative value indicates that there is no limit, and all requests will be allowed.
    ///
    #[serde(
        default = "RatelimitConfig::default_max_hits",
        deserialize_with = "deserialize_int_str"
    )]
    pub default_max_hits: i32,

    ///
    /// Window duration for the rate limit. The number of hits is counted within this time window.
    ///
    /// On the server, this is a rolling window using token buckets.
    ///
    /// # Rolling window details
    ///
    ///
    /// Suppose you allow 1000 request within a 10 seconds rolling window,
    /// every second we refill 100 tokens to the bucket.
    ///
    /// If a client sends 1000 requests before the first second, the bucket will be empty and the next request will be rejected.
    /// It must wait for the next second to get 100 tokens refilled, and then it can send 100 requests before the next second, and so on.
    #[serde(default = "RatelimitConfig::default_window", with = "humantime_serde")]
    pub window: Duration,
}

impl Default for RatelimitConfig {
    fn default() -> Self {
        Self {
            default_max_hits: RatelimitConfig::default_max_hits(),
            window: RatelimitConfig::default_window(),
        }
    }
}

impl RatelimitConfig {
    pub const fn default_max_hits() -> i32 {
        1000
    }

    pub const fn default_window() -> Duration {
        Duration::from_secs(10)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FileBackedAuthConfig {
    pub subscription_resolver_path: PathBuf,

    #[serde(default)]
    pub ratelimit: Option<RatelimitConfig>,
}

impl HttpBackedAuthConfig {
    pub fn default_forwarded_headers() -> Vec<String> {
        vec!["x-token".to_string()]
    }

    pub const fn default_max_concurrent_auth_requests() -> NonZeroUsize {
        DEFAULT_TRITON_AUTH_MAX_CONCURRENT_REQUESTS
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrustedMetadataAuthConfig {
    // Reserved for trusted-metadata-specific options.
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(flatten)]
    pub kind: AuthKind,
    #[serde(default = "AuthConfig::default_ratelimit")]
    pub ratelimit: Option<RatelimitConfig>,
    #[serde(default)]
    pub billing: Option<BillingConfig>,
}

///
/// Configuration for billing events. This is used to configure the billing system for the gRPC server.
///
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BillingConfig {
    /// Billing events are sent to a remote HTTP service.
    Http(HttpBackedBillingConfig),
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpBackedBillingConfig {
    ///
    /// The URL of the billing endpoint. The gRPC server will send billing events to this endpoint.
    pub billing_endpoint_url: Url,
    ///
    /// The interval at which the gRPC server will report billing events to the billing endpoint.
    #[serde(
        default = "HttpBackedBillingConfig::default_report_interval",
        with = "humantime_serde"
    )]
    pub report_interval: Duration,
}

impl HttpBackedBillingConfig {
    pub const fn default_report_interval() -> Duration {
        REPORT_INTERVAL
    }
}

impl AuthConfig {
    pub fn default_ratelimit() -> Option<RatelimitConfig> {
        Some(RatelimitConfig::default())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum AuthKind {
    Http(HttpBackedAuthConfig),
    File(FileBackedAuthConfig),
    ///
    /// Trusts the `x-subscription-id` header and does not perform any authentication or authorization checks and apply default rate limits.
    #[serde(rename = "trusted-metadata")]
    TrustedMetadata(TrustedMetadataAuthConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenConfig {
    pub address: GrpcAddress,
    pub tls: Option<GrpcTlsConfig>,
    pub auth: Option<AuthConfig>,
}

impl ConfigGrpc {
    const fn default_max_ip_conncur() -> u64 {
        u64::MAX
    }

    const fn max_decoding_message_size_default() -> usize {
        4 * 1024 * 1024
    }

    const fn snapshot_plugin_channel_capacity_default() -> Option<usize> {
        None
    }

    const fn snapshot_client_channel_capacity_default() -> usize {
        50_000_000
    }

    const fn channel_capacity_default() -> usize {
        250_000
    }

    const fn unary_concurrency_limit_default() -> usize {
        Semaphore::MAX_PERMITS
    }

    const fn subscription_limit_default() -> NonZeroUsize {
        NonZeroUsize::new(1000).unwrap()
    }

    const fn default_filter_name_size_limit() -> usize {
        128
    }

    const fn default_filter_names_size_limit() -> usize {
        4_096
    }

    const fn default_filter_names_cleanup_interval() -> Duration {
        Duration::from_secs(1)
    }

    /// The default [`ConfigGrpc::filter_apply_interval`].
    const fn default_filter_apply_interval() -> Duration {
        Duration::from_secs(5)
    }

    /// The default [`ConfigGrpc::route_index_interval`].
    const fn default_route_index_interval() -> Duration {
        Duration::from_millis(1)
    }

    /// The default [`ConfigGrpc::route_index_budget`].
    const fn default_route_index_budget() -> Duration {
        Duration::from_micros(100)
    }

    const fn default_replay_stored_slots() -> u64 {
        150
    }

    const fn default_contact_info_channel_capacity() -> usize {
        100_000
    }

    const fn default_client_unresponsive_timeout() -> Option<Duration> {
        Some(Duration::from_secs(60))
    }

    /// Resolves every queue capacity and update weight, applying the fallbacks.
    pub fn resolved_capacities(&self) -> GrpcCapacities {
        let capacities = &self.capacities;
        let channel_capacity = self.channel_capacity;
        let scale = |value: Option<u32>| value.unwrap_or(1).max(1);
        GrpcCapacities {
            geyser_subscriber_weight: capacities
                .geyser_subscriber_weight_capacity
                .unwrap_or(channel_capacity),
            deshred_subscriber: capacities
                .deshred_channel_capacity
                .unwrap_or(channel_capacity),
            contact_info_subscriber: capacities
                .contact_info_channel_capacity
                .unwrap_or(channel_capacity),
            processed_broadcast: capacities
                .internal_processed_channel_capacity
                .or(self.processed_broadcast_capacity)
                .unwrap_or(channel_capacity),
            confirmed_broadcast: capacities
                .internal_confirmed_channel_capacity
                .or(self.confirmed_broadcast_capacity)
                .unwrap_or(channel_capacity),
            finalized_broadcast: capacities
                .internal_finalized_channel_capacity
                .or(self.finalized_broadcast_capacity)
                .unwrap_or(channel_capacity),
            weight_scales: WeightScales {
                account: scale(capacities.weight_scale_account),
                transaction: scale(capacities.weight_scale_transaction),
                entry: scale(capacities.weight_scale_entry),
                slot: scale(capacities.weight_scale_slot),
                block_footer: scale(capacities.weight_scale_block_footer),
                block: scale(capacities.weight_scale_block),
                account_data_unit: capacities
                    .weight_account_data_unit
                    .unwrap_or(DEFAULT_ACCOUNT_DATA_UNIT),
                transaction_data_unit: capacities
                    .weight_transaction_data_unit
                    .unwrap_or(DEFAULT_TRANSACTION_DATA_UNIT),
                filter_name_data_unit: capacities
                    .weight_filter_name_data_unit
                    .unwrap_or(DEFAULT_FILTER_NAME_DATA_UNIT),
            },
        }
    }
}

/// Per-queue capacities and update weights. Every field is optional.
#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcCapacities {
    /// Weight budget of a Subscribe client queue. Defaults to `channel_capacity`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub geyser_subscriber_weight_capacity: Option<usize>,
    /// Capacity of a SubscribeDeshred client queue, in updates. Defaults to `channel_capacity`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub deshred_channel_capacity: Option<usize>,
    /// Capacity of a contact info (gossip) client queue, in updates. Defaults to
    /// `channel_capacity`. The shared gossip ring stays `grpc.contact_info_channel_capacity`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub contact_info_channel_capacity: Option<usize>,
    /// Capacity of the processed broadcast ring. Defaults to `processed_broadcast_capacity`,
    /// then `channel_capacity`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub internal_processed_channel_capacity: Option<usize>,
    /// Capacity of the confirmed broadcast ring. Defaults to `confirmed_broadcast_capacity`,
    /// then `channel_capacity`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub internal_confirmed_channel_capacity: Option<usize>,
    /// Capacity of the finalized broadcast ring. Defaults to `finalized_broadcast_capacity`,
    /// then `channel_capacity`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub internal_finalized_channel_capacity: Option<usize>,
    /// Weight of an account update in a Subscribe queue. Defaults to `1`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_scale_account: Option<u32>,
    /// Weight of a transaction update in a Subscribe queue. Defaults to `1`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_scale_transaction: Option<u32>,
    /// Weight of an entry update in a Subscribe queue. Defaults to `1`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_scale_entry: Option<u32>,
    /// Weight of a slot update in a Subscribe queue. Defaults to `1`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_scale_slot: Option<u32>,
    /// Weight of a block footer update in a Subscribe queue. Defaults to `1`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_scale_block_footer: Option<u32>,
    /// Multiplier on a block's weight of `1` plus its transactions and accounts. Defaults to `1`.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_scale_block: Option<u32>,
    /// Bytes of account data that add one weight unit to an account, in account updates and in
    /// blocks. Defaults to `4096`; `0` counts every account as one unit.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_account_data_unit: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_transaction_data_unit: Option<usize>,
    /// Bytes of filter names that add one unscaled weight unit to a Subscribe queue update.
    /// Defaults to `1024`; `0` turns it off.
    #[serde(default, deserialize_with = "deserialize_int_str_maybe")]
    pub weight_filter_name_data_unit: Option<usize>,
}

/// Queue capacities and update weights after [`ConfigGrpc::resolved_capacities`] applies the
/// fallbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcCapacities {
    pub geyser_subscriber_weight: usize,
    pub deshred_subscriber: usize,
    pub contact_info_subscriber: usize,
    pub processed_broadcast: usize,
    pub confirmed_broadcast: usize,
    pub finalized_broadcast: usize,
    pub weight_scales: WeightScales,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsIdentityPair {
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcCompression {
    #[serde(
        deserialize_with = "ConfigGrpcCompression::deserialize_compression",
        default = "ConfigGrpcCompression::default_compression"
    )]
    pub accept: Vec<CompressionEncoding>,
    #[serde(
        deserialize_with = "ConfigGrpcCompression::deserialize_compression",
        default = "ConfigGrpcCompression::default_compression"
    )]
    pub send: Vec<CompressionEncoding>,
}

impl Default for ConfigGrpcCompression {
    fn default() -> Self {
        Self {
            accept: Self::default_compression(),
            send: Self::default_compression(),
        }
    }
}

impl ConfigGrpcCompression {
    fn deserialize_compression<'de, D>(
        deserializer: D,
    ) -> Result<Vec<CompressionEncoding>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<&str>::deserialize(deserializer)?
            .into_iter()
            .map(|value| match value {
                "gzip" => Ok(CompressionEncoding::Gzip),
                "zstd" => Ok(CompressionEncoding::Zstd),
                value => Err(de::Error::custom(format!(
                    "Unknown compression format: {value}"
                ))),
            })
            .collect::<Result<_, _>>()
    }

    fn default_compression() -> Vec<CompressionEncoding> {
        vec![CompressionEncoding::Gzip, CompressionEncoding::Zstd]
    }
}

/// The dotted path of the `grpc.data_plane` object, the prefix of its warnings' key paths.
const DATA_PLANE_KEY_PATH: &str = "grpc.data_plane";

/// The gzip levels flate2 and zlib accept.
const GZIP_LEVEL_RANGE: RangeInclusive<u32> = 0..=9;

/// The zstd levels the zstd library accepts, from its fastest negative level to its strongest.
const ZSTD_LEVEL_RANGE: RangeInclusive<i32> = -131_072..=22;

/// The `grpc.data_plane` object. Unlike the rest of the config it is lenient: a bad entry adds
/// one warning to `load_warnings` and keeps its default, so a rollback still loads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigGrpcDataPlane {
    /// `subscribe_body`: what writes Subscribe responses.
    pub subscribe_body: SubscribeBodyKind,
    /// `compression`: how Subscribe responses are compressed.
    pub compression: ConfigGrpcDataPlaneCompression,
    /// `threads`: the data plane's own threads.
    pub threads: ConfigGrpcDataPlaneThreads,
    /// `staging`: how the plugin's Subscribe body stages frames.
    pub staging: ConfigGrpcDataPlaneStaging,
    /// `telemetry`: metrics and log lines beyond the baseline set.
    pub telemetry: ConfigGrpcDataPlaneTelemetry,
    /// `extensions`: sections for an embedding crate, by name, as the file holds them.
    pub extensions: serde_json::Map<String, serde_json::Value>,
    /// One warning for each entry that parsing ignored or replaced with its default.
    pub load_warnings: Vec<DataPlaneConfigWarning>,
}

impl<'de> Deserialize<'de> for ConfigGrpcDataPlane {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_json::Value::deserialize(deserializer).map(Self::from_json_value)
    }
}

impl ConfigGrpcDataPlane {
    /// Parses the object leniently. It never fails: every problem becomes a warning in
    /// `load_warnings`.
    pub fn from_json_value(data_plane_value: serde_json::Value) -> Self {
        let mut load_warnings = Vec::new();
        let mut data_plane = Self::default();
        if let Some(mut data_plane_section) = LenientSection::open_section(
            DATA_PLANE_KEY_PATH.to_owned(),
            data_plane_value,
            &mut load_warnings,
        ) {
            data_plane_section.read_into(
                &mut data_plane.subscribe_body,
                "subscribe_body",
                &mut load_warnings,
                parse_json_value,
            );
            if let Some(compression_section) =
                data_plane_section.take_section("compression", &mut load_warnings)
            {
                data_plane
                    .compression
                    .read_from(compression_section, &mut load_warnings);
            }
            if let Some(threads_section) =
                data_plane_section.take_section("threads", &mut load_warnings)
            {
                data_plane
                    .threads
                    .read_from(threads_section, &mut load_warnings);
            }
            if let Some(mut staging_section) =
                data_plane_section.take_section("staging", &mut load_warnings)
            {
                staging_section.read_into(
                    &mut data_plane.staging.copy_threshold_bytes,
                    "copy_threshold_bytes",
                    &mut load_warnings,
                    parse_byte_size_value,
                );
                staging_section.warn_unknown_keys(&mut load_warnings);
            }
            if let Some(telemetry_section) =
                data_plane_section.take_section("telemetry", &mut load_warnings)
            {
                data_plane
                    .telemetry
                    .read_from(telemetry_section, &mut load_warnings);
            }
            data_plane_section.read_into(
                &mut data_plane.extensions,
                "extensions",
                &mut load_warnings,
                parse_json_value,
            );
            data_plane_section.warn_unknown_keys(&mut load_warnings);
        }
        data_plane.load_warnings = load_warnings;
        data_plane
    }

    /// One warning for each `extensions` section whose name is not in `claimed_extension_names`.
    pub fn unclaimed_extension_warnings(
        &self,
        claimed_extension_names: &[&str],
    ) -> Vec<DataPlaneConfigWarning> {
        self.extensions
            .keys()
            .filter(|extension_name| !claimed_extension_names.contains(&extension_name.as_str()))
            .map(|extension_name| DataPlaneConfigWarning {
                key_path: format!("{DATA_PLANE_KEY_PATH}.extensions.{extension_name}"),
                config_problem: DataPlaneConfigProblem::UnclaimedExtension,
            })
            .collect()
    }

    /// One warning for each compression key that overrides the build's default. Only
    /// `config-check --strict` reports them: rendered configs leave these keys unset.
    pub fn compression_override_warnings(&self) -> Vec<DataPlaneConfigWarning> {
        self.compression
            .compression_overrides
            .set_key_names()
            .into_iter()
            .map(|key_name| DataPlaneConfigWarning {
                key_path: format!("{DATA_PLANE_KEY_PATH}.compression.{key_name}"),
                config_problem: DataPlaneConfigProblem::CompressionOverrideSet,
            })
            .collect()
    }

    /// The one line that shows the settings in force, with each compression key over
    /// `build_compression_defaults`.
    pub fn effective_config_line(&self, build_compression_defaults: CompressionDefaults) -> String {
        let effective_compression =
            build_compression_defaults.with_overrides(self.compression.compression_overrides);
        let mut effective_config_line = format!(
            "{DATA_PLANE_KEY_PATH} in effect: subscribe_body={}",
            self.subscribe_body.config_value()
        );
        let _ = write!(
            effective_config_line,
            " compression.mode={} compression.gzip_level={} compression.zstd_level={} \
             compression.prefer_zstd={} compression.zstd_frame={} \
             compression.keys_set_in_config=[{}] compression.group_slots={} \
             compression.inline_max_bytes={} compression.big_body_bytes={} \
             compression.processed_pool_threads={} compression.confirmed_pool_threads={} \
             compression.waiter_backstop={:?} compression.retained_budget_bytes={} \
             compression.block_shape_budget_bytes={}",
            self.compression.mode.config_value(),
            effective_compression.gzip_level,
            effective_compression.zstd_level,
            effective_compression.prefer_zstd,
            effective_compression.zstd_frame_form.config_value(),
            self.compression
                .compression_overrides
                .set_key_names()
                .join(","),
            self.compression.group_slots,
            self.compression.inline_max_bytes,
            self.compression.big_body_bytes,
            self.compression.processed_pool_threads,
            self.compression.confirmed_pool_threads,
            self.compression.waiter_backstop,
            self.compression.retained_budget_bytes,
            self.compression.block_shape_budget_bytes,
        );
        let _ = write!(
            effective_config_line,
            " threads.filter_compile_threads={} threads.control_cpus={} \
             threads.compressor_cpus={} staging.copy_threshold_bytes={} \
             telemetry.extended_metrics={} telemetry.summary_interval={:?} extensions=[{}]",
            self.threads.filter_compile_threads,
            taskset_config_value(self.threads.control_cpus.as_deref()),
            taskset_config_value(self.threads.compressor_cpus.as_deref()),
            self.staging.copy_threshold_bytes,
            self.telemetry.extended_metrics,
            self.telemetry.summary_interval,
            self.extensions
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(","),
        );
        effective_config_line
    }

    /// Logs every load warning and unclaimed extension section, then the effective config line.
    /// The plugin calls it once at load, after its logger is set up.
    pub fn log_at_load(
        &self,
        build_compression_defaults: CompressionDefaults,
        claimed_extension_names: &[&str],
    ) {
        for load_warning in &self.load_warnings {
            log::warn!("{load_warning}");
        }
        for unclaimed_extension_warning in
            self.unclaimed_extension_warnings(claimed_extension_names)
        {
            log::warn!("{unclaimed_extension_warning}");
        }
        log::info!("{}", self.effective_config_line(build_compression_defaults));
    }
}

/// `grpc.data_plane.subscribe_body`: tonic's encoder or the plugin's own Subscribe body.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscribeBodyKind {
    #[default]
    Tonic,
    Plugin,
}

impl SubscribeBodyKind {
    /// The config value that selects this kind.
    pub const fn config_value(self) -> &'static str {
        match self {
            Self::Tonic => "tonic",
            Self::Plugin => "plugin",
        }
    }
}

/// `grpc.data_plane.compression.mode`: compress once per group of members, or per member.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionSharingMode {
    #[default]
    Shared,
    PerStream,
}

impl CompressionSharingMode {
    /// The config value that selects this mode.
    pub const fn config_value(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::PerStream => "per_stream",
        }
    }
}

/// The zstd frame form. `Streaming` is what tonic's encoder writes, with no content size;
/// `OneShot` is what a one-shot compressor writes, with the content size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZstdFrameForm {
    Streaming,
    OneShot,
}

impl ZstdFrameForm {
    /// The `compression.zstd_frame` value that selects this form.
    pub const fn config_value(self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::OneShot => "one_shot",
        }
    }
}

/// A build's compression defaults. Each of the four compression keys overrides one field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionDefaults {
    pub gzip_level: u32,
    pub zstd_level: i32,
    /// Send a Subscribe stream in zstd whenever the client accepts it, instead of the client's
    /// first choice. Other methods keep the negotiation of the tonic the build links.
    pub prefer_zstd: bool,
    pub zstd_frame_form: ZstdFrameForm,
}

impl CompressionDefaults {
    /// The public build's defaults, the calls tonic 0.14.9 makes: gzip 6, zstd 3, the client's
    /// order and streaming zstd frames.
    pub const PUBLIC: Self = Self {
        gzip_level: 6,
        zstd_level: 3,
        prefer_zstd: false,
        zstd_frame_form: ZstdFrameForm::Streaming,
    };

    /// These defaults, with each field the config sets taken from `compression_overrides`.
    pub const fn with_overrides(self, compression_overrides: CompressionOverrides) -> Self {
        Self {
            gzip_level: match compression_overrides.gzip_level {
                Some(gzip_level) => gzip_level,
                None => self.gzip_level,
            },
            zstd_level: match compression_overrides.zstd_level {
                Some(zstd_level) => zstd_level,
                None => self.zstd_level,
            },
            prefer_zstd: match compression_overrides.prefer_zstd {
                Some(prefer_zstd) => prefer_zstd,
                None => self.prefer_zstd,
            },
            zstd_frame_form: match compression_overrides.zstd_frame_form {
                Some(zstd_frame_form) => zstd_frame_form,
                None => self.zstd_frame_form,
            },
        }
    }
}

/// The keys `compression.gzip_level`, `zstd_level`, `prefer_zstd` and `zstd_frame`. Each is
/// unset by default, which keeps the build's [`CompressionDefaults`] field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompressionOverrides {
    pub gzip_level: Option<u32>,
    pub zstd_level: Option<i32>,
    pub prefer_zstd: Option<bool>,
    pub zstd_frame_form: Option<ZstdFrameForm>,
}

impl CompressionOverrides {
    /// The names of the keys the config sets, in the config's spelling.
    pub fn set_key_names(self) -> Vec<&'static str> {
        [
            ("gzip_level", self.gzip_level.is_some()),
            ("zstd_level", self.zstd_level.is_some()),
            ("prefer_zstd", self.prefer_zstd.is_some()),
            ("zstd_frame", self.zstd_frame_form.is_some()),
        ]
        .into_iter()
        .filter_map(|(key_name, key_is_set)| key_is_set.then_some(key_name))
        .collect()
    }
}

/// `grpc.data_plane.compression`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigGrpcDataPlaneCompression {
    /// `mode`: compress once per group of members, or per member.
    pub mode: CompressionSharingMode,
    /// `gzip_level`, `zstd_level`, `prefer_zstd` and `zstd_frame`.
    pub compression_overrides: CompressionOverrides,
    /// `group_slots`: compression groups per shared body; a member of a further group compresses
    /// its own copy.
    pub group_slots: usize,
    /// `inline_max_bytes`: a processed body above it compresses on the processed pool, if any.
    pub inline_max_bytes: usize,
    /// `big_body_bytes`: a confirmed or finalized body above it compresses on its pool, if any.
    pub big_body_bytes: usize,
    /// `processed_pool_threads`: the processed compressor pool's size; 0 compresses inline.
    pub processed_pool_threads: usize,
    /// `confirmed_pool_threads`: the confirmed and finalized pool's size; 0 compresses inline.
    pub confirmed_pool_threads: usize,
    /// `waiter_backstop`: when a confirmed or finalized waiter without a waker cell checks again.
    pub waiter_backstop: Duration,
    /// `retained_budget_bytes`: the cap on retained compressed bodies.
    pub retained_budget_bytes: usize,
    /// `block_shape_budget_bytes`: the cap on retained block shapes.
    pub block_shape_budget_bytes: usize,
}

impl Default for ConfigGrpcDataPlaneCompression {
    fn default() -> Self {
        Self {
            mode: CompressionSharingMode::Shared,
            compression_overrides: CompressionOverrides::default(),
            group_slots: 4,
            inline_max_bytes: 64 * 1024,
            big_body_bytes: 1024 * 1024,
            processed_pool_threads: 0,
            confirmed_pool_threads: 0,
            waiter_backstop: Duration::from_millis(1),
            retained_budget_bytes: 512 * 1024 * 1024,
            block_shape_budget_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl ConfigGrpcDataPlaneCompression {
    fn read_from(
        &mut self,
        mut compression_section: LenientSection,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
    ) {
        compression_section.read_into(&mut self.mode, "mode", load_warnings, parse_json_value);
        self.compression_overrides.gzip_level =
            compression_section.take_optional("gzip_level", load_warnings, |level_value| {
                parse_integer_in_range(level_value, GZIP_LEVEL_RANGE)
            });
        self.compression_overrides.zstd_level =
            compression_section.take_optional("zstd_level", load_warnings, |level_value| {
                parse_integer_in_range(level_value, ZSTD_LEVEL_RANGE)
            });
        self.compression_overrides.prefer_zstd =
            compression_section.take_optional("prefer_zstd", load_warnings, parse_json_value);
        self.compression_overrides.zstd_frame_form =
            compression_section.take_optional("zstd_frame", load_warnings, parse_json_value);
        for (config_field, key_name) in [
            (&mut self.group_slots, "group_slots"),
            (&mut self.processed_pool_threads, "processed_pool_threads"),
            (&mut self.confirmed_pool_threads, "confirmed_pool_threads"),
        ] {
            compression_section.read_into(
                config_field,
                key_name,
                load_warnings,
                parse_integer_value,
            );
        }
        for (config_field, key_name) in [
            (&mut self.inline_max_bytes, "inline_max_bytes"),
            (&mut self.big_body_bytes, "big_body_bytes"),
            (&mut self.retained_budget_bytes, "retained_budget_bytes"),
            (
                &mut self.block_shape_budget_bytes,
                "block_shape_budget_bytes",
            ),
        ] {
            compression_section.read_into(
                config_field,
                key_name,
                load_warnings,
                parse_byte_size_value,
            );
        }
        compression_section.read_into(
            &mut self.waiter_backstop,
            "waiter_backstop",
            load_warnings,
            parse_duration_value,
        );
        compression_section.warn_unknown_keys(load_warnings);
    }
}

/// `grpc.data_plane.threads`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigGrpcDataPlaneThreads {
    /// `filter_compile_threads`: the filter compile lane's size; 0 compiles inline.
    pub filter_compile_threads: usize,
    /// `control_cpus`: a taskset for the control and compile lane threads.
    pub control_cpus: Option<Vec<usize>>,
    /// `compressor_cpus`: a taskset for the compressor pool threads.
    pub compressor_cpus: Option<Vec<usize>>,
}

impl ConfigGrpcDataPlaneThreads {
    fn read_from(
        &mut self,
        mut threads_section: LenientSection,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
    ) {
        threads_section.read_into(
            &mut self.filter_compile_threads,
            "filter_compile_threads",
            load_warnings,
            parse_integer_value,
        );
        self.control_cpus =
            threads_section.take_optional("control_cpus", load_warnings, parse_taskset_value);
        self.compressor_cpus =
            threads_section.take_optional("compressor_cpus", load_warnings, parse_taskset_value);
        threads_section.warn_unknown_keys(load_warnings);
    }
}

/// `grpc.data_plane.staging`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigGrpcDataPlaneStaging {
    /// `copy_threshold_bytes`: the plugin's body copies smaller frames and references larger.
    pub copy_threshold_bytes: usize,
}

impl Default for ConfigGrpcDataPlaneStaging {
    fn default() -> Self {
        Self {
            copy_threshold_bytes: 16 * 1024,
        }
    }
}

/// `grpc.data_plane.telemetry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigGrpcDataPlaneTelemetry {
    /// `extended_metrics`: register the metric families the data plane adds.
    pub extended_metrics: bool,
    /// `summary_interval`: the period of the safety summary line; zero turns it off.
    pub summary_interval: Duration,
}

impl Default for ConfigGrpcDataPlaneTelemetry {
    fn default() -> Self {
        Self {
            extended_metrics: false,
            summary_interval: Duration::from_secs(60),
        }
    }
}

impl ConfigGrpcDataPlaneTelemetry {
    fn read_from(
        &mut self,
        mut telemetry_section: LenientSection,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
    ) {
        telemetry_section.read_into(
            &mut self.extended_metrics,
            "extended_metrics",
            load_warnings,
            parse_json_value,
        );
        telemetry_section.read_into(
            &mut self.summary_interval,
            "summary_interval",
            load_warnings,
            parse_duration_value,
        );
        telemetry_section.warn_unknown_keys(load_warnings);
    }
}

/// An entry of `grpc.data_plane` that parsing ignored or replaced with its default, or that a
/// check reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPlaneConfigWarning {
    /// The entry's dotted path, such as `grpc.data_plane.compression.mode`.
    pub key_path: String,
    pub config_problem: DataPlaneConfigProblem,
}

/// What is wrong with a [`DataPlaneConfigWarning`]'s entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataPlaneConfigProblem {
    /// This build does not know the key, so it ignores it.
    UnknownKey,
    /// The value has the wrong type or is not one the key takes, so the default applies.
    InvalidValue { rejection_reason: String },
    /// No extension of this build claims the `extensions` section, so it is ignored.
    UnclaimedExtension,
    /// A compression key overrides the build's default.
    CompressionOverrideSet,
}

impl fmt::Display for DataPlaneConfigWarning {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key_path = &self.key_path;
        match &self.config_problem {
            DataPlaneConfigProblem::UnknownKey => {
                write!(formatter, "{key_path}: unknown key, ignored")
            }
            DataPlaneConfigProblem::InvalidValue { rejection_reason } => {
                write!(
                    formatter,
                    "{key_path}: {rejection_reason}; the default applies"
                )
            }
            DataPlaneConfigProblem::UnclaimedExtension => write!(
                formatter,
                "{key_path}: no extension of this build claims the section, ignored"
            ),
            DataPlaneConfigProblem::CompressionOverrideSet => write!(
                formatter,
                "{key_path}: set, so it overrides the build's compression default"
            ),
        }
    }
}

/// One JSON object of `grpc.data_plane`, read key by key. The keys left at the end are unknown.
struct LenientSection {
    key_path: String,
    remaining_entries: serde_json::Map<String, serde_json::Value>,
}

impl LenientSection {
    /// Opens an object, or warns and returns `None` when the value is not one.
    fn open_section(
        key_path: String,
        section_value: serde_json::Value,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
    ) -> Option<Self> {
        match section_value {
            serde_json::Value::Object(remaining_entries) => Some(Self {
                key_path,
                remaining_entries,
            }),
            other_value => {
                load_warnings.push(DataPlaneConfigWarning {
                    key_path,
                    config_problem: DataPlaneConfigProblem::InvalidValue {
                        rejection_reason: format!(
                            "expected an object, found {}",
                            json_type_name(&other_value)
                        ),
                    },
                });
                None
            }
        }
    }

    fn child_key_path(&self, key_name: &str) -> String {
        format!("{}.{key_name}", self.key_path)
    }

    /// Removes `key_name` and parses its value; a bad value warns and gives `None`.
    fn take_entry<T>(
        &mut self,
        key_name: &str,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
        parse_value: impl FnOnce(serde_json::Value) -> Result<T, String>,
    ) -> Option<T> {
        let entry_value = self.remaining_entries.remove(key_name)?;
        match parse_value(entry_value) {
            Ok(parsed_value) => Some(parsed_value),
            Err(rejection_reason) => {
                load_warnings.push(DataPlaneConfigWarning {
                    key_path: self.child_key_path(key_name),
                    config_problem: DataPlaneConfigProblem::InvalidValue { rejection_reason },
                });
                None
            }
        }
    }

    /// [`Self::take_entry`] for a key that is unset by default, where `null` also means unset.
    fn take_optional<T>(
        &mut self,
        key_name: &str,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
        parse_value: impl FnOnce(serde_json::Value) -> Result<T, String>,
    ) -> Option<T> {
        if self
            .remaining_entries
            .get(key_name)
            .is_some_and(serde_json::Value::is_null)
        {
            self.remaining_entries.remove(key_name);
            return None;
        }
        self.take_entry(key_name, load_warnings, parse_value)
    }

    /// [`Self::take_entry`] into `config_field`, which keeps its value if the key is absent or bad.
    fn read_into<T>(
        &mut self,
        config_field: &mut T,
        key_name: &str,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
        parse_value: impl FnOnce(serde_json::Value) -> Result<T, String>,
    ) {
        if let Some(parsed_value) = self.take_entry(key_name, load_warnings, parse_value) {
            *config_field = parsed_value;
        }
    }

    fn take_section(
        &mut self,
        key_name: &str,
        load_warnings: &mut Vec<DataPlaneConfigWarning>,
    ) -> Option<Self> {
        let section_value = self.remaining_entries.remove(key_name)?;
        Self::open_section(self.child_key_path(key_name), section_value, load_warnings)
    }

    /// Warns once for each key that no read took.
    fn warn_unknown_keys(self, load_warnings: &mut Vec<DataPlaneConfigWarning>) {
        for unknown_key in self.remaining_entries.keys() {
            load_warnings.push(DataPlaneConfigWarning {
                key_path: format!("{}.{unknown_key}", self.key_path),
                config_problem: DataPlaneConfigProblem::UnknownKey,
            });
        }
    }
}

const fn json_type_name(json_value: &serde_json::Value) -> &'static str {
    match json_value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

fn parse_json_value<T: DeserializeOwned>(json_value: serde_json::Value) -> Result<T, String> {
    serde_json::from_value(json_value).map_err(|serde_error| serde_error.to_string())
}

/// An integer, written as a number or as a string with optional `_` separators.
fn parse_integer_value<T>(json_value: serde_json::Value) -> Result<T, String>
where
    T: DeserializeOwned + FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    match json_value {
        serde_json::Value::String(integer_text) => integer_text
            .replace('_', "")
            .parse()
            .map_err(|parse_error| format!("invalid integer {integer_text:?}: {parse_error}")),
        other_value => parse_json_value(other_value),
    }
}

fn parse_integer_in_range<T>(
    json_value: serde_json::Value,
    accepted_range: RangeInclusive<T>,
) -> Result<T, String>
where
    T: DeserializeOwned + FromStr + PartialOrd + fmt::Display,
    <T as FromStr>::Err: fmt::Display,
{
    let parsed_value = parse_integer_value(json_value)?;
    if accepted_range.contains(&parsed_value) {
        Ok(parsed_value)
    } else {
        Err(format!(
            "{parsed_value} is outside {}..={}",
            accepted_range.start(),
            accepted_range.end()
        ))
    }
}

/// A byte size, written as a number of bytes or as a string such as `"64KiB"`.
fn parse_byte_size_value(json_value: serde_json::Value) -> Result<usize, String> {
    let byte_size: ByteSize = parse_json_value(json_value)?;
    usize::try_from(byte_size.as_u64()).map_err(|conversion_error| conversion_error.to_string())
}

fn parse_duration_value(json_value: serde_json::Value) -> Result<Duration, String> {
    match json_value {
        serde_json::Value::String(duration_text) => {
            humantime_serde::re::humantime::parse_duration(&duration_text)
                .map_err(|parse_error| format!("invalid duration {duration_text:?}: {parse_error}"))
        }
        other_value => Err(format!(
            "expected a duration string such as \"1ms\", found {}",
            json_type_name(&other_value)
        )),
    }
}

fn parse_taskset_value(json_value: serde_json::Value) -> Result<Vec<usize>, String> {
    match json_value {
        serde_json::Value::String(taskset_text) => parse_taskset(&taskset_text),
        other_value => Err(format!(
            "expected a taskset string such as \"0-3\", found {}",
            json_type_name(&other_value)
        )),
    }
}

fn taskset_config_value(taskset_cpus: Option<&[usize]>) -> String {
    taskset_cpus.map_or_else(
        || "none".to_owned(),
        |cpu_indexes| {
            cpu_indexes
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",")
        },
    )
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Address of Prometheus service.
    pub address: SocketAddr,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ValueIntStr<'a, T> {
    Int(T),
    Str(&'a str),
}

fn deserialize_int_str<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    match ValueIntStr::<T>::deserialize(deserializer)? {
        ValueIntStr::Int(value) => Ok(value),
        ValueIntStr::Str(value) => value
            .replace('_', "")
            .parse::<T>()
            .map_err(de::Error::custom),
    }
}

fn deserialize_int_str_maybe<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    match Option::<ValueIntStr<T>>::deserialize(deserializer)? {
        Some(ValueIntStr::Int(value)) => Ok(Some(value)),
        Some(ValueIntStr::Str(value)) => value
            .replace('_', "")
            .parse::<T>()
            .map(Some)
            .map_err(de::Error::custom),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::{ConfigGrpc, GrpcAddress, GrpcAddresses, GrpcCapacities},
            plugin::filter::message::WeightScales,
        },
        std::{path::Path, time::Duration},
    };

    #[test]
    fn capacities_fall_back_to_channel_capacity() {
        let config: ConfigGrpc = serde_json::from_str(r#"{"channel_capacity": "1_000_000"}"#)
            .expect("valid grpc config");
        assert_eq!(
            config.resolved_capacities(),
            GrpcCapacities {
                geyser_subscriber_weight: 1_000_000,
                deshred_subscriber: 1_000_000,
                contact_info_subscriber: 1_000_000,
                processed_broadcast: 1_000_000,
                confirmed_broadcast: 1_000_000,
                finalized_broadcast: 1_000_000,
                weight_scales: WeightScales::default(),
            }
        );
        assert_eq!(
            config.client_unresponsive_timeout,
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn capacities_override_channel_capacity_and_older_ring_settings() {
        let config: ConfigGrpc = serde_json::from_str(
            r#"{
                "channel_capacity": 1000000,
                "processed_broadcast_capacity": 2048,
                "confirmed_broadcast_capacity": 4096,
                "client_unresponsive_timeout": "30s",
                "capacities": {
                    "geyser_subscriber_weight_capacity": "4_000_000",
                    "deshred_channel_capacity": 50000,
                    "internal_confirmed_channel_capacity": 512,
                    "weight_scale_account": 4,
                    "weight_scale_transaction": "4",
                    "weight_scale_block": 0,
                    "weight_account_data_unit": "8_192",
                    "weight_transaction_data_unit": 2048
                }
            }"#,
        )
        .expect("valid grpc config");
        assert_eq!(
            config.resolved_capacities(),
            GrpcCapacities {
                geyser_subscriber_weight: 4_000_000,
                deshred_subscriber: 50_000,
                contact_info_subscriber: 1_000_000,
                processed_broadcast: 2048,
                confirmed_broadcast: 512,
                finalized_broadcast: 1_000_000,
                weight_scales: WeightScales {
                    account: 4,
                    transaction: 4,
                    account_data_unit: 8192,
                    transaction_data_unit: 2048,
                    ..WeightScales::default()
                },
            }
        );
        assert_eq!(
            config.client_unresponsive_timeout,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn data_units_default_to_4096_and_zero_turns_them_off() {
        let scales = |json: &str| {
            serde_json::from_str::<ConfigGrpc>(json)
                .expect("valid grpc config")
                .resolved_capacities()
                .weight_scales
        };
        let defaults = scales(r#"{}"#);
        assert_eq!(defaults.account_data_unit, 4096);
        assert_eq!(defaults.transaction_data_unit, 4096);
        let off = scales(
            r#"{"capacities": {"weight_account_data_unit": 0, "weight_transaction_data_unit": 0}}"#,
        );
        assert_eq!(off.account_data_unit, 0);
        assert_eq!(off.transaction_data_unit, 0);
    }

    #[test]
    fn filter_name_data_unit_defaults_to_1024_and_zero_turns_it_off() {
        let filter_name_data_unit = |config_json: &str| {
            serde_json::from_str::<ConfigGrpc>(config_json)
                .expect("valid grpc config")
                .resolved_capacities()
                .weight_scales
                .filter_name_data_unit
        };
        assert_eq!(filter_name_data_unit(r#"{}"#), 1024);
        assert_eq!(
            filter_name_data_unit(r#"{"capacities": {"weight_filter_name_data_unit": "2_048"}}"#),
            2048
        );
        assert_eq!(
            filter_name_data_unit(r#"{"capacities": {"weight_filter_name_data_unit": 0}}"#),
            0
        );
    }

    #[test]
    fn capacities_reject_unknown_fields() {
        let error = serde_json::from_str::<ConfigGrpc>(
            r#"{"capacities": {"geyser_subscriber_channel_capacity": 1}}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
    }

    #[test]
    fn client_unresponsive_timeout_null_disables_it() {
        let config: ConfigGrpc = serde_json::from_str(r#"{"client_unresponsive_timeout": null}"#)
            .expect("valid grpc config");
        assert_eq!(config.client_unresponsive_timeout, None);
    }

    #[test]
    fn test_deser_config_tokio() {
        let json = r#"{
            "worker_threads": 4,
            "affinity": "0-2,3"
        }"#;
        let config: super::ConfigTokio = serde_json::from_str(json).unwrap();
        assert_eq!(config.worker_threads, Some(4));
        assert_eq!(config.affinity, Some(vec![0, 1, 2, 3]));

        let json_without_affinity = r#"{
            "worker_threads": 4
        }"#;
        let config: super::ConfigTokio = serde_json::from_str(json_without_affinity).unwrap();
        assert_eq!(config.worker_threads, Some(4));
        assert_eq!(config.affinity, None);

        let json_with_null_affinity = r#"{
            "worker_threads": 4,
            "affinity": null
        }"#;
        let config: super::ConfigTokio = serde_json::from_str(json_with_null_affinity).unwrap();
        assert_eq!(config.worker_threads, Some(4));
        assert_eq!(config.affinity, None);
    }

    #[test]
    fn test_grpc_address_single_tcp() {
        let json = r#""0.0.0.0:10000""#;
        let addrs: GrpcAddresses = serde_json::from_str(json).unwrap();
        assert_eq!(addrs.inner.len(), 1);
        assert!(matches!(addrs.inner[0], GrpcAddress::Tcp(addr) if addr.port() == 10000));
    }

    #[test]
    fn test_grpc_address_single_uds() {
        let json = r#""unix:///var/run/geyser.sock""#;
        let addrs: GrpcAddresses = serde_json::from_str(json).unwrap();
        assert_eq!(addrs.inner.len(), 1);
        assert!(
            matches!(&addrs.inner[0], GrpcAddress::Unix { path, .. } if path == Path::new("/var/run/geyser.sock"))
        );
    }

    #[test]
    fn test_grpc_address_array_tcp_only() {
        let json = r#"["0.0.0.0:10000", "0.0.0.0:10001"]"#;
        let addrs: GrpcAddresses = serde_json::from_str(json).unwrap();
        assert_eq!(addrs.inner.len(), 2);
        assert!(matches!(addrs.inner[0], GrpcAddress::Tcp(addr) if addr.port() == 10000));
        assert!(matches!(addrs.inner[1], GrpcAddress::Tcp(addr) if addr.port() == 10001));
    }

    #[test]
    fn test_grpc_address_array_uds_only() {
        let json = r#"["unix:///var/run/a.sock", "unix:///var/run/b.sock"]"#;
        let addrs: GrpcAddresses = serde_json::from_str(json).unwrap();
        assert_eq!(addrs.inner.len(), 2);
        assert!(
            matches!(&addrs.inner[0], GrpcAddress::Unix { path, .. } if path == Path::new("/var/run/a.sock"))
        );
        assert!(
            matches!(&addrs.inner[1], GrpcAddress::Unix { path, .. } if path == Path::new("/var/run/b.sock"))
        );
    }

    #[test]
    fn test_grpc_address_array_mixed() {
        let json = r#"["0.0.0.0:10000", "unix:///var/run/geyser.sock", "127.0.0.1:10001"]"#;
        let addrs: GrpcAddresses = serde_json::from_str(json).unwrap();
        assert_eq!(addrs.inner.len(), 3);
        assert!(matches!(addrs.inner[0], GrpcAddress::Tcp(addr) if addr.port() == 10000));
        assert!(
            matches!(&addrs.inner[1], GrpcAddress::Unix { path, .. } if path == Path::new("/var/run/geyser.sock"))
        );
        assert!(matches!(addrs.inner[2], GrpcAddress::Tcp(addr) if addr.port() == 10001));
    }

    #[test]
    fn test_grpc_address_uds_default_mode() {
        let json = r#""unix:///var/run/geyser.sock""#;
        let addrs: GrpcAddresses = serde_json::from_str(json).unwrap();
        assert!(matches!(&addrs.inner[0], GrpcAddress::Unix { mode, .. } if *mode == 0o660));
    }

    #[test]
    fn test_grpc_address_uds_custom_mode() {
        let json = r#"{"path": "unix:///var/run/geyser.sock", "mode": 448}"#; // 448 = 0o700
        let addrs: GrpcAddresses = serde_json::from_str(json).unwrap();
        assert!(matches!(&addrs.inner[0], GrpcAddress::Unix { mode, .. } if *mode == 0o700));
    }

    #[test]
    fn test_grpc_address_mode_on_tcp_fails() {
        let json = r#"{"path": "0.0.0.0:10000", "mode": 448}"#;
        let result: Result<GrpcAddresses, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_grpc_address_empty_array() {
        let json = r#"[]"#;
        let result: Result<GrpcAddresses, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_grpc_address_invalid_tcp() {
        let json = r#""not_valid""#;
        let result: Result<GrpcAddresses, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }
}
