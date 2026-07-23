use {
    crate::{billing::REPORT_INTERVAL, plugin::filter::limits::FilterLimits},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    bytesize::ByteSize,
    serde::{de, Deserialize, Deserializer},
    std::{
        collections::HashSet,
        fmt,
        fs::read_to_string,
        net::SocketAddr,
        num::NonZeroUsize,
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

    const fn default_replay_stored_slots() -> u64 {
        150
    }
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
        crate::config::{GrpcAddress, GrpcAddresses},
        std::path::Path,
    };

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
