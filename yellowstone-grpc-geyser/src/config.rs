use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    serde::{de, Deserialize, Deserializer},
    std::{
        collections::HashSet, fmt, fs::read_to_string, net::SocketAddr, path::Path, str::FromStr,
        time::Duration,
    },
    tokio::sync::Semaphore,
    tonic::codec::CompressionEncoding,
    yellowstone_grpc_proto::plugin::filter::limits::FilterLimits,
};

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
    pub worker_threads: Option<usize>,
    /// Threads affinity
    #[serde(deserialize_with = "ConfigTokio::deserialize_affinity")]
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

    let mut vec = set.clone().into_iter().collect::<Vec<usize>>();
    vec.sort();

    if !set.is_empty() {
        if let Some(core_ids) = affinity_linux::get_thread_affinity()
            .map_err(|error| format!("failed to get allowed cpus: {error:?}"))?
        {
            if !set.is_subset(&core_ids) {
                return Err(format!(
                    "not allowed core index, should be set of: {core_ids:?}"
                ));
            }
        }
    }

    Ok(vec)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpc {
    /// Address of Grpc service.
    pub address: SocketAddr,
    /// TLS config
    pub tls_config: Option<ConfigGrpcServerTls>,
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
}

impl ConfigGrpc {
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
        0
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcServerTls {
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
