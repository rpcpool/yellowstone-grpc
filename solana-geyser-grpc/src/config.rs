use {
    serde::{de, Deserialize, Deserializer},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashSet, fs::read_to_string, net::SocketAddr, path::Path},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    #[serde(default)]
    pub log: ConfigLog,
    pub grpc: ConfigGrpc,
    #[serde(default)]
    pub prometheus: Option<ConfigPrometheus>,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpc {
    /// Address of Grpc service.
    pub address: SocketAddr,
    /// Capacity of the channel per connection
    #[serde(
        default = "ConfigGrpc::channel_capacity_default",
        deserialize_with = "UsizeStr::deserialize_usize"
    )]
    pub channel_capacity: usize,
    /// Limits for possible filters
    #[serde(default)]
    pub filters: Option<ConfigGrpcFilters>,
}

impl ConfigGrpc {
    const fn channel_capacity_default() -> usize {
        250_000
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFilters {
    pub accounts: ConfigGrpcFiltersAccounts,
    pub slots: ConfigGrpcFiltersSlots,
    pub transactions: ConfigGrpcFiltersTransactions,
    pub blocks: ConfigGrpcFiltersBlocks,
    pub blocks_meta: ConfigGrpcFiltersBlocksMeta,
}

impl ConfigGrpcFilters {
    pub fn check_max(len: usize, max: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            len <= max,
            "Max amount of filters reached, only {} allowed",
            max
        );
        Ok(())
    }

    pub fn check_any(is_empty: bool, any: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            !is_empty || any,
            "Broadcast `any` is not allowed, at least one filter required"
        );
        Ok(())
    }

    pub fn check_pubkey_max(len: usize, max: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            len <= max,
            "Max amount of Pubkeys reached, only {} allowed",
            max
        );
        Ok(())
    }

    pub fn check_pubkey_reject(pubkey: &Pubkey, set: &HashSet<Pubkey>) -> anyhow::Result<()> {
        anyhow::ensure!(
            !set.contains(pubkey),
            "Pubkey {} in filters not allowed",
            pubkey
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersAccounts {
    pub max: usize,
    pub any: bool,
    pub account_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_reject: HashSet<Pubkey>,
    pub owner_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub owner_reject: HashSet<Pubkey>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersSlots {
    pub max: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersTransactions {
    pub max: usize,
    pub any: bool,
    pub account_include_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_include_reject: HashSet<Pubkey>,
    pub account_exclude_max: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersBlocks {
    pub max: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcFiltersBlocksMeta {
    pub max: usize,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Address of Prometheus service.
    pub address: SocketAddr,
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
struct UsizeStr {
    value: usize,
}

impl<'de> Deserialize<'de> for UsizeStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Integer(usize),
            String(String),
        }

        match Value::deserialize(deserializer)? {
            Value::Integer(value) => Ok(UsizeStr { value }),
            Value::String(value) => value
                .replace('_', "")
                .parse::<usize>()
                .map_err(de::Error::custom)
                .map(|value| UsizeStr { value }),
        }
    }
}

impl UsizeStr {
    fn deserialize_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::deserialize(deserializer)?.value)
    }
}

fn deserialize_pubkey_set<'de, D>(deserializer: D) -> Result<HashSet<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<&str>::deserialize(deserializer)?
        .into_iter()
        .map(|value| {
            value.parse().map_err(|error| {
                de::Error::custom(format!("Invalid pubkey: {} ({:?})", value, error))
            })
        })
        .collect::<Result<_, _>>()
}
