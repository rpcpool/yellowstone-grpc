use {
    crate::dedup::{KafkaDedup, KafkaDedupMemory},
    anyhow::Context,
    serde::{
        de::{self, Deserializer},
        Deserialize, Serialize,
    },
    std::{
        collections::{HashMap, HashSet},
        net::SocketAddr,
        path::Path,
    },
    tokio::fs,
    yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        CommitmentLevel, SubscribeRequest, SubscribeRequestAccountsDataSlice,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterTransactions,
    },
};

pub trait GrpcRequestToProto<T> {
    fn to_proto(self) -> T;
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub kafka: HashMap<String, String>,
    pub dedup: Option<ConfigDedup>,
    pub grpc2kafka: Option<ConfigGrpc2Kafka>,
    pub kafka2grpc: Option<ConfigKafka2Grpc>,
}

impl Config {
    pub async fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let text = fs::read_to_string(path)
            .await
            .context("failed to read config from file")?;

        serde_json::from_str(&text).context("failed to parse config from file")
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigDedup {
    #[serde(default)]
    pub kafka: HashMap<String, String>,
    pub kafka_input: String,
    pub kafka_output: String,
    #[serde(
        default = "ConfigGrpc2Kafka::default_kafka_queue_size",
        deserialize_with = "ConfigGrpc2Kafka::deserialize_usize_str"
    )]
    pub kafka_queue_size: usize,
    pub backend: ConfigDedupBackend,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConfigDedupBackend {
    Memory,
}

impl ConfigDedupBackend {
    pub async fn create(&self) -> anyhow::Result<Box<impl KafkaDedup>> {
        Ok(match self {
            Self::Memory => Box::<KafkaDedupMemory>::default(),
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigGrpc2Kafka {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub request: ConfigGrpc2KafkaRequest,
    #[serde(default)]
    pub kafka: HashMap<String, String>,
    pub kafka_topic: String,
    #[serde(
        default = "ConfigGrpc2Kafka::default_kafka_queue_size",
        deserialize_with = "ConfigGrpc2Kafka::deserialize_usize_str"
    )]
    pub kafka_queue_size: usize,
}

impl ConfigGrpc2Kafka {
    const fn default_kafka_queue_size() -> usize {
        10_000
    }

    fn deserialize_usize_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
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
            Value::Integer(value) => Ok(value),
            Value::String(value) => value
                .replace('_', "")
                .parse::<usize>()
                .map_err(de::Error::custom),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpc2KafkaRequest {
    pub slots: HashSet<String>,
    pub accounts: HashMap<String, ConfigGrpc2KafkaRequestAccounts>,
    pub transactions: HashMap<String, ConfigGrpc2KafkaRequestTransactions>,
    pub entries: HashSet<String>,
    pub blocks: HashMap<String, ConfigGrpc2KafkaRequestBlocks>,
    pub blocks_meta: HashSet<String>,
    pub commitment: Option<ConfigGrpc2KafkaRequestCommitment>,
    pub accounts_data_slice: Vec<ConfigGrpc2KafkaRequestAccountsDataSlice>,
}

impl ConfigGrpc2KafkaRequest {
    fn map_to_proto<T>(map: HashMap<String, impl GrpcRequestToProto<T>>) -> HashMap<String, T> {
        map.into_iter().map(|(k, v)| (k, v.to_proto())).collect()
    }

    fn set_to_proto<T: Default>(set: HashSet<String>) -> HashMap<String, T> {
        set.into_iter().map(|v| (v, T::default())).collect()
    }

    fn vec_to_proto<T>(vec: Vec<impl GrpcRequestToProto<T>>) -> Vec<T> {
        vec.into_iter().map(|v| v.to_proto()).collect()
    }
}

impl GrpcRequestToProto<SubscribeRequest> for ConfigGrpc2KafkaRequest {
    fn to_proto(self) -> SubscribeRequest {
        SubscribeRequest {
            slots: ConfigGrpc2KafkaRequest::set_to_proto(self.slots),
            accounts: ConfigGrpc2KafkaRequest::map_to_proto(self.accounts),
            transactions: ConfigGrpc2KafkaRequest::map_to_proto(self.transactions),
            entry: ConfigGrpc2KafkaRequest::set_to_proto(self.entries),
            blocks: ConfigGrpc2KafkaRequest::map_to_proto(self.blocks),
            blocks_meta: ConfigGrpc2KafkaRequest::set_to_proto(self.blocks_meta),
            commitment: self.commitment.map(|v| v.to_proto() as i32),
            accounts_data_slice: ConfigGrpc2KafkaRequest::vec_to_proto(self.accounts_data_slice),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpc2KafkaRequestAccounts {
    account: Vec<String>,
    owner: Vec<String>,
    filters: Vec<ConfigGrpc2KafkaRequestAccountsFilter>,
}

impl GrpcRequestToProto<SubscribeRequestFilterAccounts> for ConfigGrpc2KafkaRequestAccounts {
    fn to_proto(self) -> SubscribeRequestFilterAccounts {
        SubscribeRequestFilterAccounts {
            account: self.account,
            owner: self.owner,
            filters: self.filters.into_iter().map(|f| f.to_proto()).collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum ConfigGrpc2KafkaRequestAccountsFilter {
    Memcmp { offset: u64, base58: String },
    DataSize(u64),
    TokenAccountState,
}

impl GrpcRequestToProto<SubscribeRequestFilterAccountsFilter>
    for ConfigGrpc2KafkaRequestAccountsFilter
{
    fn to_proto(self) -> SubscribeRequestFilterAccountsFilter {
        SubscribeRequestFilterAccountsFilter {
            filter: Some(match self {
                ConfigGrpc2KafkaRequestAccountsFilter::Memcmp { offset, base58 } => {
                    AccountsFilterDataOneof::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                        offset,
                        data: Some(AccountsFilterMemcmpOneof::Base58(base58)),
                    })
                }
                ConfigGrpc2KafkaRequestAccountsFilter::DataSize(size) => {
                    AccountsFilterDataOneof::Datasize(size)
                }
                ConfigGrpc2KafkaRequestAccountsFilter::TokenAccountState => {
                    AccountsFilterDataOneof::TokenAccountState(true)
                }
            }),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpc2KafkaRequestTransactions {
    pub vote: Option<bool>,
    pub failed: Option<bool>,
    pub signature: Option<String>,
    pub account_include: Vec<String>,
    pub account_exclude: Vec<String>,
    pub account_required: Vec<String>,
}

impl GrpcRequestToProto<SubscribeRequestFilterTransactions>
    for ConfigGrpc2KafkaRequestTransactions
{
    fn to_proto(self) -> SubscribeRequestFilterTransactions {
        SubscribeRequestFilterTransactions {
            vote: self.vote,
            failed: self.failed,
            signature: self.signature,
            account_include: self.account_include,
            account_exclude: self.account_exclude,
            account_required: self.account_required,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpc2KafkaRequestBlocks {
    pub account_include: Vec<String>,
    pub include_transactions: Option<bool>,
    pub include_accounts: Option<bool>,
    pub include_entries: Option<bool>,
}

impl GrpcRequestToProto<SubscribeRequestFilterBlocks> for ConfigGrpc2KafkaRequestBlocks {
    fn to_proto(self) -> SubscribeRequestFilterBlocks {
        SubscribeRequestFilterBlocks {
            account_include: self.account_include,
            include_transactions: self.include_transactions,
            include_accounts: self.include_accounts,
            include_entries: self.include_entries,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigGrpc2KafkaRequestCommitment {
    Processed,
    Confirmed,
    Finalized,
}

impl GrpcRequestToProto<CommitmentLevel> for ConfigGrpc2KafkaRequestCommitment {
    fn to_proto(self) -> CommitmentLevel {
        match self {
            Self::Processed => CommitmentLevel::Processed,
            Self::Confirmed => CommitmentLevel::Confirmed,
            Self::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigGrpc2KafkaRequestAccountsDataSlice {
    pub offset: u64,
    pub length: u64,
}

impl GrpcRequestToProto<SubscribeRequestAccountsDataSlice>
    for ConfigGrpc2KafkaRequestAccountsDataSlice
{
    fn to_proto(self) -> SubscribeRequestAccountsDataSlice {
        SubscribeRequestAccountsDataSlice {
            offset: self.offset,
            length: self.length,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigKafka2Grpc {
    #[serde(default)]
    pub kafka: HashMap<String, String>,
    pub kafka_topic: String,
    pub listen: SocketAddr,
    #[serde(default = "ConfigKafka2Grpc::channel_capacity_default")]
    pub channel_capacity: usize,
}

impl ConfigKafka2Grpc {
    const fn channel_capacity_default() -> usize {
        250_000
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigGrpc2KafkaRequestAccountsFilter;

    #[test]
    fn grpc_config_accounts_filter_memcmp() {
        let filter = ConfigGrpc2KafkaRequestAccountsFilter::Memcmp {
            offset: 42,
            base58: "123".to_owned(),
        };
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigGrpc2KafkaRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }

    #[test]
    fn grpc_config_accounts_filter_datasize() {
        let filter = ConfigGrpc2KafkaRequestAccountsFilter::DataSize(42);
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigGrpc2KafkaRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }

    #[test]
    fn grpc_config_accounts_filter_token() {
        let filter = ConfigGrpc2KafkaRequestAccountsFilter::TokenAccountState;
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigGrpc2KafkaRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }
}
