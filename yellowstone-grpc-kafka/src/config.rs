use {
    anyhow::Context,
    serde::{Deserialize, Serialize},
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
    pub input: Option<ConfigInput>,
    pub output: Option<ConfigOutput>,
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
pub struct ConfigInput {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub request: ConfigInputRequest,
    pub kafka_topic: String,
    #[serde(default)]
    pub kafka: HashMap<String, String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigInputRequest {
    pub slots: HashSet<String>,
    pub accounts: HashMap<String, ConfigInputRequestAccounts>,
    pub transactions: HashMap<String, ConfigInputRequestTransactions>,
    pub entries: HashSet<String>,
    pub blocks: HashMap<String, ConfigInputRequestBlocks>,
    pub blocks_meta: HashSet<String>,
    pub commitment: Option<ConfigInputRequestCommitment>,
    pub accounts_data_slice: Vec<ConfigInputRequestAccountsDataSlice>,
}

impl ConfigInputRequest {
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

impl GrpcRequestToProto<SubscribeRequest> for ConfigInputRequest {
    fn to_proto(self) -> SubscribeRequest {
        SubscribeRequest {
            slots: ConfigInputRequest::set_to_proto(self.slots),
            accounts: ConfigInputRequest::map_to_proto(self.accounts),
            transactions: ConfigInputRequest::map_to_proto(self.transactions),
            entry: ConfigInputRequest::set_to_proto(self.entries),
            blocks: ConfigInputRequest::map_to_proto(self.blocks),
            blocks_meta: ConfigInputRequest::set_to_proto(self.blocks_meta),
            commitment: self.commitment.map(|v| v.to_proto() as i32),
            accounts_data_slice: ConfigInputRequest::vec_to_proto(self.accounts_data_slice),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigInputRequestAccounts {
    account: Vec<String>,
    owner: Vec<String>,
    filters: Vec<ConfigInputRequestAccountsFilter>,
}

impl GrpcRequestToProto<SubscribeRequestFilterAccounts> for ConfigInputRequestAccounts {
    fn to_proto(self) -> SubscribeRequestFilterAccounts {
        SubscribeRequestFilterAccounts {
            account: self.account,
            owner: self.owner,
            filters: self.filters.into_iter().map(|f| f.to_proto()).collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum ConfigInputRequestAccountsFilter {
    Memcmp { offset: u64, base58: String },
    DataSize(u64),
    TokenAccountState,
}

impl GrpcRequestToProto<SubscribeRequestFilterAccountsFilter> for ConfigInputRequestAccountsFilter {
    fn to_proto(self) -> SubscribeRequestFilterAccountsFilter {
        SubscribeRequestFilterAccountsFilter {
            filter: Some(match self {
                ConfigInputRequestAccountsFilter::Memcmp { offset, base58 } => {
                    AccountsFilterDataOneof::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                        offset,
                        data: Some(AccountsFilterMemcmpOneof::Base58(base58)),
                    })
                }
                ConfigInputRequestAccountsFilter::DataSize(size) => {
                    AccountsFilterDataOneof::Datasize(size)
                }
                ConfigInputRequestAccountsFilter::TokenAccountState => {
                    AccountsFilterDataOneof::TokenAccountState(true)
                }
            }),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigInputRequestTransactions {
    pub vote: Option<bool>,
    pub failed: Option<bool>,
    pub signature: Option<String>,
    pub account_include: Vec<String>,
    pub account_exclude: Vec<String>,
    pub account_required: Vec<String>,
}

impl GrpcRequestToProto<SubscribeRequestFilterTransactions> for ConfigInputRequestTransactions {
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
pub struct ConfigInputRequestBlocks {
    pub account_include: Vec<String>,
    pub include_transactions: Option<bool>,
    pub include_accounts: Option<bool>,
    pub include_entries: Option<bool>,
}

impl GrpcRequestToProto<SubscribeRequestFilterBlocks> for ConfigInputRequestBlocks {
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
pub enum ConfigInputRequestCommitment {
    Processed,
    Confirmed,
    Finalized,
}

impl GrpcRequestToProto<CommitmentLevel> for ConfigInputRequestCommitment {
    fn to_proto(self) -> CommitmentLevel {
        match self {
            Self::Processed => CommitmentLevel::Processed,
            Self::Confirmed => CommitmentLevel::Confirmed,
            Self::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigInputRequestAccountsDataSlice {
    pub offset: u64,
    pub length: u64,
}

impl GrpcRequestToProto<SubscribeRequestAccountsDataSlice> for ConfigInputRequestAccountsDataSlice {
    fn to_proto(self) -> SubscribeRequestAccountsDataSlice {
        SubscribeRequestAccountsDataSlice {
            offset: self.offset,
            length: self.length,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigOutput {
    pub kafka_topic: String,
    #[serde(default)]
    pub kafka: HashMap<String, String>,
    pub listen: SocketAddr,
    #[serde(default = "ConfigOutput::channel_capacity_default")]
    pub channel_capacity: usize,
}

impl ConfigOutput {
    const fn channel_capacity_default() -> usize {
        250_000
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigInputRequestAccountsFilter;

    #[test]
    fn grpc_config_accounts_filter_memcmp() {
        let filter = ConfigInputRequestAccountsFilter::Memcmp {
            offset: 42,
            base58: "123".to_owned(),
        };
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigInputRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }

    #[test]
    fn grpc_config_accounts_filter_datasize() {
        let filter = ConfigInputRequestAccountsFilter::DataSize(42);
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigInputRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }

    #[test]
    fn grpc_config_accounts_filter_token() {
        let filter = ConfigInputRequestAccountsFilter::TokenAccountState;
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigInputRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }
}
