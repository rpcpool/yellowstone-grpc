use {
    anyhow::Context,
    serde::{de, Deserialize, Serialize},
    std::{
        collections::{HashMap, HashSet},
        path::Path,
    },
    tokio::fs,
    yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        CommitmentLevel, SubscribeRequest, SubscribeRequestAccountsDataSlice,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
};

pub async fn load<T>(path: impl AsRef<Path> + Copy) -> anyhow::Result<T>
where
    T: de::DeserializeOwned,
{
    let text = fs::read_to_string(path)
        .await
        .context("failed to read config from file")?;

    match path.as_ref().extension().and_then(|e| e.to_str()) {
        Some("yaml") | Some("yml") => {
            serde_yaml::from_str(&text).context("failed to parse config from file")
        }
        Some("json") => json5::from_str(&text).context("failed to parse config from file"),
        value => anyhow::bail!("unknown config extension: {value:?}"),
    }
}

pub trait GrpcRequestToProto<T> {
    fn to_proto(self) -> T;
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpcRequest {
    pub slots: HashMap<String, ConfigGrpcRequestSlots>,
    pub accounts: HashMap<String, ConfigGrpcRequestAccounts>,
    pub transactions: HashMap<String, ConfigGrpcRequestTransactions>,
    pub entries: HashSet<String>,
    pub blocks: HashMap<String, ConfigGrpcRequestBlocks>,
    pub blocks_meta: HashSet<String>,
    pub commitment: Option<ConfigGrpcRequestCommitment>,
    pub accounts_data_slice: Vec<ConfigGrpcRequestAccountsDataSlice>,
}

impl ConfigGrpcRequest {
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

impl GrpcRequestToProto<SubscribeRequest> for ConfigGrpcRequest {
    fn to_proto(self) -> SubscribeRequest {
        SubscribeRequest {
            slots: ConfigGrpcRequest::map_to_proto(self.slots),
            accounts: ConfigGrpcRequest::map_to_proto(self.accounts),
            transactions: ConfigGrpcRequest::map_to_proto(self.transactions),
            entry: ConfigGrpcRequest::set_to_proto(self.entries),
            blocks: ConfigGrpcRequest::map_to_proto(self.blocks),
            blocks_meta: ConfigGrpcRequest::set_to_proto(self.blocks_meta),
            commitment: self.commitment.map(|v| v.to_proto() as i32),
            accounts_data_slice: ConfigGrpcRequest::vec_to_proto(self.accounts_data_slice),
            ping: None,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpcRequestSlots {
    filter_by_commitment: Option<bool>,
}

impl GrpcRequestToProto<SubscribeRequestFilterSlots> for ConfigGrpcRequestSlots {
    fn to_proto(self) -> SubscribeRequestFilterSlots {
        SubscribeRequestFilterSlots {
            filter_by_commitment: self.filter_by_commitment,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpcRequestAccounts {
    account: Vec<String>,
    owner: Vec<String>,
    filters: Vec<ConfigGrpcRequestAccountsFilter>,
}

impl GrpcRequestToProto<SubscribeRequestFilterAccounts> for ConfigGrpcRequestAccounts {
    fn to_proto(self) -> SubscribeRequestFilterAccounts {
        SubscribeRequestFilterAccounts {
            account: self.account,
            owner: self.owner,
            filters: self.filters.into_iter().map(|f| f.to_proto()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ConfigGrpcRequestAccountsFilter {
    Memcmp { offset: u64, base58: String },
    DataSize(u64),
    TokenAccountState,
}

impl GrpcRequestToProto<SubscribeRequestFilterAccountsFilter> for ConfigGrpcRequestAccountsFilter {
    fn to_proto(self) -> SubscribeRequestFilterAccountsFilter {
        SubscribeRequestFilterAccountsFilter {
            filter: Some(match self {
                ConfigGrpcRequestAccountsFilter::Memcmp { offset, base58 } => {
                    AccountsFilterDataOneof::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                        offset,
                        data: Some(AccountsFilterMemcmpOneof::Base58(base58)),
                    })
                }
                ConfigGrpcRequestAccountsFilter::DataSize(size) => {
                    AccountsFilterDataOneof::Datasize(size)
                }
                ConfigGrpcRequestAccountsFilter::TokenAccountState => {
                    AccountsFilterDataOneof::TokenAccountState(true)
                }
            }),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpcRequestTransactions {
    pub vote: Option<bool>,
    pub failed: Option<bool>,
    pub signature: Option<String>,
    pub account_include: Vec<String>,
    pub account_exclude: Vec<String>,
    pub account_required: Vec<String>,
}

impl GrpcRequestToProto<SubscribeRequestFilterTransactions> for ConfigGrpcRequestTransactions {
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

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfigGrpcRequestBlocks {
    pub account_include: Vec<String>,
    pub include_transactions: Option<bool>,
    pub include_accounts: Option<bool>,
    pub include_entries: Option<bool>,
}

impl GrpcRequestToProto<SubscribeRequestFilterBlocks> for ConfigGrpcRequestBlocks {
    fn to_proto(self) -> SubscribeRequestFilterBlocks {
        SubscribeRequestFilterBlocks {
            account_include: self.account_include,
            include_transactions: self.include_transactions,
            include_accounts: self.include_accounts,
            include_entries: self.include_entries,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigGrpcRequestCommitment {
    Processed,
    Confirmed,
    Finalized,
}

impl GrpcRequestToProto<CommitmentLevel> for ConfigGrpcRequestCommitment {
    fn to_proto(self) -> CommitmentLevel {
        match self {
            Self::Processed => CommitmentLevel::Processed,
            Self::Confirmed => CommitmentLevel::Confirmed,
            Self::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct ConfigGrpcRequestAccountsDataSlice {
    pub offset: u64,
    pub length: u64,
}

impl GrpcRequestToProto<SubscribeRequestAccountsDataSlice> for ConfigGrpcRequestAccountsDataSlice {
    fn to_proto(self) -> SubscribeRequestAccountsDataSlice {
        SubscribeRequestAccountsDataSlice {
            offset: self.offset,
            length: self.length,
        }
    }
}

pub fn deserialize_usize_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: de::Deserializer<'de>,
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

#[cfg(test)]
mod tests {
    use super::ConfigGrpcRequestAccountsFilter;

    #[test]
    fn grpc_config_accounts_filter_memcmp() {
        let filter = ConfigGrpcRequestAccountsFilter::Memcmp {
            offset: 42,
            base58: "123".to_owned(),
        };
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigGrpcRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }

    #[test]
    fn grpc_config_accounts_filter_datasize() {
        let filter = ConfigGrpcRequestAccountsFilter::DataSize(42);
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigGrpcRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }

    #[test]
    fn grpc_config_accounts_filter_token() {
        let filter = ConfigGrpcRequestAccountsFilter::TokenAccountState;
        let text = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<ConfigGrpcRequestAccountsFilter>(&text).unwrap(),
            filter
        );
    }
}
