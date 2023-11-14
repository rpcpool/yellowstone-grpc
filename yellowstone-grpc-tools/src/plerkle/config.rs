use {
    crate::config::{
        deserialize_usize_str, ConfigGrpcRequestAccounts, ConfigGrpcRequestCommitment,
        ConfigGrpcRequestTransactions, GrpcRequestToProto,
    },
    maplit::hashmap,
    plerkle_messenger::MessengerConfig,
    serde::Deserialize,
    std::{collections::HashMap, net::SocketAddr},
    yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterBlocksMeta},
};

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub prometheus: Option<SocketAddr>,

    pub endpoint: String,
    pub x_token: Option<String>,

    pub accounts: Vec<ConfigGrpcRequestAccounts>,
    pub transactions: Vec<ConfigGrpcRequestTransactions>,
    pub commitment: ConfigGrpcRequestCommitment,

    #[serde(default = "Config::default_num_workers")]
    pub num_workers: usize,

    #[serde(
        default = "Config::default_account_stream_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub account_stream_size: usize,
    #[serde(
        default = "Config::default_slot_stream_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub slot_stream_size: usize,
    #[serde(
        default = "Config::default_transaction_stream_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub transaction_stream_size: usize,
    #[serde(
        default = "Config::default_block_stream_size",
        deserialize_with = "deserialize_usize_str"
    )]
    pub block_stream_size: usize,

    pub messenger_config: MessengerConfig,
}

impl Config {
    const fn default_num_workers() -> usize {
        5
    }

    const fn default_account_stream_size() -> usize {
        100_000_000
    }

    const fn default_slot_stream_size() -> usize {
        100_000
    }

    const fn default_transaction_stream_size() -> usize {
        10_000_000
    }

    const fn default_block_stream_size() -> usize {
        100_000
    }

    pub fn create_request(&self) -> SubscribeRequest {
        let mut accounts = HashMap::new();
        for (i, item) in self.accounts.iter().enumerate() {
            accounts.insert(i.to_string(), item.clone().to_proto());
        }

        let mut transactions = HashMap::new();
        for (i, item) in self.transactions.iter().enumerate() {
            transactions.insert(i.to_string(), item.clone().to_proto());
        }

        SubscribeRequest {
            slots: HashMap::new(),
            accounts,
            transactions,
            entry: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta {} },
            commitment: Some(self.commitment.to_proto() as i32),
            accounts_data_slice: vec![],
            ping: None,
        }
    }
}
