//! Subscriber profiles: a JSON list of subscribers, each with a role, a compression choice and a request.

use {
    super::link::LinkSpec,
    serde::{Deserialize, Serialize},
    std::collections::{BTreeMap, HashMap},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterEntry,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Measured: every message is timestamped and kept.
    Probe,
    /// Load only: bytes and messages are counted, nothing is decoded.
    Load,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    None,
    Zstd,
    Gzip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountsFilter {
    #[serde(default)]
    pub account: Vec<String>,
    #[serde(default)]
    pub owner: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionsFilter {
    #[serde(default)]
    pub vote: Option<bool>,
    #[serde(default)]
    pub failed: Option<bool>,
    #[serde(default)]
    pub account_include: Vec<String>,
    #[serde(default)]
    pub account_exclude: Vec<String>,
    #[serde(default)]
    pub account_required: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlocksFilter {
    #[serde(default)]
    pub account_include: Vec<String>,
    #[serde(default)]
    pub include_transactions: Option<bool>,
    #[serde(default)]
    pub include_accounts: Option<bool>,
    #[serde(default)]
    pub include_entries: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(default)]
    pub commitment: Commitment,
    #[serde(default)]
    pub accounts: BTreeMap<String, AccountsFilter>,
    #[serde(default)]
    pub transactions: BTreeMap<String, TransactionsFilter>,
    #[serde(default)]
    pub transactions_status: BTreeMap<String, TransactionsFilter>,
    /// Slot updates; `true` adds interslot updates.
    #[serde(default)]
    pub slots: Option<bool>,
    #[serde(default)]
    pub entries: bool,
    #[serde(default)]
    pub blocks_meta: bool,
    #[serde(default)]
    pub blocks: BTreeMap<String, BlocksFilter>,
    /// Deshred subscription filters; when set the subscriber uses SubscribeDeshred.
    #[serde(default)]
    pub deshred: Option<TransactionsFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscriber {
    pub name: String,
    /// Probe class used to group results, e.g. `streamlab`.
    pub class: String,
    pub role: Role,
    pub compression: Compression,
    #[serde(default = "default_count")]
    pub count: usize,
    pub request: Request,
    /// Emulated network between this subscriber and the plugin; local when unset.
    #[serde(default)]
    pub link: Option<LinkSpec>,
    /// `x-subscription-id` of the stream; streams sharing one id model one customer.
    #[serde(default)]
    pub subscription_id: Option<String>,
    /// Periodic reconnects that resume with `from_slot`.
    #[serde(default)]
    pub churn: Option<Churn>,
    /// HTTP/2 stream and connection windows of this client in KiB; the fleet's own when unset.
    #[serde(default)]
    pub windows_kib: Option<[u32; 2]>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Churn {
    /// Mean seconds between reconnects; each interval is drawn from 0.5x to 1.5x of it.
    pub every_secs: f64,
    /// Resume this many slots behind the newest slot the probes have seen; 0 resumes live.
    #[serde(default)]
    pub from_slot_back: u64,
}

const fn default_count() -> usize {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    #[serde(default)]
    pub description: String,
    pub subscribers: Vec<Subscriber>,
    /// Unary calls the fleet makes alongside the streams.
    #[serde(default)]
    pub unary: Vec<UnaryLoad>,
}

/// Unary calls of one method at a steady rate, spread over a few local connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnaryLoad {
    /// Method as the plugin's `method_call_count` labels it, e.g. `get_slot`.
    pub method: String,
    pub rate_per_sec: f64,
    pub connections: usize,
}

#[allow(clippy::large_enum_variant)]
pub enum BuiltRequest {
    Subscribe(SubscribeRequest),
    Deshred(SubscribeDeshredRequest),
}

impl Request {
    pub fn build(&self) -> BuiltRequest {
        if let Some(filter) = &self.deshred {
            return BuiltRequest::Deshred(SubscribeDeshredRequest {
                deshred_transactions: HashMap::from([(
                    "deshred".to_owned(),
                    SubscribeRequestFilterDeshredTransactions {
                        vote: filter.vote,
                        account_include: filter.account_include.clone(),
                        account_exclude: filter.account_exclude.clone(),
                        account_required: filter.account_required.clone(),
                        include_update_parent: None,
                    },
                )]),
                ping: None,
                slots: match self.slots {
                    Some(interslot) => {
                        HashMap::from([("slots".to_owned(), slots_filter(interslot))])
                    }
                    None => HashMap::new(),
                },
            });
        }
        let commitment = match self.commitment {
            Commitment::Processed => CommitmentLevel::Processed,
            Commitment::Confirmed => CommitmentLevel::Confirmed,
            Commitment::Finalized => CommitmentLevel::Finalized,
        };
        BuiltRequest::Subscribe(SubscribeRequest {
            accounts: self
                .accounts
                .iter()
                .map(|(name, filter)| {
                    (
                        name.clone(),
                        SubscribeRequestFilterAccounts {
                            account: filter.account.clone(),
                            owner: filter.owner.clone(),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            transactions: transactions_filters(&self.transactions),
            transactions_status: transactions_filters(&self.transactions_status),
            slots: match self.slots {
                Some(interslot) => HashMap::from([("slots".to_owned(), slots_filter(interslot))]),
                None => HashMap::new(),
            },
            entry: if self.entries {
                HashMap::from([("entries".to_owned(), SubscribeRequestFilterEntry::default())])
            } else {
                HashMap::new()
            },
            blocks_meta: if self.blocks_meta {
                HashMap::from([(
                    "blocks_meta".to_owned(),
                    SubscribeRequestFilterBlocksMeta {},
                )])
            } else {
                HashMap::new()
            },
            blocks: self
                .blocks
                .iter()
                .map(|(name, filter)| {
                    (
                        name.clone(),
                        SubscribeRequestFilterBlocks {
                            account_include: filter.account_include.clone(),
                            include_transactions: filter.include_transactions,
                            include_accounts: filter.include_accounts,
                            include_entries: filter.include_entries,
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            commitment: Some(commitment as i32),
            ..Default::default()
        })
    }
}

fn transactions_filters(
    filters: &BTreeMap<String, TransactionsFilter>,
) -> HashMap<String, SubscribeRequestFilterTransactions> {
    filters
        .iter()
        .map(|(name, filter)| {
            (
                name.clone(),
                SubscribeRequestFilterTransactions {
                    vote: filter.vote,
                    failed: filter.failed,
                    account_include: filter.account_include.clone(),
                    account_exclude: filter.account_exclude.clone(),
                    account_required: filter.account_required.clone(),
                    ..Default::default()
                },
            )
        })
        .collect()
}

const fn slots_filter(interslot: bool) -> SubscribeRequestFilterSlots {
    SubscribeRequestFilterSlots {
        filter_by_commitment: Some(false),
        interslot_updates: Some(interslot),
    }
}
