use {
    super::geyser::{
        MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry, MessageSlot,
        MessageTransaction, MessageTransactionInfo,
    },
    bytes::buf::BufMut,
    prost::encoding::{encode_key, encode_varint, message, WireType},
    smallvec::SmallVec,
    std::{
        borrow::Borrow,
        collections::HashSet,
        ops::Range,
        sync::Arc,
        time::{Duration, Instant},
    },
    tonic::{codec::EncodeBuf, Status},
};

#[derive(Debug, thiserror::Error)]
pub enum FilterNameError {
    #[error("oversized filter name (max allowed size {limit}), found {size}")]
    Oversized { limit: usize, size: usize },
}

pub type FilterNameResult<T> = Result<T, FilterNameError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterName(Arc<String>);

impl AsRef<str> for FilterName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for FilterName {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0[..]
    }
}

impl FilterName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Arc::new(name.into()))
    }

    pub fn is_uniq(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }
}

#[derive(Debug)]
pub struct FilterNames {
    name_size_limit: usize,
    names: HashSet<FilterName>,
    names_size_limit: usize,
    cleanup_ts: Instant,
    cleanup_interval: Duration,
}

impl FilterNames {
    pub fn new(
        name_size_limit: usize,
        names_size_limit: usize,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            name_size_limit,
            names: HashSet::with_capacity(names_size_limit),
            names_size_limit,
            cleanup_ts: Instant::now(),
            cleanup_interval,
        }
    }

    pub fn try_clean(&mut self) {
        if self.names.len() > self.names_size_limit
            && self.cleanup_ts.elapsed() > self.cleanup_interval
        {
            self.names.retain(|name| !name.is_uniq());
            self.cleanup_ts = Instant::now();
        }
    }

    pub fn get(&mut self, name: &str) -> FilterNameResult<FilterName> {
        match self.names.get(name) {
            Some(name) => Ok(name.clone()),
            None => {
                if name.len() > self.name_size_limit {
                    Err(FilterNameError::Oversized {
                        limit: self.name_size_limit,
                        size: name.len(),
                    })
                } else {
                    let name = FilterName::new(name);
                    self.names.insert(name.clone());
                    Ok(name)
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Message {
    pub filters: MessageFilters, // 1
    pub message: MessageWeak,    // 2, 3, 4, 10, 5, 6, 9, 7, 8
}

impl Message {
    pub fn new(filters: MessageFilters, message: MessageWeak) -> Self {
        Self { filters, message }
    }

    pub fn encode(self, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
        for name in self.filters.iter().map(|filter| filter.as_ref()) {
            encode_key(1, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        self.message.encode(buf)
    }
}

pub type MessageFilters = SmallVec<[FilterName; 4]>;

#[derive(Debug)]
pub enum MessageWeak {
    Account,                 // 2
    Slot,                    // 3
    Transaction,             // 4
    TransactionStatus,       // 10
    Block,                   // 5
    Ping,                    // 6
    Pong(MessageWeakPong),   // 9
    BlockMeta,               // 7
    Entry(MessageWeakEntry), // 8
}

impl MessageWeak {
    pub fn account(message: &MessageAccount, accounts_data_slice: Vec<Range<usize>>) -> Self {
        todo!()
    }

    pub fn slot(message: MessageSlot) -> Self {
        todo!()
    }

    pub fn transaction(message: &MessageTransaction) -> Self {
        todo!()
    }

    pub fn transaction_status(message: &MessageTransaction) -> Self {
        todo!()
    }

    pub fn block(message: MessageWeakBlock) -> Self {
        todo!()
    }

    pub const fn pong(id: i32) -> Self {
        Self::Pong(MessageWeakPong { id })
    }

    pub fn block_meta(message: &Arc<MessageBlockMeta>) -> Self {
        todo!()
    }

    pub fn entry(message: &Arc<MessageEntry>) -> Self {
        Self::Entry(MessageWeakEntry(Arc::clone(message)))
    }

    pub fn encode(self, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
        match self {
            MessageWeak::Account => todo!(),
            MessageWeak::Slot => todo!(),
            MessageWeak::Transaction => todo!(),
            MessageWeak::TransactionStatus => todo!(),
            MessageWeak::Block => todo!(),
            MessageWeak::Ping => encode_key(6, WireType::LengthDelimited, buf),
            MessageWeak::Pong(msg) => message::encode(9, &msg, buf),
            MessageWeak::BlockMeta => todo!(),
            MessageWeak::Entry(msg) => todo!(),
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct MessageWeakBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransactionInfo>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccountInfo>>,
    pub accounts_data_slice: Vec<Range<usize>>,
    pub entries: Vec<Arc<MessageEntry>>,
}

#[derive(prost::Message)]
pub struct MessageWeakPong {
    #[prost(int32, tag = "1")]
    pub id: i32,
}

#[derive(Debug)]
pub struct MessageWeakEntry(Arc<MessageEntry>);

// #[derive(Debug, Clone)]
// pub enum FilteredMessage2<'a> {
//     Slot(&'a MessageSlot),
//     Account(&'a MessageAccount),
//     Transaction(&'a MessageTransaction),
//     TransactionStatus(&'a MessageTransaction),
//     Entry(&'a MessageEntry),
//     Block(MessageBlock),
//     BlockMeta(&'a MessageBlockMeta),
// }

// impl<'a> FilteredMessage2<'a> {
//     fn as_proto_account(
//         message: &MessageAccountInfo,
//         accounts_data_slice: &[Range<usize>],
//     ) -> SubscribeUpdateAccountInfo {
//         let data = if accounts_data_slice.is_empty() {
//             message.data.clone()
//         } else {
//             let mut data =
//                 Vec::with_capacity(accounts_data_slice.iter().map(|s| s.end - s.start).sum());
//             for slice in accounts_data_slice {
//                 if message.data.len() >= slice.end {
//                     data.extend_from_slice(&message.data[slice.start..slice.end]);
//                 }
//             }
//             data
//         };
//         SubscribeUpdateAccountInfo {
//             pubkey: message.pubkey.as_ref().into(),
//             lamports: message.lamports,
//             owner: message.owner.as_ref().into(),
//             executable: message.executable,
//             rent_epoch: message.rent_epoch,
//             data,
//             write_version: message.write_version,
//             txn_signature: message.txn_signature.map(|s| s.as_ref().into()),
//         }
//     }

//     fn as_proto_transaction(message: &MessageTransactionInfo) -> SubscribeUpdateTransactionInfo {
//         SubscribeUpdateTransactionInfo {
//             signature: message.signature.as_ref().into(),
//             is_vote: message.is_vote,
//             transaction: Some(convert_to::create_transaction(&message.transaction)),
//             meta: Some(convert_to::create_transaction_meta(&message.meta)),
//             index: message.index as u64,
//         }
//     }

//     fn as_proto_entry(message: &MessageEntry) -> SubscribeUpdateEntry {
//         SubscribeUpdateEntry {
//             slot: message.slot,
//             index: message.index as u64,
//             num_hashes: message.num_hashes,
//             hash: message.hash.into(),
//             executed_transaction_count: message.executed_transaction_count,
//             starting_transaction_index: message.starting_transaction_index,
//         }
//     }

//     pub fn as_proto(&self, accounts_data_slice: &[Range<usize>]) -> UpdateOneof {
//         match self {
//             Self::Slot(message) => UpdateOneof::Slot(SubscribeUpdateSlot {
//                 slot: message.slot,
//                 parent: message.parent,
//                 status: message.status as i32,
//             }),
//             Self::Account(message) => UpdateOneof::Account(SubscribeUpdateAccount {
//                 account: Some(Self::as_proto_account(
//                     message.account.as_ref(),
//                     accounts_data_slice,
//                 )),
//                 slot: message.slot,
//                 is_startup: message.is_startup,
//             }),
//             Self::Transaction(message) => UpdateOneof::Transaction(SubscribeUpdateTransaction {
//                 transaction: Some(Self::as_proto_transaction(message.transaction.as_ref())),
//                 slot: message.slot,
//             }),
//             Self::TransactionStatus(message) => {
//                 UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
//                     slot: message.slot,
//                     signature: message.transaction.signature.as_ref().into(),
//                     is_vote: message.transaction.is_vote,
//                     index: message.transaction.index as u64,
//                     err: match &message.transaction.meta.status {
//                         Ok(()) => None,
//                         Err(err) => Some(SubscribeUpdateTransactionError {
//                             err: bincode::serialize(&err)
//                                 .expect("transaction error to serialize to bytes"),
//                         }),
//                     },
//                 })
//             }
//             Self::Entry(message) => UpdateOneof::Entry(Self::as_proto_entry(message)),
//             Self::Block(message) => UpdateOneof::Block(SubscribeUpdateBlock {
//                 slot: message.meta.slot,
//                 blockhash: message.meta.blockhash.clone(),
//                 rewards: Some(convert_to::create_rewards_obj(
//                     message.meta.rewards.as_slice(),
//                     message.meta.num_partitions,
//                 )),
//                 block_time: message.meta.block_time.map(convert_to::create_timestamp),
//                 block_height: message
//                     .meta
//                     .block_height
//                     .map(convert_to::create_block_height),
//                 parent_slot: message.meta.parent_slot,
//                 parent_blockhash: message.meta.parent_blockhash.clone(),
//                 executed_transaction_count: message.meta.executed_transaction_count,
//                 transactions: message
//                     .transactions
//                     .iter()
//                     .map(|tx| Self::as_proto_transaction(tx.as_ref()))
//                     .collect(),
//                 updated_account_count: message.updated_account_count,
//                 accounts: message
//                     .accounts
//                     .iter()
//                     .map(|acc| Self::as_proto_account(acc.as_ref(), accounts_data_slice))
//                     .collect(),
//                 entries_count: message.meta.entries_count,
//                 entries: message
//                     .entries
//                     .iter()
//                     .map(|entry| Self::as_proto_entry(entry.as_ref()))
//                     .collect(),
//             }),
//             Self::BlockMeta(message) => UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
//                 slot: message.slot,
//                 blockhash: message.blockhash.clone(),
//                 rewards: Some(convert_to::create_rewards_obj(
//                     message.rewards.as_slice(),
//                     message.num_partitions,
//                 )),
//                 block_time: message.block_time.map(convert_to::create_timestamp),
//                 block_height: message.block_height.map(convert_to::create_block_height),
//                 parent_slot: message.parent_slot,
//                 parent_blockhash: message.parent_blockhash.clone(),
//                 executed_transaction_count: message.executed_transaction_count,
//                 entries_count: message.entries_count,
//             }),
//         }
//     }
// }
