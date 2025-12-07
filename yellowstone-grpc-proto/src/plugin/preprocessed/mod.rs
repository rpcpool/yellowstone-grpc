use std::{collections::{HashMap, HashSet}, sync::Arc, time::SystemTime};

use bytes::{Buf, BufMut};
use prost_types::Timestamp;
use smallvec::SmallVec;
use solana_clock::Slot;
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_transaction::{simple_vote_transaction_checker::is_simple_vote_transaction, versioned::{VersionedTransaction, sanitized::SanitizedVersionedTransaction}};

use crate::{convert_to::{create_header, create_instruction, create_lookup}, geyser::{SubscribePreprocessedRequest, SubscribePreprocessedRequestFilterTransactions, SubscribePreprocessedTransaction, SubscribePreprocessedTransactionInfo, SubscribeUpdatePong}, plugin::filter::{FilterError, FilterResult, filter::{FilterTransactionsPreprocessedInner, check_preprocessed_fields, filter_transactions_inner}, limits::{FilterLimits, FilterLimitsTransactions}, name::{FilterName, FilterNames}}, prelude::Transaction, prost_repeated_encoded_len_map};
use prost::{DecodeError, encoding::{DecodeContext, WireType}};
use solana_entry::entry::Entry as SolanaEntry;
use crate::prelude::Message as SolanaStorageMessage;
use solana_signature::Signature;
use prost::Message;
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, message};


pub type FilteredPreprocessedUpdateFilters = SmallVec<[FilterName; 4]>;

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredPreprocessedUpdate {
    pub filters: FilteredPreprocessedUpdateFilters,
    pub message: FilteredPreprocessedUpdateOneof,
    pub created_at: Timestamp,
}


// TODO: Double check encoding logic 
impl prost::Message for FilteredPreprocessedUpdate {

    fn encode_raw(&self, buf: &mut impl BufMut) {
        for name in self.filters.iter().map(|filter| filter.as_ref()) {
            encode_key(1u32, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        self.message.encode_raw(buf);
        message::encode(11u32, &self.created_at, buf);
    }

    fn encoded_len(&self) -> usize {
        prost_repeated_encoded_len_map!(1u32, self.filters, |filter| filter.as_ref().len())
            + self.message.encoded_len()
            + message::encoded_len(11u32, &self.created_at)
    }

    fn merge_field(&mut self, _tag: u32, _wire_type: WireType, _buf: &mut impl Buf, _ctx: DecodeContext) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}
impl FilteredPreprocessedUpdate {
    pub fn new_empty(one_of: FilteredPreprocessedUpdateOneof) -> Self {
        Self { filters: FilteredPreprocessedUpdateFilters::new(), message: one_of, created_at: Timestamp::from(SystemTime::now()) }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilteredPreprocessedUpdateOneof {
    Ping,
    Pong(SubscribeUpdatePong),
    PreprocessedTransaction(Arc<SubscribePreprocessedTransaction>)
}

impl FilteredPreprocessedUpdateOneof {
    pub const fn ping() -> Self {
        Self::Ping
    }
}



impl prost::Message for FilteredPreprocessedUpdateOneof {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            FilteredPreprocessedUpdateOneof::Ping => {
                encode_key(6u32, WireType::LengthDelimited, buf);
                encode_varint(0, buf);
            }
            FilteredPreprocessedUpdateOneof::Pong(msg) => message::encode(9u32, msg, buf),
            FilteredPreprocessedUpdateOneof::PreprocessedTransaction(msg) => message::encode(4u32, msg.as_ref(), buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            FilteredPreprocessedUpdateOneof::Ping => key_len(6u32) + encoded_len_varint(0),
            FilteredPreprocessedUpdateOneof::Pong(msg) => message::encoded_len(9u32, msg),
            FilteredPreprocessedUpdateOneof::PreprocessedTransaction(msg) => message::encoded_len(4u32, msg.as_ref()),
        }
    }

    fn merge_field(&mut self, _tag: u32, _wire_type: WireType, _buf: &mut impl Buf, _ctx: DecodeContext) -> Result<(), DecodeError> {
        todo!()
    }

    fn clear(&mut self) {
        todo!()
    }
}


#[derive(Debug, Clone, Default)]
pub struct FilterTransactionsPreprocessed {
    filters: HashMap<FilterName, FilterTransactionsPreprocessedInner>,
}


impl FilterTransactionsPreprocessed {
    pub fn new(configs: &HashMap<String, SubscribePreprocessedRequestFilterTransactions>, limits: &FilterLimitsTransactions, names: &mut FilterNames) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;
        let mut filters = HashMap::new();
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.vote.is_none()
                    && filter.account_include.is_empty()
                    && filter.account_exclude.is_empty()
                    && filter.account_required.is_empty(),
                limits.any,
            )?;
            check_preprocessed_fields(&filter.account_include, &filter.account_exclude, &filter.account_required, limits)?;
            filters.insert(names.get(name)?, FilterTransactionsPreprocessedInner::new(filter)?);
        }
        Ok(Self { filters })
    }
}

impl FilterTransactionsPreprocessed {
    pub fn get_updates(&self, message: &PreprocessedEntries) -> Vec<FilteredPreprocessedUpdate> {
        let mut transactions = Vec::new();
        let slot = message.slot;
        for entry in &message.entries {
            for transaction in &entry.transactions {
                let filters = self
                    .filters
                    .iter()
                    .filter_map(|(name, inner)| {
                        if !filter_transactions_inner(inner, &transaction.account_keys, transaction.is_vote, &transaction.transaction.signatures) {
                            return None;
                        }
                        Some(name.clone())
                    })
                    .collect::<FilteredPreprocessedUpdateFilters>();
                if filters.is_empty() {
                    continue;
                }
                let transaction = SubscribePreprocessedTransaction{ 
                    transaction: Some(
                        SubscribePreprocessedTransactionInfo{ 
                            signature: transaction.signature.as_array().into(), 
                            is_vote: transaction.is_vote, transaction: 
                            Some(transaction.transaction.clone()) 
                    }), 
                    slot
                };
                transactions.push(FilteredPreprocessedUpdate::new_empty(FilteredPreprocessedUpdateOneof::PreprocessedTransaction(Arc::new(transaction))));
            }
        }
        transactions
    }
}

#[derive(Debug, Clone, Default)]
pub struct FilterPreprocessed {
    transactions: FilterTransactionsPreprocessed,
    ping: Option<i32>,
}


impl FilterPreprocessed {
    pub fn get_updates(&self, preprocessed_entries: &PreprocessedEntries) -> Vec<FilteredPreprocessedUpdate> {
        return self.transactions.get_updates(preprocessed_entries);
    }

    pub fn new(config: &SubscribePreprocessedRequest, limits: &FilterLimitsTransactions, names: &mut FilterNames) -> Result<Self, FilterError> {
        Ok(Self { 
            ping: config.ping.as_ref().map(|msg| msg.id),
            transactions: FilterTransactionsPreprocessed::new(&config.transactions, limits, names)?,
        })
    }

    pub fn get_pong_msg(&self) -> Option<FilteredPreprocessedUpdate> {
        if let Some(ping) = self.ping {
            return Some(FilteredPreprocessedUpdate::new_empty(FilteredPreprocessedUpdateOneof::Pong(SubscribeUpdatePong { id: ping })));
        }
        None
    }
}


#[derive(Debug, Clone)]
pub struct PreprocessedEntries {
    pub created_at: Timestamp,
    pub slot: Slot,
    pub entries: Vec<PreprocessedEntry>,
}

impl PreprocessedEntries {
    pub fn new(slot: Slot, entries: Vec<SolanaEntry>) -> Self {
        let created_at = Timestamp::from(SystemTime::now());
        let entries = entries
            .into_iter()
            .map(|entry| PreprocessedEntry::new(slot, created_at, entry))
            .collect();
        Self {
            created_at,
            slot,
            entries,
        }
    }

    pub fn size(&self) -> usize {
        let entries_size = self.entries.iter().map(|e| e.size()).sum::<usize>();
        let num_bytes_in_slot = 8;
        let num_bytes_in_created_at = 8;
        entries_size + num_bytes_in_slot + num_bytes_in_created_at
    }
}

#[derive(Debug, Clone)]
pub struct PreprocessedTransaction {
    pub transaction: Transaction,
    pub is_vote: bool,
    // Precomputed for faster lookup
    pub signature: Signature,
    pub account_keys: HashSet<Pubkey>,
    pub slot: Slot,
    pub created_at: Timestamp,
}

impl PreprocessedTransaction {
    pub fn size(&self) -> usize {
        self.transaction.encoded_len() as usize
    }
}

#[derive(Debug, Clone)]
pub struct PreprocessedEntry {
    pub num_hashes: u64,
    pub hash: String,
    pub transactions: Vec<Arc<PreprocessedTransaction>>,
}

impl PreprocessedEntry {
    pub fn new(slot: Slot, created_at: Timestamp, entry: SolanaEntry) -> Self {
        Self {
            num_hashes: entry.num_hashes,
            hash: entry.hash.to_string(),
            transactions: entry
                .transactions
                .iter()
                .map(|transaction| {
                    let sanitized_transaction =
                        SanitizedVersionedTransaction::try_from(transaction.clone()).unwrap();
                    let is_vote = is_simple_vote_transaction(&sanitized_transaction);
                    let account_keys = transaction.message.static_account_keys();
                    let signature = transaction.signatures[0];
                    let transaction = create_transaction(&transaction);
                    Arc::new(PreprocessedTransaction {
                        is_vote,
                        signature,
                        account_keys: account_keys.iter().copied().collect(),
                        transaction: transaction.clone(),
                        slot,
                        created_at,
                    })
                })
                .collect(),
        }
    }

    fn size(&self) -> usize {
        let transactions_size = self.transactions.iter().map(|t| t.size()).sum::<usize>();
        let num_bytes_in_num_hashes = 8;
        let num_bytes_in_hash = 32;
        transactions_size + num_bytes_in_num_hashes + num_bytes_in_hash as usize
    }
}

pub fn create_transaction(tx: &VersionedTransaction) -> Transaction {
    Transaction {
        signatures: tx
            .signatures
            .iter()
            .map(|signature| <Signature as AsRef<[u8]>>::as_ref(signature).into())
            .collect(),
        message: Some(create_message(&tx.message)),
    }
}

pub fn create_message(message: &VersionedMessage) -> SolanaStorageMessage {
    let versioned = match &message {
        VersionedMessage::Legacy(_) => false,
        VersionedMessage::V0(_) => true,
    };
    SolanaStorageMessage {
        header: Some(create_header(&message.header())),
        account_keys: message
            .static_account_keys()
            .into_iter()
            .map(|k| k.to_bytes().to_vec())
            .collect(),
        recent_blockhash: message.recent_blockhash().to_bytes().into(),
        instructions: message
            .instructions()
            .into_iter()
            .map(|i| create_instruction(i))
            .collect(),
        versioned,
        address_table_lookups: message
            .address_table_lookups()
            .unwrap_or_default()
            .into_iter()
            .map(|l| create_lookup(l))
            .collect(),
    }
}
