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

impl prost::Message for FilteredPreprocessedUpdate {

    fn encode_raw(&self, buf: &mut impl BufMut) {
        for name in self.filters.iter().map(|filter| filter.as_ref()) {
            encode_key(1u32, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        self.message.encode_raw(buf);
        message::encode(5u32, &self.created_at, buf);
    }

    fn encoded_len(&self) -> usize {
        prost_repeated_encoded_len_map!(1u32, self.filters, |filter| filter.as_ref().len())
            + self.message.encoded_len()
            + message::encoded_len(5u32, &self.created_at)
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
                encode_key(3u32, WireType::LengthDelimited, buf);
                encode_varint(0, buf);
            }
            FilteredPreprocessedUpdateOneof::Pong(msg) => message::encode(4u32, msg, buf),
            FilteredPreprocessedUpdateOneof::PreprocessedTransaction(msg) => message::encode(2u32, msg.as_ref(), buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            FilteredPreprocessedUpdateOneof::Ping => key_len(3u32) + encoded_len_varint(0),
            FilteredPreprocessedUpdateOneof::Pong(msg) => message::encoded_len(4u32, msg),
            FilteredPreprocessedUpdateOneof::PreprocessedTransaction(msg) => message::encoded_len(2u32, msg.as_ref()),
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
        let created_at = message.created_at;
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
                transactions.push(FilteredPreprocessedUpdate{ filters, message: FilteredPreprocessedUpdateOneof::PreprocessedTransaction(Arc::new(transaction)), created_at });
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            geyser::{
                SubscribePreprocessedRequest, SubscribePreprocessedRequestFilterTransactions,
                SubscribeRequestPing, SubscribeUpdatePong,
            },
            plugin::filter::{
                limits::FilterLimitsTransactions,
                name::{FilterName, FilterNames},
            },
        },
        prost_types::Timestamp,
        solana_entry::entry::Entry as SolanaEntry,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::{Message as SolMessage, MessageHeader},
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_transaction::{versioned::VersionedTransaction, Transaction as SolanaTransaction},
        std::{
            collections::HashMap,
            sync::Arc,
            time::{Duration, SystemTime},
        },
    };

    fn create_filter_names() -> FilterNames {
        FilterNames::new(64, 1024, Duration::from_secs(1))
    }

    fn create_preprocessed_transaction(
        keypair: &Keypair,
        account_keys: Vec<Pubkey>,
        is_vote: bool,
    ) -> PreprocessedTransaction {
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys,
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let versioned_transaction = VersionedTransaction::from(SolanaTransaction::new(
            &[keypair],
            message,
            recent_blockhash,
        ));
        let signature = versioned_transaction.signatures[0];
        let transaction = create_transaction(&versioned_transaction);
        let account_keys_set: HashSet<Pubkey> = versioned_transaction
            .message
            .static_account_keys()
            .iter()
            .copied()
            .collect();

        PreprocessedTransaction {
            transaction,
            is_vote,
            signature,
            account_keys: account_keys_set,
            slot: 100,
            created_at: Timestamp::from(SystemTime::now()),
        }
    }

    fn create_preprocessed_entry(
        _slot: Slot,
        transactions: Vec<Arc<PreprocessedTransaction>>,
    ) -> PreprocessedEntry {
        PreprocessedEntry {
            num_hashes: 0,
            hash: Hash::default().to_string(),
            transactions,
        }
    }

    fn create_preprocessed_entries(
        slot: Slot,
        entries: Vec<PreprocessedEntry>,
    ) -> PreprocessedEntries {
        PreprocessedEntries {
            created_at: Timestamp::from(SystemTime::now()),
            slot,
            entries,
        }
    }

    fn create_filters(names: &[&str]) -> FilteredPreprocessedUpdateFilters {
        let mut filters = FilteredPreprocessedUpdateFilters::new();
        for name in names {
            filters.push(FilterName::new(*name));
        }
        filters
    }

    #[test]
    fn test_filter_preprocessed_all_empty() {
        // ensure FilterPreprocessed can be created with empty values
        let config = SubscribePreprocessedRequest {
            transactions: HashMap::new(),
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names());
        assert!(filter.is_ok());
    }

    #[test]
    fn test_filter_preprocessed_transaction_empty() {
        let mut transactions = HashMap::new();

        transactions.insert(
            "serum".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: None,
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let mut limit = FilterLimitsTransactions::default();
        limit.any = false;
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names());
        // filter should fail
        assert!(filter.is_err());
    }

    #[test]
    fn test_filter_preprocessed_transaction_not_empty() {
        let mut transactions = HashMap::new();
        transactions.insert(
            "serum".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: Some(true),
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let mut limit = FilterLimitsTransactions::default();
        limit.any = false;
        let filter_res = FilterPreprocessed::new(&config, &limit, &mut create_filter_names());
        // filter should succeed
        assert!(filter_res.is_ok());
    }

    #[test]
    fn test_filter_preprocessed_get_pong_msg() {
        let config = SubscribePreprocessedRequest {
            transactions: HashMap::new(),
            ping: Some(SubscribeRequestPing { id: 42 }),
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let pong_msg = filter.get_pong_msg();
        assert!(pong_msg.is_some());
        let pong = pong_msg.unwrap();
        match pong.message {
            FilteredPreprocessedUpdateOneof::Pong(msg) => assert_eq!(msg.id, 42),
            _ => panic!("Expected Pong message"),
        }
    }

    #[test]
    fn test_filter_preprocessed_get_pong_msg_none() {
        let config = SubscribePreprocessedRequest {
            transactions: HashMap::new(),
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let pong_msg = filter.get_pong_msg();
        assert!(pong_msg.is_none());
    }

    #[test]
    fn test_filter_transactions_preprocessed_include() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = [account_key_a].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let preprocessed_tx = Arc::new(create_preprocessed_transaction(
            &keypair_b,
            vec![account_key_b, account_key_a],
            false,
        ));
        let entry = create_preprocessed_entry(100, vec![preprocessed_tx.clone()]);
        let entries = create_preprocessed_entries(100, vec![entry]);

        let updates = filter.get_updates(&entries);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            create_filters(&["serum"])
        );
        assert!(matches!(
            updates[0].message,
            FilteredPreprocessedUpdateOneof::PreprocessedTransaction(_)
        ));
    }

    #[test]
    fn test_filter_transactions_preprocessed_exclude() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_exclude = [account_key_b].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: None,
                signature: None,
                account_include: vec![],
                account_exclude,
                account_required: vec![],
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let preprocessed_tx = Arc::new(create_preprocessed_transaction(
            &keypair_b,
            vec![account_key_b, account_key_a],
            false,
        ));
        let entry = create_preprocessed_entry(100, vec![preprocessed_tx]);
        let entries = create_preprocessed_entries(100, vec![entry]);

        let updates = filter.get_updates(&entries);
        // Transaction should be excluded
        assert_eq!(updates.len(), 0);
    }

    #[test]
    fn test_filter_transactions_preprocessed_vote() {
        let mut transactions = HashMap::new();
        transactions.insert(
            "serum".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: Some(true),
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let keypair = Keypair::new();
        let preprocessed_tx = Arc::new(create_preprocessed_transaction(
            &keypair,
            vec![keypair.pubkey()],
            true, // is_vote = true
        ));
        let entry = create_preprocessed_entry(100, vec![preprocessed_tx.clone()]);
        let entries = create_preprocessed_entries(100, vec![entry]);

        let updates = filter.get_updates(&entries);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            create_filters(&["serum"])
        );
    }

    #[test]
    fn test_filter_transactions_preprocessed_vote_excluded() {
        let mut transactions = HashMap::new();
        transactions.insert(
            "serum".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: Some(true),
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let keypair = Keypair::new();
        let preprocessed_tx = Arc::new(create_preprocessed_transaction(
            &keypair,
            vec![keypair.pubkey()],
            false, // is_vote = false, should be excluded
        ));
        let entry = create_preprocessed_entry(100, vec![preprocessed_tx]);
        let entries = create_preprocessed_entries(100, vec![entry]);

        let updates = filter.get_updates(&entries);
        assert_eq!(updates.len(), 0);
    }

    #[test]
    fn test_filter_transactions_preprocessed_required() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        let account_include = [account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        let account_required = [account_key_x].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required,
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let preprocessed_tx = Arc::new(create_preprocessed_transaction(
            &keypair_x,
            vec![account_key_x, account_key_y, account_key_z],
            false,
        ));
        let entry = create_preprocessed_entry(100, vec![preprocessed_tx.clone()]);
        let entries = create_preprocessed_entries(100, vec![entry]);

        let updates = filter.get_updates(&entries);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            create_filters(&["serum"])
        );
    }

    #[test]
    fn test_preprocessed_entries_new() {
        let slot = 100u64;
        let solana_entries = vec![SolanaEntry {
            num_hashes: 42,
            hash: Hash::default(),
            transactions: vec![],
        }];

        let entries = PreprocessedEntries::new(slot, solana_entries);
        assert_eq!(entries.slot, slot);
        assert_eq!(entries.entries.len(), 1);
    }

    #[test]
    fn test_preprocessed_entries_size() {
        let slot = 100u64;
        let keypair = Keypair::new();
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey()],
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let versioned_transaction = VersionedTransaction::from(SolanaTransaction::new(
            &[&keypair],
            message,
            recent_blockhash,
        ));

        let solana_entries = vec![SolanaEntry {
            num_hashes: 42,
            hash: Hash::default(),
            transactions: vec![versioned_transaction],
        }];

        let entries = PreprocessedEntries::new(slot, solana_entries);
        let size = entries.size();
        assert!(size > 0);
    }

    #[test]
    fn test_preprocessed_transaction_size() {
        let keypair = Keypair::new();
        let preprocessed_tx = create_preprocessed_transaction(&keypair, vec![keypair.pubkey()], false);
        let size = preprocessed_tx.size();
        assert!(size > 0);
    }

    #[test]
    fn test_filtered_preprocessed_update_new_empty() {
        let update = FilteredPreprocessedUpdate::new_empty(
            FilteredPreprocessedUpdateOneof::Ping,
        );
        assert!(update.filters.is_empty());
        assert!(matches!(update.message, FilteredPreprocessedUpdateOneof::Ping));
    }

    #[test]
    fn test_filtered_preprocessed_update_encode_decode() {
        use prost::Message;

        let update = FilteredPreprocessedUpdate::new_empty(
            FilteredPreprocessedUpdateOneof::Pong(SubscribeUpdatePong { id: 42 }),
        );

        let encoded = update.encode_to_vec();
        assert!(!encoded.is_empty());

        // Note: decode is not implemented, so we can't test full round-trip
        // but we can test that encoding works
        assert_eq!(encoded.len(), update.encoded_len());
    }

    #[test]
    fn test_filtered_preprocessed_update_oneof_ping() {
        let oneof = FilteredPreprocessedUpdateOneof::ping();
        assert!(matches!(oneof, FilteredPreprocessedUpdateOneof::Ping));
    }

    #[test]
    fn test_filtered_preprocessed_update_oneof_encode_len() {
        let pong = FilteredPreprocessedUpdateOneof::Pong(SubscribeUpdatePong { id: 42 });
        let len = pong.encoded_len();
        assert!(len > 0);

        let ping = FilteredPreprocessedUpdateOneof::Ping;
        let ping_len = ping.encoded_len();
        assert!(ping_len > 0);
    }

    #[test]
    fn test_multiple_filters_match() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();

        // First filter matches account_a
        transactions.insert(
            "filter1".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: None,
                signature: None,
                account_include: vec![account_key_a.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        // Second filter matches account_b
        transactions.insert(
            "filter2".to_string(),
            SubscribePreprocessedRequestFilterTransactions {
                vote: None,
                signature: None,
                account_include: vec![account_key_b.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribePreprocessedRequest {
            transactions,
            ping: None,
        };
        let limit = FilterLimitsTransactions::default();
        let filter = FilterPreprocessed::new(&config, &limit, &mut create_filter_names()).unwrap();

        let preprocessed_tx = Arc::new(create_preprocessed_transaction(
            &keypair_a,
            vec![account_key_a, account_key_b],
            false,
        ));
        let entry = create_preprocessed_entry(100, vec![preprocessed_tx]);
        let entries = create_preprocessed_entries(100, vec![entry]);

        let updates = filter.get_updates(&entries);
        assert_eq!(updates.len(), 1);
        // Both filters should match
        assert_eq!(updates[0].filters.len(), 2);
    }
}
