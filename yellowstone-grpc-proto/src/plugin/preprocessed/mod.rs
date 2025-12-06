use std::{collections::HashSet, sync::Arc, time::SystemTime};

use bytes::{Buf, BufMut};
use prost_types::Timestamp;
use smallvec::SmallVec;
use solana_clock::Slot;
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_transaction::{simple_vote_transaction_checker::is_simple_vote_transaction, versioned::{VersionedTransaction, sanitized::SanitizedVersionedTransaction}};

use crate::{convert_to::{create_header, create_instruction, create_lookup}, geyser::{SubscribePreprocessedRequest, SubscribeUpdatePong, SubscribeUpdateTransaction}, plugin::filter::{FilterError, limits::FilterLimits, name::{FilterName, FilterNames}}, prelude::Transaction};
use prost::{DecodeError, encoding::{DecodeContext, WireType}};
use solana_entry::entry::Entry as SolanaEntry;
use crate::prelude::Message as SolanaStorageMessage;
use solana_signature::Signature;
use prost::Message;


pub type FilteredPreprocessedUpdateFilters = SmallVec<[FilterName; 4]>;

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredPreprocessedUpdate {
    pub filters: FilteredPreprocessedUpdateFilters,
    pub message: FilteredPreprocessedUpdateOneof,
    pub created_at: Timestamp,
}

impl prost::Message for FilteredPreprocessedUpdate {

    fn encode_raw(&self, _buf: &mut impl BufMut) {
        todo!()
    }

    fn encoded_len(&self) -> usize {
        todo!()
    }

    fn merge_field(&mut self, _tag: u32, _wire_type: WireType, _buf: &mut impl Buf, _ctx: DecodeContext) -> Result<(), DecodeError> {
        todo!()
    }

    fn clear(&mut self) {
        todo!()
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
    PreprocessedTransactiion(Arc<SubscribeUpdateTransaction>)
}

impl FilteredPreprocessedUpdateOneof {
    pub const fn ping() -> Self {
        Self::Ping
    }
}

impl prost::Message for FilteredPreprocessedUpdateOneof {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            FilteredPreprocessedUpdateOneof::Ping => todo!(),
            FilteredPreprocessedUpdateOneof::Pong(_pong) => todo!(),
            FilteredPreprocessedUpdateOneof::PreprocessedTransactiion(_transaction) => todo!(),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            FilteredPreprocessedUpdateOneof::Ping => todo!(),
            FilteredPreprocessedUpdateOneof::Pong(_pong) => todo!(),
            FilteredPreprocessedUpdateOneof::PreprocessedTransactiion(_transaction) => todo!(),
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
pub struct FilterPreprocessed {
    transactions: FilterTransactions,
    ping: Option<i32>,
}


impl FilterPreprocessed {
    pub fn get_updates(&self, preprocessed_entries: &PreprocessedEntries) -> Vec<FilteredPreprocessedUpdate> {
        todo!()
    }
}


// pub fn new(
//     config: &SubscribeRequest,
//     limits: &FilterLimits,
//     names: &mut FilterNames,
// ) -> FilterResult<Self> {
//     Ok(Self {
//         accounts: FilterAccounts::new(&config.accounts, &limits.accounts, names)?,
//         slots: FilterSlots::new(&config.slots, &limits.slots, names)?,
//         transactions: FilterTransactions::new(
//             &config.transactions,
//             &limits.transactions,
//             FilterTransactionsType::Transaction,
//             names,
//         )?,
//         transactions_status: FilterTransactions::new(
//             &config.transactions_status,
//             &limits.transactions_status,
//             FilterTransactionsType::TransactionStatus,
//             names,
//         )?,
//         entries: FilterEntries::new(&config.entry, &limits.entries, names)?,
//         blocks: FilterBlocks::new(&config.blocks, &limits.blocks, names)?,
//         blocks_meta: FilterBlocksMeta::new(&config.blocks_meta, &limits.blocks_meta, names)?,
//         commitment: Self::decode_commitment(config.commitment)?,
//         accounts_data_slice: FilterAccountsDataSlice::new(
//             &config.accounts_data_slice,
//             limits.accounts.data_slice_max,
//         )?,
//         ping: config.ping.as_ref().map(|msg| msg.id),
//     })
// }
impl FilterPreprocessed {

    pub fn new(subscribe_request: &SubscribePreprocessedRequest, _limits: &FilterLimits, _names: &mut FilterNames) -> Result<Self, FilterError> {
        Ok(Self { 
            ping: subscribe_request.ping.as_ref().map(|msg| msg.id),
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
