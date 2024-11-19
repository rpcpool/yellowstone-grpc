use {
    crate::{
        geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
            SubscribeUpdateAccountInfo, SubscribeUpdateBlock, SubscribeUpdateEntry,
            SubscribeUpdatePing, SubscribeUpdatePong, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            SubscribeUpdateTransactionStatus,
        },
        plugin::{
            filter::{name::FilterName, FilterAccountsDataSlice},
            message::{
                MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry, MessageSlot,
                MessageTransaction, MessageTransactionInfo,
            },
        },
    },
    bytes::buf::{Buf, BufMut},
    prost::{
        encoding::{DecodeContext, WireType},
        DecodeError,
    },
    smallvec::SmallVec,
    std::sync::Arc,
};

pub type FilteredUpdates = SmallVec<[FilteredUpdate; 2]>;

#[derive(Debug, Clone)]
pub struct FilteredUpdate {
    pub filters: FilteredUpdateFilters,
    pub message: FilteredUpdateOneof,
}

impl prost::Message for FilteredUpdate {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        self.as_subscribe_update().encode_raw(buf)
    }

    fn encoded_len(&self) -> usize {
        self.as_subscribe_update().encoded_len()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

impl FilteredUpdate {
    pub fn new(filters: FilteredUpdateFilters, message: FilteredUpdateOneof) -> Self {
        Self { filters, message }
    }

    pub fn new_empty(message: FilteredUpdateOneof) -> Self {
        Self::new(FilteredUpdateFilters::new(), message)
    }

    fn as_subscribe_update_account(
        message: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
    ) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: message.pubkey.as_ref().into(),
            lamports: message.lamports,
            owner: message.owner.as_ref().into(),
            executable: message.executable,
            rent_epoch: message.rent_epoch,
            data: data_slice.apply(&message.data),
            write_version: message.write_version,
            txn_signature: message.txn_signature.map(|s| s.as_ref().into()),
        }
    }

    fn as_subscribe_update_transaction(
        message: &MessageTransactionInfo,
    ) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: message.signature.as_ref().into(),
            is_vote: message.is_vote,
            transaction: Some(message.transaction.clone()),
            meta: Some(message.meta.clone()),
            index: message.index as u64,
        }
    }

    fn as_subscribe_update_entry(message: &MessageEntry) -> SubscribeUpdateEntry {
        SubscribeUpdateEntry {
            slot: message.slot,
            index: message.index as u64,
            num_hashes: message.num_hashes,
            hash: message.hash.to_bytes().to_vec(),
            executed_transaction_count: message.executed_transaction_count,
            starting_transaction_index: message.starting_transaction_index,
        }
    }

    pub fn as_subscribe_update(&self) -> SubscribeUpdate {
        let message = match &self.message {
            FilteredUpdateOneof::Account(msg) => UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(Self::as_subscribe_update_account(
                    msg.account.as_ref(),
                    &msg.data_slice,
                )),
                slot: msg.slot,
                is_startup: msg.is_startup,
            }),
            FilteredUpdateOneof::Slot(msg) => UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: msg.0.slot,
                parent: msg.0.parent,
                status: msg.0.status as i32,
            }),
            FilteredUpdateOneof::Transaction(msg) => {
                UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(Self::as_subscribe_update_transaction(
                        msg.transaction.as_ref(),
                    )),
                    slot: msg.slot,
                })
            }
            FilteredUpdateOneof::TransactionStatus(msg) => {
                UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                    slot: msg.slot,
                    signature: msg.transaction.signature.as_ref().into(),
                    is_vote: msg.transaction.is_vote,
                    index: msg.transaction.index as u64,
                    err: msg.transaction.meta.err.clone(),
                })
            }
            FilteredUpdateOneof::Block(msg) => UpdateOneof::Block(SubscribeUpdateBlock {
                slot: msg.meta.slot,
                blockhash: msg.meta.blockhash.clone(),
                rewards: msg.meta.rewards.clone(),
                block_time: msg.meta.block_time,
                block_height: msg.meta.block_height,
                parent_slot: msg.meta.parent_slot,
                parent_blockhash: msg.meta.parent_blockhash.clone(),
                executed_transaction_count: msg.meta.executed_transaction_count,
                transactions: msg
                    .transactions
                    .iter()
                    .map(|tx| Self::as_subscribe_update_transaction(tx.as_ref()))
                    .collect(),
                updated_account_count: msg.updated_account_count,
                accounts: msg
                    .accounts
                    .iter()
                    .map(|acc| {
                        Self::as_subscribe_update_account(acc.as_ref(), &msg.accounts_data_slice)
                    })
                    .collect(),
                entries_count: msg.meta.entries_count,
                entries: msg
                    .entries
                    .iter()
                    .map(|entry| Self::as_subscribe_update_entry(entry.as_ref()))
                    .collect(),
            }),
            FilteredUpdateOneof::Ping => UpdateOneof::Ping(SubscribeUpdatePing {}),
            FilteredUpdateOneof::Pong(msg) => UpdateOneof::Pong(*msg),
            FilteredUpdateOneof::BlockMeta(msg) => UpdateOneof::BlockMeta(msg.0.as_ref().0.clone()),
            FilteredUpdateOneof::Entry(msg) => {
                UpdateOneof::Entry(Self::as_subscribe_update_entry(&msg.0))
            }
        };

        SubscribeUpdate {
            filters: self
                .filters
                .iter()
                .map(|name| name.as_ref().to_string())
                .collect(),
            update_oneof: Some(message),
        }
    }
}

pub type FilteredUpdateFilters = SmallVec<[FilterName; 4]>;

#[derive(Debug, Clone)]
pub enum FilteredUpdateOneof {
    Account(FilteredUpdateAccount),                     // 2
    Slot(FilteredUpdateSlot),                           // 3
    Transaction(FilteredUpdateTransaction),             // 4
    TransactionStatus(FilteredUpdateTransactionStatus), // 10
    Block(Box<FilteredUpdateBlock>),                    // 5
    Ping,                                               // 6
    Pong(SubscribeUpdatePong),                          // 9
    BlockMeta(FilteredUpdateBlockMeta),                 // 7
    Entry(FilteredUpdateEntry),                         // 8
}

impl FilteredUpdateOneof {
    pub fn account(message: &MessageAccount, data_slice: FilterAccountsDataSlice) -> Self {
        Self::Account(FilteredUpdateAccount {
            slot: message.slot,
            account: Arc::clone(&message.account),
            is_startup: message.is_startup,
            data_slice,
        })
    }

    pub const fn slot(message: MessageSlot) -> Self {
        Self::Slot(FilteredUpdateSlot(message))
    }

    pub fn transaction(message: &MessageTransaction) -> Self {
        Self::Transaction(FilteredUpdateTransaction {
            transaction: Arc::clone(&message.transaction),
            slot: message.slot,
        })
    }

    pub fn transaction_status(message: &MessageTransaction) -> Self {
        Self::TransactionStatus(FilteredUpdateTransactionStatus {
            transaction: Arc::clone(&message.transaction),
            slot: message.slot,
        })
    }

    pub const fn block(message: Box<FilteredUpdateBlock>) -> Self {
        Self::Block(message)
    }

    pub const fn ping() -> Self {
        Self::Ping
    }

    pub const fn pong(id: i32) -> Self {
        Self::Pong(SubscribeUpdatePong { id })
    }

    pub const fn block_meta(message: Arc<MessageBlockMeta>) -> Self {
        Self::BlockMeta(FilteredUpdateBlockMeta(message))
    }

    pub const fn entry(message: Arc<MessageEntry>) -> Self {
        Self::Entry(FilteredUpdateEntry(message))
    }
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateAccount {
    pub account: Arc<MessageAccountInfo>,
    pub slot: u64,
    pub is_startup: bool,
    pub data_slice: FilterAccountsDataSlice,
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateSlot(MessageSlot);

#[derive(Debug, Clone)]
pub struct FilteredUpdateTransaction {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateTransactionStatus {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransactionInfo>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccountInfo>>,
    pub accounts_data_slice: FilterAccountsDataSlice,
    pub entries: Vec<Arc<MessageEntry>>,
}

#[derive(Debug, Clone)]
pub struct FilteredUpdateBlockMeta(Arc<MessageBlockMeta>);

#[derive(Debug, Clone)]
pub struct FilteredUpdateEntry(Arc<MessageEntry>);

#[cfg(test)]
pub mod tests {
    use {
        super::{FilteredUpdate, FilteredUpdateBlock, FilteredUpdateFilters, FilteredUpdateOneof},
        crate::{
            convert_to,
            geyser::{SubscribeUpdate, SubscribeUpdateBlockMeta},
            plugin::{
                filter::{name::FilterName, FilterAccountsDataSlice},
                message::{
                    CommitmentLevel, MessageAccount, MessageAccountInfo, MessageBlockMeta,
                    MessageEntry, MessageSlot, MessageTransaction, MessageTransactionInfo,
                },
            },
        },
        prost::Message as _,
        prost_011::Message as _,
        solana_sdk::{
            hash::Hash,
            message::SimpleAddressLoader,
            pubkey::Pubkey,
            signature::Signature,
            transaction::{MessageHash, SanitizedTransaction},
        },
        solana_storage_proto::convert::generated,
        solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta},
        std::{
            collections::{HashMap, HashSet},
            fs,
            ops::Range,
            str::FromStr,
            sync::Arc,
        },
    };

    pub fn create_message_filters(names: &[&str]) -> FilteredUpdateFilters {
        let mut filters = FilteredUpdateFilters::new();
        for name in names {
            filters.push(FilterName::new(*name));
        }
        filters
    }

    pub fn create_account_data_slice() -> Vec<FilterAccountsDataSlice> {
        [
            vec![],
            vec![Range { start: 0, end: 0 }],
            vec![Range { start: 2, end: 3 }],
            vec![Range { start: 1, end: 3 }, Range { start: 5, end: 10 }],
        ]
        .into_iter()
        .map(Arc::new)
        .map(FilterAccountsDataSlice::new_unchecked)
        .collect()
    }

    pub fn create_accounts_raw() -> Vec<Arc<MessageAccountInfo>> {
        let pubkey = Pubkey::from_str("28Dncoh8nmzXYEGLUcBA5SUw5WDwDBn15uUCwrWBbyuu").unwrap();
        let owner = Pubkey::from_str("5jrPJWVGrFvQ2V9wRZC3kHEZhxo9pmMir15x73oHT6mn").unwrap();
        let txn_signature = Signature::from_str("4V36qYhukXcLFuvhZaudSoJpPaFNB7d5RqYKjL2xiSKrxaBfEajqqL4X6viZkEvHJ8XcTJsqVjZxFegxhN7EC9V5").unwrap();

        let mut accounts = vec![];
        for lamports in [0, 8123] {
            for executable in [true, false] {
                for rent_epoch in [0, 4242] {
                    for data in [
                        vec![],
                        [42; 165].to_vec(),
                        [42; 1024].to_vec(),
                        [42; 2 * 1024 * 1024].to_vec(),
                    ] {
                        for write_version in [0, 1] {
                            for txn_signature in [None, Some(txn_signature)] {
                                accounts.push(Arc::new(MessageAccountInfo {
                                    pubkey,
                                    lamports,
                                    owner,
                                    executable,
                                    rent_epoch,
                                    data: data.clone(),
                                    write_version,
                                    txn_signature,
                                }));
                            }
                        }
                    }
                }
            }
        }
        accounts
    }

    pub fn create_accounts() -> Vec<(MessageAccount, FilterAccountsDataSlice)> {
        let mut vec = vec![];
        for account in create_accounts_raw() {
            for slot in [0, 42] {
                for is_startup in [true, false] {
                    for data_slice in create_account_data_slice() {
                        let msg = MessageAccount {
                            account: Arc::clone(&account),
                            slot,
                            is_startup,
                        };
                        vec.push((msg, data_slice));
                    }
                }
            }
        }
        vec
    }

    pub fn create_entries() -> Vec<Arc<MessageEntry>> {
        [
            MessageEntry {
                slot: 299888121,
                index: 42,
                num_hashes: 128,
                hash: Hash::new_from_array([98; 32]),
                executed_transaction_count: 32,
                starting_transaction_index: 1000,
            },
            MessageEntry {
                slot: 299888121,
                index: 0,
                num_hashes: 16,
                hash: Hash::new_from_array([42; 32]),
                executed_transaction_count: 32,
                starting_transaction_index: 1000,
            },
        ]
        .into_iter()
        .map(Arc::new)
        .collect()
    }

    pub fn load_predefined() -> Vec<ConfirmedBlock> {
        fs::read_dir("./fixtures/blocks")
            .expect("failed to read `blocks` dir")
            .map(|entry| {
                let path = entry.expect("failed to read `blocks` dir entry").path();
                let data = fs::read(path).expect("failed to read block");
                generated::ConfirmedBlock::decode(data.as_slice())
                    .expect("failed to decode block")
                    .try_into()
                    .expect("failed to convert decoded block")
            })
            .collect()
    }

    pub fn load_predefined_blockmeta() -> Vec<Arc<MessageBlockMeta>> {
        load_predefined_blocks()
            .into_iter()
            .map(|block| (block.meta.blockhash.clone(), block.meta))
            .collect::<HashMap<_, _>>()
            .into_values()
            .collect()
    }

    pub fn load_predefined_transactions() -> Vec<Arc<MessageTransactionInfo>> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|block| block.transactions.into_iter().map(|tx| (tx.signature, tx)))
            .collect::<HashMap<_, _>>()
            .into_values()
            .collect()
    }

    pub fn load_predefined_blocks() -> Vec<FilteredUpdateBlock> {
        load_predefined()
            .into_iter()
            .flat_map(|block| {
                let transactions = block
                    .transactions
                    .into_iter()
                    .enumerate()
                    .map(|(index, tx)| {
                        let TransactionWithStatusMeta::Complete(tx) = tx else {
                            panic!("tx with missed meta");
                        };
                        let transaction = SanitizedTransaction::try_create(
                            tx.transaction.clone(),
                            MessageHash::Compute,
                            None,
                            SimpleAddressLoader::Disabled,
                            &HashSet::new(),
                        )
                        .expect("failed to create tx");
                        MessageTransactionInfo {
                            signature: tx.transaction.signatures[0],
                            is_vote: true,
                            transaction: convert_to::create_transaction(&transaction),
                            meta: convert_to::create_transaction_meta(&tx.meta),
                            index,
                            account_keys: HashSet::new(),
                        }
                    })
                    .map(Arc::new)
                    .collect::<Vec<_>>();

                let entries = create_entries();

                let slot = block.parent_slot + 1;
                let block_meta1 = MessageBlockMeta(SubscribeUpdateBlockMeta {
                    parent_slot: block.parent_slot,
                    slot,
                    parent_blockhash: block.previous_blockhash,
                    blockhash: block.blockhash,
                    rewards: Some(convert_to::create_rewards_obj(
                        &block.rewards,
                        block.num_partitions,
                    )),
                    block_time: block.block_time.map(convert_to::create_timestamp),
                    block_height: block.block_height.map(convert_to::create_block_height),
                    executed_transaction_count: transactions.len() as u64,
                    entries_count: entries.len() as u64,
                });
                let mut block_meta2 = block_meta1.clone();
                block_meta2.rewards =
                    Some(convert_to::create_rewards_obj(&block.rewards, Some(42)));

                let block_meta1 = Arc::new(block_meta1);
                let block_meta2 = Arc::new(block_meta2);

                let accounts = create_accounts_raw();
                create_account_data_slice()
                    .into_iter()
                    .flat_map(move |data_slice| {
                        vec![
                            FilteredUpdateBlock {
                                meta: Arc::clone(&block_meta1),
                                transactions: transactions.clone(),
                                updated_account_count: accounts.len() as u64,
                                accounts: accounts.clone(),
                                accounts_data_slice: data_slice.clone(),
                                entries: entries.clone(),
                            },
                            FilteredUpdateBlock {
                                meta: Arc::clone(&block_meta2),
                                transactions: transactions.clone(),
                                updated_account_count: accounts.len() as u64,
                                accounts: accounts.clone(),
                                accounts_data_slice: data_slice,
                                entries: entries.clone(),
                            },
                        ]
                    })
            })
            .collect()
    }

    fn encode_decode_cmp(filters: &[&str], message: FilteredUpdateOneof) {
        let msg = FilteredUpdate {
            filters: create_message_filters(filters),
            message,
        };
        let update = msg.as_subscribe_update();
        assert_eq!(msg.encoded_len(), update.encoded_len());
        assert_eq!(
            SubscribeUpdate::decode(msg.encode_to_vec().as_slice()).expect("failed to decode"),
            update
        );
    }

    #[test]
    fn test_message_account() {
        for (msg, data_slice) in create_accounts() {
            encode_decode_cmp(&["123"], FilteredUpdateOneof::account(&msg, data_slice));
        }
    }

    #[test]
    fn test_message_slot() {
        for slot in [0, 42] {
            for parent in [None, Some(0), Some(42)] {
                for status in [
                    CommitmentLevel::Processed,
                    CommitmentLevel::Confirmed,
                    CommitmentLevel::Finalized,
                ] {
                    encode_decode_cmp(
                        &["123"],
                        FilteredUpdateOneof::slot(MessageSlot {
                            slot,
                            parent,
                            status,
                        }),
                    )
                }
            }
        }
    }

    #[test]
    fn test_message_transaction() {
        for transaction in load_predefined_transactions() {
            let msg = MessageTransaction {
                transaction,
                slot: 42,
            };
            encode_decode_cmp(&["123"], FilteredUpdateOneof::transaction(&msg));
            encode_decode_cmp(&["123"], FilteredUpdateOneof::transaction_status(&msg));
        }
    }

    #[test]
    fn test_message_block() {
        for block in load_predefined_blocks() {
            encode_decode_cmp(&["123"], FilteredUpdateOneof::block(Box::new(block)));
        }
    }

    #[test]
    fn test_message_ping() {
        encode_decode_cmp(&["123"], FilteredUpdateOneof::Ping)
    }

    #[test]
    fn test_message_pong() {
        encode_decode_cmp(&["123"], FilteredUpdateOneof::pong(0));
        encode_decode_cmp(&["123"], FilteredUpdateOneof::pong(42));
    }

    #[test]
    fn test_message_blockmeta() {
        for block_meta in load_predefined_blockmeta() {
            encode_decode_cmp(&["123"], FilteredUpdateOneof::block_meta(block_meta));
        }
    }

    #[test]
    fn test_message_entry() {
        for entry in create_entries() {
            encode_decode_cmp(&["123"], FilteredUpdateOneof::entry(entry));
        }
    }
}
