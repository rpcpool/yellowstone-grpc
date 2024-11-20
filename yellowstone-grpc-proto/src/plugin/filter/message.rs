use {
    crate::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
            SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
            SubscribeUpdateBlock, SubscribeUpdateEntry, SubscribeUpdatePing, SubscribeUpdatePong,
            SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            SubscribeUpdateTransactionStatus,
        },
        plugin::{
            filter::{name::FilterName, FilterAccountsDataSlice},
            message::{
                MessageAccount, MessageAccountInfo, MessageBlock, MessageBlockMeta, MessageEntry,
                MessageSlot, MessageTransaction, MessageTransactionInfo,
            },
        },
        solana::storage::confirmed_block,
    },
    bytes::buf::{Buf, BufMut},
    prost::{
        encoding::{
            encode_key, encode_varint, encoded_len_varint, key_len, message, DecodeContext,
            WireType,
        },
        DecodeError,
    },
    smallvec::SmallVec,
    solana_sdk::signature::Signature,
    std::{collections::HashSet, sync::Arc},
};

#[inline]
pub fn prost_field_encoded_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

#[inline]
fn prost_bytes_encode_raw(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value);
}

#[inline]
pub fn prost_bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    prost_field_encoded_len(tag, value.len())
}

macro_rules! prost_repeated_encoded_len_map {
    ($tag:expr, $values:expr, $get_len:expr) => {{
        key_len($tag) * $values.len()
            + $values
                .iter()
                .map($get_len)
                .map(|len| encoded_len_varint(len as u64) + len)
                .sum::<usize>()
    }};
}

pub type FilteredUpdates = SmallVec<[FilteredUpdate; 2]>;

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdate {
    pub filters: FilteredUpdateFilters,
    pub message: FilteredUpdateOneof,
}

impl prost::Message for FilteredUpdate {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        for name in self.filters.iter().map(|filter| filter.as_ref()) {
            encode_key(1u32, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        self.message.encode_raw(buf)
    }

    fn encoded_len(&self) -> usize {
        prost_repeated_encoded_len_map!(1u32, self.filters, |filter| filter.as_ref().len())
            + self.message.encoded_len()
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
            data: data_slice.get_slice(&message.data),
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
            FilteredUpdateOneof::BlockMeta(msg) => UpdateOneof::BlockMeta(msg.0.clone()),
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

    pub fn from_subscribe_update(update: SubscribeUpdate) -> Result<Self, &'static str> {
        let message = match update.update_oneof.ok_or("")? {
            UpdateOneof::Account(msg) => {
                let account = MessageAccount::from_update_oneof(msg)?;
                FilteredUpdateOneof::Account(FilteredUpdateAccount {
                    account: account.account,
                    slot: account.slot,
                    is_startup: account.is_startup,
                    data_slice: FilterAccountsDataSlice::default(),
                })
            }
            UpdateOneof::Slot(msg) => {
                let slot = MessageSlot::from_update_oneof(&msg)?;
                FilteredUpdateOneof::Slot(FilteredUpdateSlot(slot))
            }
            UpdateOneof::Transaction(msg) => {
                let tx = MessageTransaction::from_update_oneof(msg)?;
                FilteredUpdateOneof::Transaction(FilteredUpdateTransaction {
                    transaction: tx.transaction,
                    slot: tx.slot,
                })
            }
            UpdateOneof::TransactionStatus(msg) => {
                FilteredUpdateOneof::TransactionStatus(FilteredUpdateTransactionStatus {
                    transaction: Arc::new(MessageTransactionInfo {
                        signature: Signature::try_from(msg.signature.as_slice())
                            .map_err(|_| "invalid signature length")?,
                        is_vote: msg.is_vote,
                        transaction: confirmed_block::Transaction::default(),
                        meta: confirmed_block::TransactionStatusMeta {
                            err: msg.err,
                            ..confirmed_block::TransactionStatusMeta::default()
                        },
                        index: msg.index as usize,
                        account_keys: HashSet::new(),
                    }),
                    slot: msg.slot,
                })
            }
            UpdateOneof::Block(msg) => {
                let block = MessageBlock::from_update_oneof(msg)?;
                FilteredUpdateOneof::Block(Box::new(FilteredUpdateBlock {
                    meta: block.meta,
                    transactions: block.transactions,
                    updated_account_count: block.updated_account_count,
                    accounts: block.accounts,
                    accounts_data_slice: FilterAccountsDataSlice::default(),
                    entries: block.entries,
                }))
            }
            UpdateOneof::Ping(_) => FilteredUpdateOneof::Ping,
            UpdateOneof::Pong(msg) => FilteredUpdateOneof::Pong(msg),
            UpdateOneof::BlockMeta(msg) => {
                let block_meta = MessageBlockMeta(msg);
                FilteredUpdateOneof::BlockMeta(Arc::new(block_meta))
            }
            UpdateOneof::Entry(msg) => {
                let entry = MessageEntry::from_update_oneof(&msg)?;
                FilteredUpdateOneof::Entry(FilteredUpdateEntry(Arc::new(entry)))
            }
        };

        Ok(Self {
            filters: update.filters.into_iter().map(FilterName::new).collect(),
            message,
        })
    }
}

pub type FilteredUpdateFilters = SmallVec<[FilterName; 4]>;

#[derive(Debug, Clone, PartialEq)]
pub enum FilteredUpdateOneof {
    Account(FilteredUpdateAccount),                     // 2
    Slot(FilteredUpdateSlot),                           // 3
    Transaction(FilteredUpdateTransaction),             // 4
    TransactionStatus(FilteredUpdateTransactionStatus), // 10
    Block(Box<FilteredUpdateBlock>),                    // 5
    Ping,                                               // 6
    Pong(SubscribeUpdatePong),                          // 9
    BlockMeta(Arc<MessageBlockMeta>),                   // 7
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
        Self::BlockMeta(message)
    }

    pub const fn entry(message: Arc<MessageEntry>) -> Self {
        Self::Entry(FilteredUpdateEntry(message))
    }
}

impl prost::Message for FilteredUpdateOneof {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            Self::Account(msg) => message::encode(2u32, msg, buf),
            Self::Slot(msg) => message::encode(3u32, msg, buf),
            Self::Transaction(msg) => message::encode(4u32, msg, buf),
            Self::TransactionStatus(msg) => message::encode(10u32, msg, buf),
            Self::Block(msg) => message::encode(5u32, msg, buf),
            Self::Ping => {
                encode_key(6u32, WireType::LengthDelimited, buf);
                encode_varint(0, buf);
            }
            Self::Pong(msg) => message::encode(9u32, msg, buf),
            Self::BlockMeta(msg) => message::encode(7u32, &msg.0, buf),
            Self::Entry(msg) => message::encode(8u32, msg, buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::Account(msg) => message::encoded_len(2u32, msg),
            Self::Slot(msg) => message::encoded_len(3u32, msg),
            Self::Transaction(msg) => message::encoded_len(4u32, msg),
            Self::TransactionStatus(msg) => message::encoded_len(10u32, msg),
            Self::Block(msg) => message::encoded_len(5u32, msg),
            Self::Ping => key_len(6u32) + encoded_len_varint(0),
            Self::Pong(msg) => message::encoded_len(9u32, msg),
            Self::BlockMeta(msg) => message::encoded_len(7u32, &msg.0),
            Self::Entry(msg) => message::encoded_len(8u32, msg),
        }
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

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateAccount {
    pub account: Arc<MessageAccountInfo>,
    pub slot: u64,
    pub is_startup: bool,
    pub data_slice: FilterAccountsDataSlice,
}

impl prost::Message for FilteredUpdateAccount {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        Self::account_encode_raw(1u32, &self.account, &self.data_slice, buf);
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &self.slot, buf);
        }
        if self.is_startup {
            ::prost::encoding::bool::encode(3u32, &self.is_startup, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        prost_field_encoded_len(
            1u32,
            Self::account_encoded_len(&self.account, &self.data_slice),
        ) + if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(2u32, &self.slot)
        } else {
            0
        } + if self.is_startup {
            ::prost::encoding::bool::encoded_len(3u32, &self.is_startup)
        } else {
            0
        }
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

impl FilteredUpdateAccount {
    fn account_encode_raw(
        tag: u32,
        account: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
        buf: &mut impl BufMut,
    ) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::account_encoded_len(account, data_slice) as u64, buf);

        prost_bytes_encode_raw(1u32, account.pubkey.as_ref(), buf);
        if account.lamports != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &account.lamports, buf);
        }
        prost_bytes_encode_raw(3u32, account.owner.as_ref(), buf);
        if account.executable {
            ::prost::encoding::bool::encode(4u32, &account.executable, buf);
        }
        if account.rent_epoch != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &account.rent_epoch, buf);
        }
        data_slice.slice_encode_raw(6u32, &account.data, buf);
        if account.write_version != 0u64 {
            ::prost::encoding::uint64::encode(7u32, &account.write_version, buf);
        }
        if let Some(value) = &account.txn_signature {
            prost_bytes_encode_raw(8u32, value.as_ref(), buf);
        }
    }

    fn account_encoded_len(
        account: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
    ) -> usize {
        let data_len = data_slice.get_slice_len(&account.data);

        prost_bytes_encoded_len(1u32, account.pubkey.as_ref())
            + if account.lamports != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &account.lamports)
            } else {
                0
            }
            + prost_bytes_encoded_len(3u32, account.owner.as_ref())
            + if account.executable {
                ::prost::encoding::bool::encoded_len(4u32, &account.executable)
            } else {
                0
            }
            + if account.rent_epoch != 0u64 {
                ::prost::encoding::uint64::encoded_len(5u32, &account.rent_epoch)
            } else {
                0
            }
            + if data_len != 0 {
                prost_field_encoded_len(6u32, data_len)
            } else {
                0
            }
            + if account.write_version != 0u64 {
                ::prost::encoding::uint64::encoded_len(7u32, &account.write_version)
            } else {
                0
            }
            + account
                .txn_signature
                .map_or(0, |sig| prost_bytes_encoded_len(8u32, sig.as_ref()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateSlot(MessageSlot);

impl prost::Message for FilteredUpdateSlot {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        let status = CommitmentLevelProto::from(self.0.status) as i32;
        if self.0.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.0.slot, buf);
        }
        if let ::core::option::Option::Some(ref value) = self.0.parent {
            ::prost::encoding::uint64::encode(2u32, value, buf);
        }
        if status != CommitmentLevelProto::default() as i32 {
            ::prost::encoding::int32::encode(3u32, &status, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let status = CommitmentLevelProto::from(self.0.status) as i32;

        (if self.0.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.0.slot)
        } else {
            0
        }) + self.0.parent.as_ref().map_or(0, |value| {
            ::prost::encoding::uint64::encoded_len(2u32, value)
        }) + if status != CommitmentLevelProto::default() as i32 {
            ::prost::encoding::int32::encoded_len(3u32, &status)
        } else {
            0
        }
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

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateTransaction {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl prost::Message for FilteredUpdateTransaction {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        Self::tx_encode_raw(1u32, &self.transaction, buf);
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &self.slot, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        prost_field_encoded_len(1u32, Self::tx_encoded_len(&self.transaction))
            + if self.slot != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &self.slot)
            } else {
                0
            }
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

impl FilteredUpdateTransaction {
    fn tx_encode_raw(tag: u32, tx: &MessageTransactionInfo, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::tx_encoded_len(tx) as u64, buf);

        let index = tx.index as u64;

        prost_bytes_encode_raw(1u32, tx.signature.as_ref(), buf);
        if tx.is_vote {
            ::prost::encoding::bool::encode(2u32, &tx.is_vote, buf);
        }
        message::encode(3u32, &tx.transaction, buf);
        message::encode(4u32, &tx.meta, buf);
        if index != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &index, buf);
        }
    }

    fn tx_encoded_len(tx: &MessageTransactionInfo) -> usize {
        let index = tx.index as u64;

        prost_bytes_encoded_len(1u32, tx.signature.as_ref())
            + if tx.is_vote {
                ::prost::encoding::bool::encoded_len(2u32, &tx.is_vote)
            } else {
                0
            }
            + message::encoded_len(3u32, &tx.transaction)
            + message::encoded_len(4u32, &tx.meta)
            + if index != 0u64 {
                ::prost::encoding::uint64::encoded_len(5u32, &index)
            } else {
                0
            }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateTransactionStatus {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl prost::Message for FilteredUpdateTransactionStatus {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.slot, buf);
        }
        let tx = &self.transaction;
        prost_bytes_encode_raw(2u32, tx.signature.as_ref(), buf);
        if tx.is_vote {
            ::prost::encoding::bool::encode(3u32, &tx.is_vote, buf);
        }
        let index = tx.index as u64;
        if index != 0u64 {
            ::prost::encoding::uint64::encode(4u32, &index, buf);
        }
        if let Some(msg) = &tx.meta.err {
            message::encode(5u32, msg, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let tx = &self.transaction;
        let index = tx.index as u64;

        (if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + prost_bytes_encoded_len(2u32, tx.signature.as_ref())
            + if tx.is_vote {
                ::prost::encoding::bool::encoded_len(3u32, &tx.is_vote)
            } else {
                0
            }
            + if index != 0u64 {
                ::prost::encoding::uint64::encoded_len(4u32, &index)
            } else {
                0
            }
            + tx.meta
                .err
                .as_ref()
                .map_or(0, |msg| message::encoded_len(5u32, msg))
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

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransactionInfo>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccountInfo>>,
    pub accounts_data_slice: FilterAccountsDataSlice,
    pub entries: Vec<Arc<MessageEntry>>,
}

impl prost::Message for FilteredUpdateBlock {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.meta.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.meta.slot, buf);
        }
        if !self.meta.blockhash.is_empty() {
            ::prost::encoding::string::encode(2u32, &self.meta.blockhash, buf);
        }
        if let Some(msg) = &self.meta.rewards {
            message::encode(3u32, msg, buf);
        }
        if let Some(msg) = &self.meta.block_time {
            message::encode(4u32, msg, buf);
        }
        if let Some(msg) = &self.meta.block_height {
            message::encode(5u32, msg, buf);
        }
        for tx in &self.transactions {
            FilteredUpdateTransaction::tx_encode_raw(6u32, tx.as_ref(), buf);
        }
        if self.meta.parent_slot != 0u64 {
            ::prost::encoding::uint64::encode(7u32, &self.meta.parent_slot, buf);
        }
        if !self.meta.parent_blockhash.is_empty() {
            ::prost::encoding::string::encode(8u32, &self.meta.parent_blockhash, buf);
        }
        if self.meta.executed_transaction_count != 0u64 {
            ::prost::encoding::uint64::encode(9u32, &self.meta.executed_transaction_count, buf);
        }
        if self.updated_account_count != 0u64 {
            ::prost::encoding::uint64::encode(10u32, &self.updated_account_count, buf);
        }
        for account in &self.accounts {
            FilteredUpdateAccount::account_encode_raw(
                11u32,
                account.as_ref(),
                &self.accounts_data_slice,
                buf,
            );
        }
        if self.meta.entries_count != 0u64 {
            ::prost::encoding::uint64::encode(12u32, &self.meta.entries_count, buf);
        }
        for entry in &self.entries {
            encode_key(13u32, WireType::LengthDelimited, buf);
            encode_varint(
                FilteredUpdateEntry::entry_encoded_len(entry.as_ref()) as u64,
                buf,
            );
            FilteredUpdateEntry::entry_encode_raw(entry, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        (if self.meta.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.meta.slot)
        } else {
            0
        }) + if !self.meta.blockhash.is_empty() {
            ::prost::encoding::string::encoded_len(2u32, &self.meta.blockhash)
        } else {
            0
        } + self
            .meta
            .rewards
            .as_ref()
            .map_or(0, |msg| message::encoded_len(3u32, msg))
            + self
                .meta
                .block_time
                .as_ref()
                .map_or(0, |msg| message::encoded_len(4u32, msg))
            + self
                .meta
                .block_height
                .as_ref()
                .map_or(0, |msg| message::encoded_len(5u32, msg))
            + prost_repeated_encoded_len_map!(6u32, self.transactions, |tx| {
                FilteredUpdateTransaction::tx_encoded_len(tx.as_ref())
            })
            + if self.meta.parent_slot != 0u64 {
                ::prost::encoding::uint64::encoded_len(7u32, &self.meta.parent_slot)
            } else {
                0
            }
            + if !self.meta.parent_blockhash.is_empty() {
                ::prost::encoding::string::encoded_len(8u32, &self.meta.parent_blockhash)
            } else {
                0
            }
            + if self.meta.executed_transaction_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(9u32, &self.meta.executed_transaction_count)
            } else {
                0
            }
            + if self.updated_account_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(10u32, &self.updated_account_count)
            } else {
                0
            }
            + prost_repeated_encoded_len_map!(11u32, self.accounts, |account| {
                FilteredUpdateAccount::account_encoded_len(
                    account.as_ref(),
                    &self.accounts_data_slice,
                )
            })
            + if self.meta.entries_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(12u32, &self.meta.entries_count)
            } else {
                0
            }
            + prost_repeated_encoded_len_map!(13u32, self.entries, |entry| {
                FilteredUpdateEntry::entry_encoded_len(entry)
            })
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

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateEntry(Arc<MessageEntry>);

impl prost::Message for FilteredUpdateEntry {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        Self::entry_encode_raw(&self.0, buf)
    }

    fn encoded_len(&self) -> usize {
        Self::entry_encoded_len(&self.0)
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

impl FilteredUpdateEntry {
    fn entry_encode_raw(entry: &MessageEntry, buf: &mut impl BufMut) {
        let index = entry.index as u64;

        if entry.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &entry.slot, buf);
        }
        if index != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &index, buf);
        }
        if entry.num_hashes != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &entry.num_hashes, buf);
        }
        prost_bytes_encode_raw(4u32, entry.hash.as_ref(), buf);
        if entry.executed_transaction_count != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &entry.executed_transaction_count, buf);
        }
        if entry.starting_transaction_index != 0u64 {
            ::prost::encoding::uint64::encode(6u32, &entry.starting_transaction_index, buf);
        }
    }

    fn entry_encoded_len(entry: &MessageEntry) -> usize {
        let index = entry.index as u64;

        (if entry.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &entry.slot)
        } else {
            0
        }) + if index != 0u64 {
            ::prost::encoding::uint64::encoded_len(2u32, &index)
        } else {
            0
        } + if entry.num_hashes != 0u64 {
            ::prost::encoding::uint64::encoded_len(3u32, &entry.num_hashes)
        } else {
            0
        } + prost_bytes_encoded_len(4u32, entry.hash.as_ref())
            + if entry.executed_transaction_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(5u32, &entry.executed_transaction_count)
            } else {
                0
            }
            + if entry.starting_transaction_index != 0u64 {
                ::prost::encoding::uint64::encoded_len(6u32, &entry.starting_transaction_index)
            } else {
                0
            }
    }
}

#[cfg(any(test, feature = "plugin-bench"))]
pub mod tests {
    #![cfg_attr(feature = "plugin-bench", allow(dead_code))]
    #![cfg_attr(feature = "plugin-bench", allow(unused_imports))]
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
        assert_eq!(
            FilteredUpdate::from_subscribe_update(update.clone())
                .map(|msg| msg.as_subscribe_update()),
            Ok(update)
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
