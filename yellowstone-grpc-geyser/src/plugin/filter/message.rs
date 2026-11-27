use {
    crate::{
        body::{
            account_info_item_len, block_shape::BlockSelectionTotals, put_account_info_item,
            put_transaction_info, sliced_account_body_len, transaction_info_len,
            transaction_status_body_len, transaction_update_body_len, write_sliced_account_body,
            PublishedBody, SharedUpdateBody, StandardTransactionBodyWriter,
        },
        plugin::{
            filter::{name::FilterName, FilterAccountsDataSlice},
            message::{
                MessageAccount, MessageBlockFooter, MessageBlockMeta, MessageDeshredTransaction,
                MessageDeshredTransactionInfo, MessageDeshredUpdateParent, MessageEntry,
                MessageEntryUpdateParent, MessageSlot, MessageTransaction,
            },
        },
        telemetry::data_plane_counts::{DataPlaneCount, DATA_PLANE_COUNTS},
    },
    bytes::{
        buf::{Buf, BufMut},
        Bytes,
    },
    prost::{
        encoding::{
            encode_key, encode_varint, encoded_len_varint, key_len, message, DecodeContext,
            WireType,
        },
        DecodeError,
    },
    prost_types::Timestamp,
    smallvec::SmallVec,
    std::{
        ops::{Deref, Range},
        sync::Arc,
        time::SystemTime,
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SlotStatus as SlotStatusProto, SubscribeUpdate,
        SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
        SubscribeUpdateBlockFooter, SubscribeUpdateEntry, SubscribeUpdatePing, SubscribeUpdatePong,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        SubscribeUpdateTransactionStatus,
    },
};

#[inline]
pub const fn prost_field_encoded_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

#[inline]
pub fn prost_bytes_encode_raw(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value);
}

#[inline]
pub const fn prost_bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
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

/// Bytes of account data that add one weight unit to an account by default.
pub const DEFAULT_ACCOUNT_DATA_UNIT: usize = 4096;

pub const DEFAULT_TRANSACTION_DATA_UNIT: usize = 4096;

/// Bytes of filter names that add one weight unit to an update by default.
pub const DEFAULT_FILTER_NAME_DATA_UNIT: usize = 1024;

/// Weight of each update kind in a Subscribe queue. Scales below `1` count as `1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightScales {
    /// Multiplies an account update's weight, which is the account's units.
    pub account: u32,
    pub transaction: u32,
    pub entry: u32,
    pub slot: u32,
    pub block_footer: u32,
    /// Multiplies a block's weight, which is `1` plus its transactions and its accounts' units.
    pub block: u32,
    /// An account counts `1` unit plus one per this many bytes of data. `0` counts it as `1`.
    pub account_data_unit: usize,
    pub transaction_data_unit: usize,
    /// Adds one unscaled unit per this many bytes of an update's filter names. `0` adds nothing.
    pub filter_name_data_unit: usize,
}

impl Default for WeightScales {
    fn default() -> Self {
        Self {
            account: 1,
            transaction: 1,
            entry: 1,
            slot: 1,
            block_footer: 1,
            block: 1,
            account_data_unit: DEFAULT_ACCOUNT_DATA_UNIT,
            transaction_data_unit: DEFAULT_TRANSACTION_DATA_UNIT,
            filter_name_data_unit: DEFAULT_FILTER_NAME_DATA_UNIT,
        }
    }
}

fn data_units(count: usize, len: usize, unit: usize) -> u32 {
    let units = if count == 0 {
        0
    } else {
        len.checked_div(unit).map_or(count, |units| units.max(1))
    };
    u32::try_from(units).unwrap_or(u32::MAX)
}

impl WeightScales {
    /// Counts the full data, even under a data slice, because the queue holds the whole account.
    fn account_units(&self, account: &MessageAccount) -> u32 {
        let extra = account
            .data()
            .len()
            .checked_div(self.account_data_unit)
            .unwrap_or_default();
        u32::try_from(extra).unwrap_or(u32::MAX).saturating_add(1)
    }

    fn filter_name_units(&self, filter_names: &[FilterName]) -> u32 {
        let filter_names_len = filter_names
            .iter()
            .map(|filter_name| filter_name.len())
            .sum::<usize>();
        self.filter_names_len_units(filter_names_len)
    }

    /// One unscaled unit per `filter_name_data_unit` bytes of an update's filter names.
    pub fn filter_names_len_units(&self, filter_names_len: usize) -> u32 {
        let unit_count = filter_names_len
            .checked_div(self.filter_name_data_unit)
            .unwrap_or_default();
        u32::try_from(unit_count).unwrap_or(u32::MAX)
    }

    /// An account update's weight: its units, scaled.
    pub fn account_update_weight(&self, account: &MessageAccount) -> u32 {
        self.account
            .max(1)
            .saturating_mul(self.account_units(account))
    }

    /// A Transaction update's weight: one unit until some writer has written its
    /// `TransactionInfo` bytes, then units of their length, scaled.
    pub fn transaction_update_weight(&self, message_transaction: &MessageTransaction) -> u32 {
        let units = message_transaction
            .transaction_bodies()
            .built_info_len()
            .map_or(1, |transaction_info_len| {
                data_units(1, transaction_info_len, self.transaction_data_unit)
            });
        self.transaction.max(1).saturating_mul(units)
    }

    /// A Block update's weight: `1` plus its selected transactions' and accounts' units, scaled.
    pub fn block_update_weight(&self, selection_totals: &BlockSelectionTotals) -> u32 {
        let transactions = self.transaction.max(1).saturating_mul(data_units(
            selection_totals.transaction_count,
            selection_totals.transaction_info_bytes,
            self.transaction_data_unit,
        ));
        let accounts = self.account.max(1).saturating_mul(data_units(
            selection_totals.account_count,
            selection_totals.account_data_bytes,
            self.account_data_unit,
        ));
        transactions
            .saturating_add(accounts)
            .saturating_add(1)
            .saturating_mul(self.block.max(1))
    }

    /// Weighs a [`FilteredUpdate`] with these scales.
    ///
    /// # Arguments
    ///
    /// * `update` - The [`FilteredUpdate`] to weigh.
    ///
    /// # Returns
    ///
    /// The weight of `update`'s message plus one unit per `filter_name_data_unit` bytes of its
    /// filter names. Transaction status, block meta, ping and pong messages weigh `1`.
    pub(crate) fn update_weight(&self, update: &FilteredUpdate) -> u32 {
        self.message_weight(&update.message)
            .saturating_add(self.filter_name_units(&update.filters))
    }

    fn message_weight(&self, update_message: &FilteredUpdateOneof) -> u32 {
        let scale = match update_message {
            FilteredUpdateOneof::Account(update) => {
                return self.account_update_weight(&update.account);
            }
            FilteredUpdateOneof::Transaction(update) => {
                return self.transaction_update_weight(&update.transaction);
            }
            FilteredUpdateOneof::Entry(_) | FilteredUpdateOneof::EntryUpdateParent(_) => self.entry,
            FilteredUpdateOneof::Slot(_) => self.slot,
            FilteredUpdateOneof::BlockFooter(_) => self.block_footer,
            FilteredUpdateOneof::Block(block) => {
                return self.block_update_weight(&BlockSelectionTotals {
                    transaction_count: block.transactions.len(),
                    transaction_info_bytes: block.transactions_data_len,
                    account_count: block.accounts.len(),
                    account_data_bytes: block.accounts_data_len,
                });
            }
            FilteredUpdateOneof::TransactionStatus(_)
            | FilteredUpdateOneof::BlockMeta(_)
            | FilteredUpdateOneof::Ping
            | FilteredUpdateOneof::Pong(_) => 1,
        };
        scale.max(1)
    }
}

pub type FilteredUpdates = SmallVec<[FilteredUpdate; 2]>;

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdate {
    pub filters: FilteredUpdateFilters,
    pub message: FilteredUpdateOneof,
    pub created_at: Timestamp,
}

/// What an update writes after its filter names.
#[derive(Debug)]
pub enum UpdateBodyWrite<'update> {
    /// The message's shared body: its oneof field, then `created_at`.
    SharedBody(&'update SharedUpdateBody),
    /// A transaction's body, published in its claim cell.
    PublishedBody(PublishedBody),
    /// An account's body cut by the member's data slice.
    SlicedAccountBody(&'update SharedUpdateBody, &'update [Range<usize>]),
    /// A kind encoded per member: its oneof field, then the update's `created_at`.
    MemberOneof,
}

/// The filter names, then what [`FilteredUpdateOneof::update_body_write`] picks.
impl prost::Message for FilteredUpdate {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        for name in self.filters.iter().map(|filter| filter.as_ref()) {
            encode_key(1u32, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        match self.message.update_body_write() {
            UpdateBodyWrite::SharedBody(shared_body) => shared_body.write_body(buf),
            UpdateBodyWrite::PublishedBody(published_body) => {
                if let FilteredUpdateOneof::Transaction(_) = &self.message {
                    count_transaction_info_use(TransactionInfoUse::Published);
                }
                published_body.write_body(buf);
            }
            UpdateBodyWrite::SlicedAccountBody(account_body, data_slice_ranges) => {
                write_sliced_account_body(account_body, data_slice_ranges, buf)
            }
            UpdateBodyWrite::MemberOneof => {
                self.message.encode_member_oneof(buf);
                message::encode(11u32, &self.created_at, buf);
            }
        }
    }

    fn encoded_len(&self) -> usize {
        let names_len =
            prost_repeated_encoded_len_map!(1u32, self.filters, |filter| filter.as_ref().len());
        if let Some(body_len) = self.message.body_len_from_fields() {
            return names_len + body_len;
        }
        names_len
            + match self.message.update_body_write() {
                UpdateBodyWrite::SharedBody(shared_body) => shared_body.body_len(),
                UpdateBodyWrite::PublishedBody(published_body) => published_body.body_len(),
                UpdateBodyWrite::SlicedAccountBody(account_body, data_slice_ranges) => {
                    sliced_account_body_len(account_body, data_slice_ranges)
                }
                UpdateBodyWrite::MemberOneof => {
                    self.message.member_oneof_encoded_len()
                        + message::encoded_len(11u32, &self.created_at)
                }
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

impl FilteredUpdate {
    /// An update writes its message's `created_at` from the body, so it must carry that stamp.
    pub fn new(
        filters: FilteredUpdateFilters,
        message: FilteredUpdateOneof,
        created_at: Timestamp,
    ) -> Self {
        debug_assert!(
            message
                .message_created_at()
                .is_none_or(|message_created_at| message_created_at == created_at),
            "an update carries its message's created_at"
        );
        Self {
            filters,
            message,
            created_at,
        }
    }

    /// An update with no filter names. It carries its message's `created_at`, as the filters'
    /// updates do; a block, a ping or a pong is stamped now.
    pub fn new_empty(message: FilteredUpdateOneof) -> Self {
        let created_at = message
            .message_created_at()
            .unwrap_or_else(|| Timestamp::from(SystemTime::now()));
        Self::new(FilteredUpdateFilters::new(), message, created_at)
    }

    fn as_subscribe_update_account(
        message: &MessageAccount,
        data_slice: &FilterAccountsDataSlice,
    ) -> SubscribeUpdateAccountInfo {
        let acc = &message.account;
        let data_slice = data_slice.get_slice(message.data());
        SubscribeUpdateAccountInfo {
            pubkey: acc.pubkey.as_ref().into(),
            lamports: acc.lamports,
            owner: acc.owner.as_ref().into(),
            executable: acc.executable,
            rent_epoch: acc.rent_epoch,
            data: Bytes::from(data_slice),
            write_version: acc.write_version,
            txn_signature: acc.txn_signature.map(|s| s.as_ref().into()),
        }
    }

    fn as_subscribe_update_transaction(
        message: &MessageTransaction,
    ) -> SubscribeUpdateTransactionInfo {
        let tx = &message.transaction;
        SubscribeUpdateTransactionInfo {
            signature: tx.signature.as_ref().into(),
            is_vote: tx.is_vote,
            transaction: Some(tx.transaction.clone()),
            meta: Some(tx.meta.clone()),
            index: tx.index as u64,
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
            bank_id: message.bank_id,
        }
    }

    pub fn as_subscribe_update(&self) -> SubscribeUpdate {
        let message = match &self.message {
            FilteredUpdateOneof::Account(msg) => UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(Self::as_subscribe_update_account(
                    &msg.account,
                    &msg.data_slice,
                )),
                is_startup: msg.account.is_startup,
                slot: msg.account.slot,
                bank_id: msg.account.bank_id,
            }),
            FilteredUpdateOneof::Slot(msg) => UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: msg.slot,
                parent: msg.parent,
                status: msg.status as i32,
                dead_error: msg.dead_error.clone(),
                bank_id: msg.bank_id,
            }),
            FilteredUpdateOneof::Transaction(msg) => {
                UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(Self::as_subscribe_update_transaction(&msg.transaction)),
                    slot: msg.slot,
                    bank_id: msg.transaction.bank_id,
                })
            }
            FilteredUpdateOneof::TransactionStatus(msg) => {
                let tx = &msg.transaction;
                UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                    slot: tx.slot,
                    signature: tx.transaction.signature.as_ref().into(),
                    is_vote: tx.transaction.is_vote,
                    index: tx.transaction.index as u64,
                    err: tx.transaction.meta.err.clone(),
                    bank_id: tx.bank_id,
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
                bank_id: msg.meta.bank_id,
            }),
            FilteredUpdateOneof::Ping => UpdateOneof::Ping(SubscribeUpdatePing {}),
            FilteredUpdateOneof::Pong(msg) => UpdateOneof::Pong(*msg),
            FilteredUpdateOneof::BlockMeta(msg) => UpdateOneof::BlockMeta(msg.block_meta.clone()),
            FilteredUpdateOneof::EntryUpdateParent(msg) => {
                UpdateOneof::EntryUpdateParent(msg.update_parent.clone())
            }
            FilteredUpdateOneof::Entry(msg) => {
                UpdateOneof::Entry(Self::as_subscribe_update_entry(&msg.0))
            }
            FilteredUpdateOneof::BlockFooter(footer_update) => {
                UpdateOneof::BlockFooter(footer_update.member_block_footer())
            }
        };

        SubscribeUpdate {
            filters: self
                .filters
                .iter()
                .map(|name| name.as_ref().to_string())
                .collect(),
            update_oneof: Some(message),
            created_at: Some(self.created_at),
        }
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
    EntryUpdateParent(Arc<MessageEntryUpdateParent>),   // 13
    BlockFooter(FilteredUpdateBlockFooter),             // 12
}

impl FilteredUpdateOneof {
    pub const fn account(
        message: Arc<MessageAccount>,
        data_slice: FilterAccountsDataSlice,
    ) -> Self {
        Self::Account(FilteredUpdateAccount {
            account: message,
            data_slice,
        })
    }

    pub const fn slot(message: Arc<MessageSlot>) -> Self {
        Self::Slot(FilteredUpdateSlot(message))
    }

    pub fn transaction(message: Arc<MessageTransaction>) -> Self {
        let slot = message.slot;
        Self::Transaction(FilteredUpdateTransaction {
            transaction: message,
            slot,
        })
    }

    pub const fn transaction_status(message: Arc<MessageTransaction>) -> Self {
        Self::TransactionStatus(FilteredUpdateTransactionStatus {
            transaction: message,
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

    pub const fn block_footer(
        footer_message: Arc<MessageBlockFooter>,
        include_certificates: bool,
    ) -> Self {
        Self::BlockFooter(FilteredUpdateBlockFooter {
            block_footer: footer_message,
            include_certificates,
        })
    }

    /// What this member's update writes after its names: the message's body, the account body
    /// cut by the member's data slice, or a oneof field encoded for the member.
    pub fn update_body_write(&self) -> UpdateBodyWrite<'_> {
        match self {
            Self::Account(account_update) => {
                let account_body = account_update.account.account_body();
                let data_slice_ranges = account_update.data_slice.as_ref();
                if data_slice_ranges.is_empty() {
                    UpdateBodyWrite::SharedBody(account_body)
                } else {
                    UpdateBodyWrite::SlicedAccountBody(account_body, data_slice_ranges)
                }
            }
            Self::Slot(slot_update) => UpdateBodyWrite::SharedBody(slot_update.slot_body()),
            Self::Entry(entry_update) => UpdateBodyWrite::SharedBody(entry_update.0.entry_body()),
            Self::BlockMeta(block_meta) => {
                UpdateBodyWrite::SharedBody(block_meta.block_meta_body())
            }
            Self::EntryUpdateParent(update_parent) => {
                UpdateBodyWrite::SharedBody(update_parent.update_parent_body())
            }
            Self::BlockFooter(footer_update) => {
                UpdateBodyWrite::SharedBody(footer_update.footer_body())
            }
            Self::Transaction(transaction_update) => transaction_update
                .transaction
                .transaction_update_body(&StandardTransactionBodyWriter)
                .map_or(UpdateBodyWrite::MemberOneof, UpdateBodyWrite::PublishedBody),
            Self::TransactionStatus(status_update) => status_update
                .transaction
                .transaction_status_body()
                .map_or(UpdateBodyWrite::MemberOneof, UpdateBodyWrite::PublishedBody),
            Self::Block(_) | Self::Ping | Self::Pong(_) => UpdateBodyWrite::MemberOneof,
        }
    }

    /// The body length of a transaction kind, from its fields and its recorded `TransactionInfo`
    /// length, without reading a claim cell; `None` when only the body can tell.
    fn body_len_from_fields(&self) -> Option<usize> {
        match self {
            Self::Transaction(transaction_update) => {
                let message_transaction = &transaction_update.transaction;
                let transaction_info_len =
                    message_transaction.transaction_bodies().built_info_len()?;
                Some(transaction_update_body_len(
                    message_transaction,
                    transaction_info_len,
                ))
            }
            Self::TransactionStatus(status_update) => {
                Some(transaction_status_body_len(&status_update.transaction))
            }
            _ => None,
        }
    }

    /// The `created_at` of the update's message, when the update holds the message.
    fn message_created_at(&self) -> Option<Timestamp> {
        match self {
            Self::Account(account_update) => Some(account_update.account.created_at),
            Self::Slot(slot_update) => Some(slot_update.created_at),
            Self::Transaction(transaction_update) => {
                Some(transaction_update.transaction.created_at)
            }
            Self::TransactionStatus(status_update) => Some(status_update.transaction.created_at),
            Self::BlockMeta(block_meta) => Some(block_meta.created_at),
            Self::Entry(entry_update) => Some(entry_update.0.created_at),
            Self::EntryUpdateParent(update_parent) => Some(update_parent.created_at),
            Self::BlockFooter(footer_update) => Some(footer_update.block_footer.created_at),
            Self::Block(_) | Self::Ping | Self::Pong(_) => None,
        }
    }

    /// Writes the oneof field of a kind that has no shared body.
    fn encode_member_oneof(&self, output_buffer: &mut impl BufMut) {
        match self {
            Self::Transaction(transaction_update) => {
                message::encode(4u32, transaction_update, output_buffer)
            }
            Self::TransactionStatus(status_update) => {
                message::encode(10u32, status_update, output_buffer)
            }
            Self::Block(block_update) => message::encode(5u32, block_update, output_buffer),
            Self::Ping => {
                encode_key(6u32, WireType::LengthDelimited, output_buffer);
                encode_varint(0, output_buffer);
            }
            Self::Pong(pong_update) => message::encode(9u32, pong_update, output_buffer),
            Self::Account(_)
            | Self::Slot(_)
            | Self::Entry(_)
            | Self::BlockMeta(_)
            | Self::EntryUpdateParent(_)
            | Self::BlockFooter(_) => unreachable!("a kind with a shared body writes the body"),
        }
    }

    fn member_oneof_encoded_len(&self) -> usize {
        match self {
            Self::Transaction(transaction_update) => message::encoded_len(4u32, transaction_update),
            Self::TransactionStatus(status_update) => message::encoded_len(10u32, status_update),
            Self::Block(block_update) => message::encoded_len(5u32, block_update),
            Self::Ping => key_len(6u32) + encoded_len_varint(0),
            Self::Pong(pong_update) => message::encoded_len(9u32, pong_update),
            Self::Account(_)
            | Self::Slot(_)
            | Self::Entry(_)
            | Self::BlockMeta(_)
            | Self::EntryUpdateParent(_)
            | Self::BlockFooter(_) => unreachable!("a kind with a shared body writes the body"),
        }
    }
}

/// A footer update: the footer's message, and whether this member gets the certificates.
#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateBlockFooter {
    pub block_footer: Arc<MessageBlockFooter>,
    pub include_certificates: bool,
}

impl FilteredUpdateBlockFooter {
    pub fn footer_body(&self) -> &SharedUpdateBody {
        let footer_bodies = self.block_footer.footer_bodies();
        if self.include_certificates {
            &footer_bodies.footer_with_certificates
        } else {
            &footer_bodies.footer_without_certificates
        }
    }

    /// The footer as this member gets it: without the certificates unless it asked for them.
    pub fn member_block_footer(&self) -> SubscribeUpdateBlockFooter {
        let mut member_block_footer = self.block_footer.block_footer.clone();
        if !self.include_certificates {
            member_block_footer.block_final_cert = None;
            member_block_footer.skip_reward_cert = None;
            member_block_footer.notar_reward_cert = None;
        }
        member_block_footer
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateAccount {
    pub account: Arc<MessageAccount>,
    pub data_slice: FilterAccountsDataSlice,
}

/// A slot update. Subscribe streams write its message's body; the deshred stream writes the
/// fields below, under its own tags.
#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateSlot(Arc<MessageSlot>);

impl Deref for FilteredUpdateSlot {
    type Target = MessageSlot;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for FilteredUpdateSlot {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        let status = SlotStatusProto::from(self.status) as i32;
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if let ::core::option::Option::Some(ref value) = self.parent {
            ::prost::encoding::uint64::encode(2u32, value, buf);
        }
        if status != SlotStatusProto::default() as i32 {
            ::prost::encoding::int32::encode(3u32, &status, buf);
        }
        if let Some(error) = &self.dead_error {
            ::prost::encoding::string::encode(4u32, error, buf);
        }
        if let ::core::option::Option::Some(ref value) = self.bank_id {
            ::prost::encoding::uint64::encode(5u32, value, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let status = SlotStatusProto::from(self.status) as i32;

        (if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + self.parent.as_ref().map_or(0, |value| {
            ::prost::encoding::uint64::encoded_len(2u32, value)
        }) + if status != SlotStatusProto::default() as i32 {
            ::prost::encoding::int32::encoded_len(3u32, &status)
        } else {
            0
        } + if let Some(error) = &self.dead_error {
            ::prost::encoding::string::encoded_len(4u32, error)
        } else {
            0
        } + self.bank_id.as_ref().map_or(0, |value| {
            ::prost::encoding::uint64::encoded_len(5u32, value)
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
pub struct FilteredUpdateTransaction {
    pub transaction: Arc<MessageTransaction>,
    pub slot: u64,
}

/// A member's own Transaction update, written when another writer holds the body's claim or it
/// failed.
impl prost::Message for FilteredUpdateTransaction {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        put_private_transaction_info(1u32, &self.transaction, buf);
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &self.slot, buf);
        }
        if self.transaction.bank_id != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &self.transaction.bank_id, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        prost_field_encoded_len(1u32, private_transaction_info_len(&self.transaction))
            + if self.slot != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &self.slot)
            } else {
                0
            }
            + if self.transaction.bank_id != 0u64 {
                ::prost::encoding::uint64::encoded_len(3u32, &self.transaction.bank_id)
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

/// How a writer got a transaction's `TransactionInfo` bytes.
pub enum TransactionInfoUse {
    /// From the body published in the transaction's claim cell.
    Published,
    /// Written by this writer, because another writer held the claim or it failed.
    Private,
}

pub fn count_transaction_info_use(transaction_info_use: TransactionInfoUse) {
    let data_plane_count = match transaction_info_use {
        TransactionInfoUse::Published => DataPlaneCount::PreEncodedTransactionHit,
        TransactionInfoUse::Private => DataPlaneCount::PreEncodedTransactionMiss,
    };
    DATA_PLANE_COUNTS.add(data_plane_count, 1);
}

/// The `TransactionInfo` length a member writes when it writes the bytes itself.
fn private_transaction_info_len(message_transaction: &MessageTransaction) -> usize {
    message_transaction
        .transaction_bodies()
        .built_info_len()
        .unwrap_or_else(|| transaction_info_len(&message_transaction.transaction))
}

/// Writes a transaction's `TransactionInfo` as field `field_tag`, encoded by this writer, and
/// records its length for weights.
fn put_private_transaction_info(
    field_tag: u32,
    message_transaction: &MessageTransaction,
    output_buffer: &mut impl BufMut,
) {
    let transaction_info_bytes_len = private_transaction_info_len(message_transaction);
    encode_key(field_tag, WireType::LengthDelimited, output_buffer);
    encode_varint(transaction_info_bytes_len as u64, output_buffer);
    put_transaction_info(&message_transaction.transaction, output_buffer);
    message_transaction
        .transaction_bodies()
        .record_built_info_len(transaction_info_bytes_len);
    count_transaction_info_use(TransactionInfoUse::Private);
}

/// Writes a Block's transaction item: the `TransactionInfo` bytes of the transaction's
/// published body, or this writer's own when the body is not published.
pub fn put_block_transaction_item(
    field_tag: u32,
    message_transaction: &MessageTransaction,
    output_buffer: &mut impl BufMut,
) {
    let Some(published_body) =
        message_transaction.transaction_update_body(&StandardTransactionBodyWriter)
    else {
        put_private_transaction_info(field_tag, message_transaction, output_buffer);
        return;
    };
    let transaction_info_range = published_body
        .inner_item_range()
        .expect("a Transaction body holds its TransactionInfo");
    encode_key(field_tag, WireType::LengthDelimited, output_buffer);
    encode_varint(transaction_info_range.len() as u64, output_buffer);
    published_body.for_each_slice_in_range(transaction_info_range, |transaction_info_slice| {
        output_buffer.put_slice(transaction_info_slice)
    });
    count_transaction_info_use(TransactionInfoUse::Published);
}

/// The `TransactionInfo` length of a Block's transaction item.
pub fn block_transaction_item_len(message_transaction: &MessageTransaction) -> usize {
    if let Some(transaction_info_len) = message_transaction.transaction_bodies().built_info_len() {
        return transaction_info_len;
    }
    message_transaction
        .transaction_update_body(&StandardTransactionBodyWriter)
        .and_then(|published_body| published_body.inner_item_range())
        .map_or_else(
            || private_transaction_info_len(message_transaction),
            |transaction_info_range| transaction_info_range.len(),
        )
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateTransactionStatus {
    pub transaction: Arc<MessageTransaction>,
}

impl prost::Message for FilteredUpdateTransactionStatus {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.transaction.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.transaction.slot, buf);
        }
        let tx = &self.transaction.transaction;
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
        if self.transaction.bank_id != 0u64 {
            ::prost::encoding::uint64::encode(6u32, &self.transaction.bank_id, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let tx = &self.transaction.transaction;
        let index = tx.index as u64;

        (if self.transaction.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.transaction.slot)
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
            + if self.transaction.bank_id != 0u64 {
                ::prost::encoding::uint64::encoded_len(6u32, &self.transaction.bank_id)
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
pub struct FilteredUpdateDeshredTransaction {
    pub transaction: Arc<MessageDeshredTransaction>,
}

impl prost::Message for FilteredUpdateDeshredTransaction {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        Self::tx_encode_raw(1u32, &self.transaction.transaction, buf);
        if self.transaction.slot != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &self.transaction.slot, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        prost_field_encoded_len(1u32, Self::tx_encoded_len(&self.transaction.transaction))
            + if self.transaction.slot != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &self.transaction.slot)
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

impl FilteredUpdateDeshredTransaction {
    fn tx_encode_raw(tag: u32, tx: &MessageDeshredTransactionInfo, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::tx_encoded_len(tx) as u64, buf);

        prost_bytes_encode_raw(1u32, tx.signature.as_ref(), buf);
        if tx.is_vote {
            ::prost::encoding::bool::encode(2u32, &tx.is_vote, buf);
        }
        message::encode(3u32, &tx.transaction, buf);
        for pk in &tx.loaded_writable_addresses {
            prost_bytes_encode_raw(4u32, pk.as_ref(), buf);
        }
        for pk in &tx.loaded_readonly_addresses {
            prost_bytes_encode_raw(5u32, pk.as_ref(), buf);
        }
        if tx.completed_data_set_starting_shred_index != 0u32 {
            ::prost::encoding::uint32::encode(
                6u32,
                &tx.completed_data_set_starting_shred_index,
                buf,
            );
        }
        if tx.completed_data_set_ending_shred_index_exclusive != 0u32 {
            ::prost::encoding::uint32::encode(
                7u32,
                &tx.completed_data_set_ending_shred_index_exclusive,
                buf,
            );
        }
    }

    fn tx_encoded_len(tx: &MessageDeshredTransactionInfo) -> usize {
        prost_bytes_encoded_len(1u32, tx.signature.as_ref())
            + if tx.is_vote {
                ::prost::encoding::bool::encoded_len(2u32, &tx.is_vote)
            } else {
                0
            }
            + message::encoded_len(3u32, &tx.transaction)
            + prost_repeated_encoded_len_map!(4u32, tx.loaded_writable_addresses, |pk| pk
                .as_ref()
                .len())
            + prost_repeated_encoded_len_map!(5u32, tx.loaded_readonly_addresses, |pk| pk
                .as_ref()
                .len())
            + if tx.completed_data_set_starting_shred_index != 0u32 {
                ::prost::encoding::uint32::encoded_len(
                    6u32,
                    &tx.completed_data_set_starting_shred_index,
                )
            } else {
                0
            }
            + if tx.completed_data_set_ending_shred_index_exclusive != 0u32 {
                ::prost::encoding::uint32::encoded_len(
                    7u32,
                    &tx.completed_data_set_ending_shred_index_exclusive,
                )
            } else {
                0
            }
    }
}

pub type FilteredUpdatesDeshred = SmallVec<[FilteredUpdateDeshred; 2]>;

#[derive(Debug, Clone, PartialEq)]
pub struct FilteredUpdateDeshred {
    pub filters: FilteredUpdateFilters,
    pub message: FilteredUpdateDeshredOneof,
    pub created_at: Timestamp,
}

impl prost::Message for FilteredUpdateDeshred {
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

impl FilteredUpdateDeshred {
    pub const fn new(
        filters: FilteredUpdateFilters,
        message: FilteredUpdateDeshredOneof,
        created_at: Timestamp,
    ) -> Self {
        Self {
            filters,
            message,
            created_at,
        }
    }

    pub fn new_empty(message: FilteredUpdateDeshredOneof) -> Self {
        Self::new(
            FilteredUpdateFilters::new(),
            message,
            Timestamp::from(SystemTime::now()),
        )
    }

    pub fn ping() -> Self {
        Self::new_empty(FilteredUpdateDeshredOneof::Ping)
    }

    pub fn pong(id: i32) -> Self {
        Self::new_empty(FilteredUpdateDeshredOneof::pong(id))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilteredUpdateDeshredOneof {
    DeshredUpdateParent(Arc<MessageDeshredUpdateParent>), // tag 7
    DeshredTransaction(FilteredUpdateDeshredTransaction), // tag 2
    Ping,                                                 // tag 3
    Pong(SubscribeUpdatePong),                            // tag 4
    Slot(FilteredUpdateSlot),                             // tag 6
}

impl FilteredUpdateDeshredOneof {
    pub fn deshred_transaction(message: &Arc<MessageDeshredTransaction>) -> Self {
        Self::DeshredTransaction(FilteredUpdateDeshredTransaction {
            transaction: Arc::clone(message),
        })
    }

    pub const fn slot(message: Arc<MessageSlot>) -> Self {
        Self::Slot(FilteredUpdateSlot(message))
    }

    pub const fn pong(id: i32) -> Self {
        Self::Pong(SubscribeUpdatePong { id })
    }
}

impl prost::Message for FilteredUpdateDeshredOneof {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            Self::DeshredUpdateParent(msg) => message::encode(7u32, &msg.update_parent, buf),
            Self::DeshredTransaction(msg) => message::encode(2u32, msg, buf),
            Self::Ping => {
                encode_key(3u32, WireType::LengthDelimited, buf);
                encode_varint(0, buf);
            }
            Self::Pong(msg) => message::encode(4u32, msg, buf),
            Self::Slot(msg) => message::encode(6u32, msg, buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::DeshredUpdateParent(msg) => message::encoded_len(7u32, &msg.update_parent),
            Self::DeshredTransaction(msg) => message::encoded_len(2u32, msg),
            Self::Ping => key_len(3u32) + encoded_len_varint(0),
            Self::Pong(msg) => message::encoded_len(4u32, msg),
            Self::Slot(msg) => message::encoded_len(6u32, msg),
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
pub struct FilteredUpdateBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransaction>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccount>>,
    pub accounts_data_slice: FilterAccountsDataSlice,
    pub entries: Vec<Arc<MessageEntry>>,
    pub accounts_data_len: usize,
    pub transactions_data_len: usize,
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
            put_block_transaction_item(6u32, tx, buf);
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
            put_account_info_item(
                11u32,
                account.account_body(),
                self.accounts_data_slice.as_ref(),
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
        if self.meta.bank_id != 0u64 {
            ::prost::encoding::uint64::encode(14u32, &self.meta.bank_id, buf);
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
                block_transaction_item_len(tx)
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
                account_info_item_len(account.account_body(), self.accounts_data_slice.as_ref())
            })
            + if self.meta.entries_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(12u32, &self.meta.entries_count)
            } else {
                0
            }
            + prost_repeated_encoded_len_map!(13u32, self.entries, |entry| {
                FilteredUpdateEntry::entry_encoded_len(entry)
            })
            + if self.meta.bank_id != 0u64 {
                ::prost::encoding::uint64::encoded_len(14u32, &self.meta.bank_id)
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
pub struct FilteredUpdateEntry(pub Arc<MessageEntry>);

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
        if entry.bank_id != 0u64 {
            ::prost::encoding::uint64::encode(7u32, &entry.bank_id, buf);
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
            + if entry.bank_id != 0u64 {
                ::prost::encoding::uint64::encoded_len(7u32, &entry.bank_id)
            } else {
                0
            }
    }
}

#[cfg(any(test, feature = "bench"))]
pub mod tests {
    #[cfg(test)]
    use super::{FilteredUpdate, FilteredUpdateOneof};
    #[cfg(test)]
    use crate::plugin::{
        filter::fixtures,
        message::{MessageBlockFooter, MessageSlot, SlotStatus},
    };
    #[cfg(test)]
    use yellowstone_grpc_proto::geyser::{SubscribeUpdate, SubscribeUpdateBlockFooter};
    #[allow(clippy::disallowed_types, reason = "test messages' OnceLock caches")]
    use {
        super::{FilteredUpdateBlock, FilteredUpdateFilters},
        crate::plugin::{
            convert_to,
            filter::{name::FilterName, FilterAccountsDataSlice},
            message::{
                MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry,
                MessageTransaction, MessageTransactionInfo,
            },
        },
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
        agave_geyser_plugin_interface::transaction_status_meta as mirror,
        foldhash::{HashSet as FoldHashSet, HashSetExt},
        prost::Message,
        prost_types::Timestamp,
        solana_hash::Hash,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        solana_storage_proto::convert::generated,
        solana_transaction_status::{
            ConfirmedBlock, Reward, TransactionStatusMeta, TransactionTokenBalance,
            TransactionWithStatusMeta,
        },
        std::{
            collections::HashMap,
            fs,
            ops::Range,
            str::FromStr,
            sync::{Arc, OnceLock},
            time::SystemTime,
        },
        yellowstone_grpc_proto::{geyser::SubscribeUpdateBlockMeta, prelude as proto},
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
        .map(Vec::into_boxed_slice)
        .map(Arc::from)
        .map(FilterAccountsDataSlice::new_unchecked)
        .collect()
    }

    /// Every presence case of an account write's fields, each with its data.
    pub fn create_accounts_raw() -> Vec<(MessageAccountInfo, Vec<u8>)> {
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
                                let account = MessageAccountInfo {
                                    pubkey,
                                    lamports,
                                    owner,
                                    executable,
                                    rent_epoch,
                                    write_version,
                                    txn_signature,
                                };
                                accounts.push((account, data.clone()));
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
        for (account, account_data) in create_accounts_raw() {
            for slot in [0, 42] {
                for is_startup in [true, false] {
                    for data_slice in create_account_data_slice() {
                        let msg = MessageAccount::new(
                            account,
                            account_data.as_slice(),
                            slot,
                            is_startup,
                            Some(slot),
                            Timestamp::from(SystemTime::now()),
                        );
                        vec.push((msg, data_slice));
                    }
                }
            }
        }
        vec
    }

    pub fn create_entries() -> Vec<Arc<MessageEntry>> {
        [
            MessageEntry::from_entry_info(
                &ReplicaEntryInfoV2 {
                    slot: 299888121,
                    index: 42,
                    num_hashes: 128,
                    hash: Hash::new_from_array([98; 32]).as_ref(),
                    executed_transaction_count: 32,
                    starting_transaction_index: 1000,
                },
                299888121,
                Timestamp::from(SystemTime::now()),
            ),
            MessageEntry::from_entry_info(
                &ReplicaEntryInfoV2 {
                    slot: 299888121,
                    index: 0,
                    num_hashes: 16,
                    hash: Hash::new_from_array([42; 32]).as_ref(),
                    executed_transaction_count: 32,
                    starting_transaction_index: 1000,
                },
                299888121,
                Timestamp::from(SystemTime::now()),
            ),
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

    pub fn load_predefined_transactions() -> Vec<Arc<MessageTransaction>> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|block| {
                block
                    .transactions
                    .into_iter()
                    .map(|tx| (tx.transaction.signature, tx))
            })
            .collect::<HashMap<_, _>>()
            .into_values()
            .collect()
    }

    #[allow(clippy::disallowed_types, reason = "test messages' OnceLock caches")]
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
                        MessageTransaction::new(
                            MessageTransactionInfo {
                                signature: tx.transaction.signatures[0],
                                is_vote: true,
                                transaction: convert_to::create_transaction(&tx.transaction),
                                meta: create_transaction_meta(&tx.meta),
                                index,
                                account_keys: FoldHashSet::new(),
                                token_owners_all: OnceLock::new(),
                                token_owners_changed: OnceLock::new(),
                            },
                            block.parent_slot + 1,
                            block.parent_slot + 1,
                            Timestamp::from(SystemTime::now()),
                        )
                    })
                    .map(Arc::new)
                    .collect::<Vec<_>>();

                let entries = create_entries();

                let slot = block.parent_slot + 1;
                let block_meta1 = MessageBlockMeta::from_update_oneof(
                    SubscribeUpdateBlockMeta {
                        parent_slot: block.parent_slot,
                        slot,
                        parent_blockhash: block.previous_blockhash,
                        blockhash: block.blockhash,
                        rewards: Some(create_rewards_obj(&block.rewards, block.num_partitions)),
                        block_time: block.block_time.map(convert_to::create_timestamp),
                        block_height: block.block_height.map(convert_to::create_block_height),
                        executed_transaction_count: transactions.len() as u64,
                        entries_count: entries.len() as u64,
                        bank_id: slot,
                    },
                    Timestamp::from(SystemTime::now()),
                );
                let mut block_meta2_fields = block_meta1.block_meta.clone();
                block_meta2_fields.rewards = Some(create_rewards_obj(&block.rewards, Some(42)));
                let block_meta2 =
                    MessageBlockMeta::from_update_oneof(block_meta2_fields, block_meta1.created_at);

                let block_meta1 = Arc::new(block_meta1);
                let block_meta2 = Arc::new(block_meta2);

                let accounts = create_accounts_raw()
                    .into_iter()
                    .map(|(acc_info, acc_data)| {
                        Arc::new(MessageAccount::new(
                            acc_info,
                            acc_data.as_slice(),
                            block.parent_slot + 1,
                            false,
                            Some(block.parent_slot + 1),
                            Timestamp::from(SystemTime::now()),
                        ))
                    })
                    .collect::<Vec<_>>();
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
                                accounts_data_len: 0,
                                transactions_data_len: 0,
                            },
                            FilteredUpdateBlock {
                                meta: Arc::clone(&block_meta2),
                                transactions: transactions.clone(),
                                updated_account_count: accounts.len() as u64,
                                accounts: accounts.clone(),
                                accounts_data_slice: data_slice,
                                entries: entries.clone(),
                                accounts_data_len: 0,
                                transactions_data_len: 0,
                            },
                        ]
                    })
            })
            .collect()
    }

    // Fixtures decode to owned `solana_transaction_status` types. These borrow them as the geyser
    // interface mirror, as `solana-geyser-plugin-manager` does, then run the plugin conversion.
    fn create_transaction_meta(meta: &TransactionStatusMeta) -> proto::TransactionStatusMeta {
        let inner_instruction_rows = meta.inner_instructions.as_ref().map(|groups| {
            groups
                .iter()
                .map(|group| {
                    let instructions = group
                        .instructions
                        .iter()
                        .map(|ix| mirror::InnerInstruction {
                            instruction: &ix.instruction,
                            stack_height: ix.stack_height,
                        })
                        .collect::<Vec<_>>();
                    (group.index, instructions)
                })
                .collect::<Vec<_>>()
        });
        let inner_instructions = inner_instruction_rows.as_ref().map(|rows| {
            rows.iter()
                .map(|(index, instructions)| mirror::InnerInstructions {
                    index: *index,
                    instructions,
                })
                .collect::<Vec<_>>()
        });
        let log_messages = meta
            .log_messages
            .as_ref()
            .map(|logs| logs.iter().map(String::as_str).collect::<Vec<_>>());
        let pre_token_balances = meta.pre_token_balances.as_ref().map(|balances| {
            balances
                .iter()
                .map(mirror_token_balance)
                .collect::<Vec<_>>()
        });
        let post_token_balances = meta.post_token_balances.as_ref().map(|balances| {
            balances
                .iter()
                .map(mirror_token_balance)
                .collect::<Vec<_>>()
        });
        let rewards = meta
            .rewards
            .as_ref()
            .map(|rewards| rewards.iter().map(mirror_reward).collect::<Vec<_>>());

        convert_to::create_transaction_meta(&mirror::TransactionStatusMeta {
            status: meta.status.as_ref().copied(),
            fee: meta.fee,
            pre_balances: &meta.pre_balances,
            post_balances: &meta.post_balances,
            inner_instructions: inner_instructions.as_deref(),
            log_messages: log_messages.as_deref(),
            pre_token_balances: pre_token_balances.as_deref(),
            post_token_balances: post_token_balances.as_deref(),
            rewards: rewards.as_deref(),
            loaded_addresses: &meta.loaded_addresses,
            return_data: meta.return_data.as_ref().map(|return_data| {
                mirror::TransactionReturnData {
                    program_id: return_data.program_id,
                    data: &return_data.data,
                }
            }),
            compute_units_consumed: meta.compute_units_consumed,
            cost_units: meta.cost_units,
        })
    }

    fn create_rewards_obj(rewards: &[Reward], num_partitions: Option<u64>) -> proto::Rewards {
        let rewards = rewards.iter().map(mirror_reward).collect::<Vec<_>>();
        convert_to::create_rewards_obj(&rewards, num_partitions)
    }

    fn mirror_token_balance(
        balance: &TransactionTokenBalance,
    ) -> mirror::TransactionTokenBalance<'_> {
        mirror::TransactionTokenBalance {
            account_index: balance.account_index,
            mint: &balance.mint,
            ui_token_amount: mirror::UiTokenAmount {
                ui_amount: balance.ui_token_amount.ui_amount,
                decimals: balance.ui_token_amount.decimals,
                amount: &balance.ui_token_amount.amount,
                ui_amount_string: &balance.ui_token_amount.ui_amount_string,
            },
            owner: &balance.owner,
            program_id: &balance.program_id,
        }
    }

    fn mirror_reward(reward: &Reward) -> mirror::Reward<'_> {
        mirror::Reward {
            pubkey: &reward.pubkey,
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: reward.reward_type,
            commission: reward.commission,
            commission_bps: reward.commission_bps,
        }
    }

    /// The update's bytes equal prost's encoding of the same `SubscribeUpdate`, byte for byte.
    #[cfg(test)]
    fn encode_decode_cmp(filters: &[&str], message: FilteredUpdateOneof) {
        let mut msg = FilteredUpdate::new_empty(message);
        msg.filters = create_message_filters(filters);
        let update = msg.as_subscribe_update();
        let update_bytes = msg.encode_to_vec();
        assert_eq!(msg.encoded_len(), update.encoded_len());
        assert_eq!(update_bytes, update.encode_to_vec());
        assert_eq!(
            SubscribeUpdate::decode(update_bytes.as_slice()).expect("failed to decode"),
            update
        );
    }

    #[test]
    fn test_message_account() {
        for (msg, data_slice) in create_accounts() {
            let msg_arc = Arc::new(msg);
            encode_decode_cmp(&["123"], FilteredUpdateOneof::account(msg_arc, data_slice));
        }
    }

    #[test]
    fn test_message_slot() {
        for slot in [0, 42] {
            for parent in [None, Some(0), Some(42)] {
                for status in [
                    SlotStatus::Processed,
                    SlotStatus::Confirmed,
                    SlotStatus::Finalized,
                    SlotStatus::FirstShredReceived,
                    SlotStatus::Completed,
                    SlotStatus::CreatedBank,
                    SlotStatus::Dead,
                ] {
                    let bank_id = if [
                        SlotStatus::Completed,
                        SlotStatus::FirstShredReceived,
                        SlotStatus::Dead,
                    ]
                    .contains(&status)
                    {
                        None
                    } else {
                        Some(slot)
                    };
                    encode_decode_cmp(
                        &["123"],
                        FilteredUpdateOneof::slot(Arc::new(MessageSlot::new(
                            slot,
                            parent,
                            status,
                            None,
                            bank_id,
                            Timestamp::from(SystemTime::now()),
                        ))),
                    )
                }
                encode_decode_cmp(
                    &["123"],
                    FilteredUpdateOneof::slot(Arc::new(MessageSlot::new(
                        slot,
                        parent,
                        SlotStatus::Dead,
                        Some("123".to_owned()),
                        None,
                        Timestamp::from(SystemTime::now()),
                    ))),
                )
            }
        }
    }

    #[test]
    fn test_message_transaction() {
        for transaction in load_predefined_transactions() {
            encode_decode_cmp(
                &["123"],
                FilteredUpdateOneof::transaction(Arc::clone(&transaction)),
            );
            encode_decode_cmp(
                &["123"],
                FilteredUpdateOneof::transaction_status(transaction),
            );
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
    fn test_message_block_footer() {
        // An empty user agent, a maximal timestamp, absent certificates and an
        // empty bitmap are the interesting edges for the hand-rolled encoder.
        let (final_cert, skip_cert, notar_cert) = fixtures::block_footer_certificates();
        for (
            bank_hash,
            nanos,
            user_agent,
            (block_final_cert, skip_reward_cert, notar_reward_cert),
        ) in [
            (vec![0u8; 32], 0u64, Vec::new(), (None, None, None)),
            (
                vec![7u8; 32],
                u64::MAX,
                b"agave/3.0.0".to_vec(),
                (Some(final_cert), Some(skip_cert), Some(notar_cert)),
            ),
        ] {
            let message = Arc::new(MessageBlockFooter::new(
                SubscribeUpdateBlockFooter {
                    slot: 42,
                    bank_id: 7,
                    bank_hash,
                    block_producer_time_nanos: nanos,
                    block_user_agent: user_agent,
                    block_final_cert,
                    skip_reward_cert,
                    notar_reward_cert,
                },
                Timestamp::default(),
            ));
            for include_certificates in [false, true] {
                encode_decode_cmp(
                    &["123"],
                    FilteredUpdateOneof::block_footer(Arc::clone(&message), include_certificates),
                );
            }
        }
    }

    #[test]
    fn test_message_entry() {
        for entry in create_entries() {
            encode_decode_cmp(&["123"], FilteredUpdateOneof::entry(entry));
        }
    }
}
