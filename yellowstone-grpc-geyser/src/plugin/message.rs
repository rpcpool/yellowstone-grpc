#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
#[allow(clippy::disallowed_types, reason = "OnceLock caches of a message")]
use {
    super::convert_to,
    crate::{
        body::{
            block_shape::{BlockSelectionTotals, BlockShapeTable},
            encode_account_body, encode_account_body_with_clock, encode_block_footer_bodies,
            encode_block_meta_body, encode_entry_body, encode_entry_update_parent_body,
            encode_slot_body, BlockFooterBodies, PublishedBody, SharedUpdateBody,
            TransactionBodies,
        },
        extension::seams::{AccountDataSource, AccountFields, TransactionBodyWriter},
    },
    agave_geyser_plugin_interface::{
        block_footer::VersionedBlockFooter,
        geyser_plugin_interface::{
            ReplicaAccountInfoV3, ReplicaBlockFooterInfo, ReplicaBlockInfoV5,
            ReplicaContactInfoV0_0_1, ReplicaDeshredTransactionInfo,
            ReplicaDeshredTransactionInfoV2, ReplicaDeshredTransactionInfoVersions,
            ReplicaDeshredUpdateParentInfo, ReplicaEntryInfoV2, ReplicaEntryUpdateParentInfo,
            ReplicaTransactionInfoV4, SlotStatus as GeyserSlotStatus,
        },
    },
    foldhash::{HashSet as FoldHashSet, HashSetExt},
    prost_types::Timestamp,
    solana_clock::{BankId, Slot},
    solana_hash::{Hash, HASH_BYTES},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        fmt,
        net::SocketAddr,
        ops::Deref,
        sync::{Arc, OnceLock},
        time::SystemTime,
    },
    yellowstone_grpc_concurrency::counted_message::{CountedMessageOwner, CountedMessageRef},
    yellowstone_grpc_proto::{
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SlotStatus as SlotStatusProto,
            SubscribeUpdateBlockFooter, SubscribeUpdateBlockMeta,
            SubscribeUpdateDeshredUpdateParent, SubscribeUpdateEntryUpdateParent,
        },
        solana::storage::confirmed_block,
    },
};

type FromUpdateOneofResult<T> = Result<T, &'static str>;

/// The header of every captured message: the count of its queue entries, which keeps the
/// message alive while entries of it are queued.
#[derive(Default)]
pub struct CapturedMessageHeader {
    entry_references: CountedMessageRef,
}

impl CapturedMessageHeader {
    pub const fn entry_references(&self) -> &CountedMessageRef {
        &self.entry_references
    }
}

/// A clone is a new message, so it starts with no entries.
impl Clone for CapturedMessageHeader {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// The entry count is bookkeeping, so it never makes two messages differ.
impl PartialEq for CapturedMessageHeader {
    fn eq(&self, _other_header: &Self) -> bool {
        true
    }
}

impl fmt::Debug for CapturedMessageHeader {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.entry_references.fmt(debug_formatter)
    }
}

macro_rules! counted_message_owners {
    ($($message_type:ty),+ $(,)?) => {$(
        // SAFETY: entries are added only through an `Arc` of the message, and the count is a
        // field of the message's own header.
        unsafe impl CountedMessageOwner for $message_type {
            fn entry_references(&self) -> &CountedMessageRef {
                self.captured_header.entry_references()
            }
        }
    )+};
}

counted_message_owners!(
    MessageSlot,
    MessageAccount,
    MessageTransaction,
    MessageDeshredTransaction,
    MessageEntry,
    MessageBlockFooter,
    MessageBlockMeta,
    MessageBlock,
    MessageEntryUpdateParent,
    MessageDeshredUpdateParent,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommitmentLevel {
    Processed,
    Confirmed,
    Finalized,
}

impl From<CommitmentLevel> for CommitmentLevelProto {
    fn from(commitment: CommitmentLevel) -> Self {
        match commitment {
            CommitmentLevel::Processed => Self::Processed,
            CommitmentLevel::Confirmed => Self::Confirmed,
            CommitmentLevel::Finalized => Self::Finalized,
        }
    }
}

impl From<CommitmentLevelProto> for CommitmentLevel {
    fn from(status: CommitmentLevelProto) -> Self {
        match status {
            CommitmentLevelProto::Processed => Self::Processed,
            CommitmentLevelProto::Confirmed => Self::Confirmed,
            CommitmentLevelProto::Finalized => Self::Finalized,
        }
    }
}

impl CommitmentLevel {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SlotStatus {
    Processed,
    Confirmed,
    Finalized,
    FirstShredReceived,
    Completed,
    CreatedBank,
    Dead,
}

impl From<&GeyserSlotStatus> for SlotStatus {
    fn from(status: &GeyserSlotStatus) -> Self {
        match status {
            GeyserSlotStatus::Processed => Self::Processed,
            GeyserSlotStatus::Confirmed => Self::Confirmed,
            GeyserSlotStatus::Rooted => Self::Finalized,
            GeyserSlotStatus::FirstShredReceived => Self::FirstShredReceived,
            GeyserSlotStatus::Completed => Self::Completed,
            GeyserSlotStatus::CreatedBank => Self::CreatedBank,
            GeyserSlotStatus::Dead(_error) => Self::Dead,
        }
    }
}

impl From<SlotStatusProto> for SlotStatus {
    fn from(status: SlotStatusProto) -> Self {
        match status {
            SlotStatusProto::SlotProcessed => Self::Processed,
            SlotStatusProto::SlotConfirmed => Self::Confirmed,
            SlotStatusProto::SlotFinalized => Self::Finalized,
            SlotStatusProto::SlotFirstShredReceived => Self::FirstShredReceived,
            SlotStatusProto::SlotCompleted => Self::Completed,
            SlotStatusProto::SlotCreatedBank => Self::CreatedBank,
            SlotStatusProto::SlotDead => Self::Dead,
        }
    }
}

impl From<SlotStatus> for SlotStatusProto {
    fn from(status: SlotStatus) -> Self {
        match status {
            SlotStatus::Processed => Self::SlotProcessed,
            SlotStatus::Confirmed => Self::SlotConfirmed,
            SlotStatus::Finalized => Self::SlotFinalized,
            SlotStatus::FirstShredReceived => Self::SlotFirstShredReceived,
            SlotStatus::Completed => Self::SlotCompleted,
            SlotStatus::CreatedBank => Self::SlotCreatedBank,
            SlotStatus::Dead => Self::SlotDead,
        }
    }
}

impl PartialEq<SlotStatus> for CommitmentLevel {
    fn eq(&self, other: &SlotStatus) -> bool {
        match self {
            Self::Processed if *other == SlotStatus::Processed => true,
            Self::Confirmed if *other == SlotStatus::Confirmed => true,
            Self::Finalized if *other == SlotStatus::Finalized => true,
            _ => false,
        }
    }
}

impl SlotStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
            Self::FirstShredReceived => "first_shread_received",
            Self::Completed => "completed",
            Self::CreatedBank => "created_bank",
            Self::Dead => "dead",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageSlot {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: SlotStatus,
    pub dead_error: Option<String>,
    pub created_at: Timestamp,
    // FIRST_SHRED_RECEIVED and COMPLETED does not have any bank id.
    pub bank_id: Option<BankId>,
    pub captured_header: CapturedMessageHeader,
    /// The update's bytes after `filters`, built from the fields above.
    slot_body: SharedUpdateBody,
}

impl MessageSlot {
    pub fn new(
        slot: Slot,
        parent: Option<Slot>,
        status: SlotStatus,
        dead_error: Option<String>,
        bank_id: Option<BankId>,
        created_at: Timestamp,
    ) -> Self {
        let slot_body = encode_slot_body(
            slot,
            parent,
            status,
            dead_error.as_deref(),
            bank_id,
            created_at,
        );
        Self {
            slot,
            parent,
            status,
            dead_error,
            created_at,
            bank_id,
            captured_header: CapturedMessageHeader::default(),
            slot_body,
        }
    }

    pub fn from_geyser(
        slot: Slot,
        parent: Option<Slot>,
        status: &GeyserSlotStatus,
        bank_id: Option<BankId>,
    ) -> Self {
        let dead_error = if let GeyserSlotStatus::Dead(error) = status {
            Some(error.clone())
        } else {
            None
        };
        let created_at = Timestamp::from(SystemTime::now());
        Self::new(slot, parent, status.into(), dead_error, bank_id, created_at)
    }

    pub const fn slot_body(&self) -> &SharedUpdateBody {
        &self.slot_body
    }
}

/// The fields filters read of one account write. Its data lives in its message's body.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MessageAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
}

impl MessageAccountInfo {
    pub fn from_geyser(info: &ReplicaAccountInfoV3<'_>) -> Self {
        Self {
            pubkey: Pubkey::try_from(info.pubkey).expect("valid Pubkey"),
            lamports: info.lamports,
            owner: Pubkey::try_from(info.owner).expect("valid Pubkey"),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            write_version: info.write_version,
            txn_signature: info.txn.map(|txn| *txn.signature()),
        }
    }

    pub const fn from_account_fields(account_fields: &AccountFields) -> Self {
        Self {
            pubkey: account_fields.account_pubkey,
            lamports: account_fields.account_lamports,
            owner: account_fields.owner_pubkey,
            executable: account_fields.is_executable,
            rent_epoch: account_fields.rent_epoch,
            write_version: account_fields.write_version,
            txn_signature: account_fields.transaction_signature,
        }
    }

    pub const fn account_fields(&self) -> AccountFields {
        AccountFields {
            account_pubkey: self.pubkey,
            account_lamports: self.lamports,
            owner_pubkey: self.owner,
            is_executable: self.executable,
            rent_epoch: self.rent_epoch,
            write_version: self.write_version,
            transaction_signature: self.txn_signature,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageAccount {
    pub account: MessageAccountInfo,
    pub slot: Slot,
    pub is_startup: bool,
    // startup account update has no bank id.
    pub bank_id: Option<BankId>,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
    /// The update's bytes after `filters`, which hold the account's only copy of its data.
    account_body: SharedUpdateBody,
}

/// Filters read the account's fields through its message.
impl Deref for MessageAccount {
    type Target = MessageAccountInfo;

    fn deref(&self) -> &Self::Target {
        &self.account
    }
}

impl MessageAccount {
    pub fn new(
        account: MessageAccountInfo,
        account_data: impl AccountDataSource,
        slot: Slot,
        is_startup: bool,
        bank_id: Option<BankId>,
        created_at: Timestamp,
    ) -> Self {
        let account_body = encode_account_body(
            &account.account_fields(),
            account_data,
            slot,
            is_startup,
            bank_id,
            created_at,
        );
        Self {
            account,
            slot,
            is_startup,
            bank_id,
            created_at,
            captured_header: CapturedMessageHeader::default(),
            account_body,
        }
    }

    /// Builds an account write's update as capture does: it stamps `created_at` after it copies
    /// the data into the body.
    pub fn capture(
        account: MessageAccountInfo,
        account_data: impl AccountDataSource,
        slot: Slot,
        is_startup: bool,
        bank_id: Option<BankId>,
    ) -> Self {
        let (account_body, created_at) = encode_account_body_with_clock(
            &account.account_fields(),
            account_data,
            slot,
            is_startup,
            bank_id,
            || Timestamp::from(SystemTime::now()),
        );
        Self {
            account,
            slot,
            is_startup,
            bank_id,
            created_at,
            captured_header: CapturedMessageHeader::default(),
            account_body,
        }
    }

    pub fn from_geyser(
        info: &ReplicaAccountInfoV3<'_>,
        slot: Slot,
        is_startup: bool,
        bank_id: Option<BankId>,
    ) -> Self {
        if is_startup {
            assert!(
                bank_id.is_none(),
                "startup account update should have no bank id"
            );
        }
        let account = MessageAccountInfo::from_geyser(info);
        Self::capture(account, info.data, slot, is_startup, bank_id)
    }

    /// The account's data: a view into the body.
    pub fn data(&self) -> &[u8] {
        self.account_body
            .account_data()
            .expect("an account body holds its data")
    }

    pub const fn account_body(&self) -> &SharedUpdateBody {
        &self.account_body
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: confirmed_block::Transaction,
    pub meta: confirmed_block::TransactionStatusMeta,
    pub index: usize,
    pub account_keys: FoldHashSet<Pubkey>,
    /// Per-tx cache of token-balance owners under `TokenAccountsMode::All`.
    /// Lazily built on first read; shared across all filters evaluating
    /// against this tx so the pre/post scan runs at most once per tx.
    #[allow(clippy::disallowed_types, reason = "lazy token owner cache")]
    pub token_owners_all: OnceLock<FoldHashSet<Pubkey>>,
    /// Per-tx cache of token-balance owners under
    /// `TokenAccountsMode::BalanceChanged`. Same laziness as above.
    #[allow(clippy::disallowed_types, reason = "lazy token owner cache")]
    pub token_owners_changed: OnceLock<FoldHashSet<Pubkey>>,
}

impl MessageTransactionInfo {
    #[allow(clippy::disallowed_types, reason = "creates the lazy caches")]
    pub fn from_geyser(info: &ReplicaTransactionInfoV4<'_>) -> Self {
        let account_keys = info
            .transaction
            .message
            .static_account_keys() // Since V3, dynamic account are only available in `loaded_addresses`
            .iter()
            .chain(
                info.transaction_status_meta
                    .loaded_addresses
                    .writable
                    .iter(),
            )
            .chain(
                info.transaction_status_meta
                    .loaded_addresses
                    .readonly
                    .iter(),
            )
            .copied()
            .collect();

        Self {
            signature: *info.signature,
            is_vote: info.is_vote,
            transaction: convert_to::create_transaction(info.transaction),
            meta: convert_to::create_transaction_meta(info.transaction_status_meta),
            index: info.index,
            account_keys,
            token_owners_all: OnceLock::new(),
            token_owners_changed: OnceLock::new(),
        }
    }

    pub fn fill_account_keys(&mut self) -> FromUpdateOneofResult<()> {
        let mut account_keys = FoldHashSet::new();

        // static
        if let Some(pubkeys) = self
            .transaction
            .message
            .as_ref()
            .map(|msg| msg.account_keys.as_slice())
        {
            for pubkey in pubkeys {
                account_keys.insert(
                    Pubkey::try_from(pubkey.as_slice()).map_err(|_| "invalid pubkey length")?,
                );
            }
        }

        // dynamic
        for pubkey in self.meta.loaded_writable_addresses.iter() {
            account_keys
                .insert(Pubkey::try_from(pubkey.as_slice()).map_err(|_| "invalid pubkey length")?);
        }
        for pubkey in self.meta.loaded_readonly_addresses.iter() {
            account_keys
                .insert(Pubkey::try_from(pubkey.as_slice()).map_err(|_| "invalid pubkey length")?);
        }

        self.account_keys = account_keys;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageTransaction {
    pub transaction: MessageTransactionInfo,
    pub slot: u64,
    pub created_at: Timestamp,
    pub bank_id: BankId,
    pub captured_header: CapturedMessageHeader,
    /// The update bodies, each built by the first writer that needs it.
    transaction_bodies: TransactionBodies,
}

impl MessageTransaction {
    pub fn new(
        transaction: MessageTransactionInfo,
        slot: Slot,
        bank_id: BankId,
        created_at: Timestamp,
    ) -> Self {
        Self {
            transaction,
            slot,
            created_at,
            bank_id,
            captured_header: CapturedMessageHeader::default(),
            transaction_bodies: TransactionBodies::default(),
        }
    }

    pub fn from_geyser(info: &ReplicaTransactionInfoV4<'_>, slot: Slot, bank_id: BankId) -> Self {
        let transaction = MessageTransactionInfo::from_geyser(info);
        Self::new(
            transaction,
            slot,
            bank_id,
            Timestamp::from(SystemTime::now()),
        )
    }

    pub const fn transaction_bodies(&self) -> &TransactionBodies {
        &self.transaction_bodies
    }

    /// The Transaction update body, built by this call when its cell is free; `None` when
    /// another writer holds the claim or it failed.
    pub fn transaction_update_body<Writer: TransactionBodyWriter>(
        &self,
        body_writer: &Writer,
    ) -> Option<PublishedBody> {
        self.transaction_bodies
            .transaction_update_body(body_writer, self)
    }

    /// The TransactionStatus update body, built by this call when its cell is free.
    pub fn transaction_status_body(&self) -> Option<PublishedBody> {
        self.transaction_bodies.transaction_status_body(self)
    }

    /// Makes the `TransactionInfo` length known, as block take-in needs: builds the Transaction
    /// body when its cell is free, and otherwise measures the bytes another writer is building.
    pub fn ensure_transaction_info<Writer: TransactionBodyWriter>(&self, body_writer: &Writer) {
        if self.transaction_update_body(body_writer).is_none()
            && self.transaction_bodies.built_info_len().is_none()
        {
            let transaction_info_len = body_writer.transaction_info_len(&self.transaction);
            self.transaction_bodies
                .record_built_info_len(transaction_info_len);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageDeshredTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: confirmed_block::Transaction,
    pub static_account_keys: FoldHashSet<Pubkey>,
    pub loaded_writable_addresses: Vec<Pubkey>,
    pub loaded_readonly_addresses: Vec<Pubkey>,
    pub completed_data_set_starting_shred_index: u32,
    pub completed_data_set_ending_shred_index_exclusive: u32,
}

impl MessageDeshredTransactionInfo {
    pub fn from_geyser(info: &ReplicaDeshredTransactionInfo<'_>) -> Self {
        let static_account_keys: FoldHashSet<Pubkey> = info
            .transaction
            .message
            .static_account_keys()
            .iter()
            .copied()
            .collect();

        let (loaded_writable_addresses, loaded_readonly_addresses) = info
            .loaded_addresses
            .map(|la| (la.writable.clone(), la.readonly.clone()))
            .unwrap_or_default();

        Self {
            signature: *info.signature,
            is_vote: info.is_vote,
            transaction: convert_to::create_transaction(info.transaction),
            static_account_keys,
            loaded_writable_addresses,
            loaded_readonly_addresses,
            completed_data_set_starting_shred_index: 0,
            completed_data_set_ending_shred_index_exclusive: 0,
        }
    }

    pub fn from_geyser_v2(info: &ReplicaDeshredTransactionInfoV2<'_>) -> Self {
        let static_account_keys: FoldHashSet<Pubkey> = info
            .transaction
            .message
            .static_account_keys()
            .iter()
            .copied()
            .collect();

        let (loaded_writable_addresses, loaded_readonly_addresses) = info
            .loaded_addresses
            .map(|la| (la.writable.clone(), la.readonly.clone()))
            .unwrap_or_default();

        Self {
            signature: *info.signature,
            is_vote: info.is_vote,
            transaction: convert_to::create_transaction(info.transaction),
            static_account_keys,
            loaded_writable_addresses,
            loaded_readonly_addresses,
            completed_data_set_starting_shred_index: info.completed_data_set_starting_shred_index,
            completed_data_set_ending_shred_index_exclusive: info
                .completed_data_set_ending_shred_index_exclusive,
        }
    }

    /// Returns all account keys (static + dynamically loaded from ALTs).
    pub fn all_account_keys(&self) -> impl Iterator<Item = &Pubkey> {
        self.static_account_keys
            .iter()
            .chain(self.loaded_writable_addresses.iter())
            .chain(self.loaded_readonly_addresses.iter())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageDeshredTransaction {
    pub transaction: MessageDeshredTransactionInfo,
    pub slot: u64,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
}

impl MessageDeshredTransaction {
    pub fn from_geyser_versioned(
        transaction: ReplicaDeshredTransactionInfoVersions<'_>,
        slot: Slot,
    ) -> Self {
        let info = match transaction {
            ReplicaDeshredTransactionInfoVersions::V0_0_1(v1) => {
                MessageDeshredTransactionInfo::from_geyser(v1)
            }
            ReplicaDeshredTransactionInfoVersions::V0_0_2(v2) => {
                MessageDeshredTransactionInfo::from_geyser_v2(v2)
            }
        };
        Self {
            transaction: info,
            slot,
            created_at: Timestamp::from(SystemTime::now()),
            captured_header: CapturedMessageHeader::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
    pub bank_id: BankId,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
    /// The update's bytes after `filters`, built from the fields above.
    entry_body: SharedUpdateBody,
}

impl MessageEntry {
    pub fn from_geyser(info: &ReplicaEntryInfoV2, bank_id: BankId) -> Self {
        Self::from_entry_info(info, bank_id, Timestamp::from(SystemTime::now()))
    }

    pub fn from_entry_info(
        entry_info: &ReplicaEntryInfoV2,
        bank_id: BankId,
        created_at: Timestamp,
    ) -> Self {
        Self {
            slot: entry_info.slot,
            index: entry_info.index,
            num_hashes: entry_info.num_hashes,
            hash: Hash::new_from_array(<[u8; HASH_BYTES]>::try_from(entry_info.hash).unwrap()),
            executed_transaction_count: entry_info.executed_transaction_count,
            starting_transaction_index: entry_info
                .starting_transaction_index
                .try_into()
                .expect("failed convert usize to u64"),
            created_at,
            bank_id,
            captured_header: CapturedMessageHeader::default(),
            entry_body: encode_entry_body(entry_info, bank_id, created_at),
        }
    }

    pub const fn entry_body(&self) -> &SharedUpdateBody {
        &self.entry_body
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBlockFooter {
    pub block_footer: SubscribeUpdateBlockFooter,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
    /// The update's bytes after `filters`, without and with the certificates.
    footer_bodies: BlockFooterBodies,
}

impl Deref for MessageBlockFooter {
    type Target = SubscribeUpdateBlockFooter;

    fn deref(&self) -> &Self::Target {
        &self.block_footer
    }
}

impl MessageBlockFooter {
    pub fn new(block_footer: SubscribeUpdateBlockFooter, created_at: Timestamp) -> Self {
        let footer_bodies = encode_block_footer_bodies(&block_footer, created_at);
        Self {
            block_footer,
            created_at,
            captured_header: CapturedMessageHeader::default(),
            footer_bodies,
        }
    }

    pub const fn footer_bodies(&self) -> &BlockFooterBodies {
        &self.footer_bodies
    }

    pub fn from_geyser(info: &ReplicaBlockFooterInfo<'_>, bank_id: BankId) -> Self {
        let VersionedBlockFooter::V1(footer) = info.block_footer;

        Self::new(
            SubscribeUpdateBlockFooter {
                slot: info.slot,
                bank_id,
                bank_hash: footer.bank_hash.to_bytes().to_vec(),
                block_producer_time_nanos: footer.block_producer_time_nanos,
                block_user_agent: footer.block_user_agent.to_vec(),
                block_final_cert: footer
                    .block_final_cert
                    .as_ref()
                    .map(convert_to::create_block_final_cert),
                skip_reward_cert: footer
                    .skip_reward_cert
                    .as_ref()
                    .map(convert_to::create_skip_reward_cert),
                notar_reward_cert: footer
                    .notar_reward_cert
                    .as_ref()
                    .map(convert_to::create_notar_reward_cert),
            },
            Timestamp::from(SystemTime::now()),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBlockMeta {
    pub block_meta: SubscribeUpdateBlockMeta,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
    /// The update's bytes after `filters`, built from the fields above.
    block_meta_body: SharedUpdateBody,
}

impl Deref for MessageBlockMeta {
    type Target = SubscribeUpdateBlockMeta;

    fn deref(&self) -> &Self::Target {
        &self.block_meta
    }
}

impl MessageBlockMeta {
    pub fn from_geyser(info: &ReplicaBlockInfoV5<'_>, bank_id: BankId) -> Self {
        Self::from_update_oneof(
            SubscribeUpdateBlockMeta {
                parent_slot: info.parent_slot,
                slot: info.slot,
                parent_blockhash: info.parent_blockhash.to_string(),
                blockhash: info.blockhash.to_string(),
                rewards: Some(convert_to::create_rewards_obj(
                    info.rewards.rewards,
                    info.rewards.num_partitions,
                )),
                block_time: info.block_time.map(convert_to::create_timestamp),
                block_height: info.block_height.map(convert_to::create_block_height),
                executed_transaction_count: info.executed_transaction_count,
                entries_count: info.entry_count,
                bank_id,
            },
            Timestamp::from(SystemTime::now()),
        )
    }

    pub fn from_update_oneof(block_meta: SubscribeUpdateBlockMeta, created_at: Timestamp) -> Self {
        let block_meta_body = encode_block_meta_body(&block_meta, created_at);
        Self {
            block_meta,
            created_at,
            captured_header: CapturedMessageHeader::default(),
            block_meta_body,
        }
    }

    pub const fn block_meta_body(&self) -> &SharedUpdateBody {
        &self.block_meta_body
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransaction>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccount>>,
    pub entries: Vec<Arc<MessageEntry>>,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
    /// The Block bodies of the block's shapes, and the totals of its whole item lists.
    pub block_shapes: BlockShapeTable,
}

impl MessageBlock {
    pub fn new(
        meta: Arc<MessageBlockMeta>,
        transactions: Vec<Arc<MessageTransaction>>,
        accounts: Vec<Arc<MessageAccount>>,
        entries: Vec<Arc<MessageEntry>>,
    ) -> Self {
        let created_at = Timestamp::from(SystemTime::now());
        Self::new_created_at(meta, transactions, accounts, entries, created_at)
    }

    /// The block, sealed at `created_at`. Its whole totals read the `TransactionInfo` lengths
    /// that take-in recorded, as the reference matcher's weights do.
    pub fn new_created_at(
        meta: Arc<MessageBlockMeta>,
        transactions: Vec<Arc<MessageTransaction>>,
        accounts: Vec<Arc<MessageAccount>>,
        entries: Vec<Arc<MessageEntry>>,
        created_at: Timestamp,
    ) -> Self {
        let whole_block_totals = BlockSelectionTotals {
            transaction_count: transactions.len(),
            transaction_info_bytes: transactions
                .iter()
                .map(|message_transaction| {
                    let transaction_bodies = message_transaction.transaction_bodies();
                    transaction_bodies.built_info_len().unwrap_or(0)
                })
                .sum(),
            account_count: accounts.len(),
            account_data_bytes: accounts
                .iter()
                .map(|message_account| message_account.data().len())
                .sum(),
        };
        Self {
            meta,
            transactions,
            updated_account_count: accounts.len() as u64,
            accounts,
            entries,
            created_at,
            captured_header: CapturedMessageHeader::default(),
            block_shapes: BlockShapeTable::new(whole_block_totals),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageContactInfo {
    pub pubkey: Pubkey,
    pub wallclock: u64,
    pub outset: u64,
    pub shred_version: u16,
    pub version_major: u16,
    pub version_minor: u16,
    pub version_patch: u16,
    pub version_commit: u32,
    pub version_feature_set: u32,
    pub version_client_id: u16,
    pub gossip: Option<SocketAddr>,
    pub tpu_quic: Option<SocketAddr>,
    pub tpu_forwards_quic: Option<SocketAddr>,
    pub tpu_vote_udp: Option<SocketAddr>,
    pub tpu_vote_quic: Option<SocketAddr>,
    pub tvu_udp: Option<SocketAddr>,
    pub tvu_quic: Option<SocketAddr>,
    pub serve_repair_udp: Option<SocketAddr>,
    pub serve_repair_quic: Option<SocketAddr>,
    pub rpc: Option<SocketAddr>,
    pub rpc_pubsub: Option<SocketAddr>,
    pub alpenglow: Option<SocketAddr>,
    pub created_at: Timestamp,
}

impl MessageContactInfo {
    pub fn from_geyser(info: &ReplicaContactInfoV0_0_1<'_>) -> Self {
        Self {
            pubkey: Pubkey::try_from(info.pubkey).expect("valid pubkey"),
            wallclock: info.wallclock,
            outset: info.outset,
            shred_version: info.shred_version,
            version_major: info.version_major,
            version_minor: info.version_minor,
            version_patch: info.version_patch,
            version_commit: info.version_commit,
            version_feature_set: info.version_feature_set,
            version_client_id: info.version_client_id,
            gossip: info.gossip,
            tpu_quic: info.tpu_quic,
            tpu_forwards_quic: info.tpu_forwards_quic,
            tpu_vote_udp: info.tpu_vote_udp,
            tpu_vote_quic: info.tpu_vote_quic,
            tvu_udp: info.tvu_udp,
            tvu_quic: info.tvu_quic,
            serve_repair_udp: info.serve_repair_udp,
            serve_repair_quic: info.serve_repair_quic,
            rpc: info.rpc,
            rpc_pubsub: info.rpc_pubsub,
            alpenglow: info.alpenglow,
            created_at: Timestamp::from(SystemTime::now()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageContactInfoRemoved {
    pub pubkey: Pubkey,
    pub created_at: Timestamp,
}

impl MessageContactInfoRemoved {
    pub fn from_geyser(pubkey: &[u8]) -> Self {
        Self {
            pubkey: Pubkey::try_from(pubkey).expect("valid pubkey"),
            created_at: Timestamp::from(SystemTime::now()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContactInfoMessage {
    Node(Arc<MessageContactInfo>),
    Removed(Arc<MessageContactInfoRemoved>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageEntryUpdateParent {
    pub update_parent: SubscribeUpdateEntryUpdateParent,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
    /// The update's bytes after `filters`, built from the fields above.
    update_parent_body: SharedUpdateBody,
}

impl MessageEntryUpdateParent {
    pub fn new(update_parent: SubscribeUpdateEntryUpdateParent, created_at: Timestamp) -> Self {
        let update_parent_body = encode_entry_update_parent_body(&update_parent, created_at);
        Self {
            update_parent,
            created_at,
            captured_header: CapturedMessageHeader::default(),
            update_parent_body,
        }
    }

    pub const fn update_parent_body(&self) -> &SharedUpdateBody {
        &self.update_parent_body
    }

    pub fn from_geyser(info: &ReplicaEntryUpdateParentInfo<'_>) -> Self {
        Self::new(
            SubscribeUpdateEntryUpdateParent {
                slot: info.slot,
                cleared_bank_id: info.cleared_bank_id,
                parent_slot: info.parent_slot,
                parent_block_id: info.parent_block_id.as_ref().to_vec(),
            },
            Timestamp::from(SystemTime::now()),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageDeshredUpdateParent {
    pub update_parent: SubscribeUpdateDeshredUpdateParent,
    pub created_at: Timestamp,
    pub captured_header: CapturedMessageHeader,
}

impl MessageDeshredUpdateParent {
    pub fn from_geyser(info: &ReplicaDeshredUpdateParentInfo<'_>) -> Self {
        Self {
            update_parent: SubscribeUpdateDeshredUpdateParent {
                slot: info.slot,
                update_parent_fec_set_index: info.update_parent_fec_set_index,
                parent_slot: info.parent_slot,
                parent_block_id: info.parent_block_id.as_ref().to_vec(),
            },
            created_at: Timestamp::from(SystemTime::now()),
            captured_header: CapturedMessageHeader::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    DeshredUpdateParent(Arc<MessageDeshredUpdateParent>),
    EntryUpdateParent(Arc<MessageEntryUpdateParent>),
    Slot(Arc<MessageSlot>),
    Account(Arc<MessageAccount>),
    Transaction(Arc<MessageTransaction>),
    DeshredTransaction(Arc<MessageDeshredTransaction>),
    Entry(Arc<MessageEntry>),
    BlockFooter(Arc<MessageBlockFooter>),
    BlockMeta(Arc<MessageBlockMeta>),
    Block(Arc<MessageBlock>),
}

impl Message {
    #[allow(clippy::missing_const_for_fn)]
    pub fn get_slot(&self) -> u64 {
        match self {
            Self::Slot(msg) => msg.slot,
            Self::Account(msg) => msg.slot,
            Self::Transaction(msg) => msg.slot,
            Self::DeshredTransaction(msg) => msg.slot,
            Self::EntryUpdateParent(msg) => msg.update_parent.slot,
            Self::DeshredUpdateParent(msg) => msg.update_parent.slot,
            Self::Entry(msg) => msg.slot,
            Self::BlockFooter(msg) => msg.slot,
            Self::BlockMeta(msg) => msg.slot,
            Self::Block(msg) => msg.meta.slot,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::MessageBlockFooter,
        agave_geyser_plugin_interface::{
            block_footer::{
                BlockFinalizationCert, BlockFooterV1, NotarRewardCertificate,
                SkipRewardCertificate, VersionedBlockFooter, VotesAggregate,
            },
            geyser_plugin_interface::ReplicaBlockFooterInfo,
        },
        solana_bls_signatures::SignatureCompressed,
        solana_hash::Hash,
    };

    fn footer_message(footer: &VersionedBlockFooter<'_>) -> MessageBlockFooter {
        let info = ReplicaBlockFooterInfo {
            slot: 10,
            block_footer: footer,
        };
        MessageBlockFooter::from_geyser(&info, 1)
    }

    #[test]
    fn footer_certificates_become_typed_proto_messages() {
        let final_bitmap = vec![0xa5; 300];
        let notar_bitmap = vec![0x5a; 7];
        let skip_bitmap = vec![0x11; 200];
        let reward_bitmap = vec![0x22; 20_000];
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::new_from_array([1; 32]),
            block_producer_time_nanos: 42,
            block_user_agent: b"agave",
            block_final_cert: Some(BlockFinalizationCert {
                slot: 10,
                block_id: Hash::new_from_array([2; 32]),
                final_aggregate: VotesAggregate {
                    signature: SignatureCompressed([3; 96]),
                    bitmap: &final_bitmap,
                },
                notar_aggregate: Some(VotesAggregate {
                    signature: SignatureCompressed([4; 96]),
                    bitmap: &notar_bitmap,
                }),
            }),
            skip_reward_cert: Some(SkipRewardCertificate {
                slot: 11,
                signature: SignatureCompressed([5; 96]),
                bitmap: &skip_bitmap,
            }),
            notar_reward_cert: Some(NotarRewardCertificate {
                slot: 12,
                block_id: Hash::new_from_array([6; 32]),
                signature: SignatureCompressed([7; 96]),
                bitmap: &reward_bitmap,
            }),
        });
        let message = footer_message(&footer);

        // A slow finalization puts the block_id on the notarize aggregate.
        let cert = message.block_final_cert.as_ref().expect("final cert");
        assert_eq!(cert.slot, 10);
        let final_aggregate = cert.final_aggregate.as_ref().expect("final aggregate");
        assert_eq!(final_aggregate.signature, vec![3; 96]);
        assert_eq!(final_aggregate.signer_bitmap, final_bitmap);
        assert!(final_aggregate.block_id.is_empty());
        let notar_aggregate = cert.notar_aggregate.as_ref().expect("notar aggregate");
        assert_eq!(notar_aggregate.signature, vec![4; 96]);
        assert_eq!(notar_aggregate.signer_bitmap, notar_bitmap);
        assert_eq!(notar_aggregate.block_id, vec![2; 32]);

        let skip = message.skip_reward_cert.as_ref().expect("skip reward cert");
        let skip_aggregate = skip.aggregate.as_ref().expect("skip aggregate");
        assert_eq!(skip.slot, 11);
        assert_eq!(skip_aggregate.signature, vec![5; 96]);
        assert_eq!(skip_aggregate.signer_bitmap, skip_bitmap);
        assert!(skip_aggregate.block_id.is_empty());

        let notar = message
            .notar_reward_cert
            .as_ref()
            .expect("notar reward cert");
        let notar_aggregate = notar.aggregate.as_ref().expect("notar aggregate");
        assert_eq!(notar.slot, 12);
        assert_eq!(notar_aggregate.signature, vec![7; 96]);
        assert_eq!(notar_aggregate.signer_bitmap, reward_bitmap);
        assert_eq!(notar_aggregate.block_id, vec![6; 32]);
    }

    #[test]
    fn absent_footer_certificates_stay_absent() {
        let bitmap = [0xff; 3];
        let footer = VersionedBlockFooter::V1(BlockFooterV1 {
            bank_hash: Hash::default(),
            block_producer_time_nanos: 0,
            block_user_agent: &[],
            block_final_cert: Some(BlockFinalizationCert {
                slot: 10,
                block_id: Hash::new_from_array([2; 32]),
                final_aggregate: VotesAggregate {
                    signature: SignatureCompressed([8; 96]),
                    bitmap: &bitmap,
                },
                notar_aggregate: None,
            }),
            skip_reward_cert: None,
            notar_reward_cert: None,
        });
        let message = footer_message(&footer);

        // A fast finalization puts the block_id on the final aggregate.
        let cert = message.block_final_cert.as_ref().expect("final cert");
        assert!(cert.notar_aggregate.is_none());
        assert_eq!(
            cert.final_aggregate
                .as_ref()
                .expect("final aggregate")
                .block_id,
            vec![2; 32]
        );
        assert_eq!(message.skip_reward_cert, None);
        assert_eq!(message.notar_reward_cert, None);
    }
}
