use {
    super::convert_to,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockFooterInfo, ReplicaBlockInfoV4, ReplicaContactInfoV0_0_1,
        ReplicaDeshredTransactionInfo, ReplicaDeshredTransactionInfoV2,
        ReplicaDeshredTransactionInfoVersions, ReplicaEntryInfoV2, ReplicaTransactionInfoV3,
        SlotStatus as GeyserSlotStatus,
    },
    bytes::Bytes,
    foldhash::{HashSet as FoldHashSet, HashSetExt},
    prost_types::Timestamp,
    solana_clock::{BankId, Slot},
    solana_entry::block_component::VersionedBlockFooter,
    solana_hash::{Hash, HASH_BYTES},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        net::SocketAddr,
        ops::{Deref, DerefMut},
        sync::{Arc, OnceLock},
        time::SystemTime,
    },
    yellowstone_grpc_proto::{
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SlotStatus as SlotStatusProto,
            SubscribeUpdateBlockFooter, SubscribeUpdateBlockMeta,
        },
        solana::storage::confirmed_block,
    },
};

type FromUpdateOneofResult<T> = Result<T, &'static str>;

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
}

impl MessageSlot {
    pub fn from_geyser(
        slot: Slot,
        parent: Option<Slot>,
        status: &GeyserSlotStatus,
        bank_id: Option<BankId>,
    ) -> Self {
        Self {
            slot,
            parent,
            status: status.into(),
            dead_error: if let GeyserSlotStatus::Dead(error) = status {
                Some(error.clone())
            } else {
                None
            },
            created_at: Timestamp::from(SystemTime::now()),
            bank_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Bytes,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
    pub pre_encoded: OnceLock<Vec<u8>>,
}

impl MessageAccountInfo {
    pub fn from_geyser(info: &ReplicaAccountInfoV3<'_>) -> Self {
        let shared = info.data.to_vec();
        let data = Bytes::from(shared);
        Self {
            pubkey: Pubkey::try_from(info.pubkey).expect("valid Pubkey"),
            lamports: info.lamports,
            owner: Pubkey::try_from(info.owner).expect("valid Pubkey"),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            data,
            write_version: info.write_version,
            txn_signature: info.txn.map(|txn| *txn.signature()),
            pre_encoded: OnceLock::new(),
        }
    }

    pub fn get_pre_encoded(&self) -> Option<&Vec<u8>> {
        self.pre_encoded.get()
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
}

impl MessageAccount {
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
        Self {
            account: MessageAccountInfo::from_geyser(info),
            slot,
            is_startup,
            bank_id,
            created_at: Timestamp::from(SystemTime::now()),
        }
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
    pub pre_encoded: OnceLock<Vec<u8>>,
    /// Per-tx cache of token-balance owners under `TokenAccountsMode::All`.
    /// Lazily built on first read; shared across all filters evaluating
    /// against this tx so the pre/post scan runs at most once per tx.
    pub token_owners_all: OnceLock<FoldHashSet<Pubkey>>,
    /// Per-tx cache of token-balance owners under
    /// `TokenAccountsMode::BalanceChanged`. Same laziness as above.
    pub token_owners_changed: OnceLock<FoldHashSet<Pubkey>>,
}

impl MessageTransactionInfo {
    pub fn from_geyser(info: &ReplicaTransactionInfoV3<'_>) -> Self {
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
            pre_encoded: OnceLock::new(),
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

    #[inline]
    pub fn get_pre_encoded(&self) -> Option<&Vec<u8>> {
        self.pre_encoded.get()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageTransaction {
    pub transaction: MessageTransactionInfo,
    pub slot: u64,
    pub created_at: Timestamp,
    pub bank_id: BankId,
}

impl MessageTransaction {
    pub fn from_geyser(info: &ReplicaTransactionInfoV3<'_>, slot: Slot, bank_id: BankId) -> Self {
        Self {
            transaction: MessageTransactionInfo::from_geyser(info),
            slot,
            created_at: Timestamp::from(SystemTime::now()),
            bank_id,
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MessageEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
    pub bank_id: BankId,
    pub created_at: Timestamp,
}

impl MessageEntry {
    pub fn from_geyser(info: &ReplicaEntryInfoV2, bank_id: BankId) -> Self {
        Self {
            slot: info.slot,
            index: info.index,
            num_hashes: info.num_hashes,
            hash: Hash::new_from_array(<[u8; HASH_BYTES]>::try_from(info.hash).unwrap()),
            executed_transaction_count: info.executed_transaction_count,
            starting_transaction_index: info
                .starting_transaction_index
                .try_into()
                .expect("failed convert usize to u64"),
            created_at: Timestamp::from(SystemTime::now()),
            bank_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBlockFooter {
    pub block_footer: SubscribeUpdateBlockFooter,
    pub created_at: Timestamp,
}

impl Deref for MessageBlockFooter {
    type Target = SubscribeUpdateBlockFooter;

    fn deref(&self) -> &Self::Target {
        &self.block_footer
    }
}

impl MessageBlockFooter {
    pub fn from_geyser(info: &ReplicaBlockFooterInfo<'_>, bank_id: BankId) -> Self {
        let VersionedBlockFooter::V1(footer) = info.block_footer;

        Self {
            block_footer: SubscribeUpdateBlockFooter {
                slot: info.slot,
                bank_id,
                bank_hash: footer.bank_hash.to_bytes().to_vec(),
                block_producer_time_nanos: footer.block_producer_time_nanos,
                block_user_agent: footer.block_user_agent.clone(),
            },
            created_at: Timestamp::from(SystemTime::now()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBlockMeta {
    pub block_meta: SubscribeUpdateBlockMeta,
    pub created_at: Timestamp,
}

impl Deref for MessageBlockMeta {
    type Target = SubscribeUpdateBlockMeta;

    fn deref(&self) -> &Self::Target {
        &self.block_meta
    }
}

impl DerefMut for MessageBlockMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block_meta
    }
}

impl MessageBlockMeta {
    pub fn from_geyser(info: &ReplicaBlockInfoV4<'_>, bank_id: BankId) -> Self {
        Self {
            block_meta: SubscribeUpdateBlockMeta {
                parent_slot: info.parent_slot,
                slot: info.slot,
                parent_blockhash: info.parent_blockhash.to_string(),
                blockhash: info.blockhash.to_string(),
                rewards: Some(convert_to::create_rewards_obj(
                    &info.rewards.rewards,
                    info.rewards.num_partitions,
                )),
                block_time: info.block_time.map(convert_to::create_timestamp),
                block_height: info.block_height.map(convert_to::create_block_height),
                executed_transaction_count: info.executed_transaction_count,
                entries_count: info.entry_count,
                bank_id,
            },
            created_at: Timestamp::from(SystemTime::now()),
        }
    }

    pub const fn from_update_oneof(
        block_meta: SubscribeUpdateBlockMeta,
        created_at: Timestamp,
    ) -> Self {
        Self {
            block_meta,
            created_at,
        }
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
}

impl MessageBlock {
    pub fn new(
        meta: Arc<MessageBlockMeta>,
        transactions: Vec<Arc<MessageTransaction>>,
        accounts: Vec<Arc<MessageAccount>>,
        entries: Vec<Arc<MessageEntry>>,
    ) -> Self {
        Self {
            meta,
            transactions,
            updated_account_count: accounts.len() as u64,
            accounts,
            entries,
            created_at: Timestamp::from(SystemTime::now()),
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
pub enum Message {
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
            Self::Entry(msg) => msg.slot,
            Self::BlockFooter(msg) => msg.slot,
            Self::BlockMeta(msg) => msg.slot,
            Self::Block(msg) => msg.meta.slot,
        }
    }
}
