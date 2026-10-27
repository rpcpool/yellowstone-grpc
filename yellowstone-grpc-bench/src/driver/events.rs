//! Owned callback payloads built from recorded frames, and the calls that hand them to the plugin.

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoV3, ReplicaAccountInfoVersions, ReplicaBlockInfoV4,
        ReplicaBlockInfoVersions, ReplicaDeshredTransactionInfoV2,
        ReplicaDeshredTransactionInfoVersions, ReplicaDeshredUpdateParentInfo,
        ReplicaDeshredUpdateParentInfoVersions, ReplicaEntryInfoV2, ReplicaEntryInfoVersions,
        ReplicaEntryUpdateParentInfo, ReplicaEntryUpdateParentInfoVersions,
        ReplicaTransactionInfoV3, ReplicaTransactionInfoVersions, SlotStatus,
    },
    anyhow::Context,
    bytes::Bytes,
    solana_hash::Hash,
    solana_message::{
        legacy::Message as LegacyMessageBody, v0::LoadedAddresses, LegacyMessage, MessageHeader,
        SanitizedMessage,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::{sanitized::SanitizedTransaction, versioned::VersionedTransaction},
    solana_transaction_status::{RewardsAndNumPartitions, TransactionStatusMeta},
    std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    },
    yellowstone_grpc_geyser::plugin::convert_from,
    yellowstone_grpc_proto::geyser::{
        SlotStatus as ProtoSlotStatus, SubscribeUpdateAccount, SubscribeUpdateBlockMeta,
        SubscribeUpdateDeshredTransaction, SubscribeUpdateDeshredUpdateParent,
        SubscribeUpdateEntry, SubscribeUpdateEntryUpdateParent, SubscribeUpdateSlot,
        SubscribeUpdateTransaction,
    },
};

pub struct AccountEvent {
    pub slot: u64,
    pub bank_id: u64,
    pub pubkey: Vec<u8>,
    pub owner: Vec<u8>,
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Bytes,
    pub write_version: u64,
    pub txn: Option<Arc<SanitizedTransaction>>,
}

pub struct TransactionEvent {
    pub slot: u64,
    pub bank_id: u64,
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: VersionedTransaction,
    pub meta: TransactionStatusMeta,
    pub index: usize,
}

pub struct EntryEvent {
    pub slot: u64,
    pub bank_id: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: usize,
}

pub struct BlockMetaEvent {
    pub slot: u64,
    pub bank_id: u64,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub rewards: RewardsAndNumPartitions,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
}

pub struct DeshredEvent {
    pub slot: u64,
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: VersionedTransaction,
    pub loaded_addresses: Option<LoadedAddresses>,
    pub starting_shred_index: u32,
    pub ending_shred_index_exclusive: u32,
}

pub enum Event {
    Account(AccountEvent),
    Transaction(Box<TransactionEvent>),
    Entry(EntryEvent),
    BlockMeta(Box<BlockMetaEvent>),
    EntryUpdateParent {
        slot: u64,
        cleared_bank_id: u64,
        parent_slot: u64,
        parent_block_id: Hash,
    },
    BankStatus {
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
        bank_id: u64,
    },
    SlotStatus {
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    },
    Deshred(Box<DeshredEvent>),
    DeshredUpdateParent {
        slot: u64,
        fec_set_index: u32,
        parent_slot: u64,
        parent_block_id: Hash,
    },
}

impl Event {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Account(_) => "account",
            Self::Transaction(_) => "transaction",
            Self::Entry(_) => "entry",
            Self::BlockMeta(_) => "block_meta",
            Self::EntryUpdateParent { .. } => "entry_update_parent",
            Self::BankStatus { .. } => "bank_status",
            Self::SlotStatus { .. } => "slot_status",
            Self::Deshred(_) => "deshred_transaction",
            Self::DeshredUpdateParent { .. } => "deshred_update_parent",
        }
    }

    pub const fn slot(&self) -> u64 {
        match self {
            Self::Account(event) => event.slot,
            Self::Transaction(event) => event.slot,
            Self::Entry(event) => event.slot,
            Self::BlockMeta(event) => event.slot,
            Self::EntryUpdateParent { slot, .. }
            | Self::BankStatus { slot, .. }
            | Self::SlotStatus { slot, .. }
            | Self::DeshredUpdateParent { slot, .. } => *slot,
            Self::Deshred(event) => event.slot,
        }
    }

    /// Slot, bank and status name of the events whose callback times serve as reference times.
    pub fn status_record(&self) -> Option<(u64, Option<u64>, &'static str)> {
        match self {
            Self::BankStatus {
                slot,
                status,
                bank_id,
                ..
            } => Some((*slot, Some(*bank_id), status.as_str())),
            Self::SlotStatus { slot, status, .. } => Some((*slot, None, status.as_str())),
            Self::BlockMeta(event) => Some((event.slot, Some(event.bank_id), "block_meta")),
            _ => None,
        }
    }

    pub fn emit(&self, plugin: &dyn GeyserPlugin) -> anyhow::Result<()> {
        let result = match self {
            Self::Account(event) => {
                let info = ReplicaAccountInfoV3 {
                    pubkey: &event.pubkey,
                    lamports: event.lamports,
                    owner: &event.owner,
                    executable: event.executable,
                    rent_epoch: event.rent_epoch,
                    data: &event.data,
                    write_version: event.write_version,
                    txn: event.txn.as_deref(),
                };
                plugin.update_account_for_bank(
                    ReplicaAccountInfoVersions::V0_0_3(&info),
                    event.slot,
                    event.bank_id,
                )
            }
            Self::Transaction(event) => {
                let message_hash = Hash::default();
                let info = ReplicaTransactionInfoV3 {
                    signature: &event.signature,
                    message_hash: &message_hash,
                    is_vote: event.is_vote,
                    transaction: &event.transaction,
                    transaction_status_meta: &event.meta,
                    index: event.index,
                };
                plugin.notify_transaction_for_bank(
                    ReplicaTransactionInfoVersions::V0_0_3(&info),
                    event.slot,
                    event.bank_id,
                )
            }
            Self::Entry(event) => {
                let info = ReplicaEntryInfoV2 {
                    slot: event.slot,
                    index: event.index,
                    num_hashes: event.num_hashes,
                    hash: &event.hash,
                    executed_transaction_count: event.executed_transaction_count,
                    starting_transaction_index: event.starting_transaction_index,
                };
                plugin.notify_entry_for_bank(ReplicaEntryInfoVersions::V0_0_2(&info), event.bank_id)
            }
            Self::BlockMeta(event) => {
                let info = ReplicaBlockInfoV4 {
                    parent_slot: event.parent_slot,
                    parent_blockhash: &event.parent_blockhash,
                    slot: event.slot,
                    blockhash: &event.blockhash,
                    rewards: &event.rewards,
                    block_time: event.block_time,
                    block_height: event.block_height,
                    executed_transaction_count: event.executed_transaction_count,
                    entry_count: event.entry_count,
                };
                plugin.notify_block_metadata_for_bank(
                    ReplicaBlockInfoVersions::V0_0_4(&info),
                    event.bank_id,
                )
            }
            Self::EntryUpdateParent {
                slot,
                cleared_bank_id,
                parent_slot,
                parent_block_id,
            } => plugin.notify_entry_update_parent(ReplicaEntryUpdateParentInfoVersions::V0_0_1(
                &ReplicaEntryUpdateParentInfo {
                    slot: *slot,
                    cleared_bank_id: *cleared_bank_id,
                    parent_slot: *parent_slot,
                    parent_block_id,
                },
            )),
            Self::BankStatus {
                slot,
                parent,
                status,
                bank_id,
            } => plugin.update_bank_status(*slot, *parent, status, *bank_id),
            Self::SlotStatus {
                slot,
                parent,
                status,
            } => plugin.update_slot_status(*slot, *parent, status),
            Self::Deshred(event) => {
                let info = ReplicaDeshredTransactionInfoV2 {
                    signature: &event.signature,
                    is_vote: event.is_vote,
                    transaction: &event.transaction,
                    loaded_addresses: event.loaded_addresses.as_ref(),
                    completed_data_set_starting_shred_index: event.starting_shred_index,
                    completed_data_set_ending_shred_index_exclusive: event
                        .ending_shred_index_exclusive,
                };
                plugin.notify_deshred_transaction(
                    ReplicaDeshredTransactionInfoVersions::V0_0_2(&info),
                    event.slot,
                )
            }
            Self::DeshredUpdateParent {
                slot,
                fec_set_index,
                parent_slot,
                parent_block_id,
            } => plugin.notify_deshred_update_parent(
                ReplicaDeshredUpdateParentInfoVersions::V0_0_1(&ReplicaDeshredUpdateParentInfo {
                    slot: *slot,
                    update_parent_fec_set_index: *fec_set_index,
                    parent_slot: *parent_slot,
                    parent_block_id,
                }),
            ),
        };
        result.map_err(|error| anyhow::anyhow!("{} callback failed: {error}", self.kind()))
    }
}

/// Builds events from decoded frames. It caches one stub transaction per signature for account writes.
#[derive(Default)]
pub struct Converter {
    stubs: HashMap<Signature, Arc<SanitizedTransaction>>,
}

const STUB_CACHE_LIMIT: usize = 200_000;

impl Converter {
    pub fn account(&mut self, msg: SubscribeUpdateAccount) -> anyhow::Result<Option<Event>> {
        let Some(bank_id) = msg.bank_id else {
            return Ok(None);
        };
        let info = msg.account.context("account update without info")?;
        let txn = match info.txn_signature.as_deref() {
            Some(signature) => Some(self.stub(signature_from(signature)?)),
            None => None,
        };
        Ok(Some(Event::Account(AccountEvent {
            slot: msg.slot,
            bank_id,
            pubkey: info.pubkey,
            owner: info.owner,
            lamports: info.lamports,
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            data: info.data,
            write_version: info.write_version,
            txn,
        })))
    }

    /// The plugin reads only `signature()` from an account's transaction, so a minimal valid one suffices.
    fn stub(&mut self, signature: Signature) -> Arc<SanitizedTransaction> {
        if self.stubs.len() >= STUB_CACHE_LIMIT {
            self.stubs.clear();
        }
        Arc::clone(self.stubs.entry(signature).or_insert_with(|| {
            let body = LegacyMessageBody {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![Pubkey::default()],
                recent_blockhash: Hash::default(),
                instructions: vec![],
            };
            let message = SanitizedMessage::Legacy(LegacyMessage::new(body, &HashSet::new()));
            Arc::new(
                SanitizedTransaction::try_new_from_fields(
                    message,
                    Hash::default(),
                    false,
                    vec![signature],
                )
                .expect("stub transaction is well formed"),
            )
        }))
    }
}

pub fn transaction(msg: SubscribeUpdateTransaction) -> anyhow::Result<Event> {
    let info = msg.transaction.context("transaction update without info")?;
    let transaction =
        convert_from::create_tx_versioned(info.transaction.context("transaction without body")?)
            .map_err(anyhow::Error::msg)?;
    let meta = convert_from::create_tx_meta(info.meta.context("transaction without meta")?)
        .map_err(anyhow::Error::msg)?;
    Ok(Event::Transaction(Box::new(TransactionEvent {
        slot: msg.slot,
        bank_id: msg.bank_id,
        signature: signature_from(&info.signature)?,
        is_vote: info.is_vote,
        transaction,
        meta,
        index: usize::try_from(info.index)?,
    })))
}

pub fn entry(msg: SubscribeUpdateEntry) -> anyhow::Result<Event> {
    Ok(Event::Entry(EntryEvent {
        slot: msg.slot,
        bank_id: msg.bank_id,
        index: usize::try_from(msg.index)?,
        num_hashes: msg.num_hashes,
        hash: msg.hash,
        executed_transaction_count: msg.executed_transaction_count,
        starting_transaction_index: usize::try_from(msg.starting_transaction_index)?,
    }))
}

pub fn block_meta(msg: SubscribeUpdateBlockMeta) -> anyhow::Result<Event> {
    let rewards = convert_from::create_rewards_obj(msg.rewards.unwrap_or_default())
        .map_err(anyhow::Error::msg)?;
    Ok(Event::BlockMeta(Box::new(BlockMetaEvent {
        slot: msg.slot,
        bank_id: msg.bank_id,
        parent_slot: msg.parent_slot,
        parent_blockhash: msg.parent_blockhash,
        blockhash: msg.blockhash,
        rewards,
        block_time: msg.block_time.map(|time| time.timestamp),
        block_height: msg.block_height.map(|height| height.block_height),
        executed_transaction_count: msg.executed_transaction_count,
        entry_count: msg.entries_count,
    })))
}

pub fn entry_update_parent(msg: SubscribeUpdateEntryUpdateParent) -> anyhow::Result<Event> {
    Ok(Event::EntryUpdateParent {
        slot: msg.slot,
        cleared_bank_id: msg.cleared_bank_id,
        parent_slot: msg.parent_slot,
        parent_block_id: hash_from(&msg.parent_block_id)?,
    })
}

/// Maps a recorded slot update onto the callback agave uses for it.
pub fn slot_status(msg: SubscribeUpdateSlot) -> anyhow::Result<Option<Event>> {
    let status = ProtoSlotStatus::try_from(msg.status)
        .map_err(|_| anyhow::anyhow!("unknown slot status {}", msg.status))?;
    let (status, bank_scoped) = match status {
        ProtoSlotStatus::SlotProcessed => (SlotStatus::Processed, true),
        ProtoSlotStatus::SlotConfirmed => (SlotStatus::Confirmed, true),
        ProtoSlotStatus::SlotFinalized => (SlotStatus::Rooted, true),
        ProtoSlotStatus::SlotCreatedBank => (SlotStatus::CreatedBank, true),
        ProtoSlotStatus::SlotFirstShredReceived => (SlotStatus::FirstShredReceived, false),
        ProtoSlotStatus::SlotCompleted => (SlotStatus::Completed, false),
        ProtoSlotStatus::SlotDead => (SlotStatus::Dead(msg.dead_error.unwrap_or_default()), false),
    };
    Ok(match (bank_scoped, msg.bank_id) {
        (true, Some(bank_id)) => Some(Event::BankStatus {
            slot: msg.slot,
            parent: msg.parent,
            status,
            bank_id,
        }),
        (true, None) => None,
        (false, _) => Some(Event::SlotStatus {
            slot: msg.slot,
            parent: msg.parent,
            status,
        }),
    })
}

pub fn deshred_transaction(msg: SubscribeUpdateDeshredTransaction) -> anyhow::Result<Event> {
    let info = msg.transaction.context("deshred update without info")?;
    let transaction = convert_from::create_tx_versioned(
        info.transaction
            .context("deshred transaction without body")?,
    )
    .map_err(anyhow::Error::msg)?;
    // Repeated fields cannot tell None from empty, so empty means no ALT resolution.
    let loaded_addresses =
        if info.loaded_writable_addresses.is_empty() && info.loaded_readonly_addresses.is_empty() {
            None
        } else {
            Some(
                convert_from::create_loaded_addresses(
                    info.loaded_writable_addresses,
                    info.loaded_readonly_addresses,
                )
                .map_err(anyhow::Error::msg)?,
            )
        };
    Ok(Event::Deshred(Box::new(DeshredEvent {
        slot: msg.slot,
        signature: signature_from(&info.signature)?,
        is_vote: info.is_vote,
        transaction,
        loaded_addresses,
        starting_shred_index: info.completed_data_set_starting_shred_index,
        ending_shred_index_exclusive: info.completed_data_set_ending_shred_index_exclusive,
    })))
}

pub fn deshred_update_parent(msg: SubscribeUpdateDeshredUpdateParent) -> anyhow::Result<Event> {
    Ok(Event::DeshredUpdateParent {
        slot: msg.slot,
        fec_set_index: msg.update_parent_fec_set_index,
        parent_slot: msg.parent_slot,
        parent_block_id: hash_from(&msg.parent_block_id)?,
    })
}

fn signature_from(bytes: &[u8]) -> anyhow::Result<Signature> {
    Signature::try_from(bytes)
        .map_err(|_| anyhow::anyhow!("invalid signature length {}", bytes.len()))
}

fn hash_from(bytes: &[u8]) -> anyhow::Result<Hash> {
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid hash length {}", bytes.len()))?;
    Ok(Hash::new_from_array(array))
}
