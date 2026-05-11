use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use prost::Message as ProstMessage;
use prost_types::Timestamp;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use yellowstone_grpc_proto::prelude as proto;
use yellowstone_shmem_client::codec::{DecodeError, ShmemDecoder};
use yellowstone_shmem_common::{EventType, GeyserMessage, SlotStatus};

use crate::plugin::message::{
    Message, MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry, MessageSlot,
    MessageTransaction, MessageTransactionInfo,
};

/// Implements [`ShmemDecoder`] by deserializing the bincode envelope
/// into a [`GeyserMessage`], then converting it into a dragons mouth
/// [`Message`] that `geyser_loop` already understands.
pub struct ProstShmemDecoder;

impl ShmemDecoder for ProstShmemDecoder {
    fn decode(
        &self,
        slot: u64,
        event_type: u8,
        bytes: &[u8],
    ) -> Result<GeyserMessage, DecodeError> {
        let et = EventType::try_from(event_type)
            .map_err(|_| DecodeError::DecodeError(format!("unknown event_type: {event_type}")))?;
        match et {
            EventType::Slot => decode_slot(bytes),
            EventType::Account => decode_account(bytes),
            EventType::Transaction => decode_transaction(slot, bytes),
            EventType::Entry => decode_entry(bytes),
            EventType::BlockMeta => decode_block_meta(bytes),
        }
    }
}

fn decode_slot(bytes: &[u8]) -> Result<GeyserMessage, DecodeError> {
    let p = proto::SubscribeUpdateSlot::decode(bytes)
        .map_err(|e| DecodeError::DecodeError(e.to_string()))?;
    let status =
        match proto::SlotStatus::try_from(p.status).unwrap_or(proto::SlotStatus::SlotProcessed) {
            proto::SlotStatus::SlotProcessed => SlotStatus::Processed,
            proto::SlotStatus::SlotConfirmed => SlotStatus::Confirmed,
            proto::SlotStatus::SlotFinalized => SlotStatus::Rooted,
            proto::SlotStatus::SlotFirstShredReceived => SlotStatus::FirstShredReceived,
            proto::SlotStatus::SlotCompleted => SlotStatus::Completed,
            proto::SlotStatus::SlotCreatedBank => SlotStatus::CreatedBank,
            proto::SlotStatus::SlotDead => SlotStatus::Dead(p.dead_error.unwrap_or_default()),
        };
    Ok(GeyserMessage::Slot(yellowstone_shmem_common::MessageSlot {
        slot: p.slot,
        parent: p.parent,
        status,
    }))
}

fn decode_account(bytes: &[u8]) -> Result<GeyserMessage, DecodeError> {
    let p = proto::SubscribeUpdateAccount::decode(bytes)
        .map_err(|e| DecodeError::DecodeError(e.to_string()))?;
    let info = p
        .account
        .ok_or_else(|| DecodeError::DecodeError("missing account".into()))?;
    Ok(GeyserMessage::Account(
        yellowstone_shmem_common::MessageAccount {
            account: yellowstone_shmem_common::MessageAccountInfo {
                pubkey: info.pubkey,
                lamports: info.lamports,
                owner: info.owner,
                executable: info.executable,
                rent_epoch: info.rent_epoch,
                data: info.data.to_vec(),
                write_version: info.write_version,
                txn_signature: info.txn_signature,
            },
            slot: p.slot,
            is_startup: p.is_startup,
        },
    ))
}

fn decode_transaction(slot: u64, bytes: &[u8]) -> Result<GeyserMessage, DecodeError> {
    let p = proto::SubscribeUpdateTransactionInfo::decode(bytes)
        .map_err(|e| DecodeError::DecodeError(e.to_string()))?;

    let account_keys = p
        .transaction
        .as_ref()
        .and_then(|tx| tx.message.as_ref())
        .map(|msg| msg.account_keys.clone())
        .unwrap_or_default();

    let transaction_bytes = p.transaction.map(|t| t.encode_to_vec()).unwrap_or_default();
    let meta_bytes = p.meta.map(|m| m.encode_to_vec()).unwrap_or_default();

    Ok(GeyserMessage::Transaction(
        yellowstone_shmem_common::MessageTransaction {
            transaction: yellowstone_shmem_common::MessageTransactionInfo {
                signature: p.signature,
                is_vote: p.is_vote,
                transaction: transaction_bytes,
                meta: meta_bytes,
                index: p.index as usize,
                account_keys,
            },
            slot,
        },
    ))
}

fn decode_entry(bytes: &[u8]) -> Result<GeyserMessage, DecodeError> {
    let p = proto::SubscribeUpdateEntry::decode(bytes)
        .map_err(|e| DecodeError::DecodeError(e.to_string()))?;
    Ok(GeyserMessage::Entry(
        yellowstone_shmem_common::MessageEntry {
            slot: p.slot,
            index: p.index as usize,
            num_hashes: p.num_hashes,
            hash: p.hash,
            executed_transaction_count: p.executed_transaction_count,
            starting_transaction_index: p.starting_transaction_index,
        },
    ))
}

fn decode_block_meta(bytes: &[u8]) -> Result<GeyserMessage, DecodeError> {
    let p = proto::SubscribeUpdateBlockMeta::decode(bytes)
        .map_err(|e| DecodeError::DecodeError(e.to_string()))?;
    Ok(GeyserMessage::BlockMeta(
        yellowstone_shmem_common::MessageBlockMeta {
            parent_slot: p.parent_slot,
            parent_blockhash: p.parent_blockhash,
            slot: p.slot,
            blockhash: p.blockhash,
            rewards: p.rewards.map(|r| r.encode_to_vec()).unwrap_or_default(),
            block_time: p.block_time.map(|t| t.timestamp),
            block_height: p.block_height.map(|h| h.block_height),
            executed_transaction_count: p.executed_transaction_count,
            entry_count: p.entries_count,
        },
    ))
}

impl ProstShmemDecoder {
    /// Converts a decoded [`GeyserMessage`] into a dragons mouth [`Message`].
    pub fn to_dm_message(msg: GeyserMessage) -> Result<Message, String> {
        let now = Timestamp::from(SystemTime::now());

        match msg {
            GeyserMessage::Slot(s) => {
                let status = convert_slot_status(&s.status);
                Ok(Message::Slot(MessageSlot {
                    slot: s.slot,
                    parent: s.parent,
                    status,
                    dead_error: None,
                    created_at: now,
                }))
            }

            GeyserMessage::Account(a) => {
                let pubkey =
                    Pubkey::try_from(a.account.pubkey.as_slice()).map_err(|_| "invalid pubkey")?;
                let owner =
                    Pubkey::try_from(a.account.owner.as_slice()).map_err(|_| "invalid owner")?;
                let txn_signature = a
                    .account
                    .txn_signature
                    .map(|s| Signature::try_from(s.as_slice()))
                    .transpose()
                    .map_err(|_| "invalid txn_signature")?;

                Ok(Message::Account(MessageAccount {
                    account: Arc::new(MessageAccountInfo {
                        pubkey,
                        lamports: a.account.lamports,
                        owner,
                        executable: a.account.executable,
                        rent_epoch: a.account.rent_epoch,
                        data: a.account.data.into(),
                        write_version: a.account.write_version,
                        txn_signature,
                        pre_encoded: OnceLock::new(),
                    }),
                    slot: a.slot,
                    is_startup: a.is_startup,
                    created_at: now,
                }))
            }

            GeyserMessage::Transaction(t) => {
                let signature = Signature::try_from(t.transaction.signature.as_slice())
                    .map_err(|_| "invalid signature")?;

                let transaction = proto::Transaction::decode(t.transaction.transaction.as_slice())
                    .map_err(|e| format!("decode transaction: {e}"))?;

                let meta = proto::TransactionStatusMeta::decode(t.transaction.meta.as_slice())
                    .map_err(|e| format!("decode meta: {e}"))?;

                let account_keys: HashSet<Pubkey> = t
                    .transaction
                    .account_keys
                    .iter()
                    .filter_map(|k| Pubkey::try_from(k.as_slice()).ok())
                    .collect();

                Ok(Message::Transaction(MessageTransaction {
                    transaction: Arc::new(MessageTransactionInfo {
                        signature,
                        is_vote: t.transaction.is_vote,
                        transaction,
                        meta,
                        index: t.transaction.index,
                        account_keys,
                        pre_encoded: OnceLock::new(),
                    }),
                    slot: t.slot,
                    created_at: now,
                }))
            }

            GeyserMessage::Entry(e) => {
                use solana_hash::Hash;
                let hash = Hash::new_from_array(
                    <[u8; 32]>::try_from(e.hash.as_slice()).map_err(|_| "invalid hash length")?,
                );
                Ok(Message::Entry(Arc::new(MessageEntry {
                    slot: e.slot,
                    index: e.index,
                    num_hashes: e.num_hashes,
                    hash,
                    executed_transaction_count: e.executed_transaction_count,
                    starting_transaction_index: e.starting_transaction_index,
                    created_at: now,
                })))
            }

            GeyserMessage::BlockMeta(b) => {
                let rewards = proto::Rewards::decode(b.rewards.as_slice())
                    .map_err(|e| format!("decode rewards: {e}"))?;

                Ok(Message::BlockMeta(Arc::new(MessageBlockMeta {
                    block_meta: yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
                        parent_slot: b.parent_slot,
                        parent_blockhash: b.parent_blockhash,
                        slot: b.slot,
                        blockhash: b.blockhash,
                        rewards: Some(rewards),
                        block_time: b.block_time.map(|t| proto::UnixTimestamp { timestamp: t }),
                        block_height: b
                            .block_height
                            .map(|h| proto::BlockHeight { block_height: h }),
                        executed_transaction_count: b.executed_transaction_count,
                        entries_count: b.entry_count,
                    },
                    created_at: now,
                })))
            }
        }
    }
}

const fn convert_slot_status(status: &SlotStatus) -> crate::plugin::message::SlotStatus {
    use crate::plugin::message::SlotStatus as DmSlotStatus;
    match status {
        SlotStatus::Processed => DmSlotStatus::Processed,
        SlotStatus::Confirmed => DmSlotStatus::Confirmed,
        SlotStatus::Rooted => DmSlotStatus::Finalized,
        SlotStatus::FirstShredReceived => DmSlotStatus::FirstShredReceived,
        SlotStatus::Completed => DmSlotStatus::Completed,
        SlotStatus::CreatedBank => DmSlotStatus::CreatedBank,
        SlotStatus::Dead(_e) => DmSlotStatus::Dead,
    }
}
