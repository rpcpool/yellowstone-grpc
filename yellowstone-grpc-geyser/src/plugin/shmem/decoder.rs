use foldhash::HashSet as FoldHashSet;
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use std::{
    sync::{Arc, OnceLock},
    time::SystemTime,
};
use yellowstone_grpc_proto::prelude as proto;
use yellowstone_shmem_client::{
    codec::{DecodeError, ShmemDecoder},
    SnapshotAccount,
};
use yellowstone_shmem_common::{
    EventType, GeyserMessage, SlotStatus, HEADER_SIZE, PAYLOAD_VERSION,
};

use crate::plugin::message::{
    Message, MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry, MessageSlot,
    MessageTransaction, MessageTransactionInfo,
};

/// Implements [`ShmemDecoder`] by deserializing the bincode envelope
/// into a [`GeyserMessage`], then converting it into a dragons mouth
/// [`Message`] that `geyser_loop` already understands.
pub struct ProstShmemDecoder;

impl ShmemDecoder for ProstShmemDecoder {
    fn decode(&self, bytes: &[u8]) -> Result<GeyserMessage, DecodeError> {
        if bytes.len() < HEADER_SIZE {
            return Err(DecodeError::DecodeError(format!(
                "payload too short: {} < {HEADER_SIZE}",
                bytes.len()
            )));
        }
        if bytes[0] != PAYLOAD_VERSION {
            return Err(DecodeError::DecodeError(format!(
                "version mismatch: got {}, expected {PAYLOAD_VERSION}",
                bytes[0]
            )));
        }
        let slot = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let event_type = bytes[9];
        let plugin_ts_ns = i64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let body = &bytes[HEADER_SIZE..];

        let et = EventType::try_from(event_type)
            .map_err(|_| DecodeError::DecodeError(format!("unknown event_type: {event_type}")))?;

        match et {
            EventType::Slot => decode_slot(body),
            EventType::Account => decode_account(body, plugin_ts_ns),
            EventType::Transaction => decode_transaction(slot, body),
            EventType::Entry => decode_entry(body),
            EventType::BlockMeta => decode_block_meta(body),
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

fn decode_account(bytes: &[u8], created_at_ns: i64) -> Result<GeyserMessage, DecodeError> {
    // Minimum size: fixed fields with no data and always-present 64-byte sig slot.
    const MIN_SIZE: usize = 32 + 8 + 32 + 1 + 8 + 8 + 1 + 64 + 8 + 8;
    if bytes.len() < MIN_SIZE {
        return Err(DecodeError::DecodeError(format!(
            "decode_account: buffer too small: {} < {MIN_SIZE}",
            bytes.len()
        )));
    }

    let mut o = 0usize;

    unsafe fn read_u8(bytes: &[u8], o: &mut usize) -> u8 {
        let v = bytes[*o];
        *o += 1;
        v
    }

    unsafe fn read_u64(bytes: &[u8], o: &mut usize) -> u64 {
        let v = u64::from_le_bytes(bytes[*o..*o + 8].try_into().unwrap());
        *o += 8;
        v
    }

    unsafe fn read_bytes(bytes: &[u8], o: &mut usize, len: usize) -> Vec<u8> {
        let v = bytes[*o..*o + len].to_vec();
        *o += len;
        v
    }

    unsafe {
        let pubkey = read_bytes(bytes, &mut o, 32);
        let lamports = read_u64(bytes, &mut o);
        let owner = read_bytes(bytes, &mut o, 32);
        let executable = read_u8(bytes, &mut o) != 0;
        let rent_epoch = read_u64(bytes, &mut o);
        let write_version = read_u64(bytes, &mut o);

        let txn_signature = if read_u8(bytes, &mut o) != 0 {
            Some(read_bytes(bytes, &mut o, 64))
        } else {
            o += 64; // skip zeroed bytes
            None
        };

        let data_len = read_u64(bytes, &mut o) as usize;

        if o + data_len > bytes.len() {
            return Err(DecodeError::DecodeError(format!(
                "decode_account: data_len {data_len} exceeds buffer length {}",
                bytes.len()
            )));
        }

        let data = read_bytes(bytes, &mut o, data_len);
        let slot = read_u64(bytes, &mut o);

        Ok(GeyserMessage::Account(
            yellowstone_shmem_common::MessageAccount {
                account: yellowstone_shmem_common::MessageAccountInfo {
                    pubkey,
                    lamports,
                    owner,
                    executable,
                    rent_epoch,
                    data,
                    write_version,
                    txn_signature,
                },
                slot,
                created_at_ns,
            },
        ))
    }
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
    pub fn to_dm_message(msg: GeyserMessage, created_at: Timestamp) -> Result<Message, String> {
        let message = Self::convert(msg, created_at)?;
        Ok(message)
    }

    fn convert(msg: GeyserMessage, created_at: Timestamp) -> Result<Message, String> {
        let now = created_at;

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

                let created_at = prost_types::Timestamp {
                    seconds: a.created_at_ns / 1_000_000_000,
                    nanos: (a.created_at_ns % 1_000_000_000) as i32,
                };

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
                    is_startup: false, // hardcode so as not to break proto
                    created_at,
                }))
            }

            GeyserMessage::Transaction(t) => {
                let signature = Signature::try_from(t.transaction.signature.as_slice())
                    .map_err(|_| "invalid signature")?;

                let transaction = proto::Transaction::decode(t.transaction.transaction.as_slice())
                    .map_err(|e| format!("decode transaction: {e}"))?;

                let meta = proto::TransactionStatusMeta::decode(t.transaction.meta.as_slice())
                    .map_err(|e| format!("decode meta: {e}"))?;

                let account_keys: FoldHashSet<Pubkey> = t
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
                        token_owners_all: OnceLock::new(),
                        token_owners_changed: OnceLock::new(),
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

/// Converts a raw [`SnapshotAccount`] from the mmap region
/// into a dragons-mouth [`Message`] for client delivery.
pub fn snapshot_account_to_message(account: SnapshotAccount) -> Result<Message, &'static str> {
    let pubkey = Pubkey::try_from(account.pubkey.as_slice()).map_err(|_| "invalid pubkey")?;
    let owner = Pubkey::try_from(account.owner.as_slice()).map_err(|_| "invalid owner")?;

    Ok(Message::Account(MessageAccount {
        account: Arc::new(MessageAccountInfo {
            pubkey,
            lamports: account.lamports,
            owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: account.data.into(),
            write_version: account.write_version,
            txn_signature: None,
            pre_encoded: OnceLock::new(),
        }),
        slot: account.slot,
        is_startup: true,
        created_at: Timestamp::from(SystemTime::now()),
    }))
}
