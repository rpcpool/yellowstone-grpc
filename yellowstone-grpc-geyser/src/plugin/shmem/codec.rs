use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
};
use prost::Message as ProstMessage;
use yellowstone_grpc_proto::geyser::SlotStatus as ProtoSlotStatus;
use yellowstone_shmem_common::GeyserMessage;
use yellowstone_shmem_plugin::codec::GeyserCodec;

use crate::plugin::convert_to;

/// Implements [`GeyserCodec`] using prost for sub-fields (transaction,
/// meta, rewards) and bincode for the outer [`GeyserMessage`] envelope.
///
/// Sub-fields are encoded as proto bytes so the decoder on the client
/// side can reconstruct dragons mouth's internal `Message` types
/// directly from the proto types they already use.
pub struct ProstGeyserCodec;

impl GeyserCodec for ProstGeyserCodec {
    fn encode_transaction(&self, transaction: &ReplicaTransactionInfoVersions) -> Vec<u8> {
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_)
            | ReplicaTransactionInfoVersions::V0_0_2(_) => {
                unreachable!("only V0_0_3 is supported")
            }
            ReplicaTransactionInfoVersions::V0_0_3(t) => {
                convert_to::create_transaction(t.transaction).encode_to_vec()
            }
        }
    }

    fn encode_meta(&self, transaction: &ReplicaTransactionInfoVersions) -> Vec<u8> {
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_)
            | ReplicaTransactionInfoVersions::V0_0_2(_) => {
                unreachable!("only V0_0_3 is supported")
            }
            ReplicaTransactionInfoVersions::V0_0_3(t) => {
                convert_to::create_transaction_meta(t.transaction_status_meta).encode_to_vec()
            }
        }
    }

    fn encode_rewards(&self, blockinfo: &ReplicaBlockInfoVersions) -> Vec<u8> {
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(_) => vec![],
            ReplicaBlockInfoVersions::V0_0_2(b) => {
                convert_to::create_rewards_obj(b.rewards, None).encode_to_vec()
            }
            ReplicaBlockInfoVersions::V0_0_3(b) => {
                convert_to::create_rewards_obj(b.rewards, None).encode_to_vec()
            }
            ReplicaBlockInfoVersions::V0_0_4(b) => convert_to::create_rewards_obj(
                &b.rewards.rewards,
                b.rewards.num_partitions,
            )
            .encode_to_vec(),
        }
    }

    fn encoded_size(&self, message: &GeyserMessage) -> usize {
        match message {
            GeyserMessage::Account(a) => DirectCopyCodec::account_encoded_size(a),
            _ => self.encode_message_inner(message).len(),
        }
    }

    fn encode_into(&self, message: &GeyserMessage, buf: &mut [u8]) -> usize {
        match message {
            GeyserMessage::Account(a) => {
                unsafe { DirectCopyCodec::encode_account_into(a, buf.as_mut_ptr()) };
                DirectCopyCodec::account_encoded_size(a)
            }
            _ => {
                let bytes = self.encode_message_inner(message);
                buf[..bytes.len()].copy_from_slice(&bytes);
                bytes.len()
            }
        }
    }
}

impl ProstGeyserCodec {
    fn encode_message_inner(&self, message: &GeyserMessage) -> Vec<u8> {
        use yellowstone_shmem_common::SlotStatus;
        use yellowstone_grpc_proto::prelude as proto;
        
        match message {
            GeyserMessage::Slot(s) => {
                let status = match s.status {
                    SlotStatus::Processed          => ProtoSlotStatus::SlotProcessed,
                    SlotStatus::Confirmed          => ProtoSlotStatus::SlotConfirmed,
                    SlotStatus::Rooted             => ProtoSlotStatus::SlotFinalized,
                    SlotStatus::FirstShredReceived => ProtoSlotStatus::SlotFirstShredReceived,
                    SlotStatus::Completed          => ProtoSlotStatus::SlotCompleted,
                    SlotStatus::CreatedBank        => ProtoSlotStatus::SlotCreatedBank,
                    SlotStatus::Dead(_)            => ProtoSlotStatus::SlotDead,
                };
                proto::SubscribeUpdateSlot {
                    slot:   s.slot,
                    parent: s.parent,
                    status: status as i32,
                    dead_error: match &s.status {
                        SlotStatus::Dead(e) => Some(e.clone()),
                        _ => None,
                    },
                }.encode_to_vec()
            }

            GeyserMessage::Account(a) => proto::SubscribeUpdateAccount {
                account: Some(proto::SubscribeUpdateAccountInfo {
                    pubkey:        a.account.pubkey.clone(),
                    lamports:      a.account.lamports,
                    owner:         a.account.owner.clone(),
                    executable:    a.account.executable,
                    rent_epoch:    a.account.rent_epoch,
                    data:          a.account.data.clone().into(),
                    write_version: a.account.write_version,
                    txn_signature: a.account.txn_signature.clone(),
                }),
                slot:       a.slot,
                is_startup: a.is_startup,
            }.encode_to_vec(),

            GeyserMessage::Transaction(t) => proto::SubscribeUpdateTransactionInfo {
                signature:   t.transaction.signature.clone(),
                is_vote:     t.transaction.is_vote,
                transaction: Some(proto::Transaction::decode(
                    t.transaction.transaction.as_slice()
                ).unwrap_or_default()),
                meta: Some(proto::TransactionStatusMeta::decode(
                    t.transaction.meta.as_slice()
                ).unwrap_or_default()),
                index: t.transaction.index as u64,
            }.encode_to_vec(),

            GeyserMessage::Entry(e) => proto::SubscribeUpdateEntry {
                slot:                       e.slot,
                index:                      e.index as u64,
                num_hashes:                 e.num_hashes,
                hash:                       e.hash.clone(),
                executed_transaction_count: e.executed_transaction_count,
                starting_transaction_index: e.starting_transaction_index,
            }.encode_to_vec(),

            GeyserMessage::BlockMeta(b) => proto::SubscribeUpdateBlockMeta {
                parent_slot:                b.parent_slot,
                parent_blockhash:           b.parent_blockhash.clone(),
                slot:                       b.slot,
                blockhash:                  b.blockhash.clone(),
                rewards:                    Some(proto::Rewards::decode(
                    b.rewards.as_slice()
                ).unwrap_or_default()),
                block_time:                 b.block_time.map(|t| proto::UnixTimestamp { timestamp: t }),
                block_height:               b.block_height.map(|h| proto::BlockHeight { block_height: h }),
                executed_transaction_count: b.executed_transaction_count,
                entries_count:              b.entry_count,
            }.encode_to_vec(),
        }
    }
}

use yellowstone_shmem_common::MessageAccount;

pub struct DirectCopyCodec;

impl DirectCopyCodec {
    /// Byte layout (decoder must mirror this exactly):
    ///   pubkey        [32]
    ///   lamports      [8]   LE u64
    ///   owner         [32]
    ///   executable    [1]
    ///   rent_epoch    [8]   LE u64
    ///   write_version [8]   LE u64
    ///   sig_present   [1]
    ///   txn_signature [64]  (zeroed if None, always written)
    ///   data_len      [8]   LE u64
    ///   data          [data_len]
    ///   slot          [8]   LE u64
    ///   is_startup    [1]
    pub fn account_encoded_size(a: &MessageAccount) -> usize {
        32 + 8 + 32 + 1 + 8 + 8 + 1 + 64 + 8 + a.account.data.len() + 8 + 1
    }

    /// # Safety
    /// `dst` must point to at least `account_encoded_size(a)` writable bytes.
    pub unsafe fn encode_account_into(a: &MessageAccount, dst: *mut u8) {
        let mut o = 0usize;

        unsafe fn write_u8(dst: *mut u8, o: &mut usize, v: u8) {
            *dst.add(*o) = v;
            *o += 1;
        }

        unsafe fn write_u64(dst: *mut u8, o: &mut usize, v: u64) {
            std::ptr::copy_nonoverlapping(v.to_le_bytes().as_ptr(), dst.add(*o), 8);
            *o += 8;
        }

        unsafe fn write_bytes(dst: *mut u8, o: &mut usize, src: &[u8]) {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst.add(*o), src.len());
            *o += src.len();
        }

        write_bytes(dst, &mut o, &a.account.pubkey);
        write_u64(dst, &mut o, a.account.lamports);
        write_bytes(dst, &mut o, &a.account.owner);
        write_u8(dst, &mut o, a.account.executable as u8);
        write_u64(dst, &mut o, a.account.rent_epoch);
        write_u64(dst, &mut o, a.account.write_version);

        match &a.account.txn_signature {
            Some(sig) => {
                write_u8(dst, &mut o, 1u8);
                write_bytes(dst, &mut o, sig);
            }
            None => {
                write_u8(dst, &mut o, 0u8);
                std::ptr::write_bytes(dst.add(o), 0, 64);
                o += 64;
            }
        }

        write_u64(dst, &mut o, a.account.data.len() as u64);
        write_bytes(dst, &mut o, &a.account.data);
        write_u64(dst, &mut o, a.slot);
        write_u8(dst, &mut o, a.is_startup as u8);
    }
}