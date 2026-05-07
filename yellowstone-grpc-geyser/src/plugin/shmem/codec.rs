use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
};
use prost::Message as ProstMessage;
use yellowstone_shmem_common::GeyserMessage;
use yellowstone_shmem_plugin::codec::GeyserCodec;
use yellowstone_grpc_proto::geyser::SlotStatus as ProtoSlotStatus;

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

    /// Encodes the full [`GeyserMessage`] envelope using bincode.
    ///
    /// Sub-fields (`transaction`, `meta`, `rewards`) are already
    /// prost-encoded bytes at this point — they are carried as opaque
    /// `Vec<u8>` through the envelope without re-encoding.
    fn encode_message(&self, message: &GeyserMessage) -> Vec<u8> {
    use prost::Message as ProstMessage;
    use yellowstone_grpc_proto::prelude as proto;
    use yellowstone_shmem_common::SlotStatus;

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

        GeyserMessage::Account(a) => {
            proto::SubscribeUpdateAccount {
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
            }.encode_to_vec()
        }

        GeyserMessage::Transaction(t) => {
            proto::SubscribeUpdateTransactionInfo {
                signature:   t.transaction.signature.clone(),
                is_vote:     t.transaction.is_vote,
                transaction: Some(proto::Transaction::decode(
                    t.transaction.transaction.as_slice()
                ).unwrap_or_default()),
                meta: Some(proto::TransactionStatusMeta::decode(
                    t.transaction.meta.as_slice()
                ).unwrap_or_default()),
                index: t.transaction.index as u64,
            }.encode_to_vec()
        }

        GeyserMessage::Entry(e) => {
            proto::SubscribeUpdateEntry {
                slot:                       e.slot,
                index:                      e.index as u64,
                num_hashes:                 e.num_hashes,
                hash:                       e.hash.clone(),
                executed_transaction_count: e.executed_transaction_count,
                starting_transaction_index: e.starting_transaction_index,
            }.encode_to_vec()
        }

        GeyserMessage::BlockMeta(b) => {
            proto::SubscribeUpdateBlockMeta {
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
            }.encode_to_vec()
        }
    }
}
}