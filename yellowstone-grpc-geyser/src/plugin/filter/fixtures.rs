#[allow(clippy::disallowed_types, reason = "test messages' OnceLock caches")]
use {
    crate::plugin::{
        convert_to,
        filter::name::FilterNames,
        message::{
            MessageAccount, MessageAccountInfo, MessageBlock, MessageBlockFooter, MessageBlockMeta,
            MessageEntry, MessageSlot, MessageTransaction, MessageTransactionInfo, SlotStatus,
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
    prost_types::Timestamp,
    solana_hash::Hash,
    solana_message::{legacy::Message as LegacyMessage, MessageHeader, VersionedMessage},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        sync::{Arc, OnceLock},
        time::Duration,
    },
    yellowstone_grpc_proto::{
        geyser::{
            BlockFooterFinalCert, BlockFooterNotarRewardCert, BlockFooterSignatureKind,
            BlockFooterSkipRewardCert, BlockFooterVotesAggregate, SubscribeUpdateBlockFooter,
            SubscribeUpdateBlockMeta,
        },
        solana::storage::confirmed_block,
    },
};

pub const TOKEN_ACCOUNT_LEN: usize = 165;
pub const TOKEN_ACCOUNT_STATE_OFFSET: usize = 108;

pub fn filter_names() -> FilterNames {
    FilterNames::new(64, 1024, Duration::from_secs(1))
}

pub fn deterministic_pubkeys(tag: u8, n: usize) -> Vec<Pubkey> {
    (0..n)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = tag;
            bytes[1..9].copy_from_slice(&(i as u64).to_le_bytes());
            for (slot, byte) in bytes.iter_mut().enumerate().skip(9) {
                *byte = (slot as u8).wrapping_mul(31).wrapping_add(i as u8);
            }
            Pubkey::new_from_array(bytes)
        })
        .collect()
}

/// An account write at slot 100 in bank 100, with write version 7.
pub fn account_write(
    pubkey: Pubkey,
    owner: Pubkey,
    data: Vec<u8>,
    lamports: u64,
    txn_signature: Option<Signature>,
) -> MessageAccount {
    let account = MessageAccountInfo {
        pubkey,
        lamports,
        owner,
        executable: false,
        rent_epoch: 0,
        write_version: 7,
        txn_signature,
    };
    MessageAccount::new(
        account,
        data.as_slice(),
        100,
        false,
        Some(100),
        Timestamp::default(),
    )
}

pub fn message_account(account: MessageAccount) -> Arc<MessageAccount> {
    Arc::new(account)
}

pub fn simple_message_account(pubkey: Pubkey, owner: Pubkey) -> Arc<MessageAccount> {
    message_account(account_write(pubkey, owner, Vec::new(), 1000, None))
}

/// An empty account write of `owner` at `slot` in bank `bank_id`.
pub fn bank_account(pubkey: Pubkey, owner: Pubkey, slot: u64, bank_id: u64) -> Arc<MessageAccount> {
    let account = account_write(pubkey, owner, Vec::new(), 1000, None).account;
    Arc::new(MessageAccount::new(
        account,
        b"".as_slice(),
        slot,
        false,
        Some(bank_id),
        Timestamp::default(),
    ))
}

pub fn message_slot(slot: u64, status: SlotStatus) -> MessageSlot {
    let bank_id = if [
        SlotStatus::Completed,
        SlotStatus::Dead,
        SlotStatus::FirstShredReceived,
    ]
    .contains(&status)
    {
        None
    } else {
        Some(slot)
    };
    MessageSlot::new(
        slot,
        Some(slot.saturating_sub(1)),
        status,
        None,
        bank_id,
        Timestamp::default(),
    )
}

pub fn message_entry(slot: u64, index: usize) -> Arc<MessageEntry> {
    bank_entry(slot, index, slot)
}

pub fn bank_entry(slot: u64, index: usize, bank_id: u64) -> Arc<MessageEntry> {
    Arc::new(MessageEntry::from_entry_info(
        &ReplicaEntryInfoV2 {
            slot,
            index,
            num_hashes: 128,
            hash: Hash::default().as_ref(),
            executed_transaction_count: 4,
            starting_transaction_index: 0,
        },
        bank_id,
        Timestamp::default(),
    ))
}

pub fn message_block_footer(slot: u64, bank_id: u64) -> Arc<MessageBlockFooter> {
    Arc::new(MessageBlockFooter::new(
        SubscribeUpdateBlockFooter {
            slot,
            bank_id,
            bank_hash: vec![1; 32],
            block_producer_time_nanos: 1_000,
            block_user_agent: b"agave".to_vec(),
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        },
        Timestamp::default(),
    ))
}

pub fn block_footer_certificates() -> (
    BlockFooterFinalCert,
    BlockFooterSkipRewardCert,
    BlockFooterNotarRewardCert,
) {
    let aggregate = |byte, block_id| BlockFooterVotesAggregate {
        signature_kind: BlockFooterSignatureKind::CompressedBls12381G2.into(),
        signature: vec![byte; 96],
        block_id,
        signer_bitmap: vec![byte; 3],
    };
    (
        BlockFooterFinalCert {
            slot: 42,
            final_aggregate: Some(aggregate(1, Vec::new())),
            notar_aggregate: Some(aggregate(2, vec![4; 32])),
        },
        BlockFooterSkipRewardCert {
            slot: 42,
            aggregate: Some(BlockFooterVotesAggregate {
                signer_bitmap: Vec::new(),
                ..aggregate(3, Vec::new())
            }),
        },
        BlockFooterNotarRewardCert {
            slot: 42,
            aggregate: Some(aggregate(4, vec![5; 32])),
        },
    )
}

pub fn message_block_meta(slot: u64) -> Arc<MessageBlockMeta> {
    bank_block_meta(slot, 0)
}

pub fn bank_block_meta(slot: u64, bank_id: u64) -> Arc<MessageBlockMeta> {
    Arc::new(MessageBlockMeta::from_update_oneof(
        SubscribeUpdateBlockMeta {
            slot,
            blockhash: Hash::default().to_string(),
            parent_slot: slot.saturating_sub(1),
            parent_blockhash: Hash::default().to_string(),
            executed_transaction_count: 0,
            entries_count: 0,
            bank_id,
            ..Default::default()
        },
        Timestamp::default(),
    ))
}

pub fn message_block(
    slot: u64,
    transactions: Vec<Arc<MessageTransaction>>,
    accounts: Vec<Arc<MessageAccount>>,
    entries: Vec<Arc<MessageEntry>>,
) -> Arc<MessageBlock> {
    Arc::new(MessageBlock::new_created_at(
        message_block_meta(slot),
        transactions,
        accounts,
        entries,
        Timestamp::default(),
    ))
}

#[allow(clippy::disallowed_types, reason = "test messages' OnceLock caches")]
pub fn message_transaction(
    signature: Signature,
    account_keys: Vec<Pubkey>,
    is_vote: bool,
    meta: confirmed_block::TransactionStatusMeta,
) -> Arc<MessageTransaction> {
    let message = VersionedMessage::Legacy(LegacyMessage {
        header: MessageHeader {
            num_required_signatures: 1,
            ..MessageHeader::default()
        },
        account_keys: account_keys.clone(),
        recent_blockhash: Hash::default(),
        instructions: Vec::new(),
    });
    let versioned = VersionedTransaction {
        signatures: vec![signature],
        message,
    };

    Arc::new(MessageTransaction::new(
        MessageTransactionInfo {
            signature,
            is_vote,
            transaction: convert_to::create_transaction(&versioned),
            meta,
            index: 1,
            account_keys: account_keys.into_iter().collect(),
            token_owners_all: OnceLock::new(),
            token_owners_changed: OnceLock::new(),
        },
        100,
        100,
        Timestamp::default(),
    ))
}
