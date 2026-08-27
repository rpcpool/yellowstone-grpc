use {
    crate::plugin::{
        convert_to,
        filter::name::FilterNames,
        message::{
            MessageAccount, MessageAccountInfo, MessageBlock, MessageBlockMeta, MessageEntry,
            MessageSlot, MessageTransaction, MessageTransactionInfo, SlotStatus,
        },
    },
    bytes::Bytes,
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
    yellowstone_grpc_proto::{geyser::SubscribeUpdateBlockMeta, solana::storage::confirmed_block},
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

pub fn token_account_data(needle: Option<(usize, &[u8])>) -> Vec<u8> {
    let mut data = vec![0; TOKEN_ACCOUNT_LEN];
    data[TOKEN_ACCOUNT_STATE_OFFSET] = 1;
    if let Some((offset, bytes)) = needle {
        data[offset..offset + bytes.len()].copy_from_slice(bytes);
    }
    data
}

pub fn account_info(
    pubkey: Pubkey,
    owner: Pubkey,
    data: Vec<u8>,
    lamports: u64,
    txn_signature: Option<Signature>,
) -> MessageAccountInfo {
    MessageAccountInfo {
        pubkey,
        lamports,
        owner,
        executable: false,
        rent_epoch: 0,
        data: Bytes::from(data),
        write_version: 7,
        txn_signature,
        pre_encoded: OnceLock::new(),
    }
}

pub fn message_account(account: MessageAccountInfo) -> Arc<MessageAccount> {
    Arc::new(MessageAccount {
        account,
        slot: 100,
        is_startup: false,
        created_at: Timestamp::default(),
        bank_id: Some(100),
    })
}

pub fn simple_message_account(pubkey: Pubkey, owner: Pubkey) -> Arc<MessageAccount> {
    message_account(account_info(pubkey, owner, Vec::new(), 1000, None))
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
    MessageSlot {
        slot,
        parent: Some(slot.saturating_sub(1)),
        status,
        dead_error: None,
        created_at: Timestamp::default(),
        bank_id,
    }
}

pub fn message_entry(slot: u64, index: usize) -> Arc<MessageEntry> {
    Arc::new(MessageEntry {
        slot,
        index,
        num_hashes: 128,
        hash: Hash::default(),
        executed_transaction_count: 4,
        starting_transaction_index: 0,
        created_at: Timestamp::default(),
        bank_id: slot,
    })
}

pub fn message_block_meta(slot: u64) -> Arc<MessageBlockMeta> {
    Arc::new(MessageBlockMeta {
        block_meta: SubscribeUpdateBlockMeta {
            slot,
            blockhash: Hash::default().to_string(),
            parent_slot: slot.saturating_sub(1),
            parent_blockhash: Hash::default().to_string(),
            executed_transaction_count: 0,
            entries_count: 0,
            ..Default::default()
        },
        created_at: Timestamp::default(),
    })
}

pub fn message_block(
    slot: u64,
    transactions: Vec<Arc<MessageTransaction>>,
    accounts: Vec<Arc<MessageAccount>>,
    entries: Vec<Arc<MessageEntry>>,
) -> Arc<MessageBlock> {
    Arc::new(MessageBlock {
        meta: message_block_meta(slot),
        updated_account_count: accounts.len() as u64,
        transactions,
        accounts,
        entries,
        created_at: Timestamp::default(),
    })
}

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

    Arc::new(MessageTransaction {
        transaction: MessageTransactionInfo {
            signature,
            is_vote,
            transaction: convert_to::create_transaction(&versioned),
            meta,
            index: 1,
            account_keys: account_keys.into_iter().collect(),
            pre_encoded: OnceLock::new(),
            token_owners_all: OnceLock::new(),
            token_owners_changed: OnceLock::new(),
        },
        slot: 100,
        created_at: Timestamp::default(),
        bank_id: 100,
    })
}
