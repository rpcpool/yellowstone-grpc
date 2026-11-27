//! Block shape bodies: one Block update, assembled from the block's fields and item bodies.

use {
    super::{
        account::{account_info_item_len, put_account_info_item},
        field_encoding::{
            bytes_field_len, created_at_field_len, delimited_field_len, put_bytes_field,
            put_created_at_field, put_delimited_header, put_uint64_field, uint64_field_len,
            BLOCK_UPDATE_TAG,
        },
        SharedUpdateBody,
    },
    bytes::BufMut,
    prost::encoding::message,
    prost_types::Timestamp,
    std::ops::Range,
    yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta,
};

/// `SubscribeUpdateBlock` field tags.
const BLOCK_SLOT_TAG: u32 = 1;
const BLOCKHASH_TAG: u32 = 2;
const REWARDS_TAG: u32 = 3;
const BLOCK_TIME_TAG: u32 = 4;
const BLOCK_HEIGHT_TAG: u32 = 5;
const TRANSACTIONS_TAG: u32 = 6;
const PARENT_SLOT_TAG: u32 = 7;
const PARENT_BLOCKHASH_TAG: u32 = 8;
const EXECUTED_TRANSACTION_COUNT_TAG: u32 = 9;
const UPDATED_ACCOUNT_COUNT_TAG: u32 = 10;
const ACCOUNTS_TAG: u32 = 11;
const ENTRIES_COUNT_TAG: u32 = 12;
const ENTRIES_TAG: u32 = 13;
const BLOCK_BANK_ID_TAG: u32 = 14;

/// One Block shape: the block's fields and the items its blocks filter selects, in block
/// order. The encoder visits each kind twice, so every visit must yield the same items.
pub trait BlockShapeSource {
    fn block_meta(&self) -> &SubscribeUpdateBlockMeta;

    fn updated_account_count(&self) -> u64;

    /// The data slice that cuts every account item; no ranges keep all the data.
    fn accounts_data_slice(&self) -> &[Range<usize>];

    /// The Block's `created_at`, which is when the bank sealed.
    fn block_created_at(&self) -> Timestamp;

    /// Visits each selected transaction's `TransactionInfo` length.
    fn for_each_transaction_info_len(&self, visit_info_len: impl FnMut(usize));

    /// Writes each selected transaction as field `field_tag`: key, length, then `TransactionInfo`.
    fn put_transaction_items(&self, field_tag: u32, output_buffer: &mut impl BufMut);

    /// Visits each selected account's unsliced body.
    fn for_each_account_body(&self, visit_account_body: impl FnMut(&SharedUpdateBody));

    fn for_each_entry_body(&self, visit_entry_body: impl FnMut(&SharedUpdateBody));
}

fn entry_item_range(entry_body: &SharedUpdateBody) -> Range<usize> {
    entry_body
        .inner_item_range()
        .expect("an entry body has its Entry range")
}

fn block_content_len(block_shape_source: &impl BlockShapeSource) -> usize {
    let block_meta = block_shape_source.block_meta();
    let accounts_data_slice = block_shape_source.accounts_data_slice();
    let rewards_len = (block_meta.rewards.as_ref()).map_or(0, |block_rewards| {
        message::encoded_len(REWARDS_TAG, block_rewards)
    });
    let block_time_len = (block_meta.block_time.as_ref()).map_or(0, |block_time| {
        message::encoded_len(BLOCK_TIME_TAG, block_time)
    });
    let block_height_len = (block_meta.block_height.as_ref()).map_or(0, |block_height| {
        message::encoded_len(BLOCK_HEIGHT_TAG, block_height)
    });
    let executed_transaction_count = block_meta.executed_transaction_count;
    let updated_account_count = block_shape_source.updated_account_count();
    let mut content_len = uint64_field_len(BLOCK_SLOT_TAG, block_meta.slot)
        + bytes_field_len(BLOCKHASH_TAG, block_meta.blockhash.len())
        + rewards_len
        + block_time_len
        + block_height_len
        + uint64_field_len(PARENT_SLOT_TAG, block_meta.parent_slot)
        + bytes_field_len(PARENT_BLOCKHASH_TAG, block_meta.parent_blockhash.len())
        + uint64_field_len(EXECUTED_TRANSACTION_COUNT_TAG, executed_transaction_count)
        + uint64_field_len(UPDATED_ACCOUNT_COUNT_TAG, updated_account_count)
        + uint64_field_len(ENTRIES_COUNT_TAG, block_meta.entries_count)
        + uint64_field_len(BLOCK_BANK_ID_TAG, block_meta.bank_id);
    block_shape_source.for_each_transaction_info_len(|transaction_info_len| {
        content_len += delimited_field_len(TRANSACTIONS_TAG, transaction_info_len);
    });
    block_shape_source.for_each_account_body(|account_body| {
        let account_item_len = account_info_item_len(account_body, accounts_data_slice);
        content_len += delimited_field_len(ACCOUNTS_TAG, account_item_len);
    });
    block_shape_source.for_each_entry_body(|entry_body| {
        content_len += delimited_field_len(ENTRIES_TAG, entry_item_range(entry_body).len());
    });
    content_len
}

/// Writes the Block's fields in tag order, as prost writes `SubscribeUpdateBlock`.
fn put_block_content(block_shape_source: &impl BlockShapeSource, output_buffer: &mut impl BufMut) {
    let block_meta = block_shape_source.block_meta();
    let accounts_data_slice = block_shape_source.accounts_data_slice();
    put_uint64_field(BLOCK_SLOT_TAG, block_meta.slot, output_buffer);
    put_bytes_field(
        BLOCKHASH_TAG,
        block_meta.blockhash.as_bytes(),
        output_buffer,
    );
    if let Some(block_rewards) = &block_meta.rewards {
        message::encode(REWARDS_TAG, block_rewards, output_buffer);
    }
    if let Some(block_time) = &block_meta.block_time {
        message::encode(BLOCK_TIME_TAG, block_time, output_buffer);
    }
    if let Some(block_height) = &block_meta.block_height {
        message::encode(BLOCK_HEIGHT_TAG, block_height, output_buffer);
    }
    block_shape_source.put_transaction_items(TRANSACTIONS_TAG, output_buffer);
    put_uint64_field(PARENT_SLOT_TAG, block_meta.parent_slot, output_buffer);
    let parent_blockhash_bytes = block_meta.parent_blockhash.as_bytes();
    put_bytes_field(PARENT_BLOCKHASH_TAG, parent_blockhash_bytes, output_buffer);
    let executed_transaction_count = block_meta.executed_transaction_count;
    put_uint64_field(
        EXECUTED_TRANSACTION_COUNT_TAG,
        executed_transaction_count,
        output_buffer,
    );
    let updated_account_count = block_shape_source.updated_account_count();
    put_uint64_field(
        UPDATED_ACCOUNT_COUNT_TAG,
        updated_account_count,
        output_buffer,
    );
    block_shape_source.for_each_account_body(|account_body| {
        put_account_info_item(
            ACCOUNTS_TAG,
            account_body,
            accounts_data_slice,
            output_buffer,
        )
    });
    put_uint64_field(ENTRIES_COUNT_TAG, block_meta.entries_count, output_buffer);
    block_shape_source.for_each_entry_body(|entry_body| {
        let inner_item_range = entry_item_range(entry_body);
        put_delimited_header(ENTRIES_TAG, inner_item_range.len(), output_buffer);
        entry_body.for_each_slice_in_range(inner_item_range, |item_bytes| {
            output_buffer.put_slice(item_bytes)
        });
    });
    put_uint64_field(BLOCK_BANK_ID_TAG, block_meta.bank_id, output_buffer);
}

pub fn block_shape_body_len(block_shape_source: &impl BlockShapeSource) -> usize {
    delimited_field_len(BLOCK_UPDATE_TAG, block_content_len(block_shape_source))
        + created_at_field_len(&block_shape_source.block_created_at())
}

/// Appends the Block shape's body to `output_buffer`, without allocating.
pub fn write_block_shape_body(
    block_shape_source: &impl BlockShapeSource,
    output_buffer: &mut impl BufMut,
) {
    let content_len = block_content_len(block_shape_source);
    put_delimited_header(BLOCK_UPDATE_TAG, content_len, output_buffer);
    put_block_content(block_shape_source, output_buffer);
    put_created_at_field(&block_shape_source.block_created_at(), output_buffer);
}

/// Builds the Block shape's body in one buffer of its own.
pub fn encode_block_shape_body(block_shape_source: &impl BlockShapeSource) -> SharedUpdateBody {
    let mut body_buffer = Vec::with_capacity(block_shape_body_len(block_shape_source));
    write_block_shape_body(block_shape_source, &mut body_buffer);
    SharedUpdateBody::from_filled_buffer(body_buffer, None, None)
}
