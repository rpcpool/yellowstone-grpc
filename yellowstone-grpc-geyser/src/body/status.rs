//! One-buffer bodies of slot statuses, entries, block metas, footers, update-parent markers,
//! pings and pongs.

use {
    super::{
        field_encoding::{
            bytes_field_len, created_at_field_len, delimited_field_len, optional_uint64_field_len,
            put_bytes_field, put_created_at_field, put_delimited_field, put_delimited_header,
            put_optional_uint64_field, put_uint64_field, put_uint64_fields, uint64_field_len,
            uint64_fields_len, BLOCK_FOOTER_UPDATE_TAG, BLOCK_META_UPDATE_TAG,
            ENTRY_UPDATE_PARENT_UPDATE_TAG, ENTRY_UPDATE_TAG, SLOT_UPDATE_TAG,
        },
        SharedUpdateBody,
    },
    crate::plugin::message::SlotStatus,
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
    prost::{encoding::int32, Message},
    prost_types::Timestamp,
    solana_clock::{BankId, Slot},
    yellowstone_grpc_proto::geyser::{
        SlotStatus as SlotStatusProto, SubscribeUpdateBlockFooter, SubscribeUpdateBlockMeta,
        SubscribeUpdateEntryUpdateParent,
    },
};

/// Builds a body of `content_len` bytes of oneof content, written by `put_content`, then
/// `created_at`. The inner item, when asked for, is that content.
pub fn encode_one_buffer_body(
    update_field_tag: u32,
    content_len: usize,
    created_at: &Timestamp,
    content_is_inner_item: bool,
    put_content: impl FnOnce(&mut Vec<u8>),
) -> SharedUpdateBody {
    let body_len =
        delimited_field_len(update_field_tag, content_len) + created_at_field_len(created_at);
    let mut body_buffer = Vec::with_capacity(body_len);
    put_delimited_header(update_field_tag, content_len, &mut body_buffer);
    let content_start = body_buffer.len();
    put_content(&mut body_buffer);
    debug_assert_eq!(body_buffer.len() - content_start, content_len);
    put_created_at_field(created_at, &mut body_buffer);
    let content_range = content_start..content_start + content_len;
    SharedUpdateBody::from_filled_buffer(
        body_buffer,
        content_is_inner_item.then_some(content_range),
        None,
    )
}

/// Builds the body of a prost message written whole as the oneof field.
fn encode_message_body(
    update_field_tag: u32,
    update_message: &impl Message,
    created_at: &Timestamp,
) -> SharedUpdateBody {
    let put_message = |body_buffer: &mut Vec<u8>| update_message.encode_raw(body_buffer);
    let content_len = update_message.encoded_len();
    encode_one_buffer_body(
        update_field_tag,
        content_len,
        created_at,
        false,
        put_message,
    )
}

/// `SubscribeUpdateSlot` field tags.
const STATUS_SLOT_TAG: u32 = 1;
const STATUS_PARENT_TAG: u32 = 2;
const STATUS_TAG: u32 = 3;
const DEAD_ERROR_TAG: u32 = 4;
const STATUS_BANK_ID_TAG: u32 = 5;

/// Builds a slot status body. `dead_error` and `bank_id` are written whenever they are set,
/// even empty or 0, as `optional` fields are.
pub fn encode_slot_body(
    status_slot: Slot,
    parent_slot: Option<Slot>,
    slot_status: SlotStatus,
    dead_error: Option<&str>,
    bank_id: Option<BankId>,
    created_at: Timestamp,
) -> SharedUpdateBody {
    let status_value = SlotStatusProto::from(slot_status) as i32;
    let status_field_len = if status_value == 0 {
        0
    } else {
        int32::encoded_len(STATUS_TAG, &status_value)
    };
    let dead_error_field_len = dead_error.map_or(0, |error_text| {
        delimited_field_len(DEAD_ERROR_TAG, error_text.len())
    });
    let content_len = uint64_field_len(STATUS_SLOT_TAG, status_slot)
        + optional_uint64_field_len(STATUS_PARENT_TAG, parent_slot)
        + status_field_len
        + dead_error_field_len
        + optional_uint64_field_len(STATUS_BANK_ID_TAG, bank_id);
    let put_status_fields = |body_buffer: &mut Vec<u8>| {
        put_uint64_field(STATUS_SLOT_TAG, status_slot, body_buffer);
        put_optional_uint64_field(STATUS_PARENT_TAG, parent_slot, body_buffer);
        if status_value != 0 {
            int32::encode(STATUS_TAG, &status_value, body_buffer);
        }
        if let Some(error_text) = dead_error {
            put_delimited_field(DEAD_ERROR_TAG, error_text.as_bytes(), body_buffer);
        }
        put_optional_uint64_field(STATUS_BANK_ID_TAG, bank_id, body_buffer);
    };
    encode_one_buffer_body(
        SLOT_UPDATE_TAG,
        content_len,
        &created_at,
        false,
        put_status_fields,
    )
}

/// `SubscribeUpdateEntry` field tags.
const ENTRY_SLOT_TAG: u32 = 1;
const ENTRY_INDEX_TAG: u32 = 2;
const NUM_HASHES_TAG: u32 = 3;
const ENTRY_HASH_TAG: u32 = 4;
const EXECUTED_TRANSACTION_COUNT_TAG: u32 = 5;
const STARTING_TRANSACTION_INDEX_TAG: u32 = 6;
const ENTRY_BANK_ID_TAG: u32 = 7;

/// Builds an entry body; its inner item is the `Entry` message. The hash is always written.
pub fn encode_entry_body(
    entry_info: &ReplicaEntryInfoV2<'_>,
    bank_id: BankId,
    created_at: Timestamp,
) -> SharedUpdateBody {
    let fields_before_hash = [
        (ENTRY_SLOT_TAG, entry_info.slot),
        (ENTRY_INDEX_TAG, entry_info.index as u64),
        (NUM_HASHES_TAG, entry_info.num_hashes),
    ];
    let fields_after_hash = [
        (
            EXECUTED_TRANSACTION_COUNT_TAG,
            entry_info.executed_transaction_count,
        ),
        (
            STARTING_TRANSACTION_INDEX_TAG,
            entry_info.starting_transaction_index as u64,
        ),
        (ENTRY_BANK_ID_TAG, bank_id),
    ];
    let content_len = uint64_fields_len(&fields_before_hash)
        + delimited_field_len(ENTRY_HASH_TAG, entry_info.hash.len())
        + uint64_fields_len(&fields_after_hash);
    let put_entry_fields = |body_buffer: &mut Vec<u8>| {
        put_uint64_fields(&fields_before_hash, body_buffer);
        put_delimited_field(ENTRY_HASH_TAG, entry_info.hash, body_buffer);
        put_uint64_fields(&fields_after_hash, body_buffer);
    };
    encode_one_buffer_body(
        ENTRY_UPDATE_TAG,
        content_len,
        &created_at,
        true,
        put_entry_fields,
    )
}

pub fn encode_block_meta_body(
    block_meta: &SubscribeUpdateBlockMeta,
    created_at: Timestamp,
) -> SharedUpdateBody {
    encode_message_body(BLOCK_META_UPDATE_TAG, block_meta, &created_at)
}

pub fn encode_entry_update_parent_body(
    update_parent: &SubscribeUpdateEntryUpdateParent,
    created_at: Timestamp,
) -> SharedUpdateBody {
    encode_message_body(ENTRY_UPDATE_PARENT_UPDATE_TAG, update_parent, &created_at)
}

/// The two bodies of one block footer: filters without `include_certificates` get the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockFooterBodies {
    pub footer_without_certificates: SharedUpdateBody,
    pub footer_with_certificates: SharedUpdateBody,
}

/// `SubscribeUpdateBlockFooter` field tags; the certificates follow these five fields.
const FOOTER_SLOT_TAG: u32 = 1;
const FOOTER_BANK_ID_TAG: u32 = 2;
const BANK_HASH_TAG: u32 = 3;
const PRODUCER_TIME_NANOS_TAG: u32 = 4;
const USER_AGENT_TAG: u32 = 5;

pub fn encode_block_footer_bodies(
    block_footer: &SubscribeUpdateBlockFooter,
    created_at: Timestamp,
) -> BlockFooterBodies {
    let fields_before_hash = [
        (FOOTER_SLOT_TAG, block_footer.slot),
        (FOOTER_BANK_ID_TAG, block_footer.bank_id),
    ];
    let producer_time_nanos = block_footer.block_producer_time_nanos;
    let content_len = uint64_fields_len(&fields_before_hash)
        + bytes_field_len(BANK_HASH_TAG, block_footer.bank_hash.len())
        + uint64_field_len(PRODUCER_TIME_NANOS_TAG, producer_time_nanos)
        + bytes_field_len(USER_AGENT_TAG, block_footer.block_user_agent.len());
    let put_footer_fields = |body_buffer: &mut Vec<u8>| {
        put_uint64_fields(&fields_before_hash, body_buffer);
        put_bytes_field(BANK_HASH_TAG, &block_footer.bank_hash, body_buffer);
        put_uint64_field(PRODUCER_TIME_NANOS_TAG, producer_time_nanos, body_buffer);
        put_bytes_field(USER_AGENT_TAG, &block_footer.block_user_agent, body_buffer);
    };
    BlockFooterBodies {
        footer_without_certificates: encode_one_buffer_body(
            BLOCK_FOOTER_UPDATE_TAG,
            content_len,
            &created_at,
            false,
            put_footer_fields,
        ),
        footer_with_certificates: encode_message_body(
            BLOCK_FOOTER_UPDATE_TAG,
            block_footer,
            &created_at,
        ),
    }
}
