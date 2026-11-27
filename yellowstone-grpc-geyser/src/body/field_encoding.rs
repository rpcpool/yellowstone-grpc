//! Lengths and writers of single proto3 fields, byte for byte as prost writes them.

use {
    bytes::BufMut,
    prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, message, WireType},
    prost_types::Timestamp,
};

/// `SubscribeUpdate` field tags: the oneof kinds, then `created_at`.
pub const ACCOUNT_UPDATE_TAG: u32 = 2;
pub const SLOT_UPDATE_TAG: u32 = 3;
pub const TRANSACTION_UPDATE_TAG: u32 = 4;
pub const BLOCK_UPDATE_TAG: u32 = 5;
pub const BLOCK_META_UPDATE_TAG: u32 = 7;
pub const ENTRY_UPDATE_TAG: u32 = 8;
pub const TRANSACTION_STATUS_UPDATE_TAG: u32 = 10;
pub const CREATED_AT_TAG: u32 = 11;
pub const BLOCK_FOOTER_UPDATE_TAG: u32 = 12;
pub const ENTRY_UPDATE_PARENT_UPDATE_TAG: u32 = 13;

/// A `uint64` field: proto3 writes it only when it is not 0.
pub const fn uint64_field_len(field_tag: u32, field_value: u64) -> usize {
    if field_value == 0 {
        0
    } else {
        key_len(field_tag) + encoded_len_varint(field_value)
    }
}

/// `uint64` fields given as (tag, value) pairs in tag order, so one list sizes and writes them.
pub fn uint64_fields_len(uint64_fields: &[(u32, u64)]) -> usize {
    let field_len =
        |&(field_tag, field_value): &(u32, u64)| uint64_field_len(field_tag, field_value);
    uint64_fields.iter().map(field_len).sum()
}

pub fn put_uint64_fields(uint64_fields: &[(u32, u64)], output_buffer: &mut impl BufMut) {
    for &(field_tag, field_value) in uint64_fields {
        put_uint64_field(field_tag, field_value, output_buffer);
    }
}

/// An `optional uint64` field: written whenever it is set, even to 0.
pub const fn optional_uint64_field_len(field_tag: u32, field_value: Option<u64>) -> usize {
    match field_value {
        Some(set_value) => key_len(field_tag) + encoded_len_varint(set_value),
        None => 0,
    }
}

pub const fn bool_field_len(field_tag: u32, field_value: bool) -> usize {
    if field_value {
        key_len(field_tag) + 1
    } else {
        0
    }
}

/// The key and length in front of `content_len` bytes of a length-delimited field.
pub const fn delimited_header_len(field_tag: u32, content_len: usize) -> usize {
    key_len(field_tag) + encoded_len_varint(content_len as u64)
}

/// A length-delimited field that is always written: a set message or set optional bytes.
pub const fn delimited_field_len(field_tag: u32, content_len: usize) -> usize {
    delimited_header_len(field_tag, content_len) + content_len
}

/// A proto3 `bytes` or `string` field: absent when empty.
pub const fn bytes_field_len(field_tag: u32, content_len: usize) -> usize {
    if content_len == 0 {
        0
    } else {
        delimited_field_len(field_tag, content_len)
    }
}

pub fn created_at_field_len(created_at: &Timestamp) -> usize {
    message::encoded_len(CREATED_AT_TAG, created_at)
}

pub fn put_uint64_field(field_tag: u32, field_value: u64, output_buffer: &mut impl BufMut) {
    if field_value != 0 {
        encode_key(field_tag, WireType::Varint, output_buffer);
        encode_varint(field_value, output_buffer);
    }
}

pub fn put_optional_uint64_field(
    field_tag: u32,
    field_value: Option<u64>,
    output_buffer: &mut impl BufMut,
) {
    if let Some(set_value) = field_value {
        encode_key(field_tag, WireType::Varint, output_buffer);
        encode_varint(set_value, output_buffer);
    }
}

pub fn put_bool_field(field_tag: u32, field_value: bool, output_buffer: &mut impl BufMut) {
    if field_value {
        encode_key(field_tag, WireType::Varint, output_buffer);
        output_buffer.put_u8(1);
    }
}

pub fn put_delimited_header(field_tag: u32, content_len: usize, output_buffer: &mut impl BufMut) {
    encode_key(field_tag, WireType::LengthDelimited, output_buffer);
    encode_varint(content_len as u64, output_buffer);
}

pub fn put_delimited_field(field_tag: u32, field_content: &[u8], output_buffer: &mut impl BufMut) {
    put_delimited_header(field_tag, field_content.len(), output_buffer);
    output_buffer.put_slice(field_content);
}

pub fn put_bytes_field(field_tag: u32, field_content: &[u8], output_buffer: &mut impl BufMut) {
    if !field_content.is_empty() {
        put_delimited_field(field_tag, field_content, output_buffer);
    }
}

pub fn put_created_at_field(created_at: &Timestamp, output_buffer: &mut impl BufMut) {
    message::encode(CREATED_AT_TAG, created_at, output_buffer);
}
