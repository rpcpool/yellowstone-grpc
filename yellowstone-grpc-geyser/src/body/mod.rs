#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
//! Shared update bodies: the bytes after `filters`, encoded once per message.

pub mod account;
pub mod block;
pub mod block_shape;
pub mod field_encoding;
pub mod status;
pub mod transaction;

use {
    crate::compression::slot::CompressedGroupSlots,
    arc_swap::ArcSwapOption,
    bytes::{BufMut, Bytes},
    std::{fmt, ops::Range},
};
pub use {
    account::{
        account_info_item_len, encode_account_body, encode_account_body_with_clock,
        put_account_info_item, sliced_account_body_len, write_sliced_account_body,
    },
    block::{
        block_shape_body_len, encode_block_shape_body, write_block_shape_body, BlockShapeSource,
    },
    status::{
        encode_block_footer_bodies, encode_block_meta_body, encode_entry_body,
        encode_entry_update_parent_body, encode_slot_body, BlockFooterBodies,
    },
    transaction::{
        encode_transaction_status_body, encode_transaction_update_body, put_transaction_info,
        put_transaction_status_body, put_transaction_update_body, transaction_info_len,
        transaction_status_body_len, transaction_update_body_len, PublishedBody,
        StandardTransactionBodyWriter, TransactionBodies,
    },
};

/// Exactly the bytes `FilteredUpdate::encode_raw` writes after `filters` for one update kind
/// and variant: the oneof field, then `created_at`.
pub struct SharedUpdateBody {
    body_segments: BodySegments,
    inner_item_range: Option<Range<u32>>,
    account_data_range: Option<Range<u32>>,
    /// Empty until the first compressed writer attaches the table, which then stays for good.
    compressed_group_slots: ArcSwapOption<CompressedGroupSlots>,
}

/// A clone is a new body for compression: it starts without group slots.
impl Clone for SharedUpdateBody {
    fn clone(&self) -> Self {
        Self {
            body_segments: self.body_segments.clone(),
            inner_item_range: self.inner_item_range.clone(),
            account_data_range: self.account_data_range.clone(),
            compressed_group_slots: ArcSwapOption::empty(),
        }
    }
}

/// Bodies are equal when their bytes and ranges are; the group slots are a cache.
impl PartialEq for SharedUpdateBody {
    fn eq(&self, other_body: &Self) -> bool {
        self.body_segments == other_body.body_segments
            && self.inner_item_range == other_body.inner_item_range
            && self.account_data_range == other_body.account_data_range
    }
}

impl Eq for SharedUpdateBody {}

impl fmt::Debug for SharedUpdateBody {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("SharedUpdateBody")
            .field("body_segments", &self.body_segments)
            .field("inner_item_range", &self.inner_item_range)
            .field("account_data_range", &self.account_data_range)
            .finish_non_exhaustive()
    }
}

/// A body's bytes: one buffer, or a head and a tail around account data held by reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodySegments {
    Contiguous(Bytes),
    SharedAccountData {
        body_head: Bytes,
        account_data: Bytes,
        body_tail: Bytes,
    },
}

fn narrow_body_range(body_range: Range<usize>) -> Range<u32> {
    let narrow_body_offset =
        |body_offset: usize| u32::try_from(body_offset).expect("a body is below 4 GiB");
    narrow_body_offset(body_range.start)..narrow_body_offset(body_range.end)
}

const fn widen_body_range(body_range: &Range<u32>) -> Range<usize> {
    body_range.start as usize..body_range.end as usize
}

impl SharedUpdateBody {
    fn from_body_segments(
        body_segments: BodySegments,
        inner_item_range: Option<Range<usize>>,
        account_data_range: Option<Range<usize>>,
    ) -> Self {
        Self {
            body_segments,
            inner_item_range: inner_item_range.map(narrow_body_range),
            account_data_range: account_data_range.map(narrow_body_range),
            compressed_group_slots: ArcSwapOption::empty(),
        }
    }

    /// Takes a buffer filled to its capacity, so the `Bytes` keeps the buffer's allocation.
    fn from_filled_buffer(
        body_buffer: Vec<u8>,
        inner_item_range: Option<Range<usize>>,
        account_data_range: Option<Range<usize>>,
    ) -> Self {
        debug_assert_eq!(body_buffer.len(), body_buffer.capacity());
        Self::from_body_segments(
            BodySegments::Contiguous(Bytes::from(body_buffer)),
            inner_item_range,
            account_data_range,
        )
    }

    pub const fn body_segments(&self) -> &BodySegments {
        &self.body_segments
    }

    pub const fn body_len(&self) -> usize {
        match &self.body_segments {
            BodySegments::Contiguous(body_bytes) => body_bytes.len(),
            BodySegments::SharedAccountData {
                body_head,
                account_data,
                body_tail,
            } => body_head.len() + account_data.len() + body_tail.len(),
        }
    }

    /// Visits every segment in order; together they are the whole body.
    pub fn for_each_segment(&self, mut visit_segment: impl FnMut(&Bytes)) {
        match &self.body_segments {
            BodySegments::Contiguous(body_bytes) => visit_segment(body_bytes),
            BodySegments::SharedAccountData {
                body_head,
                account_data,
                body_tail,
            } => {
                visit_segment(body_head);
                visit_segment(account_data);
                visit_segment(body_tail);
            }
        }
    }

    /// Visits the bytes of `body_range`, one piece per segment it touches, in order.
    pub fn for_each_slice_in_range(
        &self,
        body_range: Range<usize>,
        mut visit_slice: impl FnMut(&[u8]),
    ) {
        assert!(
            body_range.start <= body_range.end && body_range.end <= self.body_len(),
            "the range lies inside the body"
        );
        let mut segment_start = 0;
        self.for_each_segment(|segment_bytes| {
            let segment_end = segment_start + segment_bytes.len();
            let piece_start = body_range.start.max(segment_start);
            let piece_end = body_range.end.min(segment_end);
            if piece_start < piece_end {
                visit_slice(&segment_bytes[piece_start - segment_start..piece_end - segment_start]);
            }
            segment_start = segment_end;
        });
    }

    /// Appends the whole body to `output_buffer`.
    pub fn write_body(&self, output_buffer: &mut impl BufMut) {
        self.for_each_segment(|segment_bytes| output_buffer.put_slice(segment_bytes));
    }

    /// The `AccountInfo` or `Entry` message inside the body, without its key and length, which
    /// Block assembly writes as one of its items.
    pub fn inner_item_range(&self) -> Option<Range<usize>> {
        self.inner_item_range.as_ref().map(widen_body_range)
    }

    /// The unsliced account data in the body, which account filters read.
    pub fn account_data(&self) -> Option<&[u8]> {
        let account_data_range = self.account_data_range()?;
        match &self.body_segments {
            BodySegments::Contiguous(body_bytes) => Some(&body_bytes[account_data_range]),
            BodySegments::SharedAccountData { account_data, .. } => Some(account_data),
        }
    }

    fn account_data_range(&self) -> Option<Range<usize>> {
        self.account_data_range.as_ref().map(widen_body_range)
    }

    /// The body's compression group slots, which [`CompressedGroupSlots::load_or_attach`] attaches.
    pub const fn compressed_group_slots(&self) -> &ArcSwapOption<CompressedGroupSlots> {
        &self.compressed_group_slots
    }

    /// The segment that starts the body, with the body's outer key and length.
    const fn first_segment(&self) -> &Bytes {
        match &self.body_segments {
            BodySegments::Contiguous(body_bytes) => body_bytes,
            BodySegments::SharedAccountData { body_head, .. } => body_head,
        }
    }
}
