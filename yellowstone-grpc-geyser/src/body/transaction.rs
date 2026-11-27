//! Transaction bodies: the `TransactionInfo` bytes that the transaction-writer slot writes, and
//! each transaction's update bodies, built once by the writer that claims them.

use {
    super::{
        field_encoding::{
            bool_field_len, created_at_field_len, delimited_field_len, delimited_header_len,
            put_bool_field, put_created_at_field, put_delimited_field, put_delimited_header,
            put_uint64_field, uint64_field_len, TRANSACTION_STATUS_UPDATE_TAG,
            TRANSACTION_UPDATE_TAG,
        },
        status::encode_one_buffer_body,
        SharedUpdateBody,
    },
    crate::{
        compression::slot,
        extension::seams::TransactionBodyWriter,
        plugin::message::{MessageTransaction, MessageTransactionInfo},
    },
    bytes::BufMut,
    prost::encoding::message,
    solana_signature::Signature,
    std::{
        fmt,
        ops::Deref,
        sync::{
            atomic::{fence, AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    },
    yellowstone_grpc_concurrency::claim_cell::{ClaimCell, ClaimCellContents, ReadyValueGuard},
};

/// `SubscribeUpdateTransactionInfo` field tags.
const SIGNATURE_TAG: u32 = 1;
const IS_VOTE_TAG: u32 = 2;
const TRANSACTION_TAG: u32 = 3;
const META_TAG: u32 = 4;
const INDEX_TAG: u32 = 5;

/// `SubscribeUpdateTransaction` field tags.
const TRANSACTION_INFO_TAG: u32 = 1;
const TRANSACTION_SLOT_TAG: u32 = 2;
const TRANSACTION_BANK_ID_TAG: u32 = 3;

/// `SubscribeUpdateTransactionStatus` field tags.
const STATUS_SLOT_TAG: u32 = 1;
const STATUS_SIGNATURE_TAG: u32 = 2;
const STATUS_IS_VOTE_TAG: u32 = 3;
const STATUS_INDEX_TAG: u32 = 4;
const STATUS_ERR_TAG: u32 = 5;
const STATUS_BANK_ID_TAG: u32 = 6;

const SIGNATURE_FIELD_LEN: usize = delimited_field_len(SIGNATURE_TAG, size_of::<Signature>());

/// The length of the `TransactionInfo` message that [`put_transaction_info`] writes.
pub fn transaction_info_len(transaction_info: &MessageTransactionInfo) -> usize {
    SIGNATURE_FIELD_LEN
        + bool_field_len(IS_VOTE_TAG, transaction_info.is_vote)
        + message::encoded_len(TRANSACTION_TAG, &transaction_info.transaction)
        + message::encoded_len(META_TAG, &transaction_info.meta)
        + uint64_field_len(INDEX_TAG, transaction_info.index as u64)
}

/// Appends the `TransactionInfo` message, without its key and length, as prost encodes it.
pub fn put_transaction_info(
    transaction_info: &MessageTransactionInfo,
    output_buffer: &mut impl BufMut,
) {
    let signature_bytes = transaction_info.signature.as_ref();
    put_delimited_field(SIGNATURE_TAG, signature_bytes, output_buffer);
    put_bool_field(IS_VOTE_TAG, transaction_info.is_vote, output_buffer);
    message::encode(
        TRANSACTION_TAG,
        &transaction_info.transaction,
        output_buffer,
    );
    message::encode(META_TAG, &transaction_info.meta, output_buffer);
    put_uint64_field(INDEX_TAG, transaction_info.index as u64, output_buffer);
}

/// The transaction-writer slot's standard adapter: prost's encoding of the converted message.
#[derive(Debug, Clone, Copy, Default)]
pub struct StandardTransactionBodyWriter;

impl TransactionBodyWriter for StandardTransactionBodyWriter {
    fn write_transaction_info(
        &self,
        transaction_info: &MessageTransactionInfo,
        transaction_info_bytes: &mut Vec<u8>,
    ) {
        put_transaction_info(transaction_info, transaction_info_bytes);
    }

    fn transaction_info_len(&self, transaction_info: &MessageTransactionInfo) -> usize {
        transaction_info_len(transaction_info)
    }
}

/// The lengths of a Transaction update body's content.
struct TransactionUpdateLengths {
    transaction_info_len: usize,
    update_content_len: usize,
}

impl TransactionUpdateLengths {
    const fn of_message(
        message_transaction: &MessageTransaction,
        transaction_info_len: usize,
    ) -> Self {
        let update_content_len = delimited_field_len(TRANSACTION_INFO_TAG, transaction_info_len)
            + uint64_field_len(TRANSACTION_SLOT_TAG, message_transaction.slot)
            + uint64_field_len(TRANSACTION_BANK_ID_TAG, message_transaction.bank_id);
        Self {
            transaction_info_len,
            update_content_len,
        }
    }

    const fn transaction_info_start(&self) -> usize {
        delimited_header_len(TRANSACTION_UPDATE_TAG, self.update_content_len)
            + delimited_header_len(TRANSACTION_INFO_TAG, self.transaction_info_len)
    }
}

/// The length of a Transaction update body whose `TransactionInfo` is `transaction_info_len`
/// bytes long.
pub fn transaction_update_body_len(
    message_transaction: &MessageTransaction,
    transaction_info_len: usize,
) -> usize {
    let body_lengths =
        TransactionUpdateLengths::of_message(message_transaction, transaction_info_len);
    delimited_field_len(TRANSACTION_UPDATE_TAG, body_lengths.update_content_len)
        + created_at_field_len(&message_transaction.created_at)
}

/// Builds a Transaction update body in one buffer. Its inner item is the `TransactionInfo`
/// message, which Block assembly reuses.
pub fn encode_transaction_update_body<Writer: TransactionBodyWriter>(
    body_writer: &Writer,
    message_transaction: &MessageTransaction,
) -> SharedUpdateBody {
    let transaction_info_len = body_writer.transaction_info_len(&message_transaction.transaction);
    let body_lengths =
        TransactionUpdateLengths::of_message(message_transaction, transaction_info_len);
    let body_len = transaction_update_body_len(message_transaction, transaction_info_len);
    let mut body_buffer = Vec::with_capacity(body_len);
    put_delimited_header(
        TRANSACTION_UPDATE_TAG,
        body_lengths.update_content_len,
        &mut body_buffer,
    );
    put_delimited_header(
        TRANSACTION_INFO_TAG,
        body_lengths.transaction_info_len,
        &mut body_buffer,
    );
    body_writer.write_transaction_info(&message_transaction.transaction, &mut body_buffer);
    let transaction_info_start = body_lengths.transaction_info_start();
    let transaction_info_range =
        transaction_info_start..transaction_info_start + body_lengths.transaction_info_len;
    assert_eq!(
        body_buffer.len(),
        transaction_info_range.end,
        "the transaction writer wrote another length than it measured"
    );
    put_uint64_field(
        TRANSACTION_SLOT_TAG,
        message_transaction.slot,
        &mut body_buffer,
    );
    put_uint64_field(
        TRANSACTION_BANK_ID_TAG,
        message_transaction.bank_id,
        &mut body_buffer,
    );
    put_created_at_field(&message_transaction.created_at, &mut body_buffer);
    SharedUpdateBody::from_filled_buffer(body_buffer, Some(transaction_info_range), None)
}

/// Writes a Transaction update body into `output_buffer`, as the claimant would build it, for
/// a writer that found the claim held or failed; it records the `TransactionInfo` length.
pub fn put_transaction_update_body(
    message_transaction: &MessageTransaction,
    output_buffer: &mut impl BufMut,
) {
    let transaction_bodies = message_transaction.transaction_bodies();
    let transaction_info_len = transaction_bodies
        .built_info_len()
        .unwrap_or_else(|| transaction_info_len(&message_transaction.transaction));
    let body_lengths =
        TransactionUpdateLengths::of_message(message_transaction, transaction_info_len);
    put_delimited_header(
        TRANSACTION_UPDATE_TAG,
        body_lengths.update_content_len,
        output_buffer,
    );
    put_delimited_header(TRANSACTION_INFO_TAG, transaction_info_len, output_buffer);
    put_transaction_info(&message_transaction.transaction, output_buffer);
    put_uint64_field(
        TRANSACTION_SLOT_TAG,
        message_transaction.slot,
        output_buffer,
    );
    put_uint64_field(
        TRANSACTION_BANK_ID_TAG,
        message_transaction.bank_id,
        output_buffer,
    );
    put_created_at_field(&message_transaction.created_at, output_buffer);
    transaction_bodies.record_built_info_len(transaction_info_len);
}

/// Writes a TransactionStatus update body into `output_buffer`, as its claimant would build it.
pub fn put_transaction_status_body(
    message_transaction: &MessageTransaction,
    output_buffer: &mut impl BufMut,
) {
    put_delimited_header(
        TRANSACTION_STATUS_UPDATE_TAG,
        transaction_status_content_len(message_transaction),
        output_buffer,
    );
    put_transaction_status_content(message_transaction, output_buffer);
    put_created_at_field(&message_transaction.created_at, output_buffer);
}

/// The `SubscribeUpdateTransactionStatus` content of a transaction, as prost encodes it.
fn transaction_status_content_len(message_transaction: &MessageTransaction) -> usize {
    let transaction_info = &message_transaction.transaction;
    let error_field_len = transaction_info
        .meta
        .err
        .as_ref()
        .map_or(0, |transaction_error| {
            message::encoded_len(STATUS_ERR_TAG, transaction_error)
        });
    uint64_field_len(STATUS_SLOT_TAG, message_transaction.slot)
        + delimited_field_len(STATUS_SIGNATURE_TAG, size_of::<Signature>())
        + bool_field_len(STATUS_IS_VOTE_TAG, transaction_info.is_vote)
        + uint64_field_len(STATUS_INDEX_TAG, transaction_info.index as u64)
        + error_field_len
        + uint64_field_len(STATUS_BANK_ID_TAG, message_transaction.bank_id)
}

/// Appends the `SubscribeUpdateTransactionStatus` content of a transaction.
fn put_transaction_status_content(
    message_transaction: &MessageTransaction,
    output_buffer: &mut impl BufMut,
) {
    let transaction_info = &message_transaction.transaction;
    put_uint64_field(STATUS_SLOT_TAG, message_transaction.slot, output_buffer);
    put_delimited_field(
        STATUS_SIGNATURE_TAG,
        transaction_info.signature.as_ref(),
        output_buffer,
    );
    put_bool_field(STATUS_IS_VOTE_TAG, transaction_info.is_vote, output_buffer);
    put_uint64_field(
        STATUS_INDEX_TAG,
        transaction_info.index as u64,
        output_buffer,
    );
    if let Some(transaction_error) = &transaction_info.meta.err {
        message::encode(STATUS_ERR_TAG, transaction_error, output_buffer);
    }
    put_uint64_field(
        STATUS_BANK_ID_TAG,
        message_transaction.bank_id,
        output_buffer,
    );
}

/// The length of a TransactionStatus update body, from the transaction's fields alone.
pub fn transaction_status_body_len(message_transaction: &MessageTransaction) -> usize {
    delimited_field_len(
        TRANSACTION_STATUS_UPDATE_TAG,
        transaction_status_content_len(message_transaction),
    ) + created_at_field_len(&message_transaction.created_at)
}

/// Builds a TransactionStatus update body. It holds no `TransactionInfo` bytes.
pub fn encode_transaction_status_body(
    message_transaction: &MessageTransaction,
) -> SharedUpdateBody {
    encode_one_buffer_body(
        TRANSACTION_STATUS_UPDATE_TAG,
        transaction_status_content_len(message_transaction),
        &message_transaction.created_at,
        false,
        |body_buffer| put_transaction_status_content(message_transaction, body_buffer),
    )
}

/// A published body: read through its cell's guard, or the claimant's own `Arc`.
pub enum PublishedBody {
    ReadThroughGuard(ReadyValueGuard<SharedUpdateBody>),
    BuiltByThisWriter(Arc<SharedUpdateBody>),
}

impl Deref for PublishedBody {
    type Target = SharedUpdateBody;

    fn deref(&self) -> &SharedUpdateBody {
        match self {
            Self::ReadThroughGuard(value_guard) => value_guard,
            Self::BuiltByThisWriter(published_body) => published_body,
        }
    }
}

impl fmt::Debug for PublishedBody {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        SharedUpdateBody::fmt(self, debug_formatter)
    }
}

/// A transaction's update bodies, each built once by the writer that claims its cell. A writer
/// that finds a cell claimed or failed writes privately and never waits.
pub struct TransactionBodies {
    transaction_update: ClaimCell<SharedUpdateBody>,
    transaction_status_update: ClaimCell<SharedUpdateBody>,
    /// The `TransactionInfo` length once some writer has written those bytes; 0 before.
    built_info_len: AtomicUsize,
    /// Set by the bank's retirement: a body published after it retires its own groups.
    groups_retired: AtomicBool,
}

impl Default for TransactionBodies {
    fn default() -> Self {
        Self {
            transaction_update: ClaimCell::new(()),
            transaction_status_update: ClaimCell::new(()),
            built_info_len: AtomicUsize::new(0),
            groups_retired: AtomicBool::new(false),
        }
    }
}

/// A clone belongs to a new message, so it starts with nothing built.
impl Clone for TransactionBodies {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// The bodies are a cache of the message's fields, so they never make two messages differ.
impl PartialEq for TransactionBodies {
    fn eq(&self, _other_bodies: &Self) -> bool {
        true
    }
}

impl fmt::Debug for TransactionBodies {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("TransactionBodies")
            .field("built_info_len", &self.built_info_len())
            .finish_non_exhaustive()
    }
}

impl TransactionBodies {
    /// The `TransactionInfo` length once some writer has written those bytes, as weights count
    /// it; `None` before, when a transaction weighs one unit.
    pub fn built_info_len(&self) -> Option<usize> {
        match self.built_info_len.load(Ordering::Relaxed) {
            0 => None,
            info_len => Some(info_len),
        }
    }

    /// Notes the length of `TransactionInfo` bytes that a writer has written.
    pub fn record_built_info_len(&self, transaction_info_len: usize) {
        self.built_info_len
            .store(transaction_info_len, Ordering::Relaxed);
    }

    /// The published Transaction update body, or the one this call claims, builds with
    /// `body_writer` and publishes; `None` when another writer holds the claim or it failed.
    pub fn transaction_update_body<Writer: TransactionBodyWriter>(
        &self,
        body_writer: &Writer,
        message_transaction: &MessageTransaction,
    ) -> Option<PublishedBody> {
        claim_or_read_body(&self.transaction_update, &self.groups_retired, || {
            let update_body = encode_transaction_update_body(body_writer, message_transaction);
            if let Some(transaction_info_range) = update_body.inner_item_range() {
                self.record_built_info_len(transaction_info_range.len());
            }
            update_body
        })
    }

    /// Retires the compression groups of every body published so far, and makes a body published
    /// later retire its own. Returns how many group slots retired.
    pub fn retire_compressed_groups(&self) -> usize {
        self.groups_retired.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        [&self.transaction_update, &self.transaction_status_update]
            .into_iter()
            .map(|body_cell| match body_cell.read_contents() {
                ClaimCellContents::Ready(published_body) => {
                    slot::retire_compressed_groups(&published_body)
                }
                _ => 0,
            })
            .sum()
    }

    /// The published TransactionStatus update body, or the one this call builds and publishes.
    pub fn transaction_status_body(
        &self,
        message_transaction: &MessageTransaction,
    ) -> Option<PublishedBody> {
        claim_or_read_body(
            &self.transaction_status_update,
            &self.groups_retired,
            || encode_transaction_status_body(message_transaction),
        )
    }
}

/// Reads a READY cell, or claims an EMPTY one and publishes what `build_body` builds. A cell
/// claimed by another writer, failed or retired gives `None`: the caller writes privately.
fn claim_or_read_body(
    body_cell: &ClaimCell<SharedUpdateBody>,
    groups_retired: &AtomicBool,
    build_body: impl FnOnce() -> SharedUpdateBody,
) -> Option<PublishedBody> {
    match body_cell.try_claim() {
        Ok(claim_guard) => {
            let published_body = claim_guard.publish(Arc::new(build_body()));
            // Pairs with the retirement's flag and fence: one of the two sees the other's write.
            fence(Ordering::SeqCst);
            if groups_retired.load(Ordering::SeqCst) {
                slot::retire_compressed_groups(&published_body);
            }
            Some(PublishedBody::BuiltByThisWriter(published_body))
        }
        Err(ClaimCellContents::Ready(value_guard)) => {
            Some(PublishedBody::ReadThroughGuard(value_guard))
        }
        Err(
            ClaimCellContents::Empty
            | ClaimCellContents::Claimed
            | ClaimCellContents::Failed
            | ClaimCellContents::Retired,
        ) => None,
    }
}
