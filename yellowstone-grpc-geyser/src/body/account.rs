//! Account update bodies, unsliced and cut by a data slice, and their Block items.

use {
    super::{
        field_encoding::{
            bool_field_len, bytes_field_len, created_at_field_len, delimited_field_len,
            delimited_header_len, optional_uint64_field_len, put_bool_field, put_created_at_field,
            put_delimited_field, put_delimited_header, put_optional_uint64_field, put_uint64_field,
            uint64_field_len, ACCOUNT_UPDATE_TAG,
        },
        BodySegments, SharedUpdateBody,
    },
    crate::extension::seams::{AccountDataSource, AccountFields},
    bytes::{BufMut, Bytes},
    prost::encoding::{decode_varint, key_len},
    prost_types::Timestamp,
    solana_clock::{BankId, Slot},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::ops::Range,
};

/// `SubscribeUpdateAccount` field tags.
const ACCOUNT_INFO_TAG: u32 = 1;
const ACCOUNT_SLOT_TAG: u32 = 2;
const IS_STARTUP_TAG: u32 = 3;
const ACCOUNT_BANK_ID_TAG: u32 = 4;

/// `SubscribeUpdateAccountInfo` field tags.
const PUBKEY_TAG: u32 = 1;
const LAMPORTS_TAG: u32 = 2;
const OWNER_TAG: u32 = 3;
const EXECUTABLE_TAG: u32 = 4;
const RENT_EPOCH_TAG: u32 = 5;
const ACCOUNT_DATA_TAG: u32 = 6;
const WRITE_VERSION_TAG: u32 = 7;
const TRANSACTION_SIGNATURE_TAG: u32 = 8;

const PUBKEY_FIELD_LEN: usize = delimited_field_len(PUBKEY_TAG, size_of::<Pubkey>());
const OWNER_FIELD_LEN: usize = delimited_field_len(OWNER_TAG, size_of::<Pubkey>());
const SIGNATURE_FIELD_LEN: usize =
    delimited_field_len(TRANSACTION_SIGNATURE_TAG, size_of::<Signature>());

/// `AccountInfo` fields 1-5, which come before the data.
const fn fields_before_data_len(account_fields: &AccountFields) -> usize {
    PUBKEY_FIELD_LEN
        + uint64_field_len(LAMPORTS_TAG, account_fields.account_lamports)
        + OWNER_FIELD_LEN
        + bool_field_len(EXECUTABLE_TAG, account_fields.is_executable)
        + uint64_field_len(RENT_EPOCH_TAG, account_fields.rent_epoch)
}

/// `AccountInfo` fields 7-8, which come after the data.
const fn fields_after_data_len(account_fields: &AccountFields) -> usize {
    let signature_field_len = if account_fields.transaction_signature.is_some() {
        SIGNATURE_FIELD_LEN
    } else {
        0
    };
    uint64_field_len(WRITE_VERSION_TAG, account_fields.write_version) + signature_field_len
}

const fn data_field_header_len(account_data_len: usize) -> usize {
    if account_data_len == 0 {
        0
    } else {
        delimited_header_len(ACCOUNT_DATA_TAG, account_data_len)
    }
}

/// The lengths of one unsliced account body's parts, in body order.
struct AccountBodyLengths {
    fields_before_data_len: usize,
    account_data_len: usize,
    fields_after_data_len: usize,
    update_fields_after_info_len: usize,
    created_at_len: usize,
}

impl AccountBodyLengths {
    const fn account_info_len(&self) -> usize {
        self.fields_before_data_len
            + bytes_field_len(ACCOUNT_DATA_TAG, self.account_data_len)
            + self.fields_after_data_len
    }

    const fn account_update_len(&self) -> usize {
        delimited_field_len(ACCOUNT_INFO_TAG, self.account_info_len())
            + self.update_fields_after_info_len
    }

    const fn inner_item_start(&self) -> usize {
        delimited_header_len(ACCOUNT_UPDATE_TAG, self.account_update_len())
            + delimited_header_len(ACCOUNT_INFO_TAG, self.account_info_len())
    }

    /// Where the data starts, which is also the length of a three-segment body's head.
    const fn account_data_start(&self) -> usize {
        self.inner_item_start()
            + self.fields_before_data_len
            + data_field_header_len(self.account_data_len)
    }

    const fn body_len(&self) -> usize {
        delimited_field_len(ACCOUNT_UPDATE_TAG, self.account_update_len()) + self.created_at_len
    }
}

fn put_account_body_head(
    account_fields: &AccountFields,
    body_lengths: &AccountBodyLengths,
    output_buffer: &mut impl BufMut,
) {
    let account_update_len = body_lengths.account_update_len();
    put_delimited_header(ACCOUNT_UPDATE_TAG, account_update_len, output_buffer);
    put_delimited_header(
        ACCOUNT_INFO_TAG,
        body_lengths.account_info_len(),
        output_buffer,
    );
    put_delimited_field(
        PUBKEY_TAG,
        account_fields.account_pubkey.as_ref(),
        output_buffer,
    );
    put_uint64_field(LAMPORTS_TAG, account_fields.account_lamports, output_buffer);
    put_delimited_field(
        OWNER_TAG,
        account_fields.owner_pubkey.as_ref(),
        output_buffer,
    );
    put_bool_field(EXECUTABLE_TAG, account_fields.is_executable, output_buffer);
    put_uint64_field(RENT_EPOCH_TAG, account_fields.rent_epoch, output_buffer);
    if body_lengths.account_data_len != 0 {
        put_delimited_header(
            ACCOUNT_DATA_TAG,
            body_lengths.account_data_len,
            output_buffer,
        );
    }
}

fn put_account_body_tail(
    account_fields: &AccountFields,
    account_slot: Slot,
    is_startup: bool,
    bank_id: Option<BankId>,
    created_at: &Timestamp,
    output_buffer: &mut impl BufMut,
) {
    put_uint64_field(
        WRITE_VERSION_TAG,
        account_fields.write_version,
        output_buffer,
    );
    if let Some(transaction_signature) = &account_fields.transaction_signature {
        let signature_bytes = transaction_signature.as_ref();
        put_delimited_field(TRANSACTION_SIGNATURE_TAG, signature_bytes, output_buffer);
    }
    put_uint64_field(ACCOUNT_SLOT_TAG, account_slot, output_buffer);
    put_bool_field(IS_STARTUP_TAG, is_startup, output_buffer);
    put_optional_uint64_field(ACCOUNT_BANK_ID_TAG, bank_id, output_buffer);
    put_created_at_field(created_at, output_buffer);
}

/// Builds an unsliced account update body. Data the source shares stays by reference, as the
/// middle of three segments; other data is copied into one buffer with the fields.
pub fn encode_account_body<AccountData: AccountDataSource>(
    account_fields: &AccountFields,
    account_data: AccountData,
    account_slot: Slot,
    is_startup: bool,
    bank_id: Option<BankId>,
    created_at: Timestamp,
) -> SharedUpdateBody {
    let read_fixed_created_at = || created_at;
    let (account_body, _created_at) = encode_account_body_with_clock(
        account_fields,
        account_data,
        account_slot,
        is_startup,
        bank_id,
        read_fixed_created_at,
    );
    account_body
}

/// Builds the body as [`encode_account_body`] does, but reads `created_at` from `read_clock`
/// once the data is copied, as capture stamps it. Returns the body and that `created_at`.
pub fn encode_account_body_with_clock<AccountData: AccountDataSource>(
    account_fields: &AccountFields,
    account_data: AccountData,
    account_slot: Slot,
    is_startup: bool,
    bank_id: Option<BankId>,
    mut read_clock: impl FnMut() -> Timestamp,
) -> (SharedUpdateBody, Timestamp) {
    let shared_account_data = account_data
        .share_account_data()
        .filter(|shared_data_bytes| !shared_data_bytes.is_empty());
    let account_data_len = shared_account_data.as_ref().map_or_else(
        || account_data.account_data_bytes().len(),
        |shared_data_bytes| shared_data_bytes.len(),
    );
    // Sizes the buffer: the stamp taken after the copy almost always has the same length.
    let sizing_created_at = read_clock();
    let body_lengths = AccountBodyLengths {
        fields_before_data_len: fields_before_data_len(account_fields),
        account_data_len,
        fields_after_data_len: fields_after_data_len(account_fields),
        update_fields_after_info_len: uint64_field_len(ACCOUNT_SLOT_TAG, account_slot)
            + bool_field_len(IS_STARTUP_TAG, is_startup)
            + optional_uint64_field_len(ACCOUNT_BANK_ID_TAG, bank_id),
        created_at_len: created_at_field_len(&sizing_created_at),
    };
    let inner_item_start = body_lengths.inner_item_start();
    let inner_item_range = inner_item_start..inner_item_start + body_lengths.account_info_len();
    let account_data_start = body_lengths.account_data_start();
    let account_data_range = account_data_start..account_data_start + account_data_len;
    let put_body_tail = |created_at: &Timestamp, output_buffer: &mut Vec<u8>| {
        put_account_body_tail(
            account_fields,
            account_slot,
            is_startup,
            bank_id,
            created_at,
            output_buffer,
        )
    };

    let Some(shared_data_bytes) = shared_account_data else {
        let mut body_buffer = Vec::with_capacity(body_lengths.body_len());
        put_account_body_head(account_fields, &body_lengths, &mut body_buffer);
        body_buffer.put_slice(account_data.account_data_bytes());
        let created_at = read_clock();
        put_body_tail(&created_at, &mut body_buffer);
        // A stamp whose varint length changed during the copy leaves the buffer off its size.
        body_buffer.shrink_to_fit();
        let account_body = SharedUpdateBody::from_filled_buffer(
            body_buffer,
            Some(inner_item_range),
            Some(account_data_range),
        );
        return (account_body, created_at);
    };
    // Shared data is not copied, so the sizing stamp is the stamp.
    let created_at = sizing_created_at;
    let mut body_head = Vec::with_capacity(account_data_start);
    put_account_body_head(account_fields, &body_lengths, &mut body_head);
    let mut body_tail = Vec::with_capacity(body_lengths.body_len() - account_data_range.end);
    put_body_tail(&created_at, &mut body_tail);
    debug_assert_eq!(body_head.len(), body_head.capacity());
    debug_assert_eq!(body_tail.len(), body_tail.capacity());
    let account_body = SharedUpdateBody::from_body_segments(
        BodySegments::SharedAccountData {
            body_head: Bytes::from(body_head),
            account_data: shared_data_bytes,
            body_tail: Bytes::from(body_tail),
        },
        Some(inner_item_range),
        Some(account_data_range),
    );
    (account_body, created_at)
}

/// Visits the bytes a data slice keeps, as `FilterAccountsDataSlice` does: no ranges keep all
/// the data, and a range past the end of the data keeps nothing.
fn for_each_data_slice(
    account_data: &[u8],
    data_slice_ranges: &[Range<usize>],
    mut visit_slice: impl FnMut(&[u8]),
) {
    if data_slice_ranges.is_empty() {
        visit_slice(account_data);
        return;
    }
    for data_slice_range in data_slice_ranges {
        if account_data.len() >= data_slice_range.end {
            visit_slice(&account_data[data_slice_range.clone()]);
        }
    }
}

/// An account body cut by a data slice: the unsliced body's fields around the kept data.
struct SlicedAccountBody<'body> {
    account_body: &'body SharedUpdateBody,
    data_slice_ranges: &'body [Range<usize>],
    account_data: &'body [u8],
    fields_before_data: Range<usize>,
    fields_after_data: Range<usize>,
    sliced_data_len: usize,
    sliced_info_len: usize,
    sliced_update_len: usize,
}

impl<'body> SlicedAccountBody<'body> {
    fn from_account_body(
        account_body: &'body SharedUpdateBody,
        data_slice_ranges: &'body [Range<usize>],
    ) -> Self {
        let (Some(inner_item_range), Some(account_data_range), Some(account_data)) = (
            account_body.inner_item_range(),
            account_body.account_data_range(),
            account_body.account_data(),
        ) else {
            panic!("only an account body can be sliced");
        };
        let fields_before_data = inner_item_range.start
            ..account_data_range.start - data_field_header_len(account_data_range.len());
        let fields_after_data = account_data_range.end..inner_item_range.end;
        let mut sliced_data_len = 0;
        for_each_data_slice(account_data, data_slice_ranges, |slice_bytes| {
            sliced_data_len += slice_bytes.len()
        });
        let sliced_info_len = fields_before_data.len()
            + bytes_field_len(ACCOUNT_DATA_TAG, sliced_data_len)
            + fields_after_data.len();
        let mut update_len_bytes = &account_body.first_segment()[key_len(ACCOUNT_UPDATE_TAG)..];
        let unsliced_update_len = decode_varint(&mut update_len_bytes)
            .expect("an account body starts with its update's key and length")
            as usize;
        Self {
            account_body,
            data_slice_ranges,
            account_data,
            fields_before_data,
            fields_after_data,
            sliced_data_len,
            sliced_info_len,
            sliced_update_len: unsliced_update_len
                - delimited_field_len(ACCOUNT_INFO_TAG, inner_item_range.len())
                + delimited_field_len(ACCOUNT_INFO_TAG, sliced_info_len),
        }
    }

    const fn inner_item_start(&self) -> usize {
        delimited_header_len(ACCOUNT_UPDATE_TAG, self.sliced_update_len)
            + delimited_header_len(ACCOUNT_INFO_TAG, self.sliced_info_len)
    }

    const fn body_len(&self) -> usize {
        self.inner_item_start() + self.sliced_info_len + self.account_body.body_len()
            - self.fields_after_data.end
    }

    fn put_unsliced_range(&self, body_range: Range<usize>, output_buffer: &mut impl BufMut) {
        self.account_body
            .for_each_slice_in_range(body_range, |range_bytes| {
                output_buffer.put_slice(range_bytes)
            });
    }

    /// Writes the sliced `AccountInfo` fields, without the message's key and length.
    fn put_sliced_info(&self, output_buffer: &mut impl BufMut) {
        self.put_unsliced_range(self.fields_before_data.clone(), output_buffer);
        if self.sliced_data_len != 0 {
            put_delimited_header(ACCOUNT_DATA_TAG, self.sliced_data_len, output_buffer);
            for_each_data_slice(self.account_data, self.data_slice_ranges, |slice_bytes| {
                output_buffer.put_slice(slice_bytes)
            });
        }
        self.put_unsliced_range(self.fields_after_data.clone(), output_buffer);
    }

    fn put_body(&self, output_buffer: &mut impl BufMut) {
        put_delimited_header(ACCOUNT_UPDATE_TAG, self.sliced_update_len, output_buffer);
        put_delimited_header(ACCOUNT_INFO_TAG, self.sliced_info_len, output_buffer);
        self.put_sliced_info(output_buffer);
        let after_account_info = self.fields_after_data.end..self.account_body.body_len();
        self.put_unsliced_range(after_account_info, output_buffer);
    }
}

/// The length of the body that `data_slice_ranges` cuts from `account_body`.
pub fn sliced_account_body_len(
    account_body: &SharedUpdateBody,
    data_slice_ranges: &[Range<usize>],
) -> usize {
    SlicedAccountBody::from_account_body(account_body, data_slice_ranges).body_len()
}

/// Appends the body that `data_slice_ranges` cuts from `account_body`, without allocating.
pub fn write_sliced_account_body(
    account_body: &SharedUpdateBody,
    data_slice_ranges: &[Range<usize>],
    output_buffer: &mut impl BufMut,
) {
    SlicedAccountBody::from_account_body(account_body, data_slice_ranges).put_body(output_buffer);
}

/// The length of the `AccountInfo` item that `data_slice_ranges` cuts from `account_body`,
/// without the item's key and length.
pub fn account_info_item_len(
    account_body: &SharedUpdateBody,
    data_slice_ranges: &[Range<usize>],
) -> usize {
    SlicedAccountBody::from_account_body(account_body, data_slice_ranges).sliced_info_len
}

/// Appends that `AccountInfo` item as field `item_field_tag`, with its key and length.
pub fn put_account_info_item(
    item_field_tag: u32,
    account_body: &SharedUpdateBody,
    data_slice_ranges: &[Range<usize>],
    output_buffer: &mut impl BufMut,
) {
    let sliced_account_body = SlicedAccountBody::from_account_body(account_body, data_slice_ranges);
    put_delimited_header(
        item_field_tag,
        sliced_account_body.sliced_info_len,
        output_buffer,
    );
    sliced_account_body.put_sliced_info(output_buffer);
}
