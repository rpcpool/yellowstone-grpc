//! Compression group slots (arch 4.3): a shared body compresses its whole message once per group
//! of members with the same encoding and the same sorted filter-name bytes.

use {
    super::{
        encoding::CompressingEncoding,
        pool::{CompressorPools, GroupCompressionJob, OwnedGroupSlot, PooledGroupJob},
        stream_compressor::WholeMessageCompressor,
        ResponseEncoding,
    },
    crate::{
        body::{BodySegments, SharedUpdateBody},
        config::ConfigGrpcDataPlaneCompression,
        extension::seams::CompressionError,
        plugin::filter::name::{
            FilterGenerationNames, FilterKindNames, FilterNameKind, FilterNameMask,
        },
        subscribe::key::FilteredUpdateKey,
        telemetry::data_plane_counts::{
            CompressionOutcome, DataPlaneCount, PoolSafetyEvent, WaitedClaim, DATA_PLANE_COUNTS,
        },
    },
    arc_swap::Guard,
    bytes::Bytes,
    std::{
        fmt,
        future::Future,
        ops::Deref,
        pin::Pin,
        ptr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, LazyLock,
        },
        task::{Context, Poll},
        time::Duration,
    },
    tokio::time::{Instant, Sleep},
    yellowstone_grpc_concurrency::{
        claim_cell::{ClaimCell, ClaimCellContents, ClaimCellHolder, ClaimGuard, ReadyValueGuard},
        keyed_slot_table::{KeyedSlotTable, SlotLookup},
        pool_job_state::{PoolJobState, TakeBackOutcome},
        slot_waker_cells::{SlotWakerCells, WakerCellIndex, WakerRegistration},
    },
};

/// Waker cells per group slot; a confirmed or finalized waiter past them waits on its timer.
pub const GROUP_SLOT_WAKER_CELLS: usize = 8;
/// The most segments of one message: 31 name entries, then a body of up to three segments.
pub const MAX_MESSAGE_SEGMENTS: usize = FilterNameMask::MAX_MASKED_NAMES + 3;
/// A waiter's backstop is at most an hour, so its deadline never overflows.
pub const MAX_WAITER_BACKSTOP: Duration = Duration::from_secs(3600);
/// A waiter's backstop is at least 100 µs, so a waiter without a cell never spins.
pub const MIN_WAITER_BACKSTOP: Duration = Duration::from_micros(100);

pub type GroupSlotWakerCells = SlotWakerCells<GROUP_SLOT_WAKER_CELLS>;
/// A group's message, claimed by its first writer; a failed claim wakes the slot's waiters.
pub type CompressedMessageCell = ClaimCell<CompressedMessage, GroupSlotWakerCells>;
/// A body's attached table, kept alive by an arc-swap guard.
pub type CompressedGroupSlotsGuard = Guard<Option<Arc<CompressedGroupSlots>>>;

/// One complete gzip member or zstd frame of a whole message, its sorted name entries and then
/// its body, with the message's uncompressed length. It counts against the retained budget.
pub struct CompressedMessage {
    compressed_bytes: Bytes,
    uncompressed_len: usize,
    retained_budget: Arc<RetainedCompressionBudget>,
}

impl CompressedMessage {
    pub fn new_retained(
        compressed_bytes: &[u8],
        uncompressed_len: usize,
        retained_budget: &Arc<RetainedCompressionBudget>,
    ) -> Self {
        // A spare byte of capacity makes the `Bytes` shared from the start, so a clone never
        // allocates its shared header on a writer's path.
        let mut owned_bytes = Vec::with_capacity(compressed_bytes.len() + 1);
        owned_bytes.extend_from_slice(compressed_bytes);
        retained_budget
            .retained_bytes
            .fetch_add(compressed_bytes.len(), Ordering::Relaxed);
        Self {
            compressed_bytes: Bytes::from(owned_bytes),
            uncompressed_len,
            retained_budget: Arc::clone(retained_budget),
        }
    }

    pub const fn compressed_bytes(&self) -> &Bytes {
        &self.compressed_bytes
    }
}

impl Drop for CompressedMessage {
    fn drop(&mut self) {
        self.retained_budget
            .retained_bytes
            .fetch_sub(self.compressed_bytes.len(), Ordering::Relaxed);
    }
}

impl fmt::Debug for CompressedMessage {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("CompressedMessage")
            .field("compressed_len", &self.compressed_bytes.len())
            .field("uncompressed_len", &self.uncompressed_len)
            .finish()
    }
}

/// The bytes of every live group message of one plugin, against `compression.retained_budget_bytes`.
#[derive(Debug)]
pub struct RetainedCompressionBudget {
    retained_bytes: AtomicUsize,
    retained_budget_bytes: usize,
}

impl RetainedCompressionBudget {
    pub const fn new(retained_budget_bytes: usize) -> Self {
        Self {
            retained_bytes: AtomicUsize::new(0),
            retained_budget_bytes,
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Relaxed)
    }

    fn has_room(&self) -> bool {
        self.retained_bytes() < self.retained_budget_bytes
    }
}

/// The `compression.*` keys a group lookup reads, and the budget all writers of a plugin share.
#[derive(Debug, Clone)]
pub struct GroupCompressionSettings {
    /// `compression.group_slots`: 0 compresses every member privately.
    pub group_slot_count: usize,
    /// `compression.waiter_backstop`: a waiter without a waker cell checks again after it.
    pub waiter_backstop: Duration,
    pub retained_budget: Arc<RetainedCompressionBudget>,
}

impl GroupCompressionSettings {
    pub fn from_config(compression_config: &ConfigGrpcDataPlaneCompression) -> Self {
        Self {
            group_slot_count: compression_config.group_slots,
            waiter_backstop: compression_config.waiter_backstop,
            retained_budget: Arc::new(RetainedCompressionBudget::new(
                compression_config.retained_budget_bytes,
            )),
        }
    }
}

/// One writer's counts of shared and private compressions, kept off shared cache lines.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompressionGroupCounters {
    /// Groups this writer claimed and compressed once for all their members.
    pub group_compressions: u64,
    /// Members sent with a group's published message.
    pub shared_group_reads: u64,
    /// Groups keyed beside a racing writer's equal key: one extra compression each.
    pub racing_duplicate_groups: u64,
    /// Members compressed privately with `compression.group_slots` at 0.
    pub ungrouped_compressions: u64,
    /// Members compressed privately because every slot held another group.
    pub group_table_full_compressions: u64,
    /// Members compressed privately because retained messages were over their budget.
    pub retained_budget_full_compressions: u64,
    /// Processed members compressed privately because their group was in flight.
    pub in_flight_compressions: u64,
    /// Members compressed privately because their group's claim failed or its message retired.
    pub failed_or_retired_compressions: u64,
    /// Waits that found every waker cell taken and armed their own timer.
    pub backstop_waits: u64,
    /// Members of a compressed stream compressed per stream: control frames, ineligible
    /// entries, and every member in `per_stream` mode.
    pub stream_compressions: u64,
}

impl CompressionGroupCounters {
    /// The members this writer compressed privately for `private_compression_cause`.
    pub const fn private_compressions(
        &self,
        private_compression_cause: PrivateCompressionCause,
    ) -> u64 {
        match private_compression_cause {
            PrivateCompressionCause::NoGroupSlots => self.ungrouped_compressions,
            PrivateCompressionCause::GroupTableFull => self.group_table_full_compressions,
            PrivateCompressionCause::RetainedBudgetFull => self.retained_budget_full_compressions,
            PrivateCompressionCause::GroupInFlight => self.in_flight_compressions,
            PrivateCompressionCause::GroupFailedOrRetired => self.failed_or_retired_compressions,
        }
    }

    /// The counter of the members this writer compressed privately for `private_compression_cause`.
    pub const fn private_compressions_mut(
        &mut self,
        private_compression_cause: PrivateCompressionCause,
    ) -> &mut u64 {
        match private_compression_cause {
            PrivateCompressionCause::NoGroupSlots => &mut self.ungrouped_compressions,
            PrivateCompressionCause::GroupTableFull => &mut self.group_table_full_compressions,
            PrivateCompressionCause::RetainedBudgetFull => {
                &mut self.retained_budget_full_compressions
            }
            PrivateCompressionCause::GroupInFlight => &mut self.in_flight_compressions,
            PrivateCompressionCause::GroupFailedOrRetired => {
                &mut self.failed_or_retired_compressions
            }
        }
    }
}

/// The table a retired body gets when it had none: no slots, so a later member compresses
/// privately instead of attaching a table that keeps bytes until the body drops.
static RETIRED_GROUP_SLOTS: LazyLock<Arc<CompressedGroupSlots>> =
    LazyLock::new(|| Arc::new(CompressedGroupSlots::new(0)));

/// A slot's key: the claimant's encoding, name hash and names, held until the body drops. The
/// body fixes the variant, so the key leaves it out.
struct CompressedGroupKey {
    response_encoding: ResponseEncoding,
    filter_name_hash: u64,
    generation_names: Arc<FilterGenerationNames>,
    filter_name_kind: FilterNameKind,
    name_mask: FilterNameMask,
}

impl CompressedGroupKey {
    fn of_member(filtered_update_key: &FilteredUpdateKey<'_>) -> Self {
        Self {
            response_encoding: filtered_update_key.response_encoding,
            filter_name_hash: filtered_update_key.filter_name_hash,
            generation_names: Arc::clone(filtered_update_key.generation_names()),
            filter_name_kind: filtered_update_key.filter_name_kind(),
            name_mask: filtered_update_key.name_mask(),
        }
    }

    /// Whether a member of `filtered_update_key` belongs to this group, its names compared by bytes.
    fn matches_member(&self, filtered_update_key: &FilteredUpdateKey<'_>) -> bool {
        self.response_encoding == filtered_update_key.response_encoding
            && self.filter_name_hash == filtered_update_key.filter_name_hash
            && self
                .generation_names
                .name_selection(self.filter_name_kind, self.name_mask)
                .is_some_and(|group_names| {
                    group_names.same_names_as(&filtered_update_key.filter_names())
                })
    }
}

/// The table tag of a member's key: its name hash, with the encoding in the top bits.
const fn group_key_tag(filtered_update_key: &FilteredUpdateKey<'_>) -> u64 {
    let encoding_bits: u64 = match filtered_update_key.response_encoding {
        ResponseEncoding::Identity => 0,
        ResponseEncoding::Gzip => 1,
        ResponseEncoding::Zstd => 2,
    };
    filtered_update_key.filter_name_hash ^ (encoding_bits << 62)
}

/// A shared body's compression groups: fixed slots, each keyed once with one group's encoding
/// and names, holding the group's message behind a claim.
#[derive(Debug)]
pub struct CompressedGroupSlots {
    group_slot_table: KeyedSlotTable<CompressedGroupKey, CompressedMessageCell>,
}

/// The slot of one member's group.
#[derive(Clone, Copy)]
pub struct GroupSlotRef<'slots> {
    group_slot_index: usize,
    message_cell: &'slots CompressedMessageCell,
}

impl CompressedGroupSlots {
    pub fn new(group_slot_count: usize) -> Self {
        Self {
            group_slot_table: KeyedSlotTable::new(group_slot_count, CompressedMessageCell::default),
        }
    }

    /// The body's table, attached with one CAS if the body has none yet; a writer that loses
    /// the race drops its own table. `None` when `group_slot_count` is 0.
    pub fn load_or_attach(
        shared_body: &SharedUpdateBody,
        group_slot_count: usize,
    ) -> Option<CompressedGroupSlotsGuard> {
        if group_slot_count == 0 {
            return None;
        }
        let attached_slots = shared_body.compressed_group_slots();
        let loaded_slots = attached_slots.load();
        if loaded_slots.is_some() {
            return Some(loaded_slots);
        }
        drop(loaded_slots);
        drop(attached_slots.compare_and_swap(
            &None::<Arc<Self>>,
            Some(Arc::new(Self::new(group_slot_count))),
        ));
        Some(attached_slots.load())
    }

    /// The slot of the member's group: a keyed slot whose names are equal by bytes, else a free
    /// slot keyed now. `None` when every slot holds another group.
    pub fn find_or_key_group(
        &self,
        filtered_update_key: &FilteredUpdateKey<'_>,
        group_counters: &mut CompressionGroupCounters,
    ) -> Option<GroupSlotRef<'_>> {
        let key_tag = group_key_tag(filtered_update_key);
        let matches_member =
            |group_key: &CompressedGroupKey| group_key.matches_member(filtered_update_key);
        let (keyed_slot, passed_reserved_slot) =
            match self
                .group_slot_table
                .find_or_key_slot(key_tag, matches_member, || {
                    CompressedGroupKey::of_member(filtered_update_key)
                }) {
                SlotLookup::Matched(matched_slot) => (matched_slot, false),
                SlotLookup::Keyed {
                    keyed_slot,
                    passed_reserved_slot,
                } => (keyed_slot, passed_reserved_slot),
                SlotLookup::TableFull => return None,
            };
        if passed_reserved_slot
            && self
                .group_slot_table
                .find_keyed_slot(key_tag, matches_member)
                .is_some_and(|first_slot| first_slot.slot_index() != keyed_slot.slot_index())
        {
            group_counters.racing_duplicate_groups += 1;
        }
        Some(GroupSlotRef {
            group_slot_index: keyed_slot.slot_index(),
            message_cell: keyed_slot.slot_value(),
        })
    }

    /// The message cell of the slot at `group_slot_index`.
    pub fn group_slot(&self, group_slot_index: usize) -> Option<&CompressedMessageCell> {
        self.group_slot_table.slot_value(group_slot_index)
    }

    pub fn group_slot_count(&self) -> usize {
        self.group_slot_table.slot_count()
    }

    /// Retires every slot's message; retiring a claim in flight wakes its waiters, who then
    /// compress privately. A reader keeps the message it holds. Returns how many slots retired.
    pub fn retire_group_messages(&self) -> usize {
        self.group_slot_table
            .slot_values()
            .filter(|message_cell| message_cell.mark_retired())
            .count()
    }
}

/// Retires the body's group messages and puts the retired sentinel in place of its table, so
/// the table frees and no later member attaches one. Returns how many slots retired.
pub fn retire_compressed_groups(shared_body: &SharedUpdateBody) -> usize {
    let attached_slots = shared_body.compressed_group_slots();
    let already_retired = attached_slots
        .load()
        .as_ref()
        .is_some_and(|group_slots| Arc::ptr_eq(group_slots, &RETIRED_GROUP_SLOTS));
    if already_retired {
        return 0;
    }
    attached_slots
        .swap(Some(Arc::clone(&RETIRED_GROUP_SLOTS)))
        .as_deref()
        .map_or(0, CompressedGroupSlots::retire_group_messages)
}

/// How a writer treats a group whose claimant is still compressing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InFlightGroupPolicy {
    /// Processed: compress a private copy, never wait.
    CompressPrivately,
    /// Confirmed and finalized: wait until the claimant publishes or fails.
    WaitForClaimant,
}

/// Why a writer compressed a member's own copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateCompressionCause {
    NoGroupSlots,
    GroupTableFull,
    RetainedBudgetFull,
    /// A processed member found its group in flight.
    GroupInFlight,
    GroupFailedOrRetired,
}

impl PrivateCompressionCause {
    pub const ALL: [Self; 5] = [
        Self::NoGroupSlots,
        Self::GroupTableFull,
        Self::RetainedBudgetFull,
        Self::GroupInFlight,
        Self::GroupFailedOrRetired,
    ];
}

/// The outcome a private compression counts as in `compressions_total`.
pub const fn private_compression_outcome(
    private_compression_cause: PrivateCompressionCause,
) -> CompressionOutcome {
    match private_compression_cause {
        PrivateCompressionCause::NoGroupSlots => CompressionOutcome::PrivateUngrouped,
        PrivateCompressionCause::GroupTableFull => CompressionOutcome::PrivateGroupTableFull,
        PrivateCompressionCause::RetainedBudgetFull => CompressionOutcome::PrivateBudgetFull,
        PrivateCompressionCause::GroupInFlight => CompressionOutcome::PrivateInFlight,
        PrivateCompressionCause::GroupFailedOrRetired => CompressionOutcome::PrivateFailedOrRetired,
    }
}

/// A group's message, as one writer holds it.
#[derive(Debug)]
pub enum SharedCompressedMessage {
    /// Read from the slot and kept alive by an arc-swap guard, so hold it briefly.
    Read(ReadyValueGuard<CompressedMessage>),
    /// Compressed and published by this writer.
    Published(Arc<CompressedMessage>),
}

impl Deref for SharedCompressedMessage {
    type Target = CompressedMessage;

    fn deref(&self) -> &CompressedMessage {
        match self {
            Self::Read(ready_message) => ready_message,
            Self::Published(published_message) => published_message,
        }
    }
}

/// What a writer has for one member's compressed update.
#[derive(Debug)]
pub enum MemberCompression {
    /// The group's message: the bytes every member of the group sends.
    Shared(SharedCompressedMessage),
    /// The member's own copy, handed to the writer's `take_private`.
    Private(PrivateCompressionCause),
    /// A confirmed or finalized member's group is in flight in this slot: wait with
    /// [`ClaimEndWait`], then compress the member again.
    InFlight {
        group_slots: Arc<CompressedGroupSlots>,
        group_slot_index: usize,
    },
    /// A processed claimant's job is on a pool: after its pass the writer calls
    /// [`take_back_queued_job`], and waits in the slot when a pool thread runs it.
    PoolQueued {
        pooled_job: PooledGroupJob,
        group_slots: Arc<CompressedGroupSlots>,
        group_slot_index: usize,
    },
}

/// What one writer brings to a compression: the build's compressor, the plugin's settings, its
/// counters and the pools. A warm writer allocates nothing to read or copy a group.
pub struct GroupCompressionContext<'writer> {
    pub stream_compressor: &'writer dyn WholeMessageCompressor,
    pub group_settings: &'writer GroupCompressionSettings,
    pub group_counters: &'writer mut CompressionGroupCounters,
    /// The pools a claimant hands big bodies to; `None` compresses every claim inline.
    pub compressor_pools: Option<&'writer CompressorPools>,
}

/// One member's compressed update: its body, its key, the encoding its key names, and how its
/// level treats a group in flight.
#[derive(Clone, Copy)]
pub struct CompressedMember<'member> {
    pub shared_body: &'member SharedUpdateBody,
    pub filtered_update_key: &'member FilteredUpdateKey<'member>,
    pub compressing_encoding: CompressingEncoding,
    pub in_flight_policy: InFlightGroupPolicy,
}

/// Fills `segment_slices` with a member's whole message: the name entries `name_mask` selects in
/// order, then the body's segments. Returns how many it filled.
pub fn fill_message_segments<'message>(
    kind_names: &'message FilterKindNames,
    name_mask: FilterNameMask,
    body_segments: &'message BodySegments,
    segment_slices: &mut [&'message [u8]; MAX_MESSAGE_SEGMENTS],
) -> usize {
    let (body_slices, body_segment_count): ([&[u8]; 3], usize) = match body_segments {
        BodySegments::Contiguous(body_bytes) => ([body_bytes, &[], &[]], 1),
        BodySegments::SharedAccountData {
            body_head,
            account_data,
            body_tail,
        } => ([body_head, account_data, body_tail], 3),
    };
    let name_entries = name_mask
        .selected_name_indexes()
        .map(|name_index| kind_names.encoded_name_entry(name_index));
    let mut segment_count = 0;
    for (segment_slice, message_segment) in segment_slices
        .iter_mut()
        .zip(name_entries.chain(body_slices[..body_segment_count].iter().copied()))
    {
        *segment_slice = message_segment;
        segment_count += 1;
    }
    segment_count
}

/// Compresses a member's whole message, its name entries under `name_mask` and then the body, as
/// one gzip member or zstd frame, into a message charged to `retained_budget`.
pub fn compress_group_message(
    stream_compressor: &dyn WholeMessageCompressor,
    compressing_encoding: CompressingEncoding,
    kind_names: &FilterKindNames,
    name_mask: FilterNameMask,
    body_segments: &BodySegments,
    retained_budget: &Arc<RetainedCompressionBudget>,
) -> Result<Arc<CompressedMessage>, CompressionError> {
    let mut segment_slices: [&[u8]; MAX_MESSAGE_SEGMENTS] = [&[]; MAX_MESSAGE_SEGMENTS];
    let segment_count =
        fill_message_segments(kind_names, name_mask, body_segments, &mut segment_slices);
    let message_segments = &segment_slices[..segment_count];
    let uncompressed_len = message_segments
        .iter()
        .map(|message_segment| message_segment.len())
        .sum();
    let mut compressed_message = None;
    stream_compressor.compress_message_segments(
        compressing_encoding,
        message_segments,
        &mut |compressed_bytes| {
            compressed_message = Some(Arc::new(CompressedMessage::new_retained(
                compressed_bytes,
                uncompressed_len,
                retained_budget,
            )));
        },
    )?;
    Ok(compressed_message.expect("a compression hands its bytes over"))
}

/// Compresses the member's own copy of its whole message and hands it to `take_private`.
fn compress_private_copy(
    stream_compressor: &dyn WholeMessageCompressor,
    compressed_member: CompressedMember<'_>,
    take_private: &mut dyn FnMut(&mut Vec<u8>),
) -> Result<(), CompressionError> {
    let CompressedMember {
        shared_body,
        filtered_update_key,
        compressing_encoding,
        ..
    } = compressed_member;
    let kind_names = filtered_update_key
        .generation_names()
        .kind_names(filtered_update_key.filter_name_kind());
    let mut segment_slices: [&[u8]; MAX_MESSAGE_SEGMENTS] = [&[]; MAX_MESSAGE_SEGMENTS];
    let segment_count = fill_message_segments(
        kind_names,
        filtered_update_key.name_mask(),
        shared_body.body_segments(),
        &mut segment_slices,
    );
    stream_compressor.compress_message_segments(
        compressing_encoding,
        &segment_slices[..segment_count],
        take_private,
    )
}

/// Compresses one member's whole message once per group (arch 4.3). A private copy goes to
/// `take_private`; every claim publishes and wakes the slot's waiters, or fails and wakes them.
pub fn compress_member(
    compressed_member: CompressedMember<'_>,
    compression_context: &mut GroupCompressionContext<'_>,
    take_private: &mut dyn FnMut(&mut Vec<u8>),
) -> Result<MemberCompression, CompressionError> {
    debug_assert_eq!(
        compressed_member.filtered_update_key.response_encoding,
        compressed_member.compressing_encoding.response_encoding()
    );
    let private_compression_cause =
        match share_through_group(compressed_member, compression_context)? {
            Ok(member_compression) => return Ok(member_compression),
            Err(private_compression_cause) => private_compression_cause,
        };
    compress_private_copy(
        compression_context.stream_compressor,
        compressed_member,
        take_private,
    )?;
    *(compression_context.group_counters).private_compressions_mut(private_compression_cause) += 1;
    Ok(MemberCompression::Private(private_compression_cause))
}

/// The member's group message, or the wait for it, or why the member compresses privately.
fn share_through_group(
    compressed_member: CompressedMember<'_>,
    compression_context: &mut GroupCompressionContext<'_>,
) -> Result<Result<MemberCompression, PrivateCompressionCause>, CompressionError> {
    let CompressedMember {
        shared_body,
        filtered_update_key,
        in_flight_policy,
        ..
    } = compressed_member;
    let group_settings = compression_context.group_settings;
    let Some(attached_slots) =
        CompressedGroupSlots::load_or_attach(shared_body, group_settings.group_slot_count)
    else {
        return Ok(Err(PrivateCompressionCause::NoGroupSlots));
    };
    let Some(group_slots) = attached_slots.as_ref() else {
        return Ok(Err(PrivateCompressionCause::NoGroupSlots));
    };
    if group_slots.group_slot_count() == 0 {
        return Ok(Err(PrivateCompressionCause::GroupFailedOrRetired));
    }
    let Some(group_slot) =
        group_slots.find_or_key_group(filtered_update_key, compression_context.group_counters)
    else {
        return Ok(Err(PrivateCompressionCause::GroupTableFull));
    };
    let message_cell = group_slot.message_cell;
    let mut cell_contents = message_cell.read_contents();
    if matches!(cell_contents, ClaimCellContents::Empty) {
        if !group_settings.retained_budget.has_room() {
            return Ok(Err(PrivateCompressionCause::RetainedBudgetFull));
        }
        match message_cell.try_claim() {
            Ok(claim_guard) => {
                return hand_off_or_compress(
                    claim_guard,
                    group_slots,
                    group_slot.group_slot_index,
                    compressed_member,
                    compression_context,
                )
                .map(Ok);
            }
            Err(current_contents) => cell_contents = current_contents,
        }
    }
    Ok(match (cell_contents, in_flight_policy) {
        (ClaimCellContents::Ready(ready_message), _) => {
            compression_context.group_counters.shared_group_reads += 1;
            Ok(MemberCompression::Shared(SharedCompressedMessage::Read(
                ready_message,
            )))
        }
        (ClaimCellContents::Claimed, InFlightGroupPolicy::WaitForClaimant) => {
            Ok(MemberCompression::InFlight {
                group_slots: Arc::clone(group_slots),
                group_slot_index: group_slot.group_slot_index,
            })
        }
        (ClaimCellContents::Claimed, InFlightGroupPolicy::CompressPrivately) => {
            Err(PrivateCompressionCause::GroupInFlight)
        }
        (ClaimCellContents::Failed | ClaimCellContents::Retired | ClaimCellContents::Empty, _) => {
            Err(PrivateCompressionCause::GroupFailedOrRetired)
        }
    })
}

/// A claimant's compression: on a pool thread when a pool runs for its level and the body is
/// above the pool's bound, else inline. A full or stopping ring sends it inline too.
fn hand_off_or_compress(
    claim_guard: ClaimGuard<&CompressedMessageCell>,
    group_slots: &Arc<CompressedGroupSlots>,
    group_slot_index: usize,
    compressed_member: CompressedMember<'_>,
    compression_context: &mut GroupCompressionContext<'_>,
) -> Result<MemberCompression, CompressionError> {
    let CompressedMember {
        shared_body,
        filtered_update_key,
        compressing_encoding,
        in_flight_policy,
    } = compressed_member;
    let compressor_pool = compression_context
        .compressor_pools
        .and_then(|compressor_pools| compressor_pools.pool_for(in_flight_policy))
        .filter(|compressor_pool| shared_body.body_len() > compressor_pool.body_bytes_threshold());
    let Some(compressor_pool) = compressor_pool else {
        let published_message =
            compress_and_publish(claim_guard, compressed_member, compression_context)?;
        return Ok(MemberCompression::Shared(
            SharedCompressedMessage::Published(published_message),
        ));
    };
    let owned_group_slot = OwnedGroupSlot::new(Arc::clone(group_slots), group_slot_index)
        .expect("the claimed slot is in its table");
    let pooled_job = Arc::new(PoolJobState::new(GroupCompressionJob {
        group_claim: claim_guard.into_held(owned_group_slot),
        compressing_encoding,
        generation_names: Arc::clone(filtered_update_key.generation_names()),
        filter_name_kind: filtered_update_key.filter_name_kind(),
        name_mask: filtered_update_key.name_mask(),
        body_segments: shared_body.body_segments().clone(),
        retained_budget: Arc::clone(&compression_context.group_settings.retained_budget),
    }));
    if !compressor_pool.submit(&pooled_job) {
        if let TakeBackOutcome::TookBack(group_job) = pooled_job.try_take_back() {
            let published_message = compress_and_publish(
                group_job.group_claim,
                compressed_member,
                compression_context,
            )?;
            return Ok(MemberCompression::Shared(
                SharedCompressedMessage::Published(published_message),
            ));
        }
    }
    let group_slots = Arc::clone(group_slots);
    Ok(match in_flight_policy {
        InFlightGroupPolicy::CompressPrivately => MemberCompression::PoolQueued {
            pooled_job,
            group_slots,
            group_slot_index,
        },
        InFlightGroupPolicy::WaitForClaimant => MemberCompression::InFlight {
            group_slots,
            group_slot_index,
        },
    })
}

/// What a processed claimant's take-back found.
#[derive(Debug)]
pub enum QueuedJobOutcome {
    /// The job had not started: the writer compressed and published it inline.
    PublishedInline(Arc<CompressedMessage>),
    /// A pool thread runs or ran it, or its slot retired: the writer waits for the claim's end
    /// with [`ClaimEndWait`], then compresses the member again.
    RunsOnPool,
}

/// After its pass, a processed claimant compresses its job inline when no pool thread started
/// it (arch 4.3), or waits for the claim's end. An inline codec error fails the slot.
pub fn take_back_queued_job(
    pooled_job: &PooledGroupJob,
    compression_context: &mut GroupCompressionContext<'_>,
) -> Result<QueuedJobOutcome, CompressionError> {
    let TakeBackOutcome::TookBack(group_job) = pooled_job.try_take_back() else {
        return Ok(QueuedJobOutcome::RunsOnPool);
    };
    DATA_PLANE_COUNTS.add(DataPlaneCount::PoolSafety(PoolSafetyEvent::JobTakenBack), 1);
    let job_encoding = group_job.compressing_encoding;
    Ok(
        match group_job.compress_and_publish(compression_context.stream_compressor)? {
            Some(published_message) => {
                compression_context.group_counters.group_compressions += 1;
                DATA_PLANE_COUNTS.add(
                    DataPlaneCount::Compression(job_encoding, CompressionOutcome::TakenBack),
                    1,
                );
                QueuedJobOutcome::PublishedInline(published_message)
            }
            None => QueuedJobOutcome::RunsOnPool,
        },
    )
}

/// Compresses the member's whole message and publishes it; the publish wakes the slot's waiters.
fn compress_and_publish<Holder>(
    claim_guard: ClaimGuard<Holder>,
    compressed_member: CompressedMember<'_>,
    compression_context: &mut GroupCompressionContext<'_>,
) -> Result<Arc<CompressedMessage>, CompressionError>
where
    Holder: ClaimCellHolder<CellValue = CompressedMessage, EndHook = GroupSlotWakerCells>,
{
    let CompressedMember {
        shared_body,
        filtered_update_key,
        compressing_encoding,
        ..
    } = compressed_member;
    let compressed_message = compress_group_message(
        compression_context.stream_compressor,
        compressing_encoding,
        filtered_update_key
            .generation_names()
            .kind_names(filtered_update_key.filter_name_kind()),
        filtered_update_key.name_mask(),
        shared_body.body_segments(),
        &compression_context.group_settings.retained_budget,
    )?;
    let published_message = claim_guard.publish(compressed_message);
    compression_context.group_counters.group_compressions += 1;
    DATA_PLANE_COUNTS.add(
        DataPlaneCount::Compression(compressing_encoding, CompressionOutcome::Claimed),
        1,
    );
    Ok(published_message)
}

/// A table of claim cells that writers wait on; a waiter's entry keeps it alive, so its address
/// names it.
pub trait WaitedTable {}

impl WaitedTable for CompressedGroupSlots {}

/// One slot of one table that a writer waits on: the table's address and the slot's index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitedSlot {
    table_address: usize,
    slot_index: usize,
}

impl WaitedSlot {
    pub fn new(waited_table: &impl WaitedTable, slot_index: usize) -> Self {
        Self {
            table_address: ptr::from_ref(waited_table).addr(),
            slot_index,
        }
    }
}

/// The waker cell a waiter holds, in one waited slot.
#[derive(Debug, Clone, Copy)]
struct ParkedWaiter {
    waited_slot: WaitedSlot,
    waker_cell_index: WakerCellIndex,
}

/// Where a wait that arms its own timer counts: in its stream's count and the process's word.
pub struct BackstopWaitCount<'counts> {
    pub stream_backstop_waits: &'counts mut u64,
    pub waited_claim: WaitedClaim,
}

/// A confirmed or finalized writer's wait for a claim to end: parked in a waker cell of the claimed
/// cell, or on its own timer while every cell is taken. Its holder keeps the table alive.
#[derive(Debug, Default)]
pub struct ClaimEndWait {
    parked_waiter: Option<ParkedWaiter>,
    backstop_armed: bool,
}

impl ClaimEndWait {
    /// Ready once the claim on `waited_cell`, the cell of `waited_slot`, has ended, or when the slot
    /// has no cell; then write the member again. The timer is armed only without a waker cell.
    pub fn poll_claim_end<CellValue, const WAKER_CELLS: usize>(
        &mut self,
        waited_cell: Option<&ClaimCell<CellValue, SlotWakerCells<WAKER_CELLS>>>,
        waited_slot: WaitedSlot,
        task_context: &mut Context<'_>,
        mut backstop_timer: Pin<&mut Sleep>,
        waiter_backstop: Duration,
        backstop_wait_count: BackstopWaitCount<'_>,
    ) -> Poll<()> {
        debug_assert!(
            (self.parked_slot()).is_none_or(|parked_slot| parked_slot == waited_slot),
            "a parked wait is polled for its own slot"
        );
        let Some(waited_cell) = waited_cell else {
            *self = Self::default();
            return Poll::Ready(());
        };
        let mut timer_expired_in_this_poll = false;
        loop {
            if !matches!(waited_cell.read_contents(), ClaimCellContents::Claimed) {
                self.cancel_wait(Some(waited_cell), waited_slot);
                return Poll::Ready(());
            }
            let waker_cells = waited_cell.end_hook();
            let waker_registration = match self.parked_cell_index(waited_slot) {
                Some(waker_cell_index) => {
                    waker_cells.repark_waker(waker_cell_index, task_context.waker())
                }
                None => waker_cells.park_waker(task_context.waker()),
            };
            self.parked_waiter = None;
            match waker_registration {
                WakerRegistration::Parked(waker_cell_index) => {
                    self.parked_waiter = Some(ParkedWaiter {
                        waited_slot,
                        waker_cell_index,
                    });
                    self.backstop_armed = false;
                    return Poll::Pending;
                }
                // The cells wake only after the slot stops reading CLAIMED, so the next read sees it.
                WakerRegistration::ClaimEnded => {}
                WakerRegistration::NoFreeCell => {
                    if !self.backstop_armed {
                        let clamped_backstop =
                            waiter_backstop.clamp(MIN_WAITER_BACKSTOP, MAX_WAITER_BACKSTOP);
                        backstop_timer
                            .as_mut()
                            .reset(Instant::now() + clamped_backstop);
                        self.backstop_armed = true;
                        *backstop_wait_count.stream_backstop_waits += 1;
                        DATA_PLANE_COUNTS.add(
                            DataPlaneCount::ClaimBackstopWait(backstop_wait_count.waited_claim),
                            1,
                        );
                    }
                    if backstop_timer.as_mut().poll(task_context).is_pending() {
                        return Poll::Pending;
                    }
                    self.backstop_armed = false;
                    if timer_expired_in_this_poll {
                        // A zero backstop expires at once: yield instead of spinning here.
                        task_context.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    timer_expired_in_this_poll = true;
                }
            }
        }
    }

    /// Frees the waiter's cell in `waited_cell`, the cell of `waited_slot`, for a member that stops
    /// waiting there, at the end of its wait or of its stream.
    pub fn cancel_wait<CellValue, const WAKER_CELLS: usize>(
        &mut self,
        waited_cell: Option<&ClaimCell<CellValue, SlotWakerCells<WAKER_CELLS>>>,
        waited_slot: WaitedSlot,
    ) {
        if let (Some(waker_cell_index), Some(waited_cell)) =
            (self.parked_cell_index(waited_slot), waited_cell)
        {
            waited_cell.end_hook().cancel_parking(waker_cell_index);
        }
        *self = Self::default();
    }

    /// The slot this wait holds a waker cell in, if it holds one.
    pub fn parked_slot(&self) -> Option<WaitedSlot> {
        self.parked_waiter
            .map(|parked_waiter| parked_waiter.waited_slot)
    }

    /// The cell this wait holds in `waited_slot`, if it holds one there.
    fn parked_cell_index(&self, waited_slot: WaitedSlot) -> Option<WakerCellIndex> {
        self.parked_waiter
            .filter(|parked_waiter| parked_waiter.waited_slot == waited_slot)
            .map(|parked_waiter| parked_waiter.waker_cell_index)
    }
}
