//! Subscribe queue entries. A routed member's update is a counted entry of its message with the
//! member's filter and matched update; its bytes are written only when the member's stream writes.

use {
    crate::{
        body::{
            block_shape::{
                build_and_publish_shape_body, claim_or_read_shape_cell, BlockShapeBudget,
                BlockShapeEvent, BlockShapeTable, BlockShapeWriteSettings, PrivateShapeCause,
                PublishedShapeBody, ShapeCellRead,
            },
            block_shape_body_len, encode_block_shape_body, put_transaction_status_body,
            put_transaction_update_body, sliced_account_body_len, transaction_info_len,
            transaction_status_body_len, transaction_update_body_len, write_block_shape_body,
            write_sliced_account_body, PublishedBody, SharedUpdateBody,
            StandardTransactionBodyWriter,
        },
        plugin::{
            filter::{
                message::{
                    count_transaction_info_use, FilteredUpdate, TransactionInfoUse, WeightScales,
                },
                name::{FilterNameKind, FilterNameMask, FilterNameSelection},
                variant::UpdateBodyVariant,
                Filter, MatchedUpdate, MemberBlockShape,
            },
            message::{
                CommitmentLevel, Message, MessageAccount, MessageBlock, MessageBlockFooter,
                MessageBlockMeta, MessageEntry, MessageEntryUpdateParent, MessageSlot,
                MessageTransaction,
            },
        },
    },
    bytes::{Buf, BufMut, Bytes},
    prost::{
        encoding::{DecodeContext, WireType},
        DecodeError,
    },
    std::{
        fmt,
        ops::Range,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
    yellowstone_grpc_concurrency::counted_message::{
        AddMessageEntries, AddedMessageEntries, CountedMessageEntry,
    },
};

/// One item of a client's Subscribe queue.
#[derive(Debug)]
pub enum SubscribeQueueEntry {
    /// A member's update by counted reference: routed, unrouted, replay and snapshot members.
    Indexed(IndexedUpdateRef),
    /// An update its producer built with the reference matcher: members whose mask is
    /// recomputed, a Join's drained updates, blocks, pings and pongs.
    Filtered(FilteredUpdate),
}

impl SubscribeQueueEntry {
    /// The entry's weight in its member's queue.
    pub fn update_weight(&self, weight_scales: &WeightScales) -> u32 {
        match self {
            Self::Indexed(indexed_update) => indexed_update.update_weight(weight_scales),
            Self::Filtered(filtered_update) => weight_scales.update_weight(filtered_update),
        }
    }

    /// Whether the entry is a Block member's, whose length only a walk or its shape's body tells.
    pub const fn is_block_shape_entry(&self) -> bool {
        match self {
            Self::Indexed(indexed_update) => indexed_update.is_block_shape_entry(),
            Self::Filtered(_) => false,
        }
    }

    /// The shape table of a Block member's block.
    pub fn block_shapes(&self) -> Option<&BlockShapeTable> {
        match self {
            Self::Indexed(indexed_update) => indexed_update.block_shapes(),
            Self::Filtered(_) => None,
        }
    }
}

impl prost::Message for SubscribeQueueEntry {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            Self::Indexed(indexed_update) => indexed_update.encode_raw(buf),
            Self::Filtered(filtered_update) => filtered_update.encode_raw(buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::Indexed(indexed_update) => indexed_update.encoded_len(),
            Self::Filtered(filtered_update) => filtered_update.encoded_len(),
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

/// A counted entry of a routed message, of each kind that routing delivers.
#[derive(Debug)]
pub enum IndexedMessageEntry {
    Account(CountedMessageEntry<MessageAccount>),
    Slot(CountedMessageEntry<MessageSlot>),
    Transaction(CountedMessageEntry<MessageTransaction>),
    Entry(CountedMessageEntry<MessageEntry>),
    BlockMeta(CountedMessageEntry<MessageBlockMeta>),
    BlockFooter(CountedMessageEntry<MessageBlockFooter>),
    EntryUpdateParent(CountedMessageEntry<MessageEntryUpdateParent>),
    /// A Block member's block, with the weight its routing measured and its shape slot and pin.
    Block {
        message_block: CountedMessageEntry<MessageBlock>,
        update_weight: u32,
        block_entry_flags: BlockEntryFlags,
    },
}

/// How many blocks one stream's queued Block entries may pin; past it a member gets the
/// reference updates, which pin only what they select. Room for a burst of finalized blocks.
pub const BLOCKS_PINNED_PER_STREAM: usize = 8;

/// The blocks one session's queued Block entries pin. Every filter of the session, and each
/// clone of one, shares the count.
#[derive(Debug, Default)]
pub struct BlockPinCounter {
    pinned_blocks: AtomicUsize,
}

impl BlockPinCounter {
    /// Pins one more block, unless [`BLOCKS_PINNED_PER_STREAM`] are pinned already.
    pub fn try_pin_block(&self) -> bool {
        self.pinned_blocks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pinned_blocks| {
                (pinned_blocks < BLOCKS_PINNED_PER_STREAM).then_some(pinned_blocks + 1)
            })
            .is_ok()
    }

    pub fn unpin_block(&self) {
        let pinned_before = self.pinned_blocks.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(pinned_before > 0, "each pin is released once");
    }
}

/// A Block entry's shape slot, if its table had room, and whether it holds its member's pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockEntryFlags(u8);

impl BlockEntryFlags {
    const HOLDS_BLOCK_PIN: u8 = 0x80;
    const NO_SHAPE_SLOT: u8 = 0x7f;

    pub const fn new(shape_slot_index: Option<u8>, holds_block_pin: bool) -> Self {
        let slot_bits = match shape_slot_index {
            Some(slot_index) => {
                assert!(slot_index < Self::NO_SHAPE_SLOT, "a shape slot fits 7 bits");
                slot_index
            }
            None => Self::NO_SHAPE_SLOT,
        };
        let pin_bit = if holds_block_pin {
            Self::HOLDS_BLOCK_PIN
        } else {
            0
        };
        Self(slot_bits | pin_bit)
    }

    pub const fn shape_slot_index(self) -> Option<u8> {
        match self.0 & !Self::HOLDS_BLOCK_PIN {
            Self::NO_SHAPE_SLOT => None,
            slot_index => Some(slot_index),
        }
    }

    pub const fn holds_block_pin(self) -> bool {
        self.0 & Self::HOLDS_BLOCK_PIN != 0
    }
}

/// How a Block member's write may use its shape's cell.
#[derive(Debug, Clone, Copy)]
pub enum ShapeCellPolicy<'settings> {
    /// The tonic body: reads a published body, else writes privately.
    NeverClaims,
    /// A processed member: claims within the budget, and writes privately while a claim is in
    /// flight.
    ClaimsWithin(&'settings Arc<BlockShapeBudget>),
    /// A confirmed or finalized member: claims within the budget, and waits for a claim in flight.
    ClaimsOrWaitsWithin(&'settings Arc<BlockShapeBudget>),
}

impl<'settings> ShapeCellPolicy<'settings> {
    const fn shape_budget(self) -> Option<&'settings Arc<BlockShapeBudget>> {
        match self {
            Self::NeverClaims => None,
            Self::ClaimsWithin(shape_budget) | Self::ClaimsOrWaitsWithin(shape_budget) => {
                Some(shape_budget)
            }
        }
    }

    const fn waits_for_claimant(self) -> bool {
        matches!(self, Self::ClaimsOrWaitsWithin(_))
    }
}

/// A confirmed or finalized Block member whose shape another writer is building: the slot it
/// waits on, in its block's shape table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShapeWait {
    pub shape_slot_index: u8,
}

/// The blocks filter of a Block member's one-name mask: its name index.
pub const fn blocks_filter_index(filter_name_mask: FilterNameMask) -> usize {
    filter_name_mask.mask_bits().trailing_zeros() as usize
}

/// The entries one message hands out to its routed members, added with one count add.
pub enum AddedIndexedEntries {
    Account(AddedMessageEntries<MessageAccount>),
    Slot(AddedMessageEntries<MessageSlot>),
    Transaction(AddedMessageEntries<MessageTransaction>),
    Entry(AddedMessageEntries<MessageEntry>),
    BlockMeta(AddedMessageEntries<MessageBlockMeta>),
    BlockFooter(AddedMessageEntries<MessageBlockFooter>),
    EntryUpdateParent(AddedMessageEntries<MessageEntryUpdateParent>),
}

impl AddedIndexedEntries {
    /// Adds `entry_count` entries to `routed_message`, or gives `None` for a kind that routing
    /// never delivers. Entries never handed out release when this drops.
    pub fn add_to(routed_message: &Message, entry_count: usize) -> Option<Self> {
        Some(match routed_message {
            Message::Account(message_account) => {
                Self::Account(message_account.add_message_entries(entry_count))
            }
            Message::Slot(message_slot) => {
                Self::Slot(message_slot.add_message_entries(entry_count))
            }
            Message::Transaction(message_transaction) => {
                Self::Transaction(message_transaction.add_message_entries(entry_count))
            }
            Message::Entry(message_entry) => {
                Self::Entry(message_entry.add_message_entries(entry_count))
            }
            Message::BlockMeta(message_block_meta) => {
                Self::BlockMeta(message_block_meta.add_message_entries(entry_count))
            }
            Message::BlockFooter(message_block_footer) => {
                Self::BlockFooter(message_block_footer.add_message_entries(entry_count))
            }
            Message::EntryUpdateParent(message_update_parent) => {
                Self::EntryUpdateParent(message_update_parent.add_message_entries(entry_count))
            }
            Message::Block(_)
            | Message::DeshredTransaction(_)
            | Message::DeshredUpdateParent(_) => return None,
        })
    }

    /// The added entries not handed out yet.
    pub fn entries_left(&self) -> usize {
        match self {
            Self::Account(added_entries) => added_entries.len(),
            Self::Slot(added_entries) => added_entries.len(),
            Self::Transaction(added_entries) => added_entries.len(),
            Self::Entry(added_entries) => added_entries.len(),
            Self::BlockMeta(added_entries) => added_entries.len(),
            Self::BlockFooter(added_entries) => added_entries.len(),
            Self::EntryUpdateParent(added_entries) => added_entries.len(),
        }
    }

    /// The next added entry, or `None` once every entry is handed out.
    pub fn next_entry(&mut self) -> Option<IndexedMessageEntry> {
        match self {
            Self::Account(added_entries) => added_entries.next().map(IndexedMessageEntry::Account),
            Self::Slot(added_entries) => added_entries.next().map(IndexedMessageEntry::Slot),
            Self::Transaction(added_entries) => {
                added_entries.next().map(IndexedMessageEntry::Transaction)
            }
            Self::Entry(added_entries) => added_entries.next().map(IndexedMessageEntry::Entry),
            Self::BlockMeta(added_entries) => {
                added_entries.next().map(IndexedMessageEntry::BlockMeta)
            }
            Self::BlockFooter(added_entries) => {
                added_entries.next().map(IndexedMessageEntry::BlockFooter)
            }
            Self::EntryUpdateParent(added_entries) => added_entries
                .next()
                .map(IndexedMessageEntry::EntryUpdateParent),
        }
    }
}

/// A routed member's update: its message by counted entry, the member's filter, and the body
/// variant and names that the filter matched.
pub struct IndexedUpdateRef {
    message_entry: IndexedMessageEntry,
    member_filter: Arc<Filter>,
    matched_update: MatchedUpdate,
}

impl fmt::Debug for IndexedUpdateRef {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("IndexedUpdateRef")
            .field("message_entry", &self.message_entry)
            .field("matched_update", &self.matched_update)
            .finish_non_exhaustive()
    }
}

/// What a routed member's update writes after its names.
enum IndexedBodyWrite<'entry> {
    SharedBody(&'entry SharedUpdateBody),
    PublishedBody(PublishedBody),
    SlicedAccountBody(&'entry SharedUpdateBody, &'entry [Range<usize>]),
    /// Another writer holds the transaction's claim, or it failed: this member writes its own.
    PrivateTransaction(&'entry MessageTransaction),
    PrivateTransactionStatus(&'entry MessageTransaction),
    /// The Block body of the member's shape, published in its slot.
    ShapeBody(PublishedShapeBody),
    /// The member writes its Block body itself, for `private_shape_cause`.
    PrivateBlockShape {
        message_block: &'entry MessageBlock,
        member_block_shape: MemberBlockShape<'entry>,
        private_shape_cause: PrivateShapeCause,
    },
}

/// Where a frame's content goes: copied pieces into a buffer, and shared body segments, which a
/// sink may reference instead of copying.
pub trait FrameContentSink {
    type CopyBuffer: BufMut;

    fn copy_buffer(&mut self) -> &mut Self::CopyBuffer;

    fn put_shared_segment(&mut self, shared_segment: &Bytes);
}

/// A sink that copies every piece into one buffer, as `encode_raw` writes a message.
pub struct CopyingContentSink<'buffer, CopyBuffer: BufMut> {
    copy_target: &'buffer mut CopyBuffer,
}

impl<'buffer, CopyBuffer: BufMut> CopyingContentSink<'buffer, CopyBuffer> {
    pub const fn new(copy_target: &'buffer mut CopyBuffer) -> Self {
        Self { copy_target }
    }
}

impl<CopyBuffer: BufMut> FrameContentSink for CopyingContentSink<'_, CopyBuffer> {
    type CopyBuffer = CopyBuffer;

    fn copy_buffer(&mut self) -> &mut CopyBuffer {
        self.copy_target
    }

    fn put_shared_segment(&mut self, shared_segment: &Bytes) {
        self.copy_target.put_slice(shared_segment);
    }
}

/// A routed member's message content, resolved once: its length and its bytes come from the same
/// names and the same body, so a claim settled in between cannot make them differ.
pub struct ResolvedFrameContent<'entry> {
    name_selection: FilterNameSelection<'entry>,
    body_write: IndexedBodyWrite<'entry>,
    update_body_variant: UpdateBodyVariant,
}

impl ResolvedFrameContent<'_> {
    /// The content's length in bytes: the names, then the body.
    pub fn content_len(&self) -> usize {
        self.name_selection.encoded_entries_len()
            + match &self.body_write {
                IndexedBodyWrite::SharedBody(shared_body) => shared_body.body_len(),
                IndexedBodyWrite::PublishedBody(published_body) => published_body.body_len(),
                IndexedBodyWrite::SlicedAccountBody(account_body, data_slice_ranges) => {
                    sliced_account_body_len(account_body, data_slice_ranges)
                }
                IndexedBodyWrite::PrivateTransaction(message_transaction) => {
                    let private_info_len = message_transaction
                        .transaction_bodies()
                        .built_info_len()
                        .unwrap_or_else(|| transaction_info_len(&message_transaction.transaction));
                    transaction_update_body_len(message_transaction, private_info_len)
                }
                IndexedBodyWrite::PrivateTransactionStatus(message_transaction) => {
                    transaction_status_body_len(message_transaction)
                }
                IndexedBodyWrite::ShapeBody(shape_body) => shape_body.body_len(),
                IndexedBodyWrite::PrivateBlockShape {
                    member_block_shape, ..
                } => block_shape_body_len(member_block_shape),
            }
    }

    /// The bytes of the shared body segments of at least `copy_threshold_bytes`, which a frame
    /// writer may reference instead of copying.
    pub fn referenced_len(&self, copy_threshold_bytes: usize) -> usize {
        let mut referenced_len = 0;
        if let Some(shared_body) = self.shared_body() {
            shared_body.for_each_segment(|shared_segment| {
                if shared_segment.len() >= copy_threshold_bytes {
                    referenced_len += shared_segment.len();
                }
            });
        }
        referenced_len
    }

    /// Writes the names, then the body; a shared body goes segment by segment.
    pub fn write_content(&self, content_sink: &mut impl FrameContentSink) {
        self.name_selection
            .write_encoded_entries(content_sink.copy_buffer());
        match &self.body_write {
            IndexedBodyWrite::SharedBody(shared_body) => {
                shared_body.for_each_segment(|shared_segment| {
                    content_sink.put_shared_segment(shared_segment)
                });
            }
            IndexedBodyWrite::PublishedBody(published_body) => {
                self.count_published_body_use();
                published_body.for_each_segment(|shared_segment| {
                    content_sink.put_shared_segment(shared_segment)
                });
            }
            IndexedBodyWrite::SlicedAccountBody(account_body, data_slice_ranges) => {
                write_sliced_account_body(
                    account_body,
                    data_slice_ranges,
                    content_sink.copy_buffer(),
                );
            }
            IndexedBodyWrite::PrivateTransaction(message_transaction) => {
                put_transaction_update_body(message_transaction, content_sink.copy_buffer());
                count_transaction_info_use(TransactionInfoUse::Private);
            }
            IndexedBodyWrite::PrivateTransactionStatus(message_transaction) => {
                put_transaction_status_body(message_transaction, content_sink.copy_buffer());
            }
            IndexedBodyWrite::ShapeBody(shape_body) => {
                shape_body.for_each_segment(|shared_segment| {
                    content_sink.put_shared_segment(shared_segment)
                });
            }
            IndexedBodyWrite::PrivateBlockShape {
                message_block,
                member_block_shape,
                private_shape_cause,
            } => {
                write_block_shape_body(member_block_shape, content_sink.copy_buffer());
                let private_write = BlockShapeEvent::PrivateWrite(*private_shape_cause);
                message_block.block_shapes.count_shape_event(private_write);
            }
        }
    }

    /// The body every member of this message and variant shares, if the content has one.
    pub fn shared_body(&self) -> Option<&SharedUpdateBody> {
        match &self.body_write {
            IndexedBodyWrite::SharedBody(shared_body) => Some(shared_body),
            IndexedBodyWrite::PublishedBody(published_body) => Some(published_body),
            IndexedBodyWrite::ShapeBody(shape_body) => Some(shape_body),
            _ => None,
        }
    }

    /// Counts a write of a published transaction body in `pre_encoded_cache_hit{type="txn"}`.
    pub fn count_published_body_use(&self) {
        if matches!(self.body_write, IndexedBodyWrite::PublishedBody(_))
            && self.update_body_variant == UpdateBodyVariant::TransactionUpdate
        {
            count_transaction_info_use(TransactionInfoUse::Published);
        }
    }
}

impl IndexedUpdateRef {
    /// A routed member's update; its mask selects names of its kind and never the fallback.
    pub const fn new(
        message_entry: IndexedMessageEntry,
        member_filter: Arc<Filter>,
        matched_update: MatchedUpdate,
    ) -> Self {
        Self {
            message_entry,
            member_filter,
            matched_update,
        }
    }

    pub const fn matched_update(&self) -> &MatchedUpdate {
        &self.matched_update
    }

    pub const fn member_filter(&self) -> &Arc<Filter> {
        &self.member_filter
    }

    /// The kind of names the member's mask selects from.
    pub const fn name_kind(&self) -> FilterNameKind {
        match &self.message_entry {
            IndexedMessageEntry::Account(_) => FilterNameKind::Accounts,
            IndexedMessageEntry::Slot(_) => FilterNameKind::Slots,
            IndexedMessageEntry::Transaction(_) => match self.matched_update.update_body_variant {
                UpdateBodyVariant::TransactionStatusUpdate => FilterNameKind::TransactionsStatus,
                _ => FilterNameKind::Transactions,
            },
            IndexedMessageEntry::Entry(_) | IndexedMessageEntry::EntryUpdateParent(_) => {
                FilterNameKind::Entries
            }
            IndexedMessageEntry::BlockMeta(_) => FilterNameKind::BlocksMeta,
            IndexedMessageEntry::BlockFooter(_) => FilterNameKind::BlockFooter,
            IndexedMessageEntry::Block { .. } => FilterNameKind::Blocks,
        }
    }

    /// The member's message content, resolved once for one write that never waits. A Block member
    /// claims its shape's empty slot only within `block_shape_budget`; `None` never claims.
    pub fn resolve_frame_content(
        &self,
        block_shape_budget: Option<&Arc<BlockShapeBudget>>,
    ) -> ResolvedFrameContent<'_> {
        let shape_policy =
            block_shape_budget.map_or(ShapeCellPolicy::NeverClaims, ShapeCellPolicy::ClaimsWithin);
        match self.resolve_with_policy(shape_policy) {
            Ok(resolved_content) => resolved_content,
            Err(_) => unreachable!("a write that never waits gets no shape wait"),
        }
    }

    /// The plugin body's resolve of one write: the content, or the shape slot a confirmed or
    /// finalized Block member waits on while another writer builds it. Processed never waits.
    pub fn resolve_for_write(
        &self,
        shape_settings: &BlockShapeWriteSettings,
    ) -> Result<ResolvedFrameContent<'_>, ShapeWait> {
        let shape_budget = &shape_settings.shape_budget;
        let shape_policy = match self.member_filter.get_commitment_level() {
            CommitmentLevel::Processed => ShapeCellPolicy::ClaimsWithin(shape_budget),
            CommitmentLevel::Confirmed | CommitmentLevel::Finalized => {
                ShapeCellPolicy::ClaimsOrWaitsWithin(shape_budget)
            }
        };
        self.resolve_with_policy(shape_policy)
    }

    fn resolve_with_policy(
        &self,
        shape_policy: ShapeCellPolicy<'_>,
    ) -> Result<ResolvedFrameContent<'_>, ShapeWait> {
        Ok(ResolvedFrameContent {
            name_selection: self.name_selection(),
            body_write: self.body_write(shape_policy)?,
            update_body_variant: self.matched_update.update_body_variant,
        })
    }

    /// The shape table of a Block member's block.
    pub fn block_shapes(&self) -> Option<&BlockShapeTable> {
        match &self.message_entry {
            IndexedMessageEntry::Block { message_block, .. } => Some(&message_block.block_shapes),
            _ => None,
        }
    }

    /// Whether the entry is a Block member's, whose length only a walk or its shape's body tells.
    pub const fn is_block_shape_entry(&self) -> bool {
        matches!(self.message_entry, IndexedMessageEntry::Block { .. })
    }

    fn name_selection(&self) -> FilterNameSelection<'_> {
        self.member_filter
            .filter_generation_names()
            .kind_names(self.name_kind())
            .name_selection(self.matched_update.filter_name_mask)
            .expect("a routed entry's mask selects names of its kind")
    }

    fn body_write(
        &self,
        shape_policy: ShapeCellPolicy<'_>,
    ) -> Result<IndexedBodyWrite<'_>, ShapeWait> {
        let update_body_variant = self.matched_update.update_body_variant;
        Ok(match &self.message_entry {
            IndexedMessageEntry::Account(message_account) => match update_body_variant {
                UpdateBodyVariant::SlicedAccount(_) => IndexedBodyWrite::SlicedAccountBody(
                    message_account.account_body(),
                    self.member_filter.accounts_data_slice().as_ref(),
                ),
                _ => IndexedBodyWrite::SharedBody(message_account.account_body()),
            },
            IndexedMessageEntry::Slot(message_slot) => {
                IndexedBodyWrite::SharedBody(message_slot.slot_body())
            }
            IndexedMessageEntry::Transaction(message_transaction) => match update_body_variant {
                UpdateBodyVariant::TransactionStatusUpdate => {
                    message_transaction.transaction_status_body().map_or(
                        IndexedBodyWrite::PrivateTransactionStatus(message_transaction),
                        IndexedBodyWrite::PublishedBody,
                    )
                }
                _ => message_transaction
                    .transaction_update_body(&StandardTransactionBodyWriter)
                    .map_or(
                        IndexedBodyWrite::PrivateTransaction(message_transaction),
                        IndexedBodyWrite::PublishedBody,
                    ),
            },
            IndexedMessageEntry::Entry(message_entry) => {
                IndexedBodyWrite::SharedBody(message_entry.entry_body())
            }
            IndexedMessageEntry::BlockMeta(message_block_meta) => {
                IndexedBodyWrite::SharedBody(message_block_meta.block_meta_body())
            }
            IndexedMessageEntry::BlockFooter(message_block_footer) => {
                let footer_bodies = message_block_footer.footer_bodies();
                IndexedBodyWrite::SharedBody(match update_body_variant {
                    UpdateBodyVariant::BlockFooterWithCertificates => {
                        &footer_bodies.footer_with_certificates
                    }
                    _ => &footer_bodies.footer_without_certificates,
                })
            }
            IndexedMessageEntry::EntryUpdateParent(message_update_parent) => {
                IndexedBodyWrite::SharedBody(message_update_parent.update_parent_body())
            }
            IndexedMessageEntry::Block {
                message_block,
                block_entry_flags,
                ..
            } => return self.block_body_write(message_block, *block_entry_flags, shape_policy),
        })
    }

    /// A Block member's body: its shape's published body, the one it builds as the claimant, a
    /// wait for a claim in flight, or its own copy.
    fn block_body_write<'entry>(
        &'entry self,
        message_block: &'entry MessageBlock,
        block_entry_flags: BlockEntryFlags,
        shape_policy: ShapeCellPolicy<'_>,
    ) -> Result<IndexedBodyWrite<'entry>, ShapeWait> {
        let block_shapes = &message_block.block_shapes;
        let shape_slot_index = block_entry_flags.shape_slot_index();
        let keyed_shape = shape_slot_index
            .and_then(|shape_slot_index| block_shapes.keyed_shape(shape_slot_index));
        let member_block_shape = self.member_filter.member_block_shape(
            blocks_filter_index(self.matched_update.filter_name_mask),
            message_block,
            keyed_shape.and_then(|keyed_shape| keyed_shape.selected_items),
        );
        let private_shape_cause = match (keyed_shape, shape_slot_index) {
            (Some(keyed_shape), Some(shape_slot_index)) => {
                let shape_budget = shape_policy.shape_budget();
                match claim_or_read_shape_cell(keyed_shape.shape_cell, shape_budget) {
                    ShapeCellRead::Published(shape_body) => {
                        return Ok(IndexedBodyWrite::ShapeBody(shape_body))
                    }
                    ShapeCellRead::Claimed(shape_claim) => {
                        block_shapes.count_shape_event(BlockShapeEvent::ShapeBuild);
                        let shape_budget = shape_budget.expect("a claim is made within a budget");
                        let shape_body =
                            build_and_publish_shape_body(shape_claim, shape_budget, || {
                                encode_block_shape_body(&member_block_shape)
                            });
                        return Ok(IndexedBodyWrite::ShapeBody(
                            PublishedShapeBody::BuiltByThisWriter(shape_body),
                        ));
                    }
                    ShapeCellRead::Unavailable(PrivateShapeCause::InFlight)
                        if shape_policy.waits_for_claimant() =>
                    {
                        return Err(ShapeWait { shape_slot_index })
                    }
                    ShapeCellRead::Unavailable(private_shape_cause) => private_shape_cause,
                }
            }
            _ => PrivateShapeCause::TableFull,
        };
        Ok(IndexedBodyWrite::PrivateBlockShape {
            message_block,
            member_block_shape,
            private_shape_cause,
        })
    }

    /// The body length of a transaction kind from its fields and its recorded `TransactionInfo`
    /// length, without reading a claim cell; `None` when only the body can tell.
    fn body_len_from_fields(&self) -> Option<usize> {
        let IndexedMessageEntry::Transaction(message_transaction) = &self.message_entry else {
            return None;
        };
        match self.matched_update.update_body_variant {
            UpdateBodyVariant::TransactionStatusUpdate => {
                Some(transaction_status_body_len(message_transaction))
            }
            _ => message_transaction
                .transaction_bodies()
                .built_info_len()
                .map(|transaction_info_len| {
                    transaction_update_body_len(message_transaction, transaction_info_len)
                }),
        }
    }

    /// The entry's weight in its member's queue: what [`WeightScales::update_weight`] gives the
    /// same update. A Block entry carries the weight its routing measured with the queue's scales.
    pub fn update_weight(&self, weight_scales: &WeightScales) -> u32 {
        let filter_names_len = (self.name_selection().selected_names())
            .map(|filter_name| filter_name.len())
            .sum();
        let message_weight = match &self.message_entry {
            IndexedMessageEntry::Account(message_account) => {
                weight_scales.account_update_weight(message_account)
            }
            IndexedMessageEntry::Transaction(message_transaction) => {
                match self.matched_update.update_body_variant {
                    UpdateBodyVariant::TransactionStatusUpdate => 1,
                    _ => weight_scales.transaction_update_weight(message_transaction),
                }
            }
            IndexedMessageEntry::Entry(_) | IndexedMessageEntry::EntryUpdateParent(_) => {
                weight_scales.entry.max(1)
            }
            IndexedMessageEntry::Slot(_) => weight_scales.slot.max(1),
            IndexedMessageEntry::BlockFooter(_) => weight_scales.block_footer.max(1),
            IndexedMessageEntry::BlockMeta(_) => 1,
            IndexedMessageEntry::Block { update_weight, .. } => *update_weight,
        };
        message_weight.saturating_add(weight_scales.filter_names_len_units(filter_names_len))
    }
}

/// A Block entry that holds its member's pin on the block releases it when it drops.
impl Drop for IndexedUpdateRef {
    fn drop(&mut self) {
        if let IndexedMessageEntry::Block {
            block_entry_flags, ..
        } = &self.message_entry
        {
            if block_entry_flags.holds_block_pin() {
                self.member_filter.block_pins().unpin_block();
            }
        }
    }
}

/// The member's names, then the body its variant reads, through the frame writer's resolution.
impl prost::Message for IndexedUpdateRef {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        self.resolve_frame_content(None)
            .write_content(&mut CopyingContentSink::new(buf));
    }

    fn encoded_len(&self) -> usize {
        let names_len = self.name_selection().encoded_entries_len();
        if let Some(body_len) = self.body_len_from_fields() {
            return names_len + body_len;
        }
        let never_waiting_write = match self.body_write(ShapeCellPolicy::NeverClaims) {
            Ok(body_write) => body_write,
            Err(_) => unreachable!("a write that never claims gets no shape wait"),
        };
        names_len
            + match never_waiting_write {
                IndexedBodyWrite::SharedBody(shared_body) => shared_body.body_len(),
                IndexedBodyWrite::PublishedBody(published_body) => published_body.body_len(),
                IndexedBodyWrite::SlicedAccountBody(account_body, data_slice_ranges) => {
                    sliced_account_body_len(account_body, data_slice_ranges)
                }
                IndexedBodyWrite::PrivateTransaction(message_transaction) => {
                    let private_info_len = message_transaction
                        .transaction_bodies()
                        .built_info_len()
                        .unwrap_or_else(|| transaction_info_len(&message_transaction.transaction));
                    transaction_update_body_len(message_transaction, private_info_len)
                }
                IndexedBodyWrite::PrivateTransactionStatus(message_transaction) => {
                    transaction_status_body_len(message_transaction)
                }
                IndexedBodyWrite::ShapeBody(shape_body) => shape_body.body_len(),
                IndexedBodyWrite::PrivateBlockShape {
                    member_block_shape, ..
                } => block_shape_body_len(&member_block_shape),
            }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}
