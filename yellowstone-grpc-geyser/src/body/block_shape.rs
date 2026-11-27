//! One block's shape table: a slot per blocks-filter shape, keyed with what the shape selects,
//! holding the shape's Block body that the first writer to claim the slot builds.

use {
    super::SharedUpdateBody,
    crate::{
        compression::slot::{retire_compressed_groups, WaitedTable},
        plugin::filter::variant::NormalizedBlockShape,
        telemetry::data_plane_counts::{DataPlaneCount, DATA_PLANE_COUNTS},
    },
    std::{
        fmt,
        ops::Deref,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
    yellowstone_grpc_concurrency::{
        claim_cell::{ClaimCell, ClaimCellContents, ClaimCellHolder, ClaimGuard, ReadyValueGuard},
        keyed_slot_table::{KeyedSlotRef, KeyedSlotTable, SlotLookup},
        slot_waker_cells::SlotWakerCells,
    },
};

/// The shapes one block's table holds; a member of a ninth shape writes its body privately.
pub const BLOCK_SHAPE_SLOTS: usize = 8;

/// The waiters a shape's cell parks at once; a waiter past them checks the cell again at its
/// backstop.
pub const SHAPE_SLOT_WAKER_CELLS: usize = 16;

pub type ShapeSlotWakerCells = SlotWakerCells<SHAPE_SLOT_WAKER_CELLS>;

/// What a plugin body's writes need for Block shapes: the budget they claim within, and the
/// backstop of a waiter that finds every waker cell taken.
#[derive(Debug, Clone)]
pub struct BlockShapeWriteSettings {
    pub shape_budget: Arc<BlockShapeBudget>,
    pub waiter_backstop: Duration,
}

/// What one blocks filter selects from one block, in the units that weights count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockSelectionTotals {
    pub transaction_count: usize,
    /// The selected transactions' recorded `TransactionInfo` lengths, summed.
    pub transaction_info_bytes: usize,
    pub account_count: usize,
    /// The selected accounts' unsliced data lengths, summed.
    pub account_data_bytes: usize,
}

impl BlockSelectionTotals {
    /// The totals of a shape without an account selection: every item of the kinds it keeps.
    pub const fn kept_kinds_of(
        whole_block_totals: Self,
        keeps_transactions: bool,
        keeps_accounts: bool,
    ) -> Self {
        let mut kept_totals = Self {
            transaction_count: 0,
            transaction_info_bytes: 0,
            account_count: 0,
            account_data_bytes: 0,
        };
        if keeps_transactions {
            kept_totals.transaction_count = whole_block_totals.transaction_count;
            kept_totals.transaction_info_bytes = whole_block_totals.transaction_info_bytes;
        }
        if keeps_accounts {
            kept_totals.account_count = whole_block_totals.account_count;
            kept_totals.account_data_bytes = whole_block_totals.account_data_bytes;
        }
        kept_totals
    }
}

/// Which of one block's transactions and accounts a blocks filter with an account selection
/// keeps: one bit per item, transactions first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedBlockItems {
    selection_words: Box<[u64]>,
    account_bit_offset: usize,
}

impl SelectedBlockItems {
    /// No item selected yet, for a block of `transaction_count` transactions and `account_count`
    /// accounts.
    pub fn new(transaction_count: usize, account_count: usize) -> Self {
        let selection_bits = transaction_count + account_count;
        Self {
            selection_words: vec![0; selection_bits.div_ceil(64)].into_boxed_slice(),
            account_bit_offset: transaction_count,
        }
    }

    pub fn select_transaction(&mut self, transaction_index: usize) {
        self.select_bit(transaction_index);
    }

    pub fn select_account(&mut self, account_index: usize) {
        self.select_bit(self.account_bit_offset + account_index);
    }

    pub fn selects_transaction(&self, transaction_index: usize) -> bool {
        self.selects_bit(transaction_index)
    }

    pub fn selects_account(&self, account_index: usize) -> bool {
        self.selects_bit(self.account_bit_offset + account_index)
    }

    fn select_bit(&mut self, bit_index: usize) {
        self.selection_words[bit_index / 64] |= 1 << (bit_index % 64);
    }

    fn selects_bit(&self, bit_index: usize) -> bool {
        self.selection_words[bit_index / 64] & (1 << (bit_index % 64)) != 0
    }
}

/// What one member measured of its shape: the totals, and the items when its filter selects
/// by account keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasuredSelection {
    pub selection_totals: BlockSelectionTotals,
    pub selected_items: Option<SelectedBlockItems>,
}

/// A keyed slot's key: the normalized blocks filter, and what it selects from the block.
pub struct BlockShapeKey {
    normalized_shape: NormalizedBlockShape,
    selection_totals: BlockSelectionTotals,
    selected_items: Option<SelectedBlockItems>,
}

/// A keyed shape: its body cell, and the items it selects when it selects by account keys.
#[derive(Clone, Copy)]
pub struct KeyedShape<'table> {
    pub shape_cell: &'table BlockShapeCell,
    pub selected_items: Option<&'table SelectedBlockItems>,
}

/// The bytes of every published shape body of one plugin, against
/// `compression.block_shape_budget_bytes`.
#[derive(Debug)]
pub struct BlockShapeBudget {
    retained_bytes: AtomicUsize,
    budget_bytes: usize,
}

impl BlockShapeBudget {
    pub const fn new(budget_bytes: usize) -> Self {
        Self {
            retained_bytes: AtomicUsize::new(0),
            budget_bytes,
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Relaxed)
    }

    fn has_room(&self) -> bool {
        self.retained_bytes() < self.budget_bytes
    }
}

/// A shape's published Block body, charged to the plugin's shape budget until it drops.
pub struct RetainedShapeBody {
    shape_body: SharedUpdateBody,
    shape_budget: Arc<BlockShapeBudget>,
}

impl RetainedShapeBody {
    pub fn new_retained(
        shape_body: SharedUpdateBody,
        shape_budget: &Arc<BlockShapeBudget>,
    ) -> Self {
        shape_budget
            .retained_bytes
            .fetch_add(shape_body.body_len(), Ordering::Relaxed);
        Self {
            shape_body,
            shape_budget: Arc::clone(shape_budget),
        }
    }
}

impl Deref for RetainedShapeBody {
    type Target = SharedUpdateBody;

    fn deref(&self) -> &SharedUpdateBody {
        &self.shape_body
    }
}

impl Drop for RetainedShapeBody {
    fn drop(&mut self) {
        self.shape_budget
            .retained_bytes
            .fetch_sub(self.shape_body.body_len(), Ordering::Relaxed);
    }
}

impl fmt::Debug for RetainedShapeBody {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("RetainedShapeBody")
            .field("body_len", &self.shape_body.body_len())
            .finish_non_exhaustive()
    }
}

pub type BlockShapeCell = ClaimCell<RetainedShapeBody, ShapeSlotWakerCells>;

/// A published shape body: read through its cell's guard, or the claimant's own `Arc`.
pub enum PublishedShapeBody {
    ReadThroughGuard(ReadyValueGuard<RetainedShapeBody>),
    BuiltByThisWriter(Arc<RetainedShapeBody>),
}

impl Deref for PublishedShapeBody {
    type Target = SharedUpdateBody;

    fn deref(&self) -> &SharedUpdateBody {
        match self {
            Self::ReadThroughGuard(value_guard) => value_guard,
            Self::BuiltByThisWriter(published_body) => published_body,
        }
    }
}

/// Why a Block member wrote its body itself instead of its shape's published body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateShapeCause {
    /// Every slot held another shape when the member was routed.
    TableFull,
    /// Another writer was building the shape's body.
    InFlight,
    /// The claimant failed, or the bank's shapes were retired.
    FailedOrRetired,
    /// Published shape bodies filled `compression.block_shape_budget_bytes`.
    BudgetFull,
    /// The tonic body, which never claims a shape.
    NeverClaims,
}

/// What a block's shape table counts, one counter each, and the process's striped counts too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockShapeEvent {
    /// A writer claimed a shape's slot and built its body.
    ShapeBuild,
    PrivateWrite(PrivateShapeCause),
    /// A member's routing walked the block for its selection totals.
    SelectionWalk,
    /// A member keyed its shape beside a racing member's equal key.
    RacingDuplicateShape,
    /// A member past `BLOCKS_PINNED_PER_STREAM` got the reference updates of the block.
    PinnedBlockFallback,
    /// A write's pass over one kept kind matched keys, for a member whose shape has no slot.
    KeyMatchingPass,
}

pub const BLOCK_SHAPE_EVENT_KINDS: usize = 10;

impl BlockShapeEvent {
    pub const ALL: [Self; BLOCK_SHAPE_EVENT_KINDS] = [
        Self::ShapeBuild,
        Self::PrivateWrite(PrivateShapeCause::TableFull),
        Self::PrivateWrite(PrivateShapeCause::InFlight),
        Self::PrivateWrite(PrivateShapeCause::FailedOrRetired),
        Self::PrivateWrite(PrivateShapeCause::BudgetFull),
        Self::PrivateWrite(PrivateShapeCause::NeverClaims),
        Self::SelectionWalk,
        Self::RacingDuplicateShape,
        Self::PinnedBlockFallback,
        Self::KeyMatchingPass,
    ];

    pub const fn counter_index(self) -> usize {
        match self {
            Self::ShapeBuild => 0,
            Self::PrivateWrite(PrivateShapeCause::TableFull) => 1,
            Self::PrivateWrite(PrivateShapeCause::InFlight) => 2,
            Self::PrivateWrite(PrivateShapeCause::FailedOrRetired) => 3,
            Self::PrivateWrite(PrivateShapeCause::BudgetFull) => 4,
            Self::PrivateWrite(PrivateShapeCause::NeverClaims) => 5,
            Self::SelectionWalk => 6,
            Self::RacingDuplicateShape => 7,
            Self::PinnedBlockFallback => 8,
            Self::KeyMatchingPass => 9,
        }
    }
}

/// Counters of block shape events, one per event kind.
struct BlockShapeEventCounters {
    event_counts: [AtomicU64; BLOCK_SHAPE_EVENT_KINDS],
}

impl BlockShapeEventCounters {
    const fn new() -> Self {
        Self {
            event_counts: [const { AtomicU64::new(0) }; BLOCK_SHAPE_EVENT_KINDS],
        }
    }

    fn add(&self, shape_event: BlockShapeEvent, event_count: u64) {
        self.event_counts[shape_event.counter_index()].fetch_add(event_count, Ordering::Relaxed);
    }
}

/// Where a member's shape sits in its block's table, and what the shape selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShapeSlotLookup {
    /// `None` when every slot held another shape.
    pub shape_slot_index: Option<u8>,
    pub selection_totals: BlockSelectionTotals,
}

impl ShapeSlotLookup {
    fn of_keyed_slot(keyed_slot: &KeyedSlotRef<'_, BlockShapeKey, BlockShapeCell>) -> Self {
        Self {
            shape_slot_index: Some(narrow_slot_index(keyed_slot.slot_index())),
            selection_totals: keyed_slot.with_slot_key(|shape_key| shape_key.selection_totals),
        }
    }
}

fn narrow_slot_index(slot_index: usize) -> u8 {
    u8::try_from(slot_index).expect("a shape table has at most 255 slots")
}

/// One block's shapes, a slot per blocks-filter shape, and the totals of the whole block.
pub struct BlockShapeTable {
    keyed_shapes: KeyedSlotTable<BlockShapeKey, BlockShapeCell>,
    /// Every transaction and every account of the block, which a shape without an account
    /// selection keeps whole.
    whole_block_totals: BlockSelectionTotals,
    event_counters: BlockShapeEventCounters,
}

impl BlockShapeTable {
    pub fn new(whole_block_totals: BlockSelectionTotals) -> Self {
        Self {
            keyed_shapes: KeyedSlotTable::new(BLOCK_SHAPE_SLOTS, BlockShapeCell::default),
            whole_block_totals,
            event_counters: BlockShapeEventCounters::new(),
        }
    }

    pub const fn whole_block_totals(&self) -> BlockSelectionTotals {
        self.whole_block_totals
    }

    /// The slot keyed with an equal shape, else one keyed now with what `measure_selection`
    /// measured before any slot is reserved. Racing members each measure; the first slot wins.
    pub fn find_or_key_shape(
        &self,
        normalized_shape: &NormalizedBlockShape,
        measure_selection: impl FnOnce() -> MeasuredSelection,
    ) -> ShapeSlotLookup {
        let key_tag = normalized_shape.block_shape_id().content_hash();
        let matches_shape =
            |shape_key: &BlockShapeKey| shape_key.normalized_shape.same_shape_as(normalized_shape);
        if let Some(keyed_slot) = self.keyed_shapes.find_keyed_slot(key_tag, matches_shape) {
            return ShapeSlotLookup::of_keyed_slot(&keyed_slot);
        }
        let MeasuredSelection {
            selection_totals,
            selected_items,
        } = measure_selection();
        let new_shape_key = || BlockShapeKey {
            normalized_shape: normalized_shape.clone(),
            selection_totals,
            selected_items,
        };
        match self
            .keyed_shapes
            .find_or_key_slot(key_tag, matches_shape, new_shape_key)
        {
            SlotLookup::Matched(matched_slot) => ShapeSlotLookup::of_keyed_slot(&matched_slot),
            SlotLookup::Keyed {
                keyed_slot,
                passed_reserved_slot,
            } => {
                let first_slot = passed_reserved_slot
                    .then(|| self.keyed_shapes.find_keyed_slot(key_tag, matches_shape))
                    .flatten()
                    .filter(|first_slot| first_slot.slot_index() != keyed_slot.slot_index());
                match first_slot {
                    Some(first_slot) => {
                        self.count_shape_event(BlockShapeEvent::RacingDuplicateShape);
                        ShapeSlotLookup::of_keyed_slot(&first_slot)
                    }
                    None => ShapeSlotLookup {
                        shape_slot_index: Some(narrow_slot_index(keyed_slot.slot_index())),
                        selection_totals,
                    },
                }
            }
            SlotLookup::TableFull => ShapeSlotLookup {
                shape_slot_index: None,
                selection_totals,
            },
        }
    }

    /// The shape keyed in the slot at `shape_slot_index`.
    pub fn keyed_shape(&self, shape_slot_index: u8) -> Option<KeyedShape<'_>> {
        let keyed_slot = self
            .keyed_shapes
            .keyed_slot(usize::from(shape_slot_index))?;
        Some(KeyedShape {
            shape_cell: keyed_slot.slot_value(),
            selected_items: keyed_slot.slot_key().selected_items.as_ref(),
        })
    }

    /// Retires every shape cell, so a later writer builds privately, and each published body's
    /// groups. Returns the bodies and group slots retired.
    pub fn retire_shape_bodies(&self) -> usize {
        let mut retired_count = 0;
        for shape_cell in self.keyed_shapes.slot_values() {
            if let ClaimCellContents::Ready(published_body) = shape_cell.read_contents() {
                retired_count += 1 + retire_compressed_groups(&published_body);
            }
            // Retiring a claim in flight wakes its waiters, who write privately.
            shape_cell.mark_retired();
        }
        retired_count
    }

    pub fn count_shape_event(&self, shape_event: BlockShapeEvent) {
        self.event_counters.add(shape_event, 1);
        DATA_PLANE_COUNTS.add(DataPlaneCount::BlockShape(shape_event), 1);
    }

    pub fn keyed_shape_count(&self) -> usize {
        self.keyed_shapes.keyed_slot_count()
    }
}

/// What a member found in its shape's cell.
pub enum ShapeCellRead<'table> {
    Published(PublishedShapeBody),
    /// This writer holds the claim: it builds the body, or hands the build off.
    Claimed(ClaimGuard<&'table BlockShapeCell>),
    /// No body to read, for this cause: the member writes privately, or waits on `InFlight`.
    Unavailable(PrivateShapeCause),
}

/// Reads the member's shape cell, and claims an empty one within `shape_budget`; with no budget
/// it never claims. It never waits.
pub fn claim_or_read_shape_cell<'table>(
    shape_cell: &'table BlockShapeCell,
    shape_budget: Option<&Arc<BlockShapeBudget>>,
) -> ShapeCellRead<'table> {
    let claim_attempt = match shape_budget {
        Some(shape_budget) if shape_budget.has_room() => shape_cell.try_claim(),
        _ => Err(shape_cell.read_contents()),
    };
    match claim_attempt {
        Ok(claim_guard) => ShapeCellRead::Claimed(claim_guard),
        Err(ClaimCellContents::Ready(value_guard)) => {
            ShapeCellRead::Published(PublishedShapeBody::ReadThroughGuard(value_guard))
        }
        Err(ClaimCellContents::Empty) => ShapeCellRead::Unavailable(match shape_budget {
            Some(_) => PrivateShapeCause::BudgetFull,
            None => PrivateShapeCause::NeverClaims,
        }),
        Err(ClaimCellContents::Claimed) => ShapeCellRead::Unavailable(PrivateShapeCause::InFlight),
        Err(ClaimCellContents::Failed | ClaimCellContents::Retired) => {
            ShapeCellRead::Unavailable(PrivateShapeCause::FailedOrRetired)
        }
    }
}

/// Builds the shape body under `shape_claim`, charges it to `shape_budget` and publishes it; the
/// publish wakes the slot's waiters. A cell retired during the claim keeps nothing.
pub fn build_and_publish_shape_body<CellHolder>(
    shape_claim: ClaimGuard<CellHolder>,
    shape_budget: &Arc<BlockShapeBudget>,
    build_shape_body: impl FnOnce() -> SharedUpdateBody,
) -> Arc<RetainedShapeBody>
where
    CellHolder: ClaimCellHolder<CellValue = RetainedShapeBody, EndHook = ShapeSlotWakerCells>,
{
    let retained_body = RetainedShapeBody::new_retained(build_shape_body(), shape_budget);
    shape_claim.publish(Arc::new(retained_body))
}

/// A waiting entry holds its block, so this table keeps one address while a wait names it.
impl WaitedTable for BlockShapeTable {}

/// A clone belongs to a new block: its totals, with no shape keyed.
impl Clone for BlockShapeTable {
    fn clone(&self) -> Self {
        Self::new(self.whole_block_totals)
    }
}

/// The table is a cache of the block's items, so it never makes two blocks differ.
impl PartialEq for BlockShapeTable {
    fn eq(&self, _other_table: &Self) -> bool {
        true
    }
}

impl fmt::Debug for BlockShapeTable {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("BlockShapeTable")
            .field("keyed_shape_count", &self.keyed_shape_count())
            .field("whole_block_totals", &self.whole_block_totals)
            .finish_non_exhaustive()
    }
}
