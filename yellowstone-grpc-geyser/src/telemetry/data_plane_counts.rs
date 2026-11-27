//! The data plane's counts: one process-wide set spread over stripes of whole 128-byte lines. A
//! thread adds to the stripe its thread-local index names, so threads rarely share a line, and a
//! scrape sums every stripe. `reset_metrics` zeroes the whole set at each load.

use {
    crate::{
        body::block_shape::{BlockShapeEvent, BLOCK_SHAPE_EVENT_KINDS},
        compression::{CompressingEncoding, ResponseEncoding},
    },
    std::{
        cell::Cell,
        mem,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

/// The stripes of a count set: the first this many counting threads each get one of their own.
pub const COUNT_STRIPES: usize = 256;

/// One count of the data plane: the word it takes in each stripe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPlaneCount {
    /// A write of a published transaction body: `pre_encoded_cache_hit{type="txn"}`.
    PreEncodedTransactionHit,
    /// A write of a member's own transaction body: `pre_encoded_cache_miss{type="txn"}`.
    PreEncodedTransactionMiss,
    /// A Subscribe session that started, by its response encoding.
    SubscribeStreamStarted(ResponseEncoding),
    /// A Subscribe session that ended, by its response encoding.
    SubscribeStreamEnded(ResponseEncoding),
    /// One compression of a member's message by the plugin's body.
    Compression(CompressingEncoding, CompressionOutcome),
    /// An event of a compression group that is not a compression.
    CompressionGroupEvent(CompressingEncoding, CompressionGroupEvent),
    /// A claim wait that found every waker cell taken and armed its own timer.
    ClaimBackstopWait(WaitedClaim),
    /// An event of a block's shapes.
    BlockShape(BlockShapeEvent),
    /// A panic caught in a plugin callback.
    CallbackPanic(PluginCallback),
    /// An owner thread or task that ended before the unload.
    OwnerEarlyEnd,
    /// A compressor pool's safety event.
    PoolSafety(PoolSafetyEvent),
    /// Messages that entered an owner's queue, where its size gauge counted them in.
    OwnerQueueEntered(OwnerQueue),
    /// Messages that left an owner's queue, where its size gauge counted them out.
    OwnerQueueLeft(OwnerQueue),
    /// An account update that `accounts.owner_reject` dropped: `event_dropped_total{event}`.
    AccountEventDropped,
    /// Response bytes the metered bodies reported: `total_traffic_sent_bytes`.
    TrafficSentBytes,
}

/// Declares a count kind's enum with its variants in word order, `ALL` listing each variant once in
/// that order, and each variant's label value, so the three cannot drift apart.
macro_rules! count_kind_enum {
    (
        $(#[$enum_attribute:meta])*
        $enum_name:ident {
            $($(#[$variant_attribute:meta])* $variant_name:ident => $label_value:literal,)+
        }
    ) => {
        $(#[$enum_attribute])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $enum_name {
            $($(#[$variant_attribute])* $variant_name,)+
        }

        impl $enum_name {
            pub const ALL: [Self; [$($label_value),+].len()] = [$(Self::$variant_name),+];

            pub const fn label_value(self) -> &'static str {
                match self {
                    $(Self::$variant_name => $label_value,)+
                }
            }
        }
    };
}

count_kind_enum! {
    /// How one compression of a member's message came about: each compression has one outcome.
    CompressionOutcome {
        /// The claimant compressed its group's message inline, also after a full ring sent it back.
        Claimed => "claimed",
        /// A pool thread compressed the group's message.
        Pool => "pool",
        /// The claimant took its queued job back from the pool and compressed it inline.
        TakenBack => "taken_back",
        /// The stream compressed the member alone: control frames, ineligible entries, `per_stream`.
        Stream => "stream",
        /// Compressed privately: every group slot held another group.
        PrivateGroupTableFull => "private_group_table_full",
        /// Compressed privately: retained group messages were over their budget.
        PrivateBudgetFull => "private_budget_full",
        /// Compressed privately: a processed member's group was in flight.
        PrivateInFlight => "private_in_flight",
        /// Compressed privately: the group's claim failed or its message retired.
        PrivateFailedOrRetired => "private_failed_or_retired",
        /// Compressed privately: `compression.group_slots` is 0.
        PrivateUngrouped => "private_ungrouped",
    }
}

count_kind_enum! {
    /// An event of a compression group that is not a compression of its own.
    CompressionGroupEvent {
        /// A group keyed beside a racing writer's equal key, so its message is compressed again.
        DuplicateGroup => "duplicate_group",
        /// A member sent with its group's published message.
        SharedRead => "shared_read",
    }
}

count_kind_enum! {
    /// What a claim wait waits for.
    WaitedClaim {
        CompressionGroup => "compression_group",
        BlockShape => "block_shape",
    }
}

count_kind_enum! {
    /// A plugin callback that agave calls, by its method name.
    PluginCallback {
        UpdateAccountFromSnapshot => "update_account_from_snapshot",
        UpdateAccountForBank => "update_account_for_bank",
        NotifyEndOfStartup => "notify_end_of_startup",
        UpdateBankStatus => "update_bank_status",
        UpdateSlotStatus => "update_slot_status",
        NotifyTransactionForBank => "notify_transaction_for_bank",
        NotifyEntryUpdateParent => "notify_entry_update_parent",
        NotifyDeshredUpdateParent => "notify_deshred_update_parent",
        NotifyEntryForBank => "notify_entry_for_bank",
        NotifyBlockMetadataForBank => "notify_block_metadata_for_bank",
        NotifyDeshredTransaction => "notify_deshred_transaction",
        NotifyBlockFooter => "notify_block_footer",
        NotifyContactInfo => "notify_contact_info",
        NotifyContactInfoRemoved => "notify_contact_info_removed",
    }
}

count_kind_enum! {
    /// A compressor pool's safety event, read by the summary line.
    PoolSafetyEvent {
        /// A job whose compression panicked on a pool thread; its slot failed.
        JobPanic => "job_panic",
        /// A job whose codec failed on a pool thread; its slot failed.
        JobCompressionError => "job_compression_error",
        /// A queued job a stopping pool dropped, which failed its slot.
        JobDroppedAtStop => "job_dropped_at_stop",
        /// A job the full ring sent back to its claimant.
        RingFullSubmit => "ring_full_submit",
        /// A queued job its processed claimant took back after a pass.
        JobTakenBack => "job_taken_back",
    }
}

count_kind_enum! {
    /// An owner's input queue, whose size gauge is what entered it less what left it.
    OwnerQueue {
        /// geyser_loop's queue, which capture and snapshot sends fill: `message_queue_size`.
        GeyserLoop => "geyser_loop",
        /// Block reconstruction's queue, which geyser_loop fills.
        BlockReconstruction => "block_reconstruction",
    }
}

const RESPONSE_ENCODING_KINDS: usize = ResponseEncoding::ALL.len();
const COMPRESSING_ENCODING_KINDS: usize = CompressingEncoding::ALL.len();
const STREAM_STARTED_WORDS: usize = 2;
const STREAM_ENDED_WORDS: usize = STREAM_STARTED_WORDS + RESPONSE_ENCODING_KINDS;
const COMPRESSION_WORDS: usize = STREAM_ENDED_WORDS + RESPONSE_ENCODING_KINDS;
const GROUP_EVENT_WORDS: usize =
    COMPRESSION_WORDS + COMPRESSING_ENCODING_KINDS * CompressionOutcome::ALL.len();
const BACKSTOP_WAIT_WORDS: usize =
    GROUP_EVENT_WORDS + COMPRESSING_ENCODING_KINDS * CompressionGroupEvent::ALL.len();
const BLOCK_SHAPE_WORDS: usize = BACKSTOP_WAIT_WORDS + WaitedClaim::ALL.len();
const CALLBACK_PANIC_WORDS: usize = BLOCK_SHAPE_WORDS + BLOCK_SHAPE_EVENT_KINDS;
const OWNER_EARLY_END_WORD: usize = CALLBACK_PANIC_WORDS + PluginCallback::ALL.len();
const POOL_SAFETY_WORDS: usize = OWNER_EARLY_END_WORD + 1;
const OWNER_QUEUE_ENTERED_WORDS: usize = POOL_SAFETY_WORDS + PoolSafetyEvent::ALL.len();
const OWNER_QUEUE_LEFT_WORDS: usize = OWNER_QUEUE_ENTERED_WORDS + OwnerQueue::ALL.len();
const ACCOUNT_EVENT_DROPPED_WORD: usize = OWNER_QUEUE_LEFT_WORDS + OwnerQueue::ALL.len();
const TRAFFIC_SENT_BYTES_WORD: usize = ACCOUNT_EVENT_DROPPED_WORD + 1;

/// How many counts each stripe holds.
pub const DATA_PLANE_COUNT_KINDS: usize = TRAFFIC_SENT_BYTES_WORD + 1;

impl DataPlaneCount {
    /// The count's word in each stripe: every count has its own.
    pub const fn word_index(self) -> usize {
        match self {
            Self::PreEncodedTransactionHit => 0,
            Self::PreEncodedTransactionMiss => 1,
            Self::SubscribeStreamStarted(response_encoding) => {
                STREAM_STARTED_WORDS + response_encoding_index(response_encoding)
            }
            Self::SubscribeStreamEnded(response_encoding) => {
                STREAM_ENDED_WORDS + response_encoding_index(response_encoding)
            }
            Self::Compression(compressing_encoding, compression_outcome) => {
                COMPRESSION_WORDS
                    + compressing_encoding_index(compressing_encoding)
                        * CompressionOutcome::ALL.len()
                    + compression_outcome as usize
            }
            Self::CompressionGroupEvent(compressing_encoding, group_event) => {
                GROUP_EVENT_WORDS
                    + compressing_encoding_index(compressing_encoding)
                        * CompressionGroupEvent::ALL.len()
                    + group_event as usize
            }
            Self::ClaimBackstopWait(waited_claim) => BACKSTOP_WAIT_WORDS + waited_claim as usize,
            Self::BlockShape(shape_event) => BLOCK_SHAPE_WORDS + shape_event.counter_index(),
            Self::CallbackPanic(plugin_callback) => CALLBACK_PANIC_WORDS + plugin_callback as usize,
            Self::OwnerEarlyEnd => OWNER_EARLY_END_WORD,
            Self::PoolSafety(pool_safety_event) => POOL_SAFETY_WORDS + pool_safety_event as usize,
            Self::OwnerQueueEntered(owner_queue) => {
                OWNER_QUEUE_ENTERED_WORDS + owner_queue as usize
            }
            Self::OwnerQueueLeft(owner_queue) => OWNER_QUEUE_LEFT_WORDS + owner_queue as usize,
            Self::AccountEventDropped => ACCOUNT_EVENT_DROPPED_WORD,
            Self::TrafficSentBytes => TRAFFIC_SENT_BYTES_WORD,
        }
    }
}

/// Every count of every kind, in word order.
pub fn every_data_plane_count() -> impl Iterator<Item = DataPlaneCount> {
    [
        DataPlaneCount::PreEncodedTransactionHit,
        DataPlaneCount::PreEncodedTransactionMiss,
    ]
    .into_iter()
    .chain(ResponseEncoding::ALL.map(DataPlaneCount::SubscribeStreamStarted))
    .chain(ResponseEncoding::ALL.map(DataPlaneCount::SubscribeStreamEnded))
    .chain(
        CompressingEncoding::ALL
            .into_iter()
            .flat_map(|compressing_encoding| {
                CompressionOutcome::ALL.map(|compression_outcome| {
                    DataPlaneCount::Compression(compressing_encoding, compression_outcome)
                })
            }),
    )
    .chain(
        CompressingEncoding::ALL
            .into_iter()
            .flat_map(|compressing_encoding| {
                CompressionGroupEvent::ALL.map(|group_event| {
                    DataPlaneCount::CompressionGroupEvent(compressing_encoding, group_event)
                })
            }),
    )
    .chain(WaitedClaim::ALL.map(DataPlaneCount::ClaimBackstopWait))
    .chain(BlockShapeEvent::ALL.map(DataPlaneCount::BlockShape))
    .chain(PluginCallback::ALL.map(DataPlaneCount::CallbackPanic))
    .chain([DataPlaneCount::OwnerEarlyEnd])
    .chain(PoolSafetyEvent::ALL.map(DataPlaneCount::PoolSafety))
    .chain(OwnerQueue::ALL.map(DataPlaneCount::OwnerQueueEntered))
    .chain(OwnerQueue::ALL.map(DataPlaneCount::OwnerQueueLeft))
    .chain([
        DataPlaneCount::AccountEventDropped,
        DataPlaneCount::TrafficSentBytes,
    ])
}

const fn response_encoding_index(response_encoding: ResponseEncoding) -> usize {
    match response_encoding {
        ResponseEncoding::Identity => 0,
        ResponseEncoding::Gzip => 1,
        ResponseEncoding::Zstd => 2,
    }
}

const fn compressing_encoding_index(compressing_encoding: CompressingEncoding) -> usize {
    match compressing_encoding {
        CompressingEncoding::Gzip => 0,
        CompressingEncoding::Zstd => 1,
    }
}

/// One stripe: a word per count, on whole 128-byte lines no other stripe shares.
#[repr(align(128))]
struct CountStripe {
    count_words: [AtomicU64; DATA_PLANE_COUNT_KINDS],
}

const _: () = assert!(mem::size_of::<CountStripe>().is_multiple_of(128));
const _: () = assert!(!mem::needs_drop::<CountStripe>());

impl CountStripe {
    const fn new() -> Self {
        Self {
            count_words: [const { AtomicU64::new(0) }; DATA_PLANE_COUNT_KINDS],
        }
    }
}

/// A set of the data plane's counts over [`COUNT_STRIPES`] stripes. The data path counts into
/// [`DATA_PLANE_COUNTS`]; tests build their own.
pub struct DataPlaneCounts {
    count_stripes: [CountStripe; COUNT_STRIPES],
    /// How many resets the set has had, so a reader can tell a reset from a fall.
    reset_generation: AtomicU64,
}

/// The process's counts: the data path adds to them and the metrics collectors sum them.
pub static DATA_PLANE_COUNTS: DataPlaneCounts = DataPlaneCounts::new();

/// The stripe the next thread that counts takes, modulo [`COUNT_STRIPES`].
static NEXT_COUNT_STRIPE: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// This thread's stripe, taken at its first count; past [`COUNT_STRIPES`] until then.
    static COUNT_STRIPE_INDEX: Cell<usize> = const { Cell::new(usize::MAX) };
}

impl DataPlaneCounts {
    pub const fn new() -> Self {
        Self {
            count_stripes: [const { CountStripe::new() }; COUNT_STRIPES],
            reset_generation: AtomicU64::new(0),
        }
    }

    /// How many resets the set has had.
    pub fn reset_generation(&self) -> u64 {
        self.reset_generation.load(Ordering::Acquire)
    }

    /// Adds `amount` to `data_plane_count` in the calling thread's stripe.
    #[inline]
    pub fn add(&self, data_plane_count: DataPlaneCount, amount: u64) {
        self.add_to_stripe(current_count_stripe(), data_plane_count, amount);
    }

    /// An atomic add, as threads past [`COUNT_STRIPES`] share their stripes.
    #[inline]
    fn add_to_stripe(&self, stripe_index: usize, data_plane_count: DataPlaneCount, amount: u64) {
        let count_stripe = &self.count_stripes[stripe_index];
        count_stripe.count_words[data_plane_count.word_index()]
            .fetch_add(amount, Ordering::Relaxed);
    }

    /// `data_plane_count` summed over every stripe.
    pub fn sum(&self, data_plane_count: DataPlaneCount) -> u64 {
        let word_index = data_plane_count.word_index();
        self.count_stripes
            .iter()
            .map(|count_stripe| count_stripe.count_words[word_index].load(Ordering::Relaxed))
            .fold(0, u64::wrapping_add)
    }

    /// Every count that moved, with its sum, for the unload line. A queue's entered and left
    /// halves stay out: only their difference, the queue's size, means anything.
    pub fn moved_counts(&self) -> Vec<(DataPlaneCount, u64)> {
        every_data_plane_count()
            .filter(|data_plane_count| {
                !matches!(
                    data_plane_count,
                    DataPlaneCount::OwnerQueueEntered(_) | DataPlaneCount::OwnerQueueLeft(_)
                )
            })
            .map(|data_plane_count| (data_plane_count, self.sum(data_plane_count)))
            .filter(|(_, count_sum)| *count_sum > 0)
            .collect()
    }

    /// `owner_queue`'s size: what entered less what left, read left first. A message can leave
    /// before its sender counts it in, so the size can be below 0 for a moment.
    pub fn owner_queue_size(&self, owner_queue: OwnerQueue) -> i64 {
        let left_messages = self.sum(DataPlaneCount::OwnerQueueLeft(owner_queue));
        let entered_messages = self.sum(DataPlaneCount::OwnerQueueEntered(owner_queue));
        entered_messages.wrapping_sub(left_messages) as i64
    }

    /// Zeroes every word of every stripe.
    pub fn reset(&self) {
        for count_stripe in &self.count_stripes {
            for count_word in &count_stripe.count_words {
                count_word.store(0, Ordering::Relaxed);
            }
        }
        self.reset_generation.fetch_add(1, Ordering::Release);
    }
}

impl Default for DataPlaneCounts {
    fn default() -> Self {
        Self::new()
    }
}

/// The calling thread's stripe: its cached one, or the next in turn at its first count.
#[inline]
fn current_count_stripe() -> usize {
    let cached_stripe = COUNT_STRIPE_INDEX.get();
    if cached_stripe < COUNT_STRIPES {
        return cached_stripe;
    }
    take_next_count_stripe()
}

/// How many threads took a stripe in this process; past [`COUNT_STRIPES`], threads share them.
pub fn count_stripes_taken() -> usize {
    NEXT_COUNT_STRIPE.load(Ordering::Relaxed)
}

#[cold]
fn take_next_count_stripe() -> usize {
    let taken_stripe = NEXT_COUNT_STRIPE.fetch_add(1, Ordering::Relaxed) % COUNT_STRIPES;
    COUNT_STRIPE_INDEX.set(taken_stripe);
    taken_stripe
}
