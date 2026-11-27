//! Compressor pools (arch 4.3): `solGrpcZipPrNN` threads compress big processed group messages
//! and `solGrpcZipCfNN` threads big confirmed and finalized ones, off the writers. Both default
//! to no threads, and then every claimant compresses inline.

use {
    super::{
        encoding::CompressingEncoding,
        slot::{
            compress_group_message, CompressedGroupSlots, CompressedMessage, CompressedMessageCell,
            InFlightGroupPolicy, RetainedCompressionBudget,
        },
        stream_compressor::WholeMessageCompressor,
    },
    crate::{
        body::BodySegments,
        config::ConfigGrpcDataPlaneCompression,
        extension::seams::{CompressionError, ThreadRole},
        plugin::filter::name::{FilterGenerationNames, FilterNameKind, FilterNameMask},
        telemetry::data_plane_counts::{
            CompressionOutcome, DataPlaneCount, PoolSafetyEvent, DATA_PLANE_COUNTS,
        },
        threads::PluginThreads,
    },
    arc_swap::ArcSwapOption,
    crossbeam_queue::ArrayQueue,
    std::{
        io,
        ops::Deref,
        panic::{self, AssertUnwindSafe},
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc,
        },
        thread::{self, Thread},
        time::Duration,
    },
    yellowstone_grpc_concurrency::{
        claim_cell::{ClaimCellContents, ClaimGuard},
        pool_job_state::PoolJobState,
    },
};

/// Jobs a pool's ring holds per worker: a full ring sends the claimant inline, which bounds a
/// confirmed or finalized waiter's queue.
pub const QUEUED_JOBS_PER_WORKER: usize = 4;

/// How long an idle worker parks before it checks its ring again, whatever wakes it.
pub const IDLE_PARK_BACKSTOP: Duration = Duration::from_millis(1);

/// A pool logs its first compression error and then one in this many.
pub const LOGGED_ERROR_INTERVAL: u64 = 1_000;

/// A group slot held through its body's table, so a claim on it can move to a pool thread.
pub struct OwnedGroupSlot {
    group_slots: Arc<CompressedGroupSlots>,
    group_slot_index: usize,
}

impl OwnedGroupSlot {
    /// The slot at `group_slot_index`, or `None` when the table has no such slot.
    pub fn new(group_slots: Arc<CompressedGroupSlots>, group_slot_index: usize) -> Option<Self> {
        group_slots.group_slot(group_slot_index)?;
        Some(Self {
            group_slots,
            group_slot_index,
        })
    }
}

impl Deref for OwnedGroupSlot {
    type Target = CompressedMessageCell;

    fn deref(&self) -> &CompressedMessageCell {
        self.group_slots
            .group_slot(self.group_slot_index)
            .expect("a slot that `new` found stays in its table")
    }
}

/// A claimed group's compression, as a pool thread or a taking-back writer runs it.
pub struct GroupCompressionJob {
    pub group_claim: ClaimGuard<OwnedGroupSlot>,
    pub compressing_encoding: CompressingEncoding,
    pub generation_names: Arc<FilterGenerationNames>,
    pub filter_name_kind: FilterNameKind,
    pub name_mask: FilterNameMask,
    pub body_segments: BodySegments,
    /// The claimant's budget, which every writer of the plugin charges and checks.
    pub retained_budget: Arc<RetainedCompressionBudget>,
}

/// A handed-off job, shared by its ring entry and, at processed, its claimant.
pub type PooledGroupJob = Arc<PoolJobState<GroupCompressionJob>>;

impl GroupCompressionJob {
    /// Compresses the member's whole message, publishes it for the group and wakes the slot's
    /// waiters. A cell that does not read CLAIMED is left alone, `None`; the claim's drop wakes.
    pub fn compress_and_publish(
        self,
        stream_compressor: &dyn WholeMessageCompressor,
    ) -> Result<Option<Arc<CompressedMessage>>, CompressionError> {
        let GroupCompressionJob {
            group_claim,
            compressing_encoding,
            generation_names,
            filter_name_kind,
            name_mask,
            body_segments,
            retained_budget,
        } = self;
        if !matches!(
            group_claim.claimed_cell().read_contents(),
            ClaimCellContents::Claimed
        ) {
            return Ok(None);
        }
        let compressed_message = compress_group_message(
            stream_compressor,
            compressing_encoding,
            generation_names.kind_names(filter_name_kind),
            name_mask,
            &body_segments,
            &retained_budget,
        )?;
        // The publish wakes the slot's waiters through the cell's hook.
        let published_message = group_claim.publish(compressed_message);
        Ok(Some(published_message))
    }
}

/// A ring entry. Dropped unrun, at a stop or with its ring, it starts its job only to drop it:
/// the claim fails, and the slot's waiters wake.
struct QueuedGroupJob {
    pooled_job: Option<PooledGroupJob>,
}

impl QueuedGroupJob {
    /// Fails the entry's job when no one started it or took it back; `true` when it did.
    fn fail_unrun_job(&mut self) -> bool {
        let unrun_job = self
            .pooled_job
            .take()
            .and_then(|pooled_job| pooled_job.try_start());
        let failed_a_job = unrun_job.is_some();
        // The job's claim drops with it, which fails the slot and wakes its waiters.
        drop(unrun_job);
        failed_a_job
    }
}

impl Drop for QueuedGroupJob {
    fn drop(&mut self) {
        self.fail_unrun_job();
    }
}

/// One pool's counts. A job that a stop meets between its push and the submit's second check
/// counts where it ends, but not in `queued_jobs`.
#[derive(Debug, Default)]
pub struct CompressorPoolCounters {
    /// Submits that queued their job and returned true.
    pub queued_jobs: AtomicU64,
    /// Jobs a pool thread compressed and published.
    pub pool_compressions: AtomicU64,
    /// Jobs whose claimant took them back before a pool thread or a drain started them.
    pub jobs_taken_back: AtomicU64,
    /// Jobs whose slot did not read CLAIMED when a pool thread started them.
    pub jobs_skipped_retired: AtomicU64,
    pub ring_full_submits: AtomicU64,
    pub job_panics: AtomicU64,
    pub job_compression_errors: AtomicU64,
    /// Jobs a stop or a stopping worker dropped unrun, which failed their slots.
    pub jobs_dropped_at_stop: AtomicU64,
}

/// A worker's park word, and its thread once it runs.
#[derive(Default)]
struct WorkerParkWord {
    worker_parked: AtomicBool,
    worker_thread: ArcSwapOption<Thread>,
}

/// One pool: its job ring, its workers' park words, its stop flag and its counts.
pub struct CompressorPool {
    job_ring: ArrayQueue<QueuedGroupJob>,
    worker_park_words: Box<[WorkerParkWord]>,
    stop_requested: AtomicBool,
    live_worker_count: AtomicUsize,
    body_bytes_threshold: usize,
    pool_counters: CompressorPoolCounters,
}

impl CompressorPool {
    fn new(worker_count: usize, body_bytes_threshold: usize) -> Self {
        Self {
            job_ring: ArrayQueue::new(worker_count * QUEUED_JOBS_PER_WORKER),
            worker_park_words: (0..worker_count)
                .map(|_| WorkerParkWord::default())
                .collect(),
            stop_requested: AtomicBool::new(false),
            live_worker_count: AtomicUsize::new(0),
            body_bytes_threshold,
            pool_counters: CompressorPoolCounters::default(),
        }
    }

    /// Bodies above it go to this pool: `inline_max_bytes` or `big_body_bytes`.
    pub const fn body_bytes_threshold(&self) -> usize {
        self.body_bytes_threshold
    }

    /// Queues `pooled_job`, and wakes a parked worker. `false` when the ring is full or the pool
    /// stops; then the claimant takes its job back, which fails once a stopping worker has it.
    pub fn submit(&self, pooled_job: &PooledGroupJob) -> bool {
        if self.stop_requested.load(Ordering::SeqCst) {
            return false;
        }
        let queued_job = QueuedGroupJob {
            pooled_job: Some(Arc::clone(pooled_job)),
        };
        if let Err(mut returned_job) = self.job_ring.push(queued_job) {
            // Never queued, so its drop must not fail the claim.
            returned_job.pooled_job = None;
            self.pool_counters
                .ring_full_submits
                .fetch_add(1, Ordering::Relaxed);
            DATA_PLANE_COUNTS.add(
                DataPlaneCount::PoolSafety(PoolSafetyEvent::RingFullSubmit),
                1,
            );
            return false;
        }
        if self.stop_requested.load(Ordering::SeqCst) {
            // A stop raced the push, and its last drain may be over: the claimant takes it back.
            return false;
        }
        self.pool_counters
            .queued_jobs
            .fetch_add(1, Ordering::Relaxed);
        self.wake_a_parked_worker();
        true
    }

    /// Stops the pool: no new job is queued, the queued ones fail their slots, and the workers
    /// end. A submit that raced the stop finds it after its push and takes its job back.
    pub fn stop(&self) {
        self.stop_requested.store(true, Ordering::SeqCst);
        for worker_park_word in self.worker_park_words.iter() {
            if let Some(worker_thread) = worker_park_word.worker_thread.load().as_ref() {
                worker_thread.unpark();
            }
        }
        self.drop_queued_jobs();
    }

    /// Drops every queued job, which fails its slot: what a stop and a stopping worker do.
    pub fn drop_queued_jobs(&self) {
        while let Some(mut queued_job) = self.job_ring.pop() {
            let ended_counter = if queued_job.fail_unrun_job() {
                DATA_PLANE_COUNTS.add(
                    DataPlaneCount::PoolSafety(PoolSafetyEvent::JobDroppedAtStop),
                    1,
                );
                &self.pool_counters.jobs_dropped_at_stop
            } else {
                &self.pool_counters.jobs_taken_back
            };
            ended_counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn wake_a_parked_worker(&self) {
        for worker_park_word in self.worker_park_words.iter() {
            let woke_a_worker = worker_park_word
                .worker_parked
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok();
            if woke_a_worker {
                if let Some(worker_thread) = worker_park_word.worker_thread.load().as_ref() {
                    worker_thread.unpark();
                }
                return;
            }
        }
    }

    /// One worker's loop: run queued jobs, park when the ring is empty, and at a stop drop
    /// what is queued and end.
    fn run_worker(&self, worker_index: usize, stream_compressor: &dyn WholeMessageCompressor) {
        let worker_park_word = &self.worker_park_words[worker_index];
        worker_park_word
            .worker_thread
            .store(Some(Arc::new(thread::current())));
        loop {
            if self.stop_requested.load(Ordering::SeqCst) {
                self.drop_queued_jobs();
                return;
            }
            if let Some(queued_job) = self.job_ring.pop() {
                if let Some(pooled_job) = &queued_job.pooled_job {
                    run_queued_job(pooled_job, stream_compressor, &self.pool_counters);
                }
                continue;
            }
            worker_park_word.worker_parked.store(true, Ordering::SeqCst);
            if !self.job_ring.is_empty() || self.stop_requested.load(Ordering::SeqCst) {
                worker_park_word
                    .worker_parked
                    .store(false, Ordering::SeqCst);
                continue;
            }
            thread::park_timeout(IDLE_PARK_BACKSTOP);
            worker_park_word
                .worker_parked
                .store(false, Ordering::SeqCst);
        }
    }
}

/// Runs a queued job on the calling thread unless its claimant took it back: what a pool worker
/// does with each job it takes from its ring.
pub fn run_queued_job(
    pooled_job: &PooledGroupJob,
    stream_compressor: &dyn WholeMessageCompressor,
    pool_counters: &CompressorPoolCounters,
) {
    let Some(group_job) = pooled_job.try_start() else {
        pool_counters
            .jobs_taken_back
            .fetch_add(1, Ordering::Relaxed);
        return;
    };
    let job_encoding = group_job.compressing_encoding;
    // An unwind drops the job's claim, which fails the slot and wakes its waiters.
    let job_result = panic::catch_unwind(AssertUnwindSafe(|| {
        group_job.compress_and_publish(stream_compressor)
    }));
    pooled_job.mark_done();
    match job_result {
        Ok(Ok(Some(_published_message))) => {
            pool_counters
                .pool_compressions
                .fetch_add(1, Ordering::Relaxed);
            DATA_PLANE_COUNTS.add(
                DataPlaneCount::Compression(job_encoding, CompressionOutcome::Pool),
                1,
            );
        }
        Ok(Ok(None)) => {
            pool_counters
                .jobs_skipped_retired
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(Err(compression_error)) => {
            let earlier_errors = pool_counters
                .job_compression_errors
                .fetch_add(1, Ordering::Relaxed);
            DATA_PLANE_COUNTS.add(
                DataPlaneCount::PoolSafety(PoolSafetyEvent::JobCompressionError),
                1,
            );
            if earlier_errors.is_multiple_of(LOGGED_ERROR_INTERVAL) {
                log::error!(
                    "pool compression failed, {earlier_errors} before: {compression_error}"
                );
            }
        }
        Err(_job_panic) => {
            pool_counters.job_panics.fetch_add(1, Ordering::Relaxed);
            DATA_PLANE_COUNTS.add(DataPlaneCount::PoolSafety(PoolSafetyEvent::JobPanic), 1);
        }
    }
}

/// A worker's hold on its pool's live count: the last worker to leave, by return or unwind,
/// stops the pool and drops what is queued, so no job waits for a worker that is gone.
struct LiveWorkerGuard {
    compressor_pool: Arc<CompressorPool>,
}

impl LiveWorkerGuard {
    fn new(compressor_pool: &Arc<CompressorPool>) -> Self {
        compressor_pool
            .live_worker_count
            .fetch_add(1, Ordering::SeqCst);
        Self {
            compressor_pool: Arc::clone(compressor_pool),
        }
    }
}

impl Drop for LiveWorkerGuard {
    fn drop(&mut self) {
        if self
            .compressor_pool
            .live_worker_count
            .fetch_sub(1, Ordering::SeqCst)
            == 1
        {
            self.compressor_pool
                .stop_requested
                .store(true, Ordering::SeqCst);
            self.compressor_pool.drop_queued_jobs();
        }
    }
}

/// The plugin's two pools; each is absent when its thread count is 0. Clones share the pools.
#[derive(Clone, Default)]
pub struct CompressorPools {
    processed_pool: Option<Arc<CompressorPool>>,
    confirmed_pool: Option<Arc<CompressorPool>>,
}

impl CompressorPools {
    /// The pools of `compression_config`, their rings ready and no worker started yet.
    pub fn new(compression_config: &ConfigGrpcDataPlaneCompression) -> Self {
        let new_pool = |worker_count: usize, body_bytes_threshold: usize| {
            (worker_count > 0)
                .then(|| Arc::new(CompressorPool::new(worker_count, body_bytes_threshold)))
        };
        Self {
            processed_pool: new_pool(
                compression_config.processed_pool_threads,
                compression_config.inline_max_bytes,
            ),
            confirmed_pool: new_pool(
                compression_config.confirmed_pool_threads,
                compression_config.big_body_bytes,
            ),
        }
    }

    /// Starts every pool's workers through `plugin_threads`. A failed spawn stops the pools, so
    /// the workers already started end, and returns the spawn error.
    pub fn start_workers(
        &self,
        plugin_threads: &PluginThreads,
        stream_compressor: &Arc<dyn WholeMessageCompressor>,
    ) -> io::Result<()> {
        let pool_roles = [
            (ThreadRole::ProcessedCompressionWorker, &self.processed_pool),
            (ThreadRole::ConfirmedCompressionWorker, &self.confirmed_pool),
        ];
        for (thread_role, compressor_pool) in pool_roles {
            let Some(compressor_pool) = compressor_pool else {
                continue;
            };
            for worker_index in 0..compressor_pool.worker_park_words.len() {
                let spawn_result = start_worker(
                    plugin_threads,
                    thread_role,
                    worker_index,
                    compressor_pool,
                    stream_compressor,
                );
                if let Err(spawn_error) = spawn_result {
                    self.stop();
                    return Err(spawn_error);
                }
            }
        }
        Ok(())
    }

    /// The pool for a writer of `in_flight_policy`: processed members never wait.
    pub fn pool_for(&self, in_flight_policy: InFlightGroupPolicy) -> Option<&CompressorPool> {
        match in_flight_policy {
            InFlightGroupPolicy::CompressPrivately => self.processed_pool.as_deref(),
            InFlightGroupPolicy::WaitForClaimant => self.confirmed_pool.as_deref(),
        }
    }

    /// Stops both pools (`on_unload` step 4): queued jobs fail their slots, and the workers end.
    pub fn stop(&self) {
        for compressor_pool in [&self.processed_pool, &self.confirmed_pool]
            .into_iter()
            .flatten()
        {
            compressor_pool.stop();
        }
    }
}

impl std::fmt::Debug for CompressorPools {
    fn fmt(&self, debug_formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug_formatter
            .debug_struct("CompressorPools")
            .field("processed_pool", &self.processed_pool.is_some())
            .field("confirmed_pool", &self.confirmed_pool.is_some())
            .finish()
    }
}

/// The pools' owner: its drop stops them, so no worker outlives a dropped service.
#[derive(Debug)]
pub struct CompressorPoolsOwner {
    compressor_pools: CompressorPools,
}

impl CompressorPoolsOwner {
    pub const fn new(compressor_pools: CompressorPools) -> Self {
        Self { compressor_pools }
    }

    /// The pools, for the writers and for an explicit stop.
    pub const fn compressor_pools(&self) -> &CompressorPools {
        &self.compressor_pools
    }
}

impl Drop for CompressorPoolsOwner {
    fn drop(&mut self) {
        self.compressor_pools.stop();
    }
}

fn start_worker(
    plugin_threads: &PluginThreads,
    thread_role: ThreadRole,
    worker_index: usize,
    compressor_pool: &Arc<CompressorPool>,
    stream_compressor: &Arc<dyn WholeMessageCompressor>,
) -> io::Result<()> {
    let live_worker = LiveWorkerGuard::new(compressor_pool);
    let worker_compressor = Arc::clone(stream_compressor);
    let thread_ordinal = u16::try_from(worker_index).unwrap_or(u16::MAX);
    let worker_handle =
        plugin_threads.spawn_plugin_thread(thread_role, thread_ordinal, move || {
            let live_worker = live_worker;
            live_worker
                .compressor_pool
                .run_worker(worker_index, &*worker_compressor);
        })?;
    drop(worker_handle);
    Ok(())
}
