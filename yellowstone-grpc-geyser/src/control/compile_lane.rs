//! The filter compile lane: `Filter::new` for `read_requests` on `solGrpcFiltNN` threads, so a
//! storm of subscribes leaves the delivery workers free. With no thread, compiles run inline.

use {
    crate::{
        extension::seams::ThreadRole,
        plugin::filter::{limits::FilterLimits, name::FilterNames, Filter, FilterResult},
        threads::PluginThreads,
    },
    std::{
        io,
        panic::{self, AssertUnwindSafe},
        sync::Arc,
        thread,
    },
    tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore},
    yellowstone_grpc_proto::geyser::SubscribeRequest,
};

/// How many compile jobs the lane's ring holds; a submitter waits while it is full.
pub const FILTER_COMPILE_RING_CAPACITY: usize = 1024;

/// A compile's outcome, with the request and the session's names that the job lent the lane.
pub struct CompiledFilter {
    pub subscribe_request: SubscribeRequest,
    pub filter_names: FilterNames,
    /// The compiled filter, or the payload of the panic that ended the compile.
    pub compile_result: thread::Result<FilterResult<Filter>>,
}

struct FilterCompileJob {
    subscribe_request: SubscribeRequest,
    filter_limits: Arc<FilterLimits>,
    filter_names: FilterNames,
    compiled_tx: oneshot::Sender<CompiledFilter>,
    /// The job's place in the ring, given back when a worker takes the job.
    ring_permit: OwnedSemaphorePermit,
}

/// A handle on the lane, cheap to clone. The workers end once every handle has dropped.
#[derive(Clone)]
pub struct FilterCompileLane {
    job_tx: crossbeam_channel::Sender<FilterCompileJob>,
    ring_permits: Arc<Semaphore>,
}

impl FilterCompileLane {
    /// Starts `worker_count` workers under [`ThreadRole::FilterCompileWorker`], or none: then
    /// there is no lane and compiles run inline. `PluginThreads` joins the workers at unload.
    pub fn start(plugin_threads: &PluginThreads, worker_count: usize) -> io::Result<Option<Self>> {
        if worker_count == 0 {
            return Ok(None);
        }
        let (job_tx, job_rx) = crossbeam_channel::bounded(FILTER_COMPILE_RING_CAPACITY);
        for worker_ordinal in 0..worker_count {
            let worker_job_rx = job_rx.clone();
            let thread_ordinal = u16::try_from(worker_ordinal).unwrap_or(u16::MAX);
            let worker_handle = plugin_threads.spawn_plugin_thread(
                ThreadRole::FilterCompileWorker,
                thread_ordinal,
                move || run_compile_worker(&worker_job_rx),
            )?;
            drop(worker_handle);
        }
        let ring_permits = Arc::new(Semaphore::new(FILTER_COMPILE_RING_CAPACITY));
        Ok(Some(Self {
            job_tx,
            ring_permits,
        }))
    }

    /// Compiles `subscribe_request` on a lane thread, waiting first for room in the ring.
    /// `None` once the lane has stopped.
    pub async fn compile(
        &self,
        subscribe_request: SubscribeRequest,
        filter_limits: &Arc<FilterLimits>,
        filter_names: FilterNames,
    ) -> Option<CompiledFilter> {
        let ring_permit = Arc::clone(&self.ring_permits).acquire_owned().await.ok()?;
        let (compiled_tx, compiled_rx) = oneshot::channel();
        let compile_job = FilterCompileJob {
            subscribe_request,
            filter_limits: Arc::clone(filter_limits),
            filter_names,
            compiled_tx,
            ring_permit,
        };
        self.job_tx.try_send(compile_job).ok()?;
        compiled_rx.await.ok()
    }
}

/// A worker's loop: takes jobs until every lane handle has dropped. A job whose submitter has
/// gone is skipped; a compile that panics hands its payload back, and the worker goes on.
fn run_compile_worker(job_rx: &crossbeam_channel::Receiver<FilterCompileJob>) {
    while let Ok(compile_job) = job_rx.recv() {
        run_compile_job(compile_job);
    }
}

/// One job: skipped when its submitter has gone, else compiled and its outcome sent back.
fn run_compile_job(compile_job: FilterCompileJob) {
    let FilterCompileJob {
        subscribe_request,
        filter_limits,
        mut filter_names,
        compiled_tx,
        ring_permit,
    } = compile_job;
    drop(ring_permit);
    if compiled_tx.is_closed() {
        return;
    }
    let compile_result = panic::catch_unwind(AssertUnwindSafe(|| {
        Filter::new(&subscribe_request, &filter_limits, &mut filter_names)
    }));
    let compiled_filter = CompiledFilter {
        subscribe_request,
        filter_names,
        compile_result,
    };
    let _ = compiled_tx.send(compiled_filter);
}
