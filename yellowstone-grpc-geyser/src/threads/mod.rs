//! The threads and runtimes the plugin starts, their placement and their joins.

pub mod instance_slots;
pub mod on_load_guard;
pub mod placement;

use {
    crate::extension::seams::{SchedulingPolicy, ThreadPlacement, ThreadPlan, ThreadRole},
    std::{
        any::Any,
        fmt, io, mem,
        panic::{self, AssertUnwindSafe},
        sync::{
            atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering},
            Arc, Mutex, MutexGuard, PoisonError,
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::runtime::Runtime,
};
pub use {
    on_load_guard::{OnLoadGuard, RUNTIME_SHUTDOWN_BOUND},
    placement::ConfiguredThreadPlacement,
};

/// How many roles [`ThreadRole`] has.
pub const THREAD_ROLE_COUNT: usize = 7;

/// Every role, each at its [`thread_role_index`].
pub const ALL_THREAD_ROLES: [ThreadRole; THREAD_ROLE_COUNT] = [
    ThreadRole::DeliveryWorker,
    ThreadRole::FilterCompileWorker,
    ThreadRole::MetricsRender,
    ThreadRole::ProcessedCompressionWorker,
    ThreadRole::ConfirmedCompressionWorker,
    ThreadRole::BankRelease,
    ThreadRole::BlockMetaStorage,
];

/// How often a join that still waits logs what keeps it waiting.
pub const STUCK_THREAD_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// How long a join sleeps between two checks.
const JOIN_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// A role's thread name: fixed, or a prefix that the thread's two-digit ordinal follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleThreadName {
    pub name_prefix: &'static str,
    pub takes_ordinal: bool,
}

/// The role-to-name table. Host scheduling rules match these names, so each one is a contract.
pub const fn role_thread_name(thread_role: ThreadRole) -> RoleThreadName {
    let (name_prefix, takes_ordinal) = match thread_role {
        ThreadRole::DeliveryWorker => ("solGeyserGrpc", true),
        ThreadRole::FilterCompileWorker => ("solGrpcFilt", true),
        ThreadRole::MetricsRender => ("solGrpcMetrics", false),
        ThreadRole::ProcessedCompressionWorker => ("solGrpcZipPr", true),
        ThreadRole::ConfirmedCompressionWorker => ("solGrpcZipCf", true),
        ThreadRole::BankRelease => ("solGrpcBankDrop", false),
        ThreadRole::BlockMetaStorage => ("solGrpcBlockMet", false),
    };
    RoleThreadName {
        name_prefix,
        takes_ordinal,
    }
}

pub const fn thread_role_index(thread_role: ThreadRole) -> usize {
    match thread_role {
        ThreadRole::DeliveryWorker => 0,
        ThreadRole::FilterCompileWorker => 1,
        ThreadRole::MetricsRender => 2,
        ThreadRole::ProcessedCompressionWorker => 3,
        ThreadRole::ConfirmedCompressionWorker => 4,
        ThreadRole::BankRelease => 5,
        ThreadRole::BlockMetaStorage => 6,
    }
}

/// The name of one thread of `thread_role`; below ordinal 100 it fits the kernel's 15 bytes.
pub fn thread_name(thread_role: ThreadRole, thread_ordinal: u16) -> String {
    let role_name = role_thread_name(thread_role);
    if role_name.takes_ordinal {
        format!("{}{thread_ordinal:02}", role_name.name_prefix)
    } else {
        role_name.name_prefix.to_owned()
    }
}

/// Waits, with no bound, until a thread that has just ended leaves the process. Call it only
/// right after the end, so the kernel cannot have given its id to another thread.
pub fn wait_for_os_thread_to_leave(os_thread_id: i32, wait_started_at: Instant) {
    wait_with_reports(
        wait_started_at,
        || placement::os_thread_exists(os_thread_id),
        || format!("thread {os_thread_id} to leave the process"),
    );
}

/// The message of a caught panic, when it carries one.
pub fn panic_payload_text(panic_payload: &(dyn Any + Send)) -> &str {
    panic_payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic_payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic without a message")
}

/// Polls `is_still_waiting` until it is false, never giving up, and logs `waited_for_text`
/// every [`STUCK_THREAD_REPORT_INTERVAL`] meanwhile.
fn wait_with_reports(
    wait_started_at: Instant,
    is_still_waiting: impl Fn() -> bool,
    waited_for_text: impl Fn() -> String,
) {
    let mut next_report_at = wait_started_at + STUCK_THREAD_REPORT_INTERVAL;
    while is_still_waiting() {
        if Instant::now() >= next_report_at {
            log::warn!(
                "unload still waits after {:?} for {}",
                wait_started_at.elapsed(),
                waited_for_text()
            );
            next_report_at += STUCK_THREAD_REPORT_INTERVAL;
        }
        thread::sleep(JOIN_POLL_INTERVAL);
    }
}

/// Starts every plugin thread and tokio runtime, applies each new thread's plan on the thread
/// itself, and counts each role's live threads. A panicking placement hook only warns.
#[derive(Debug, Clone)]
pub struct PluginThreads {
    thread_registry: Arc<ThreadRegistry>,
}

struct ThreadRegistry {
    thread_placement: Arc<dyn ThreadPlacement>,
    /// The main thread's CPUs at load; empty when they could not be read.
    process_cpu_mask: Vec<usize>,
    live_thread_counts: [AtomicUsize; THREAD_ROLE_COUNT],
    placement_warning_logged: [AtomicBool; THREAD_ROLE_COUNT],
    /// The threads of handles that dropped unjoined.
    unjoined_plugin_threads: Mutex<Vec<JoinHandle<()>>>,
    /// Each thread's OS id, kept until the thread is seen gone from the process.
    started_thread_ids: Mutex<Vec<StartedThreadId>>,
}

#[derive(Debug)]
struct StartedThreadId {
    thread_role: ThreadRole,
    os_thread_id: i32,
    has_ended: bool,
}

/// One plugin thread. Dropping it unjoined leaves the join to
/// [`PluginThreads::stop_and_join_all`].
#[derive(Debug)]
pub struct PluginThreadHandle {
    join_handle: Option<JoinHandle<()>>,
    thread_registry: Arc<ThreadRegistry>,
}

/// Counts a plugin thread live from before its spawn until it drops on the thread, at the end
/// of the body or while a panic unwinds it.
struct LiveThreadGuard {
    thread_registry: Arc<ThreadRegistry>,
    thread_role: ThreadRole,
    os_thread_id: Option<i32>,
}

/// One tokio runtime's thread names, plans and counts.
struct RuntimeThreads {
    thread_registry: Arc<ThreadRegistry>,
    thread_role: ThreadRole,
    next_thread_ordinal: AtomicU16,
    /// Plans made on the spawning thread; each new thread takes its own by its ordinal.
    pending_thread_plans: Mutex<Vec<(u16, ThreadPlan)>>,
}

fn lock_ignoring_poison<GuardedValue>(
    guarded_mutex: &Mutex<GuardedValue>,
) -> MutexGuard<'_, GuardedValue> {
    guarded_mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PluginThreads {
    /// Reads the process's CPU mask now. A thread whose plan names no CPUs, or CPUs it cannot
    /// use, runs on it.
    pub fn new(thread_placement: Arc<dyn ThreadPlacement>) -> Self {
        let process_cpu_mask = placement::read_process_cpu_mask().unwrap_or_else(|mask_error| {
            log::warn!("plugin threads keep their inherited CPU mask: {mask_error}");
            Vec::new()
        });
        Self {
            thread_registry: Arc::new(ThreadRegistry {
                thread_placement,
                process_cpu_mask,
                live_thread_counts: [const { AtomicUsize::new(0) }; THREAD_ROLE_COUNT],
                placement_warning_logged: [const { AtomicBool::new(false) }; THREAD_ROLE_COUNT],
                unjoined_plugin_threads: Mutex::new(Vec::new()),
                started_thread_ids: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Spawns `thread_body` on a thread named for its role. The thread first applies its plan:
    /// SCHED_OTHER at nice 0 and the planned CPUs, unless the placement says otherwise.
    pub fn spawn_plugin_thread<ThreadBody>(
        &self,
        thread_role: ThreadRole,
        thread_ordinal: u16,
        thread_body: ThreadBody,
    ) -> io::Result<PluginThreadHandle>
    where
        ThreadBody: FnOnce() + Send + 'static,
    {
        let thread_plan = self
            .thread_registry
            .plan_thread(thread_role, thread_ordinal);
        let live_thread_guard =
            LiveThreadGuard::count_live(Arc::clone(&self.thread_registry), thread_role);
        let join_handle = thread::Builder::new()
            .name(thread_name(thread_role, thread_ordinal))
            .spawn(move || {
                let mut live_thread_guard = live_thread_guard;
                live_thread_guard.os_thread_id = live_thread_guard
                    .thread_registry
                    .start_current_thread(thread_role, thread_ordinal, &thread_plan);
                thread_body();
            })?;
        Ok(PluginThreadHandle {
            join_handle: Some(join_handle),
            thread_registry: Arc::clone(&self.thread_registry),
        })
    }

    /// A multi-thread runtime whose workers and blocking threads take `thread_role`'s names and
    /// plans, and count as its live threads from their start hook to their stop hook.
    pub fn build_tokio_runtime(
        &self,
        thread_role: ThreadRole,
        worker_thread_count: Option<usize>,
    ) -> io::Result<Runtime> {
        let runtime_threads = Arc::new(RuntimeThreads {
            thread_registry: Arc::clone(&self.thread_registry),
            thread_role,
            next_thread_ordinal: AtomicU16::new(0),
            pending_thread_plans: Mutex::new(Vec::new()),
        });
        let naming_runtime_threads = Arc::clone(&runtime_threads);
        let starting_runtime_threads = Arc::clone(&runtime_threads);
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_thread_count) = worker_thread_count {
            runtime_builder.worker_threads(worker_thread_count);
        }
        runtime_builder
            .thread_name_fn(move || naming_runtime_threads.name_next_thread())
            .on_thread_start(move || starting_runtime_threads.start_current_thread())
            .on_thread_stop(move || runtime_threads.stop_current_thread())
            .enable_all()
            .build()
    }

    /// Returns only once every thread it started has ended: live counts at 0, dropped handles
    /// joined, and on Linux each thread gone from the process. Owners stop their threads first.
    pub fn stop_and_join_all(&self) {
        let join_started_at = Instant::now();
        self.thread_registry
            .wait_for_live_counts_to_reach_zero(join_started_at);
        let unjoined_plugin_threads = mem::take(&mut *lock_ignoring_poison(
            &self.thread_registry.unjoined_plugin_threads,
        ));
        for unjoined_plugin_thread in unjoined_plugin_threads {
            // A body that panicked has ended, so its panic counts as joined.
            let _ = unjoined_plugin_thread.join();
        }
        self.thread_registry
            .wait_for_started_threads_to_leave(join_started_at);
    }
}

impl Default for PluginThreads {
    /// The placement of a config with no CPU keys: every thread on the process mask.
    fn default() -> Self {
        Self::new(Arc::new(ConfiguredThreadPlacement::default()))
    }
}

impl ThreadRegistry {
    /// Asks the placement for a thread's plan. A `place_thread` that panics warns once per role,
    /// as a placement error does, and the thread takes the default plan.
    fn plan_thread(&self, thread_role: ThreadRole, thread_ordinal: u16) -> ThreadPlan {
        panic::catch_unwind(AssertUnwindSafe(|| {
            self.thread_placement
                .place_thread(thread_role, thread_ordinal)
        }))
        .unwrap_or_else(|placement_panic| {
            self.warn_placement_once(
                thread_role,
                format_args!(
                    "place_thread panicked; it takes the default plan, SCHED_OTHER on the \
                     process CPU mask: {}",
                    panic_payload_text(placement_panic.as_ref())
                ),
            );
            ThreadPlan::default()
        })
    }

    /// Applies `thread_plan` to the calling thread, records its OS id and runs the placement's
    /// start hook. A start hook that panics warns once per role, and the thread runs on.
    fn start_current_thread(
        &self,
        thread_role: ThreadRole,
        thread_ordinal: u16,
        thread_plan: &ThreadPlan,
    ) -> Option<i32> {
        self.apply_thread_plan(thread_role, thread_plan);
        let os_thread_id = placement::current_os_thread_id();
        if let Some(os_thread_id) = os_thread_id {
            let mut started_thread_ids = lock_ignoring_poison(&self.started_thread_ids);
            started_thread_ids.retain(|started_thread_id| {
                !started_thread_id.has_ended
                    || placement::os_thread_exists(started_thread_id.os_thread_id)
            });
            started_thread_ids.push(StartedThreadId {
                thread_role,
                os_thread_id,
                has_ended: false,
            });
        }
        let start_hook_result = panic::catch_unwind(AssertUnwindSafe(|| {
            self.thread_placement
                .on_thread_start(thread_role, thread_ordinal)
        }));
        if let Err(start_hook_panic) = start_hook_result {
            self.warn_placement_once(
                thread_role,
                format_args!(
                    "on_thread_start panicked; the thread runs on with its plan: {}",
                    panic_payload_text(start_hook_panic.as_ref())
                ),
            );
        }
        os_thread_id
    }

    fn count_live_thread(&self, thread_role: ThreadRole) {
        self.live_thread_counts[thread_role_index(thread_role)].fetch_add(1, Ordering::AcqRel);
    }

    fn end_thread(&self, thread_role: ThreadRole, os_thread_id: Option<i32>) {
        if let Some(os_thread_id) = os_thread_id {
            let mut started_thread_ids = lock_ignoring_poison(&self.started_thread_ids);
            if let Some(started_thread_id) =
                started_thread_ids.iter_mut().find(|started_thread_id| {
                    started_thread_id.os_thread_id == os_thread_id && !started_thread_id.has_ended
                })
            {
                started_thread_id.has_ended = true;
            }
        }
        self.live_thread_counts[thread_role_index(thread_role)].fetch_sub(1, Ordering::AcqRel);
    }

    /// Sets the plan's policy, then its CPUs. A step that fails warns once per role, and the
    /// thread keeps SCHED_OTHER or takes the process mask.
    fn apply_thread_plan(&self, thread_role: ThreadRole, thread_plan: &ThreadPlan) {
        if let Err(policy_error) =
            placement::set_current_thread_policy(thread_plan.scheduling_policy)
        {
            self.warn_placement_once(
                thread_role,
                format_args!(
                    "{:?}: {policy_error}; it falls back to SCHED_OTHER",
                    thread_plan.scheduling_policy
                ),
            );
            if thread_plan.scheduling_policy != SchedulingPolicy::Other {
                let _ = placement::set_current_thread_policy(SchedulingPolicy::Other);
            }
        }
        let planned_cpus = thread_plan
            .allowed_cpus
            .as_deref()
            .unwrap_or(&self.process_cpu_mask);
        if planned_cpus.is_empty() && self.process_cpu_mask.is_empty() {
            return;
        }
        if let Err(mask_error) = placement::set_current_thread_cpus(planned_cpus) {
            self.warn_placement_once(
                thread_role,
                format_args!(
                    "CPUs {planned_cpus:?}: {mask_error}; it falls back to the process CPU mask"
                ),
            );
            if !self.process_cpu_mask.is_empty() {
                let _ = placement::set_current_thread_cpus(&self.process_cpu_mask);
            }
        }
    }

    /// Logs the first placement failure of each role; later ones of that role log nothing.
    fn warn_placement_once(&self, thread_role: ThreadRole, placement_failure: fmt::Arguments<'_>) {
        let warning_logged = &self.placement_warning_logged[thread_role_index(thread_role)];
        if !warning_logged.swap(true, Ordering::Relaxed) {
            log::warn!(
                "placing a {} thread failed: {placement_failure}",
                role_thread_name(thread_role).name_prefix
            );
        }
    }

    fn live_roles_text(&self) -> String {
        ALL_THREAD_ROLES
            .iter()
            .filter_map(|thread_role| {
                let live_thread_count = self.live_thread_counts[thread_role_index(*thread_role)]
                    .load(Ordering::Acquire);
                (live_thread_count > 0).then(|| format!("{thread_role:?} {live_thread_count}"))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn wait_for_live_counts_to_reach_zero(&self, join_started_at: Instant) {
        wait_with_reports(
            join_started_at,
            || {
                self.live_thread_counts
                    .iter()
                    .any(|live_thread_count| live_thread_count.load(Ordering::Acquire) > 0)
            },
            || format!("live plugin threads: {}", self.live_roles_text()),
        );
    }

    /// Waits until no recorded OS id names a thread of this process that carries its role's
    /// name. An id the kernel gave to a new thread carries another name, so it holds no wait.
    fn wait_for_started_threads_to_leave(&self, join_started_at: Instant) {
        wait_with_reports(
            join_started_at,
            || {
                let mut started_thread_ids = lock_ignoring_poison(&self.started_thread_ids);
                started_thread_ids.retain(|started_thread_id| {
                    placement::os_thread_carries_name(
                        started_thread_id.os_thread_id,
                        role_thread_name(started_thread_id.thread_role).name_prefix,
                    )
                });
                !started_thread_ids.is_empty()
            },
            || {
                let started_thread_ids = lock_ignoring_poison(&self.started_thread_ids);
                format!("plugin threads to leave the process: {started_thread_ids:?}")
            },
        );
    }
}

impl fmt::Debug for ThreadRegistry {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("ThreadRegistry")
            .field("process_cpu_mask", &self.process_cpu_mask)
            .field("live_roles", &self.live_roles_text())
            .finish_non_exhaustive()
    }
}

impl PluginThreadHandle {
    /// Waits for the thread to end. A body that panicked has ended, so its panic counts as
    /// joined.
    pub fn join_plugin_thread(mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

impl Drop for PluginThreadHandle {
    fn drop(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            lock_ignoring_poison(&self.thread_registry.unjoined_plugin_threads).push(join_handle);
        }
    }
}

impl LiveThreadGuard {
    fn count_live(thread_registry: Arc<ThreadRegistry>, thread_role: ThreadRole) -> Self {
        thread_registry.count_live_thread(thread_role);
        Self {
            thread_registry,
            thread_role,
            os_thread_id: None,
        }
    }
}

impl Drop for LiveThreadGuard {
    fn drop(&mut self) {
        self.thread_registry
            .end_thread(self.thread_role, self.os_thread_id);
    }
}

impl RuntimeThreads {
    /// Runs on the spawning thread: names the next thread and plans it there.
    fn name_next_thread(&self) -> String {
        let thread_ordinal = self.next_thread_ordinal.fetch_add(1, Ordering::Relaxed);
        let thread_plan = self
            .thread_registry
            .plan_thread(self.thread_role, thread_ordinal);
        lock_ignoring_poison(&self.pending_thread_plans).push((thread_ordinal, thread_plan));
        thread_name(self.thread_role, thread_ordinal)
    }

    /// Runs first on each new thread, which finds its ordinal, and with it its plan, by its name.
    fn start_current_thread(&self) {
        let thread_ordinal = thread::current()
            .name()
            .and_then(|current_thread_name| {
                current_thread_name.strip_prefix(role_thread_name(self.thread_role).name_prefix)
            })
            .and_then(|ordinal_text| ordinal_text.parse::<u16>().ok())
            .unwrap_or_default();
        let pending_thread_plan = {
            let mut pending_thread_plans = lock_ignoring_poison(&self.pending_thread_plans);
            pending_thread_plans
                .iter()
                .position(|(pending_ordinal, _)| *pending_ordinal == thread_ordinal)
                .map(|plan_position| pending_thread_plans.swap_remove(plan_position).1)
        };
        let thread_plan = pending_thread_plan.unwrap_or_else(|| {
            self.thread_registry
                .plan_thread(self.thread_role, thread_ordinal)
        });
        self.thread_registry
            .start_current_thread(self.thread_role, thread_ordinal, &thread_plan);
        // Counted last, since tokio skips the stop hook of a thread whose start hook unwinds.
        self.thread_registry.count_live_thread(self.thread_role);
    }

    fn stop_current_thread(&self) {
        self.thread_registry
            .end_thread(self.thread_role, placement::current_os_thread_id());
    }
}
