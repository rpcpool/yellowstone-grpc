//! What one `on_load` started, and the order in which `on_unload` or a failed load stops it.

use {
    crate::{
        compression::pool::CompressorPoolsOwner, file_watcher::FileWatcher,
        reverse_index::BankThreadHandle, telemetry::extended_metrics::ExtendedMetricsRegistration,
        threads::PluginThreads,
    },
    std::time::{Duration, Instant},
    tokio::runtime::Runtime,
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

/// How long an unload waits for the plugin's tasks, then again for its runtime to shut down.
pub const RUNTIME_SHUTDOWN_BOUND: Duration = Duration::from_secs(30);

/// Everything one `on_load` started. Dropped before [`OnLoadGuard::wait_and_join`] ran, as on a
/// failed or panicking load, it stops and joins all of it.
#[derive(Debug)]
pub struct OnLoadGuard {
    pub plugin_threads: PluginThreads,
    pub plugin_cancellation_token: CancellationToken,
    pub plugin_task_tracker: TaskTracker,
    pub file_watcher: Option<FileWatcher>,
    pub delivery_runtime: Option<Runtime>,
    pub bank_thread_handle: Option<BankThreadHandle>,
    pub compressor_pools_owner: Option<CompressorPoolsOwner>,
    /// The extended families, removed when the guard drops, after every plugin thread joined.
    pub extended_metrics_registration: Option<ExtendedMetricsRegistration>,
    has_joined: bool,
}

/// The runtime and the file watcher, lent back once the guard holds them.
#[derive(Debug, Clone, Copy)]
pub struct HeldServiceParts<'guard> {
    pub delivery_runtime: &'guard Runtime,
    pub file_watcher: &'guard FileWatcher,
    pub plugin_threads: &'guard PluginThreads,
}

impl OnLoadGuard {
    pub fn new(plugin_threads: PluginThreads) -> Self {
        Self {
            plugin_threads,
            plugin_cancellation_token: CancellationToken::new(),
            plugin_task_tracker: TaskTracker::new(),
            file_watcher: None,
            delivery_runtime: None,
            bank_thread_handle: None,
            compressor_pools_owner: None,
            extended_metrics_registration: None,
            has_joined: false,
        }
    }

    pub fn hold_runtime_and_watcher(
        &mut self,
        delivery_runtime: Runtime,
        file_watcher: FileWatcher,
    ) -> HeldServiceParts<'_> {
        HeldServiceParts {
            delivery_runtime: self.delivery_runtime.insert(delivery_runtime),
            file_watcher: self.file_watcher.insert(file_watcher),
            plugin_threads: &self.plugin_threads,
        }
    }

    /// Cancels the plugin token, closes the task tracker and drops the file watcher, which
    /// joins its thread. The plugin's senders drop next, before [`OnLoadGuard::wait_and_join`].
    pub fn cancel_and_close(&mut self) {
        let tasks_to_cancel = self.plugin_task_tracker.len();
        log::info!("shutting down plugin: {tasks_to_cancel} tasks to cancel.");
        self.plugin_cancellation_token.cancel();
        self.plugin_task_tracker.close();
        drop(self.file_watcher.take());
        // Step 4: queued compressions fail their slots, so no session waits on a pool.
        if let Some(compressor_pools_owner) = &self.compressor_pools_owner {
            compressor_pools_owner.compressor_pools().stop();
        }
    }

    /// Waits for the tasks, shuts the runtime down, each within `runtime_shutdown_bound`, then
    /// stops the bank thread and waits, with no bound, until every plugin thread has ended.
    pub fn wait_and_join(&mut self, runtime_shutdown_bound: Duration) {
        if let Some(delivery_runtime) = self.delivery_runtime.take() {
            log::info!("waiting up to {runtime_shutdown_bound:?} for plugin tasks to shut down");
            let shutdown_started_at = Instant::now();
            let tasks_wait = delivery_runtime.block_on(async {
                tokio::time::timeout(runtime_shutdown_bound, self.plugin_task_tracker.wait()).await
            });
            if tasks_wait.is_err() {
                log::warn!(
                    "{} plugin tasks still run after {runtime_shutdown_bound:?}",
                    self.plugin_task_tracker.len()
                );
            }
            delivery_runtime.shutdown_timeout(runtime_shutdown_bound);
            log::info!(
                "tokio runtime shut down in {:?}",
                shutdown_started_at.elapsed()
            );
        }
        if let Some(bank_thread_handle) = self.bank_thread_handle.take() {
            bank_thread_handle.stop_and_join();
        }
        self.plugin_threads.stop_and_join_all();
        self.has_joined = true;
    }
}

impl Drop for OnLoadGuard {
    fn drop(&mut self) {
        if !self.has_joined {
            self.cancel_and_close();
            self.wait_and_join(RUNTIME_SHUTDOWN_BOUND);
        }
    }
}
