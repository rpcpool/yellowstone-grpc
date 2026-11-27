use {
    crate::{
        compression::PooledStreamCompressor,
        config::{Config, ConfigGrpcDataPlane},
        contact_info::ContactInfoNotification,
        extension::{
            builder::{PluginBuilder, PluginParts},
            profile::{DataPlaneProfile, SlotAdapters, StandardProfile},
            seams::{ExtensionError, ThreadPlacement, ThreadRole},
        },
        file_watcher::FileWatcher,
        grpc::{BuildCompression, GrpcService},
        metrics::{self, incr_account_event_dropped, PrometheusService},
        plugin::{
            capture::{
                panic_report_passes, reset_callback_panic_flags, run_callback, CaptureHandle,
            },
            message::{ContactInfoMessage, Message, MessageContactInfo, MessageContactInfoRemoved},
        },
        stream::tokio::BatchStreamUnboundedReceiver,
        telemetry::{
            data_plane_counts::{
                count_stripes_taken, PluginCallback, COUNT_STRIPES, DATA_PLANE_COUNTS,
            },
            extended_metrics::{
                ExtendedCollector, ExtendedMetricsRegistration, PROCESS_EXTENDED_METRICS_REGISTRY,
            },
        },
        threads::{
            instance_slots::{
                PluginInstanceGeneration, PluginThreadSlotTable, PLUGIN_THREAD_SLOT_COUNT,
            },
            panic_payload_text, ConfiguredThreadPlacement, OnLoadGuard, PluginThreads,
            RUNTIME_SHUTDOWN_BOUND,
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions,
        ReplicaBlockFooterInfoVersions, ReplicaBlockInfoVersions, ReplicaContactInfoVersions,
        ReplicaDeshredTransactionInfoVersions, ReplicaDeshredUpdateParentInfoVersions,
        ReplicaEntryInfoVersions, ReplicaEntryUpdateParentInfoVersions,
        ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
    },
    solana_clock::{BankId, Slot},
    std::{
        any::Any,
        fmt,
        panic::{self, AssertUnwindSafe, PanicHookInfo},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, Once, PoisonError,
        },
        time::Duration,
    },
    tokio::sync::{broadcast, mpsc},
    tokio_rustls::rustls,
    tokio_util::sync::DropGuard,
};

/// A loaded plugin's state: its runtime, and the channels its callbacks feed.
#[derive(Debug)]
pub struct PluginInner<P: DataPlaneProfile> {
    /// Cancels the plugin token when dropped, first, so the owners see it before their senders go.
    _owner_end_cancellation: DropGuard,
    snapshot_channel: Mutex<Option<crossbeam_channel::Sender<Box<Message>>>>,
    snapshot_channel_closed: AtomicBool,
    /// Builds every callback's messages before the callback sends them.
    capture_handle: CaptureHandle,
    grpc_channel: mpsc::UnboundedSender<Message>, // geyser_loop
    deshred_channel: broadcast::Sender<Message>,  // deshred_client_loop
    contact_info_channel: mpsc::UnboundedSender<ContactInfoNotification>,
    blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
    /// This instance's per-thread slots, under the generation `on_load` drew for it.
    thread_slot_table: Arc<PluginThreadSlotTable>,
    /// One adapter for each hot slot of the profile, built before the runtime starts.
    slot_adapters: SlotAdapters<P>,
    /// The runtime, the threads and the file watcher. Last, so every sender above drops first.
    on_load_guard: OnLoadGuard,
}

impl<P: DataPlaneProfile> PluginInner<P> {
    // Sends messages to the geyser_loop
    fn send_message(&self, message: Message) {
        if self.grpc_channel.send(message).is_ok() {
            metrics::message_queue_size_inc();
        }
    }

    // Sends messages to the deshred subscribers if their filter allows it.
    fn send_deshred_message(&self, message: Message) {
        let _ = self.deshred_channel.send(message);
    }

    // Sends messages to block meta storage
    fn send_blocks_meta_message(&self, message: Message) {
        if let Some(blocks_meta_tx) = &self.blocks_meta_tx {
            let _ = blocks_meta_tx.send(message);
        }
    }

    fn send_contact_info_message(&self, message: ContactInfoMessage, is_startup: bool) {
        let _ = self.contact_info_channel.send(ContactInfoNotification {
            message,
            is_startup,
        });
    }

    /// Cancels and closes first, then drops the senders that keep geyser_loop and
    /// `contact_info_loop` alive, then waits for the tasks and joins every thread.
    fn stop_and_join(self, runtime_shutdown_bound: Duration) {
        let Self {
            grpc_channel,
            contact_info_channel,
            mut on_load_guard,
            ..
        } = self;
        on_load_guard.cancel_and_close();
        drop(grpc_channel);
        drop(contact_info_channel);
        on_load_guard.wait_and_join(runtime_shutdown_bound);
    }
}

/// The geyser plugin over the adapters of profile `P`. [`PluginBuilder`] builds it.
#[derive(Debug)]
pub struct GrpcPlugin<P: DataPlaneProfile = StandardProfile> {
    plugin_parts: PluginParts,
    running_plugin: Option<PluginInner<P>>,
    /// The panic hook the load found, which the unload puts back.
    kept_panic_hook: Option<KeptPanicHook>,
}

/// A process's panic hook, as `std::panic::take_hook` returns it.
type PanicHook = Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send + 'static>;

/// The panic hook before the plugin's, shared with the plugin's hook until the unload.
struct KeptPanicHook(Arc<PanicHook>);

impl fmt::Debug for KeptPanicHook {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter.write_str("KeptPanicHook")
    }
}

/// The plugin over the standard profile, which the library's own `_create_plugin` builds.
pub type Plugin = GrpcPlugin<StandardProfile>;

impl<P: DataPlaneProfile> Default for GrpcPlugin<P> {
    fn default() -> Self {
        PluginBuilder::new().build_plugin()
    }
}

impl<P: DataPlaneProfile> GrpcPlugin<P> {
    /// [`PluginBuilder::build_plugin`] calls it with every part set.
    pub const fn from_plugin_parts(plugin_parts: PluginParts) -> Self {
        Self {
            plugin_parts,
            running_plugin: None,
            kept_panic_hook: None,
        }
    }

    /// `None` when `geyser_plugin` is of another type, a plugin over another profile included.
    pub fn from_geyser_plugin(geyser_plugin: &dyn GeyserPlugin) -> Option<&Self> {
        let any_plugin: &dyn Any = geyser_plugin;
        any_plugin.downcast_ref::<Self>()
    }

    pub const fn plugin_parts(&self) -> &PluginParts {
        &self.plugin_parts
    }

    /// The adapters `on_load` built, until `on_unload` drops them.
    pub fn slot_adapters(&self) -> Option<&SlotAdapters<P>> {
        self.running_plugin
            .as_ref()
            .map(|running_plugin| &running_plugin.slot_adapters)
    }

    /// The registry of every thread the loaded plugin started, until `on_unload`.
    pub fn plugin_threads(&self) -> Option<&PluginThreads> {
        self.running_plugin
            .as_ref()
            .map(|running_plugin| &running_plugin.on_load_guard.plugin_threads)
    }

    /// The loaded instance's per-thread slots, until `on_unload`.
    pub fn thread_slot_table(&self) -> Option<&Arc<PluginThreadSlotTable>> {
        self.running_plugin
            .as_ref()
            .map(|running_plugin| &running_plugin.thread_slot_table)
    }

    /// A handle to the loaded plugin's delivery runtime, until `on_unload`.
    pub fn delivery_runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        let delivery_runtime = self
            .running_plugin
            .as_ref()?
            .on_load_guard
            .delivery_runtime
            .as_ref()?;
        Some(delivery_runtime.handle().clone())
    }

    /// `on_unload` with another bound for the task wait and for the runtime shutdown. The wait
    /// for the plugin's threads has no bound.
    pub fn unload_with_shutdown_bound(&mut self, runtime_shutdown_bound: Duration) {
        let Some(running_plugin) = self.running_plugin.take() else {
            return;
        };
        let unload_result = panic::catch_unwind(AssertUnwindSafe(move || {
            running_plugin.stop_and_join(runtime_shutdown_bound);
            log::info!(
                "data plane counts since the load: {:?}; count stripes taken: {} of {}",
                DATA_PLANE_COUNTS.moved_counts(),
                count_stripes_taken(),
                COUNT_STRIPES
            );
            log::info!("plugin shutdown complete");
        }));
        if let Err(unload_panic) = unload_result {
            log::error!(
                "plugin unload panicked: {}",
                panic_payload_text(unload_panic.as_ref())
            );
        }
        self.restore_panic_hook();
    }

    /// The builder's placement hook, or else the placement from config.
    fn thread_placement_at_load(&self, plugin_config: &Config) -> Arc<dyn ThreadPlacement> {
        match &self.plugin_parts.thread_placement {
            Some(builder_thread_placement) => Arc::clone(builder_thread_placement),
            None => Arc::new(ConfiguredThreadPlacement::from_config(plugin_config)),
        }
    }

    /// Runs a callback's body for `plugin_callback`; a panic in it is caught and answers `Ok(())`.
    fn with_inner<F>(&self, plugin_callback: PluginCallback, f: F) -> PluginResult<()>
    where
        F: FnOnce(&PluginInner<P>) -> PluginResult<()>,
    {
        run_callback(plugin_callback, || {
            let running_plugin = self.running_plugin.as_ref().expect("initialized");
            running_plugin
                .thread_slot_table
                .current_thread_slot()
                .add_to_word(|plugin_thread_slot| &plugin_thread_slot.callback_count, 1);
            f(running_plugin)
        })
        .unwrap_or(Ok(()))
    }

    /// Installs the plugin's panic hook, which passes on every report but a capture panic's after
    /// its kind's first. Skipped while the thread panics, where `set_hook` would panic.
    fn install_panic_hook(&mut self) {
        if std::thread::panicking() || self.kept_panic_hook.is_some() {
            return;
        }
        let kept_panic_hook = Arc::new(panic::take_hook());
        let previous_panic_hook = Arc::clone(&kept_panic_hook);
        panic::set_hook(Box::new(move |panic_hook_info| {
            if panic_report_passes() {
                previous_panic_hook(panic_hook_info);
            }
        }));
        self.kept_panic_hook = Some(KeptPanicHook(kept_panic_hook));
    }

    /// Puts back the hook the load found, the same box, so reloads never chain hooks. Skipped
    /// while the thread panics. Instances that share one std must unload in reverse load order.
    fn restore_panic_hook(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let Some(KeptPanicHook(kept_panic_hook)) = self.kept_panic_hook.take() else {
            return;
        };
        // Drops the plugin's hook, and with it its share of the kept one.
        drop(panic::take_hook());
        match Arc::try_unwrap(kept_panic_hook) {
            Ok(previous_panic_hook) => panic::set_hook(previous_panic_hook),
            Err(shared_panic_hook) => panic::set_hook(Box::new(move |panic_hook_info| {
                shared_panic_hook(panic_hook_info);
            })),
        }
    }

    /// Logs the `grpc.data_plane` warnings and settings in force, then builds the profile's
    /// adapters with the same compression values: each config key over the builder's default.
    fn build_slot_adapters_at_load(
        &self,
        data_plane_config: &ConfigGrpcDataPlane,
    ) -> Result<SlotAdapters<P>, ExtensionError> {
        let build_compression_defaults = self.plugin_parts.compression_defaults;
        data_plane_config.log_at_load(
            build_compression_defaults,
            &self.plugin_parts.claimed_extension_names,
        );
        SlotAdapters::build_slot_adapters(
            &build_compression_defaults
                .with_overrides(data_plane_config.compression.compression_overrides),
        )
    }
}

impl<P: DataPlaneProfile> GeyserPlugin for GrpcPlugin<P> {
    fn name(&self) -> &'static str {
        self.plugin_parts.plugin_identity.plugin_name
    }

    /// Loads the configuration, then starts the runtime, the [`PrometheusService`] and the
    /// [`GrpcService`], and keeps the channels the callbacks feed.
    ///
    /// # Errors
    ///
    /// Fails if the config, a slot adapter, the runtime, the file watcher or a service cannot be
    /// loaded or started, and returns a panic as an error. Either way it stops what it started.
    fn on_load(&mut self, config_file: &str, is_reload: bool) -> PluginResult<()> {
        let load_result = panic::catch_unwind(AssertUnwindSafe(|| {
            self.load_plugin(config_file, is_reload)
        }))
        .unwrap_or_else(|load_panic| {
            Err(GeyserPluginError::Custom(
                format!(
                    "plugin load panicked: {}",
                    panic_payload_text(load_panic.as_ref())
                )
                .into(),
            ))
        });
        if load_result.is_ok() {
            self.install_panic_hook();
        }
        load_result
    }

    /// Stops everything `on_load` started, and returns only once none of the plugin's threads
    /// runs, so the validator can close the library.
    fn on_unload(&mut self) {
        self.unload_with_shutdown_bound(RUNTIME_SHUTDOWN_BOUND);
    }

    fn update_account_from_snapshot(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::UpdateAccountFromSnapshot, |inner| {
            let account = match account {
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_3(info) => info,
            };

            if inner.capture_handle.rejects_account_owner(account) {
                incr_account_event_dropped();
                return Ok(());
            }
            let message = inner.capture_handle.capture_account(account, slot, None);
            let snapshot_channel =
                (inner.snapshot_channel.lock()).unwrap_or_else(PoisonError::into_inner);
            if let Some(channel) = snapshot_channel.as_ref() {
                match channel.send(Box::new(message)) {
                    Ok(()) => metrics::message_queue_size_inc(),
                    Err(_) => {
                        if !inner.snapshot_channel_closed.swap(true, Ordering::Relaxed) {
                            log::error!("failed to send message to startup queue: channel closed")
                        }
                    }
                }
            }

            Ok(())
        })
    }

    fn update_account_for_bank(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        bank_id: BankId,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::UpdateAccountForBank, |inner| {
            let account = match account {
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_3(info) => info,
            };

            if inner.capture_handle.rejects_account_owner(account) {
                incr_account_event_dropped();
                return Ok(());
            }
            let message = inner
                .capture_handle
                .capture_account(account, slot, Some(bank_id));
            inner.send_message(message);
            Ok(())
        })
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyEndOfStartup, |inner| {
            let _snapshot_channel = (inner.snapshot_channel.lock())
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            Ok(())
        })
    }

    /// Called when a bank-scoped slot status is updated.
    ///
    /// `bank_id` identifies the concrete bank instance associated with this
    /// status update. This method is called for statuses tied to a particular
    /// `Bank`: `Confirmed`, `Processed`, `Rooted`, and `CreatedBank`.
    #[allow(unused_variables)]
    fn update_bank_status(
        &self,
        slot: Slot,
        parent: Option<u64>,
        status: &SlotStatus,
        bank_id: BankId,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::UpdateBankStatus, |inner| {
            let message =
                inner
                    .capture_handle
                    .capture_slot_status(slot, parent, status, Some(bank_id));

            match status {
                SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Rooted => {
                    // Processed/Confirmed/Finalized slot status updates are delivered by the block reconstruction loop once their bank seals.
                    // They still go through geyser_loop, which hands them to block reconstruction without broadcasting them,
                    // so they keep their order with the bank data, CreatedBank and Dead sent before them.
                    //
                    // They used to go straight to block reconstruction and could overtake everything still queued in geyser_loop:
                    //
                    // 1. A Dead for an older bank discarded the new bank whose status overtook it.
                    //    Dead names a slot, not a bank, so it discards every bank registered for that slot.
                    //
                    //    slot 9 ◄── slot 10
                    //               ├── b1   replay fails → Dead(10)
                    //               └── b2   repaired replacement, must be delivered
                    //
                    //    agave sends:    CreatedBank(b1) ─► Dead(10) ─► seal(b2) ─► Processed(b2)
                    //                                                                    │
                    //                           ┌────────────────────────────────────────┘
                    //                           │ direct channel, skips geyser_loop
                    //                           ▼
                    //    storage gets:   CreatedBank(b1) ─► Processed(b2) ─► Dead(10) ─► seal(b2)
                    //
                    //    Processed(b2)   slot_to_banks[10] = [b1, b2]
                    //    Dead(10)        discards every bank of slot 10: b1 and b2   ✘
                    //    seal(b2)        b2 is discarded, so its data is dropped
                    //    result          slot 10 is never delivered
                    //
                    // 2. A loser whose data arrived after its winner's Confirmed was replayed as confirmed.
                    //    Losers are only discarded among the banks already registered when the slot resolves.
                    //
                    //    slot 9 ◄── slot 10
                    //               ├── b1   froze, then dumped as a duplicate (loser)
                    //               └── b2   repaired, Confirmed (winner)
                    //
                    //    agave sends:    seal(b1) ─► seal(b2) ─► Confirmed(b2)
                    //                                                 │
                    //                   ┌─────────────────────────────┘
                    //                   │ direct channel, skips geyser_loop
                    //                   ▼
                    //    storage gets:   Confirmed(b2) ─► seal(b1) ─► seal(b2)
                    //
                    //    Confirmed(b2)   resolved[10] = b2, discards the other banks of slot 10: none yet
                    //    seal(b1)        b1 is not discarded, so it is added to replayed_slot[10]   ✘
                    //    seal(b2)        replayed_slot[10] = [b1, b2]
                    //    result          a Confirmed replay of slot 10 serves b1 and b2
                    //
                    // 3. A child's commitment never reached its parent when the status arrived before the data.
                    //    Passing a commitment up needs the child's parent slot, which is only known once its data arrives.
                    //
                    //    slot 9 ◄── slot 10 (b2) ◄── slot 11 (b3)
                    //               no own Confirmed  Confirmed
                    //
                    //    agave sends:    seal(b2) ─► seal(b3) ─► Confirmed(b3)
                    //                                                 │
                    //                   ┌─────────────────────────────┘
                    //                   │ direct channel, skips geyser_loop
                    //                   ▼
                    //    storage gets:   Confirmed(b3) ─► seal(b2) ─► seal(b3)
                    //
                    //    Confirmed(b3)   slot 11 floor = Confirmed; b3 has no buffer, so its parent is unknown
                    //                    → slot 10 is not raised, and the floor is marked as already raised
                    //    seal(b2)        slot 10 has no floor → nothing
                    //    seal(b3)        emits slot 11 Processed + Confirmed, but never walks up to slot 10   ✘
                    //    result          slot 10 is never delivered at Confirmed
                    inner.send_message(message.clone());
                }
                variant @ (SlotStatus::FirstShredReceived
                | SlotStatus::Dead(_)
                | SlotStatus::Completed) => {
                    unreachable!(
                        "variant {:?} is expected to be emitted from `update_slot_status`",
                        variant
                    )
                }
                SlotStatus::CreatedBank => {
                    // geyser_loop broadcasts it at processed, and block reconstruction needs it to
                    // seal the bank. At confirmed and finalized, block reconstruction sends it with
                    // the bank's content when the bank reaches that level, so it is not broadcast
                    // there now.
                    inner.send_message(message.clone());
                }
            }

            // Deshred subscribers need to receive all slot status updates.
            inner.send_deshred_message(message.clone());

            // Blocks meta subscribers need to receive all slot status updates.
            inner.send_blocks_meta_message(message);

            metrics::update_slot_status(status, slot);
            Ok(())
        })
    }

    /// Hands a slot status that names no bank to geyser_loop, the deshred subscribers and the block
    /// meta storage. Panics if the plugin is not loaded or `status` names a bank.
    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::UpdateSlotStatus, |inner| {
            match status {
                SlotStatus::FirstShredReceived | SlotStatus::Completed | SlotStatus::Dead(_) => {}
                variant @ (SlotStatus::Processed
                | SlotStatus::Confirmed
                | SlotStatus::Rooted
                | SlotStatus::CreatedBank) => {
                    unreachable!(
                        "variant {:?} expected to be emitted from `update_bank_status`",
                        variant
                    );
                }
            }
            let message = inner
                .capture_handle
                .capture_slot_status(slot, parent, status, None);
            inner.send_message(message.clone());

            // Deshred subscribers need to receive all slot status updates.
            inner.send_deshred_message(message.clone());

            // Blocks meta subscribers need to receive all slot status updates.
            inner.send_blocks_meta_message(message);

            metrics::update_slot_status(status, slot);
            Ok(())
        })
    }

    fn notify_transaction_for_bank(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: solana_clock::Slot,
        bank_id: BankId,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyTransactionForBank, |inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let transaction = match transaction {
                ReplicaTransactionInfoVersions::V0_0_4(info) => info,
            };

            let message = inner
                .capture_handle
                .capture_transaction(transaction, slot, bank_id);
            inner.send_message(message);

            Ok(())
        })
    }

    fn notify_entry_update_parent(
        &self,
        update_parent: ReplicaEntryUpdateParentInfoVersions,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyEntryUpdateParent, |inner| {
            let ReplicaEntryUpdateParentInfoVersions::V0_0_1(info) = update_parent;
            let message = inner.capture_handle.capture_entry_update_parent(info);
            inner.send_message(message);
            Ok(())
        })
    }

    fn notify_deshred_update_parent(
        &self,
        update_parent: ReplicaDeshredUpdateParentInfoVersions,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyDeshredUpdateParent, |inner| {
            let ReplicaDeshredUpdateParentInfoVersions::V0_0_1(info) = update_parent;
            let message = inner.capture_handle.capture_deshred_update_parent(info);
            inner.send_deshred_message(message);
            Ok(())
        })
    }

    fn notify_entry_for_bank(
        &self,
        entry: ReplicaEntryInfoVersions,
        bank_id: BankId,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyEntryForBank, |inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let entry = match entry {
                ReplicaEntryInfoVersions::V0_0_1(_entry) => {
                    unreachable!("ReplicaEntryInfoVersions::V0_0_1 is not supported")
                }
                ReplicaEntryInfoVersions::V0_0_2(entry) => entry,
            };

            let message = inner.capture_handle.capture_entry(entry, bank_id);
            inner.send_message(message);

            Ok(())
        })
    }

    fn notify_block_metadata_for_bank(
        &self,
        blockinfo: ReplicaBlockInfoVersions,
        bank_id: BankId,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyBlockMetadataForBank, |inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let blockinfo = match blockinfo {
                ReplicaBlockInfoVersions::V0_0_5(info) => info,
            };

            let message = inner.capture_handle.capture_block_meta(blockinfo, bank_id);

            // It's super important that block-meta message goes to the geyser loop message channel,
            // and not straight to block-reconstruction.
            // The reason why is during block freeze, in agave, some account update are emitted really late, but before block-meta.
            // In order to guarantee those account update are seen by the block-reconstruction state machine and all downstream customer,
            // we must send the block-meta message to the geyser loop, so that it be emitted to the block-reconstruction loop after all account update messages have been processed.
            inner.send_message(message.clone());
            inner.send_blocks_meta_message(message);

            Ok(())
        })
    }

    fn notify_deshred_transaction(
        &self,
        transaction: ReplicaDeshredTransactionInfoVersions,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyDeshredTransaction, |inner| {
            let message = inner
                .capture_handle
                .capture_deshred_transaction(transaction, slot);
            inner.send_deshred_message(message);

            Ok(())
        })
    }

    fn notify_block_footer(
        &self,
        block_footer: ReplicaBlockFooterInfoVersions,
        bank_id: BankId,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyBlockFooter, |inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let info = match block_footer {
                ReplicaBlockFooterInfoVersions::V0_0_2(info) => info,
            };

            let message = inner.capture_handle.capture_block_footer(info, bank_id);
            inner.send_message(message);

            Ok(())
        })
    }

    fn notify_contact_info(
        &self,
        info: ReplicaContactInfoVersions,
        is_startup: bool,
    ) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyContactInfo, |inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let info = match info {
                ReplicaContactInfoVersions::V0_0_1(info) => info,
            };
            let message = ContactInfoMessage::Node(Arc::new(MessageContactInfo::from_geyser(info)));
            inner.send_contact_info_message(message, is_startup);
            Ok(())
        })
    }

    fn notify_contact_info_removed(&self, pubkey: &[u8]) -> PluginResult<()> {
        self.with_inner(PluginCallback::NotifyContactInfoRemoved, |inner| {
            let message = ContactInfoMessage::Removed(Arc::new(
                MessageContactInfoRemoved::from_geyser(pubkey),
            ));
            // Removals only occur once the startup replay is done.
            inner.send_contact_info_message(message, false);
            Ok(())
        })
    }

    fn contact_info_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }

    fn deshred_transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn deshred_transaction_alt_resolution_enabled(&self) -> bool {
        true
    }

    fn block_footer_notifications_enabled(&self) -> bool {
        true
    }
}

impl<P: DataPlaneProfile> GrpcPlugin<P> {
    /// The body of `on_load`. Its [`OnLoadGuard`] stops and joins what it started if it returns
    /// an error or panics.
    fn load_plugin(&mut self, config_file: &str, is_reload: bool) -> PluginResult<()> {
        let plugin_config = Config::load_from_file(config_file)?;
        let capture_handle = CaptureHandle::new(
            plugin_config
                .grpc
                .filter_limits
                .accounts
                .owner_reject
                .clone(),
        );

        // Setup logger
        solana_logger::setup_with_default(&plugin_config.log.level);

        log::info!("loading plugin: {}", self.name());
        let slot_adapters = self
            .build_slot_adapters_at_load(&plugin_config.grpc.data_plane)
            .map_err(|extension_error| GeyserPluginError::Custom(Box::new(extension_error)))?;
        let build_version = self.plugin_parts.plugin_identity.build_version;
        let extended_metrics = plugin_config.grpc.data_plane.telemetry.extended_metrics;
        let summary_interval = plugin_config.grpc.data_plane.telemetry.summary_interval;
        let build_compression_defaults = self.plugin_parts.compression_defaults;
        let effective_compression_defaults = build_compression_defaults.with_overrides(
            plugin_config
                .grpc
                .data_plane
                .compression
                .compression_overrides,
        );
        let stream_codec = P::new_compression_codec(&effective_compression_defaults)
            .map_err(|extension_error| GeyserPluginError::Custom(Box::new(extension_error)))?;
        let build_compression = BuildCompression {
            build_compression_defaults,
            stream_compressor: Arc::new(PooledStreamCompressor::new(stream_codec)),
        };

        // Reset metrics to prevent accumulation across plugin reload cycles
        metrics::reset_metrics();
        reset_callback_panic_flags();

        // Declared before every sender below, so on an early return those drop before it stops.
        let mut on_load_guard = OnLoadGuard::new(PluginThreads::new(
            self.thread_placement_at_load(&plugin_config),
        ));
        // A fresh generation, so a thread that served an earlier instance claims a new slot.
        let thread_slot_table = Arc::new(PluginThreadSlotTable::new(
            PluginInstanceGeneration::draw_instance_generation(),
            PLUGIN_THREAD_SLOT_COUNT,
        ));
        let prometheus_cancellation_token = on_load_guard.plugin_cancellation_token.child_token();
        let grpc_cancellation_token = on_load_guard.plugin_cancellation_token.child_token();
        let owner_end_token = on_load_guard.plugin_cancellation_token.clone();
        let grpc_task_tracker = on_load_guard.plugin_task_tracker.clone();
        let delivery_runtime = on_load_guard
            .plugin_threads
            .build_tokio_runtime(
                ThreadRole::DeliveryWorker,
                plugin_config.tokio.worker_threads,
            )
            .map_err(|runtime_error| GeyserPluginError::Custom(Box::new(runtime_error)))?;
        let file_watcher = FileWatcher::new().map_err(|watcher_error| {
            GeyserPluginError::Custom(
                format!("failed to create file watcher: {watcher_error:?}").into(),
            )
        })?;
        let held_service_parts =
            on_load_guard.hold_runtime_and_watcher(delivery_runtime, file_watcher);
        let (grpc_channel_tx, grpc_channel_receiver) = mpsc::unbounded_channel();
        // Declared after the capture's sender, so a failed load cancels before the owners end.
        let _owner_end_cancellation = owner_end_token.drop_guard();
        let services_start_result = held_service_parts.delivery_runtime.block_on(async move {
            static CRYPTO_PROVIDER_INIT: Once = Once::new();

            CRYPTO_PROVIDER_INIT.call_once(|| {
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            });
            // Create prometheus service First so if it fails the plugin doesn't spawn geyser tasks unnecessarily.
            PrometheusService::spawn(
                plugin_config.prometheus,
                summary_interval,
                prometheus_cancellation_token,
                build_version,
                held_service_parts.plugin_threads,
            )
            .map_err(|prometheus_error| GeyserPluginError::Custom(Box::new(prometheus_error)))?;

            let grpc_channel_rx = BatchStreamUnboundedReceiver::new(grpc_channel_receiver);
            let grpc_service_result = GrpcService::create(
                plugin_config.grpc,
                is_reload,
                grpc_cancellation_token,
                grpc_task_tracker,
                held_service_parts.file_watcher,
                held_service_parts.plugin_threads,
                build_compression,
                tokio::runtime::Handle::current(),
                grpc_channel_rx,
            )
            .await
            .map_err(|grpc_error| GeyserPluginError::Custom(format!("{grpc_error:?}").into()))?;
            Ok::<_, GeyserPluginError>(grpc_service_result)
        });

        let grpc_service_result = match services_start_result {
            Ok(grpc_service_result) => grpc_service_result,
            Err(start_error) => {
                log::error!("failed to start plugin services: {start_error}");
                return Err(GeyserPluginError::Custom(format!("{start_error:?}").into()));
            }
        };
        on_load_guard.bank_thread_handle = Some(grpc_service_result.bank_thread_handle);
        on_load_guard.compressor_pools_owner = Some(grpc_service_result.compressor_pools_owner);
        if extended_metrics {
            let extended_collector =
                ExtendedCollector::new(&DATA_PLANE_COUNTS, grpc_service_result.retained_budgets);
            match ExtendedMetricsRegistration::register(
                &PROCESS_EXTENDED_METRICS_REGISTRY,
                extended_collector,
            ) {
                Ok(extended_registration) => {
                    on_load_guard.extended_metrics_registration = Some(extended_registration);
                }
                Err(register_error) => {
                    log::error!("extended metric families not registered: {register_error}");
                }
            }
        }

        self.running_plugin = Some(PluginInner {
            _owner_end_cancellation,
            snapshot_channel: Mutex::new(grpc_service_result.snapshot_tx),
            snapshot_channel_closed: AtomicBool::new(false),
            capture_handle,
            grpc_channel: grpc_channel_tx,
            deshred_channel: grpc_service_result.deshred_broadcast_tx,
            contact_info_channel: grpc_service_result.contact_info_tx,
            blocks_meta_tx: grpc_service_result.blocks_meta_tx,
            thread_slot_table,
            slot_adapters,
            on_load_guard,
        });

        Ok(())
    }
}

/// Exports `_create_plugin`, which the validator calls to build the plugin with
/// `$plugin_constructor()`. A library invokes it once.
#[macro_export]
macro_rules! export_geyser_plugin {
    ($plugin_constructor:path) => {
        /// # Safety
        /// The validator takes ownership of the returned plugin and drops it with `Box`.
        #[unsafe(no_mangle)]
        #[allow(improper_ctypes_definitions)]
        pub unsafe extern "C" fn _create_plugin(
        ) -> *mut dyn $crate::plugin_interface::geyser_plugin_interface::GeyserPlugin {
            let geyser_plugin: ::std::boxed::Box<
                dyn $crate::plugin_interface::geyser_plugin_interface::GeyserPlugin,
            > = ::std::boxed::Box::new($plugin_constructor());
            ::std::boxed::Box::into_raw(geyser_plugin)
        }
    };
}
