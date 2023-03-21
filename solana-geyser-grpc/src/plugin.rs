use {
    crate::{
        config::Config,
        grpc::{
            GrpcService, Message, MessageBlockMeta, MessageSlot, MessageTransaction,
            MessageTransactionInfo,
        },
        prom::{self, PrometheusService},
        proto::SubscribeUpdateSlotStatus,
    },
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
    },
    std::{collections::BTreeMap, time::Duration},
    tokio::{
        runtime::Runtime,
        sync::{mpsc, oneshot},
    },
};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    inner_channel: mpsc::UnboundedSender<Message>,
    grpc_shutdown_tx: oneshot::Sender<()>,
    prometheus: PrometheusService,
}

impl PluginInner {
    fn new(
        runtime: Runtime,
        grpc_channel: mpsc::UnboundedSender<Message>,
        grpc_shutdown_tx: oneshot::Sender<()>,
        prometheus: PrometheusService,
    ) -> Self {
        let (inner_channel_tx, inner_channel_rx) = mpsc::unbounded_channel();
        runtime.spawn(async move {
            Self::start_channel(inner_channel_rx, grpc_channel).await;
        });

        Self {
            runtime,
            inner_channel: inner_channel_tx,
            grpc_shutdown_tx,
            prometheus,
        }
    }

    async fn start_channel(
        mut inner_channel: mpsc::UnboundedReceiver<Message>,
        grpc_channel: mpsc::UnboundedSender<Message>,
    ) {
        let mut startup_received = false;
        let mut startup_processed_received = false;
        let mut transactions: BTreeMap<
            u64,
            (Option<MessageBlockMeta>, Vec<MessageTransactionInfo>),
        > = BTreeMap::new();

        loop {
            while let Some(message) = inner_channel.recv().await {
                if matches!(message, Message::EndOfStartup) {
                    startup_received = true;
                    continue;
                }

                if startup_received
                    && !startup_processed_received
                    && matches!(
                        message,
                        Message::Slot(MessageSlot {
                            status: SubscribeUpdateSlotStatus::Processed,
                            ..
                        })
                    )
                {
                    startup_processed_received = true;
                    continue;
                }

                // Before processed slot after end of startup message we will fail to construct full block
                if !(startup_received && startup_processed_received) {
                    continue;
                }

                // Collect info for building full block
                let mut try_send_full_block_slot = None;
                match &message {
                    Message::EndOfStartup => unreachable!(),
                    Message::Slot(msg_slot) => {
                        // Remove outdated records
                        if msg_slot.status == SubscribeUpdateSlotStatus::Finalized {
                            loop {
                                match transactions.keys().next().cloned() {
                                    // Block was dropped, not in chain
                                    Some(kslot) if kslot < msg_slot.slot => {
                                        transactions.remove(&kslot);
                                    }
                                    // Maybe log error
                                    Some(kslot) if kslot == msg_slot.slot => {
                                        if let Some((Some(_), vec)) = transactions.remove(&kslot) {
                                            prom::INVALID_FULL_BLOCKS.inc();
                                            error!(
                                                "{} transactions left for block {kslot}",
                                                vec.len()
                                            );
                                        }
                                    }
                                    _ => break,
                                }
                            }
                        }
                    }
                    Message::Account(_) => {}
                    Message::Transaction(msg_tx) => {
                        // Collect Transactions for full block message
                        let tx = msg_tx.transaction.clone();
                        transactions.entry(msg_tx.slot).or_default().1.push(tx);
                        try_send_full_block_slot = Some(msg_tx.slot);
                    }
                    Message::Block(_) => {}
                    Message::BlockMeta(msg_block) => {
                        // Save block meta for full block message
                        transactions.entry(msg_block.slot).or_default().0 = Some(msg_block.clone());
                        try_send_full_block_slot = Some(msg_block.slot);
                    }
                };

                // Re-send message to gRPC
                let _ = grpc_channel.send(message);

                // Try build full block message and send to gRPC
                if let Some(slot) = try_send_full_block_slot {
                    if matches!(
                        transactions.get(&slot),
                        Some((Some(block_meta), transactions)) if block_meta.executed_transaction_count as usize == transactions.len()
                    ) {
                        let (block_meta, mut transactions) =
                            transactions.remove(&slot).expect("checked");
                        transactions.sort_by(|tx1, tx2| tx1.index.cmp(&tx2.index));
                        let message =
                            Message::Block((block_meta.expect("checked"), transactions).into());
                        let _ = grpc_channel.send(message);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn with_inner<F>(&self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&PluginInner) -> PluginResult<()>,
    {
        f(self.inner.as_ref().expect("initialized"))
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        "GeyserGrpcPublic"
    }

    fn on_load(&mut self, config_file: &str) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;

        // Setup logger
        solana_logger::setup_with_default(&config.log.level);

        // Create inner
        let runtime = Runtime::new().map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;
        let (grpc_channel, grpc_shutdown_tx, prometheus) = runtime.block_on(async move {
            let (grpc_channel, grpc_shutdown_tx) = GrpcService::create(config.grpc)
                .map_err(|error| GeyserPluginError::Custom(error))?;
            let prometheus = PrometheusService::new(config.prometheus)
                .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;
            Ok::<_, GeyserPluginError>((grpc_channel, grpc_shutdown_tx, prometheus))
        })?;

        self.inner = Some(PluginInner::new(
            runtime,
            grpc_channel,
            grpc_shutdown_tx,
            prometheus,
        ));

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(inner.inner_channel);
            let _ = inner.grpc_shutdown_tx.send(());
            inner.prometheus.shutdown();
            inner.runtime.shutdown_timeout(Duration::from_secs(30));
        }
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        self.with_inner(|inner| {
            let _ = inner.inner_channel.send(Message::EndOfStartup);
            Ok(())
        })
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let account = match account {
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_2(info) => info,
            };

            let message = Message::Account((account, slot, is_startup).into());
            let _ = inner.inner_channel.send(message);
            Ok(())
        })
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let message = Message::Slot((slot, parent, status).into());
            let _ = inner.inner_channel.send(message);
            prom::update_slot_status(slot, status);

            Ok(())
        })
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let transaction = match transaction {
                ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaTransactionInfoVersions::V0_0_2(info) => info,
            };

            let msg_tx: MessageTransaction = (transaction, slot).into();
            let message = Message::Transaction(msg_tx);
            let _ = inner.inner_channel.send(message);

            Ok(())
        })
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        self.with_inner(|inner| {
            let blockinfo = match blockinfo {
                ReplicaBlockInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_2(info) => info,
            };

            let block_meta: MessageBlockMeta = (blockinfo).into();
            let message = Message::BlockMeta(block_meta);
            let _ = inner.inner_channel.send(message);

            Ok(())
        })
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
