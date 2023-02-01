use {
    crate::{
        config::Config,
        grpc::{
            GrpcService, Message, MessageBlockMeta, MessageTransaction, MessageTransactionInfo,
        },
        prom::{self, PrometheusService},
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
    startup_received: bool,
    startup_processed_received: bool,
    grpc_channel: mpsc::UnboundedSender<Message>,
    grpc_shutdown_tx: oneshot::Sender<()>,
    prometheus: PrometheusService,
    transactions: BTreeMap<u64, (Option<MessageBlockMeta>, Vec<MessageTransactionInfo>)>,
}

impl PluginInner {
    fn try_send_full_block(&mut self, slot: u64) {
        if matches!(
            self.transactions.get(&slot),
            Some((Some(block_meta), transactions)) if block_meta.executed_transaction_count as usize == transactions.len()
        ) {
            let (block_meta, transactions) = self.transactions.remove(&slot).expect("checked");
            let message = Message::Block((block_meta.expect("checked"), transactions).into());
            let _ = self.grpc_channel.send(message);
        }
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn with_inner<F>(&mut self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&mut PluginInner) -> PluginResult<()>,
    {
        // Before processed slot after end of startup message we will fail to construct full block
        let inner = self.inner.as_mut().expect("initialized");
        if inner.startup_received && inner.startup_processed_received {
            f(inner)
        } else {
            Ok(())
        }
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

        self.inner = Some(PluginInner {
            runtime,
            startup_received: false,
            startup_processed_received: false,
            grpc_channel,
            grpc_shutdown_tx,
            prometheus,
            transactions: BTreeMap::new(),
        });

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = inner.grpc_shutdown_tx.send(());
            drop(inner.grpc_channel);
            inner.prometheus.shutdown();
            inner.runtime.shutdown_timeout(Duration::from_secs(30));
        }
    }

    fn notify_end_of_startup(&mut self) -> PluginResult<()> {
        let inner = self.inner.as_mut().expect("initialized");
        inner.startup_received = true;
        Ok(())
    }

    fn update_account(
        &mut self,
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
            let _ = inner.grpc_channel.send(message);
            Ok(())
        })
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        let inner = self.inner.as_mut().expect("initialized");
        if inner.startup_received
            && !inner.startup_processed_received
            && status == SlotStatus::Processed
        {
            inner.startup_processed_received = true;
        }

        self.with_inner(|inner| {
            // Remove outdated records
            if status == SlotStatus::Rooted {
                loop {
                    match inner.transactions.keys().next().cloned() {
                        // Block was dropped, not in chain
                        Some(kslot) if kslot < slot => {
                            inner.transactions.remove(&kslot);
                        }
                        // Maybe log error
                        Some(kslot) if kslot == slot => {
                            if let Some((Some(_), vec)) = inner.transactions.remove(&kslot) {
                                prom::INVALID_FULL_BLOCKS.inc();
                                error!("{} transactions left for block {kslot}", vec.len());
                            }
                        }
                        _ => break,
                    }
                }
            }

            let message = Message::Slot((slot, parent, status).into());
            let _ = inner.grpc_channel.send(message);
            prom::update_slot_status(slot, status);

            Ok(())
        })
    }

    fn notify_transaction(
        &mut self,
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

            // Collect Transactions for full block message
            let tx = msg_tx.transaction.clone();
            inner.transactions.entry(slot).or_default().1.push(tx);
            inner.try_send_full_block(slot);

            let message = Message::Transaction(msg_tx);
            let _ = inner.grpc_channel.send(message);

            Ok(())
        })
    }

    fn notify_block_metadata(
        &mut self,
        blockinfo: ReplicaBlockInfoVersions<'_>,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let blockinfo = match blockinfo {
                ReplicaBlockInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_2(info) => info,
            };

            let block_meta: MessageBlockMeta = (blockinfo).into();

            // Save block meta for full block message
            inner.transactions.entry(block_meta.slot).or_default().0 = Some(block_meta.clone());
            inner.try_send_full_block(block_meta.slot);

            let message = Message::BlockMeta(block_meta);
            let _ = inner.grpc_channel.send(message);

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
