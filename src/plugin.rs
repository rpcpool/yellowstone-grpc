use {
    crate::{
        config::Config,
        grpc::{GrpcService, Message, MessageTransaction, MessageTransactionInfo},
        prom::{self, PrometheusService},
    },
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
    },
    std::time::Duration,
    tokio::{
        runtime::Runtime,
        sync::{mpsc, oneshot},
    },
};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    grpc_channel: mpsc::UnboundedSender<Message>,
    grpc_shutdown_tx: oneshot::Sender<()>,
    prometheus: PrometheusService,
    transactions: Option<(u64, Vec<MessageTransactionInfo>)>,
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
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
            grpc_channel,
            grpc_shutdown_tx,
            prometheus,
            transactions: None,
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

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        let inner = self.inner.as_ref().expect("initialized");
        let message = Message::Account((account, slot, is_startup).into());
        let _ = inner.grpc_channel.send(message);

        Ok(())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        let inner = self.inner.as_ref().expect("initialized");
        let message = Message::Slot((slot, parent, status).into());
        let _ = inner.grpc_channel.send(message);

        prom::update_slot_status(slot, status);

        Ok(())
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        let inner = self.inner.as_mut().expect("initialized");

        let msg_tx: MessageTransaction = (transaction, slot).into();
        match &mut inner.transactions {
            Some((current_slot, transactions)) if *current_slot == slot => {
                transactions.push(msg_tx.transaction.clone());
            }
            Some((current_slot, _)) => {
                prom::block_transactions::inc_tx();
                let msg = format!(
                    "got tx from block {}, while current block is {}",
                    slot, current_slot
                );
                log::error!("{}", msg);
                return Err(GeyserPluginError::Custom(msg.into()));
            }
            None => {
                inner.transactions = Some((slot, vec![msg_tx.transaction.clone()]));
            }
        }

        let message = Message::Transaction(msg_tx);
        let _ = inner.grpc_channel.send(message);

        Ok(())
    }

    fn notify_block_metadata(
        &mut self,
        blockinfo: ReplicaBlockInfoVersions<'_>,
    ) -> PluginResult<()> {
        let inner = self.inner.as_mut().expect("initialized");

        let ReplicaBlockInfoVersions::V0_0_1(block) = &blockinfo;
        let transactions = match inner.transactions.take() {
            Some((slot, transactions)) if slot == block.slot => transactions,
            Some((slot, _)) => {
                prom::block_transactions::inc_block();
                let msg = format!(
                    "invalid transactions for block {}, found {}",
                    block.slot, slot
                );
                log::error!("{}", msg);
                return Err(GeyserPluginError::Custom(msg.into()));
            }
            None => {
                prom::block_transactions::inc_block();
                let msg = format!("no transactions for block {}", block.slot);
                log::error!("{}", msg);
                return Err(GeyserPluginError::Custom(msg.into()));
            }
        };

        let message = Message::Block((&blockinfo, transactions).into());
        let _ = inner.grpc_channel.send(message);
        let message = Message::BlockMeta((&blockinfo).into());
        let _ = inner.grpc_channel.send(message);

        Ok(())
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
