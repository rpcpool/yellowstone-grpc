use {
    crate::{
        config::Config,
        grpc::{
            proto::{
                subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
                SubscribeUpdateAccountInfo, SubscribeUpdateSlot, SubscribeUpdateSlotStatus,
            },
            GrpcService,
        },
        prom::{PrometheusService, SLOT_STATUS},
    },
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, Result as PluginResult,
        SlotStatus,
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
    grpc_channel: mpsc::UnboundedSender<SubscribeUpdate>,
    grpc_shutdown_tx: oneshot::Sender<()>,
    prometheus: PrometheusService,
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
        let _ = inner.grpc_channel.send(match account {
            ReplicaAccountInfoVersions::V0_0_1(info) => SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: info.pubkey.into(),
                        lamports: info.lamports,
                        owner: info.owner.into(),
                        executable: info.executable,
                        rent_epoch: info.rent_epoch,
                        data: info.data.into(),
                        write_version: info.write_version,
                    }),
                    slot,
                    is_startup,
                })),
            },
        });

        Ok(())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        let inner = self.inner.as_ref().expect("initialized");
        let _ = inner.grpc_channel.send(SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent,
                status: match status {
                    SlotStatus::Processed => SubscribeUpdateSlotStatus::Processed,
                    SlotStatus::Confirmed => SubscribeUpdateSlotStatus::Confirmed,
                    SlotStatus::Rooted => SubscribeUpdateSlotStatus::Rooted,
                } as i32,
            })),
        });

        SLOT_STATUS
            .with_label_values(&[match status {
                SlotStatus::Processed => "processed",
                SlotStatus::Confirmed => "confirmed",
                SlotStatus::Rooted => "rooted",
            }])
            .set(slot as i64);

        Ok(())
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
