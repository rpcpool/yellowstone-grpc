use {
    crate::scenarios::RunConfig,
    anyhow::{ensure, Context, Result},
    std::str::FromStr,
    tokio::net::TcpStream,
    yellowstone_block_machine::{
        event::{
            BlockFooterEvInfo, BlockMetaEvInfo, EntryEvInfo, GeyserEventAdapter, GeyserEventInfo,
            SlotStatusKind, SlotUpdateEvInfo,
        },
        stream::SimpleBlockStore,
        wrapper::BlockMachineConfig,
    },
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient},
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate, SubscribeUpdateAccount,
    },
};

pub async fn new_client(config: &RunConfig) -> Result<GeyserGrpcClient> {
    let mut builder = GeyserGrpcClient::build_from_shared(config.endpoint.clone())
        .context("endpoint should be a valid URI")?;

    if config.endpoint.starts_with("https://") {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .context("failed to configure TLS for HTTPS endpoint")?;
    }

    let builder = builder
        .x_token(config.x_token.clone())
        .context("x-token should be valid ASCII metadata if provided")?;

    let builder = builder
        .max_decoding_message_size(100_000_000)
        .initial_connection_window_size(10_000_000)
        .initial_stream_window_size(8_000_000)
        // adaptive window overrides the explicit windows above down to the h2 default
        // 65535, which deadlocks multiplexed subscribes behind haproxy while the
        // firehose stream is unconsumed (haproxy mux-h2 parks HEADERS behind conn window)
        .http2_adaptive_window(false)
        .accept_compressed(yellowstone_grpc_proto::tonic::codec::CompressionEncoding::Zstd);

    if let Some(dial) = config.dial.clone() {
        builder
            .connect_with_connector(tower::service_fn(move |_uri| {
                let dial = dial.clone();
                async move {
                    let stream = TcpStream::connect(dial).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .context("client should build from endpoint, dial and token")
    } else {
        builder
            .connect()
            .await
            .context("client should build from endpoint and token")
    }
}

/// Version reported by the target endpoint via the `GetVersion` RPC.
#[derive(Debug, Clone)]
pub struct TargetVersion {
    pub version: String,
    pub git: String,
    pub proto: String,
    pub solana: String,
    pub raw: String,
}

impl TargetVersion {
    /// Matches `version` exactly, or `git` exactly or by prefix; a leading `v` is ignored.
    pub fn assert_matches(&self, expected: &str) -> Result<()> {
        let needle = expected.trim().trim_start_matches('v');
        let version_field = self.version.trim_start_matches('v');
        let git_field = self.git.trim_start_matches('v');
        let matches = !needle.is_empty()
            && (version_field == needle || git_field == needle || git_field.starts_with(needle));
        ensure!(
            matches,
            "target version mismatch: expected '{}', target reports version='{}' git='{}'",
            expected,
            self.version,
            self.git,
        );
        Ok(())
    }
}

pub async fn fetch_target_version(config: &RunConfig) -> Result<TargetVersion> {
    let mut client = new_client(config).await?;
    let response = client
        .get_version()
        .await
        .context("GetVersion RPC should succeed")?;
    let raw = response.version;
    let parsed: serde_json::Value =
        serde_json::from_str(&raw).context("GetVersion response should be valid JSON")?;
    let field = |name: &str| {
        parsed
            .pointer(&format!("/version/{name}"))
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string()
    };
    Ok(TargetVersion {
        version: field("version"),
        git: field("git"),
        proto: field("proto"),
        solana: field("solana"),
        raw,
    })
}

const fn slot_status_kind(value: SlotStatus) -> SlotStatusKind {
    match value {
        SlotStatus::SlotFirstShredReceived => SlotStatusKind::FirstShredReceived,
        SlotStatus::SlotCompleted => SlotStatusKind::Completed,
        SlotStatus::SlotCreatedBank => SlotStatusKind::CreatedBank,
        SlotStatus::SlotDead => SlotStatusKind::Dead,
        SlotStatus::SlotProcessed => SlotStatusKind::Processed,
        SlotStatus::SlotConfirmed => SlotStatusKind::Confirmed,
        SlotStatus::SlotFinalized => SlotStatusKind::Finalized,
    }
}

/// How many accounts, transactions and entries a block rebuilt by the block machine holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockEventCounts {
    pub accounts: usize,
    pub transactions: usize,
    pub entries: usize,
}

impl BlockEventCounts {
    /// Counts the events of a rebuilt block by kind.
    ///
    /// # Arguments
    ///
    /// * `store` - The rebuilt block's events, as [`SimpleBlockStore`] keeps them.
    ///
    /// # Returns
    ///
    /// The number of account, transaction and entry updates among them; other updates are not
    /// counted.
    pub fn of(store: &SimpleBlockStore<SubscribeUpdate>) -> Self {
        let mut counts = Self::default();
        for event in &store.events {
            match event.update_oneof {
                Some(UpdateOneof::Account(_)) => counts.accounts += 1,
                Some(UpdateOneof::Transaction(_)) => counts.transactions += 1,
                Some(UpdateOneof::Entry(_)) => counts.entries += 1,
                _ => {}
            }
        }
        counts
    }
}

/// Iterates the account updates of a block rebuilt by the block machine.
///
/// # Arguments
///
/// * `store` - The rebuilt block's events, as [`SimpleBlockStore`] keeps them.
///
/// # Returns
///
/// Each [`SubscribeUpdateAccount`] among the events, in the order the block machine kept them.
pub fn block_accounts(
    store: &SimpleBlockStore<SubscribeUpdate>,
) -> impl Iterator<Item = &SubscribeUpdateAccount> {
    store
        .events
        .iter()
        .filter_map(|event| match &event.update_oneof {
            Some(UpdateOneof::Account(account)) => Some(account),
            _ => None,
        })
}

pub struct E2EGeyserEventAdapter;

/// How the e2e scenarios configure the block machine. The default module also runs against
/// clusters without Alpenglow, which produce no block footers, so a bank's BlockMeta alone ends
/// its block.
pub const BLOCK_MACHINE_CONFIG: BlockMachineConfig = BlockMachineConfig {
    require_block_footer: false,
};

/// Owner of every sysvar account.
const SYSVAR_PROGRAM_ID: [u8; 32] =
    solana_pubkey::Pubkey::from_str_const("Sysvar1111111111111111111111111111111111111").to_bytes();

impl GeyserEventAdapter for E2EGeyserEventAdapter {
    type EventT = SubscribeUpdate;

    fn extract_geyser_ev_info(event: &Self::EventT) -> Option<GeyserEventInfo> {
        let update_oneof = event.update_oneof.as_ref()?;
        match update_oneof {
            UpdateOneof::Slot(slot_update) => Some(GeyserEventInfo::Slot(SlotUpdateEvInfo {
                slot: slot_update.slot,
                parent: slot_update.parent,
                status: slot_status_kind(slot_update.status()),
                dead_error: slot_update.dead_error.is_some(),
                bank_id: slot_update.bank_id,
            })),
            UpdateOneof::BlockMeta(block_meta) => {
                Some(GeyserEventInfo::BlockMeta(BlockMetaEvInfo {
                    slot: block_meta.slot,
                    parent_slot: block_meta.parent_slot,
                    entries_count: block_meta.entries_count,
                    executed_transaction_count: block_meta.executed_transaction_count,
                    blockhash: solana_hash::Hash::from_str(&block_meta.blockhash)
                        .ok()?
                        .to_bytes(),
                    // Genesis's parent_blockhash is empty, not a valid base58 hash.
                    parent_blockhash: solana_hash::Hash::from_str(&block_meta.parent_blockhash)
                        .map(|hash| hash.to_bytes())
                        .unwrap_or_default(),
                    block_time: block_meta
                        .block_time
                        .map_or(0, |time| time.timestamp.max(0) as u64),
                    bank_id: block_meta.bank_id,
                }))
            }
            UpdateOneof::Entry(entry) => Some(GeyserEventInfo::Entry(EntryEvInfo {
                slot: entry.slot,
                index: entry.index,
                starting_transaction_index: entry.starting_transaction_index,
                executed_transaction_count: entry.executed_transaction_count,
                hash: entry.hash.as_slice().try_into().ok()?,
                bank_id: entry.bank_id,
            })),
            UpdateOneof::Transaction(tx) => Some(GeyserEventInfo::BankData {
                slot: tx.slot,
                bank_id: tx.bank_id,
            }),
            UpdateOneof::TransactionStatus(tx) => Some(GeyserEventInfo::BankData {
                slot: tx.slot,
                bank_id: tx.bank_id,
            }),
            UpdateOneof::Account(account) => {
                // A startup/snapshot account has no bank_id and is not part of any bank.
                let bank_id = account.bank_id?;
                let info = account.account.as_ref();
                if info.is_some_and(|info| info.owner == SYSVAR_PROGRAM_ID) {
                    Some(GeyserEventInfo::SysvarAccount {
                        slot: account.slot,
                        bank_id,
                        pubkey: info
                            .and_then(|info| info.pubkey.as_slice().try_into().ok())
                            .unwrap_or_default(),
                    })
                } else {
                    Some(GeyserEventInfo::BankData {
                        slot: account.slot,
                        bank_id,
                    })
                }
            }
            UpdateOneof::BlockFooter(footer) => {
                Some(GeyserEventInfo::BlockFooter(BlockFooterEvInfo {
                    slot: footer.slot,
                    bank_id: footer.bank_id,
                    bank_hash: footer.bank_hash.as_slice().try_into().ok()?,
                    block_user_agent: String::from_utf8_lossy(&footer.block_user_agent)
                        .into_owned(),
                }))
            }
            // A block repeats content the block machine rebuilds itself, and an UpdateParent
            // has no counterpart in its abstraction.
            UpdateOneof::Block(_) | UpdateOneof::EntryUpdateParent(_) => None,
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
        }
    }
}
