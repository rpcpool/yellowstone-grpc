//! Subscribes with `subscribe_with_reconnect`, which reconnects by itself after a recoverable
//! failure, and shows what a consumer does with what a reconnect sends:
//!
//! - `ReconnectEvent::Disconnected` lists the banks received in part: drop them. With replay,
//!   the new connection resends them whole.
//! - With replay (the default), the new connection also resends banks already received whole:
//!   skip those duplicates.
//!
//! The example builds each bank from its sysvar account writes and prints it once its block
//! meta arrives.
//!
//! ```text
//! cargo run --bin client_with_reconnect -- --endpoint http://127.0.0.1:10000
//! cargo run --bin client_with_reconnect -- --no-replay --commitment confirmed
//! ```

use {
    clap::{Parser, ValueEnum},
    futures::stream::StreamExt,
    log::{info, warn},
    std::{
        collections::{BTreeSet, HashMap},
        env,
        time::Duration,
    },
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::{
        Backoff, BankRef, GeyserGrpcClient, ReconnectConfig, ReconnectEvent, ReconnectPolicy,
    },
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    },
};

/// Owner of every sysvar account: every bank writes some of them.
const SYSVAR_OWNER: &str = "Sysvar1111111111111111111111111111111111111";
/// Complete banks remembered to skip replayed duplicates. A replay goes back a few slots,
/// so a few thousand banks is plenty.
const COMPLETE_BANKS_KEPT: usize = 4096;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}

impl From<Commitment> for CommitmentLevel {
    fn from(commitment: Commitment) -> Self {
        match commitment {
            Commitment::Processed => Self::Processed,
            Commitment::Confirmed => Self::Confirmed,
            Commitment::Finalized => Self::Finalized,
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[clap(
    author,
    version,
    about = "Yellowstone gRPC client that reconnects by itself"
)]
struct Args {
    /// Service endpoint
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long, value_enum, default_value_t = Commitment::Processed)]
    commitment: Commitment,

    /// Resume at the live tip after a reconnect, losing what the server sent meanwhile.
    /// By default a reconnect replays from the last confirmed slot.
    #[clap(long)]
    no_replay: bool,

    /// With replay, how many slots before the last confirmed slot to replay from.
    #[clap(long, default_value_t = 0, conflicts_with = "no_replay")]
    slots_behind: u64,

    /// Retry budget: reconnects in a row before the stream gives up. A connection that
    /// delivers a complete bank refills it.
    #[clap(long, default_value_t = 5)]
    max_retries: u32,
}

/// The banks being built, and the ones already complete.
#[derive(Default)]
struct Banks {
    /// Account writes received so far, per bank still in progress.
    in_progress: HashMap<BankRef, usize>,
    /// Banks already printed: a replay sends them again.
    complete: BTreeSet<BankRef>,
}

impl Banks {
    /// Records an account write, unless its bank is already complete (a replayed duplicate).
    fn account(&mut self, bank: BankRef) {
        if !self.complete.contains(&bank) {
            *self.in_progress.entry(bank).or_default() += 1;
        }
    }

    /// Completes a bank. Returns its account write count, or `None` for a replayed duplicate.
    fn block_meta(&mut self, bank: BankRef) -> Option<usize> {
        if !self.complete.insert(bank) {
            return None;
        }
        while self.complete.len() > COMPLETE_BANKS_KEPT {
            self.complete.pop_first();
        }
        Some(self.in_progress.remove(&bank).unwrap_or_default())
    }

    /// Drops banks a disconnect left partial.
    fn discard(&mut self, banks: &[BankRef]) {
        for bank in banks {
            self.in_progress.remove(bank);
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();
    let args = Args::parse();

    let policy = if args.no_replay {
        ReconnectPolicy::NoReplay
    } else {
        ReconnectPolicy::Replay {
            slots_behind: args.slots_behind,
        }
    };
    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .set_reconnect_config(
            ReconnectConfig::default()
                .with_backoff(Backoff::new(
                    Duration::from_millis(100),
                    2.0,
                    args.max_retries,
                ))
                .with_policy(policy),
        )
        .connect()
        .await?;

    let request = SubscribeRequest {
        accounts: maplit::hashmap! {
            "sysvars".to_owned() => SubscribeRequestFilterAccounts {
                owner: vec![SYSVAR_OWNER.to_owned()],
                ..Default::default()
            }
        },
        blocks_meta: maplit::hashmap! {
            "blocks".to_owned() => SubscribeRequestFilterBlocksMeta::default()
        },
        commitment: Some(CommitmentLevel::from(args.commitment) as i32),
        ..Default::default()
    };
    // The sink (dropped here) can change the filters later; reconnects keep the change.
    let (_sink, mut stream) = client.subscribe_with_reconnect(Some(request)).await?;
    info!("subscribed with {policy:?}");

    let mut banks = Banks::default();
    while let Some(event) = stream.next().await {
        match event? {
            ReconnectEvent::Disconnected(disconnected) => {
                warn!(
                    "disconnected: dropping {} partial banks {:?}, reconnecting from slot {:?}",
                    disconnected.partial_banks.len(),
                    disconnected.partial_banks,
                    disconnected.replay_from_slot
                );
                banks.discard(&disconnected.partial_banks);
            }
            ReconnectEvent::Update(update) => match update.update_oneof {
                Some(UpdateOneof::Account(account)) => {
                    if let Some(bank_id) = account.bank_id {
                        banks.account(BankRef {
                            slot: account.slot,
                            bank_id,
                        });
                    }
                }
                Some(UpdateOneof::BlockMeta(meta)) => {
                    let bank = BankRef {
                        slot: meta.slot,
                        bank_id: meta.bank_id,
                    };
                    match banks.block_meta(bank) {
                        Some(writes) => info!(
                            "bank slot={} bank_id={} complete: {writes} sysvar writes, blockhash {}",
                            bank.slot, bank.bank_id, meta.blockhash
                        ),
                        None => info!("bank slot={} bank_id={} replayed again, skipped", bank.slot, bank.bank_id),
                    }
                }
                _ => {}
            },
        }
    }
    Ok(())
}
