use {
    anyhow::Context,
    backoff::{future::retry, ExponentialBackoff},
    clap::{Parser, Subcommand, ValueEnum},
    futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt},
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
    log::{error, info},
    serde_json::{json, Value},
    solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature},
    solana_transaction_status::UiTransactionEncoding,
    std::{
        collections::HashMap,
        env,
        fs::File,
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::{fs, sync::Mutex},
    tonic::transport::{channel::ClientTlsConfig, Certificate},
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, Interceptor},
    yellowstone_grpc_proto::{
        convert_from,
        geyser::SlotStatus,
        plugin::filter::message::FilteredUpdate,
        prelude::{
            subscribe_request_filter_accounts_filter::Filter as AccountsFilterOneof,
            subscribe_request_filter_accounts_filter_lamports::Cmp as AccountsFilterLamports,
            subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
            SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
            SubscribeUpdateAccountInfo, SubscribeUpdateEntry, SubscribeUpdateTransactionInfo,
        },
        prost::Message,
    },
};

type SlotsFilterMap = HashMap<String, SubscribeRequestFilterSlots>;
type AccountFilterMap = HashMap<String, SubscribeRequestFilterAccounts>;
type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type TransactionsStatusFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type EntryFilterMap = HashMap<String, SubscribeRequestFilterEntry>;
type BlocksFilterMap = HashMap<String, SubscribeRequestFilterBlocks>;
type BlocksMetaFilterMap = HashMap<String, SubscribeRequestFilterBlocksMeta>;

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    /// Path of a certificate authority file
    #[clap(long)]
    ca_certificate: Option<PathBuf>,

    #[clap(long)]
    x_token: Option<String>,

    /// Apply a timeout to connecting to the uri.
    #[clap(long)]
    connect_timeout_ms: Option<u64>,

    /// Sets the tower service default internal buffer size, default is 1024
    #[clap(long)]
    buffer_size: Option<usize>,

    /// Sets whether to use an adaptive flow control. Uses hyper’s default otherwise.
    #[clap(long)]
    http2_adaptive_window: Option<bool>,

    /// Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    #[clap(long)]
    http2_keep_alive_interval_ms: Option<u64>,

    /// Sets the max connection-level flow control for HTTP2, default is 65,535
    #[clap(long)]
    initial_connection_window_size: Option<u32>,

    ///Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control, default is 65,535
    #[clap(long)]
    initial_stream_window_size: Option<u32>,

    ///Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    #[clap(long)]
    keep_alive_timeout_ms: Option<u64>,

    /// Set http2 KEEP_ALIVE_WHILE_IDLE. Uses hyper’s default otherwise.
    #[clap(long)]
    keep_alive_while_idle: Option<bool>,

    /// Set whether TCP keepalive messages are enabled on accepted connections.
    #[clap(long)]
    tcp_keepalive_ms: Option<u64>,

    /// Set the value of TCP_NODELAY option for accepted connections. Enabled by default.
    #[clap(long)]
    tcp_nodelay: Option<bool>,

    /// Apply a timeout to each request.
    #[clap(long)]
    timeout_ms: Option<u64>,

    /// Max message size before decoding, full blocks can be super large, default is 1GiB
    #[clap(long, default_value_t = 1024 * 1024 * 1024)]
    max_decoding_message_size: usize,

    /// Commitment level: processed, confirmed or finalized
    #[clap(long)]
    commitment: Option<ArgsCommitment>,

    #[command(subcommand)]
    action: Action,
}

impl Args {
    fn get_commitment(&self) -> Option<CommitmentLevel> {
        Some(self.commitment.unwrap_or_default().into())
    }

    async fn connect(&self) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        let mut tls_config = ClientTlsConfig::new().with_native_roots();
        if let Some(path) = &self.ca_certificate {
            let bytes = fs::read(path).await?;
            tls_config = tls_config.ca_certificate(Certificate::from_pem(bytes));
        }
        let mut builder = GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(self.x_token.clone())?
            .tls_config(tls_config)?
            .max_decoding_message_size(self.max_decoding_message_size);

        if let Some(duration) = self.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(duration));
        }
        if let Some(sz) = self.buffer_size {
            builder = builder.buffer_size(sz);
        }
        if let Some(enabled) = self.http2_adaptive_window {
            builder = builder.http2_adaptive_window(enabled);
        }
        if let Some(duration) = self.http2_keep_alive_interval_ms {
            builder = builder.http2_keep_alive_interval(Duration::from_millis(duration));
        }
        if let Some(sz) = self.initial_connection_window_size {
            builder = builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = self.initial_stream_window_size {
            builder = builder.initial_stream_window_size(sz);
        }
        if let Some(duration) = self.keep_alive_timeout_ms {
            builder = builder.keep_alive_timeout(Duration::from_millis(duration));
        }
        if let Some(enabled) = self.keep_alive_while_idle {
            builder = builder.keep_alive_while_idle(enabled);
        }
        if let Some(duration) = self.tcp_keepalive_ms {
            builder = builder.tcp_keepalive(Some(Duration::from_millis(duration)));
        }
        if let Some(enabled) = self.tcp_nodelay {
            builder = builder.tcp_nodelay(enabled);
        }
        if let Some(duration) = self.timeout_ms {
            builder = builder.timeout(Duration::from_millis(duration));
        }

        builder.connect().await.map_err(Into::into)
    }
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone, Subcommand)]
enum Action {
    HealthCheck,
    HealthWatch,
    Subscribe(Box<ActionSubscribe>),
    SubscribeReplayInfo,
    Ping {
        #[clap(long, short, default_value_t = 0)]
        count: i32,
    },
    GetLatestBlockhash,
    GetBlockHeight,
    GetSlot,
    IsBlockhashValid {
        #[clap(long, short)]
        blockhash: String,
    },
    GetVersion,
}

#[derive(Debug, Clone, clap::Args)]
struct ActionSubscribe {
    /// Subscribe on accounts updates
    #[clap(long)]
    accounts: bool,

    /// Filter by presence of field txn_signature
    accounts_nonempty_txn_signature: Option<bool>,

    /// Filter by Account Pubkey
    #[clap(long)]
    accounts_account: Vec<String>,

    /// Path to a JSON array of account addresses
    #[clap(long)]
    accounts_account_path: Option<String>,

    /// Filter by Owner Pubkey
    #[clap(long)]
    accounts_owner: Vec<String>,

    /// Filter by Offset and Data, format: `offset,data in base58`
    #[clap(long)]
    accounts_memcmp: Vec<String>,

    /// Filter by Data size
    #[clap(long)]
    accounts_datasize: Option<u64>,

    /// Filter valid token accounts
    #[clap(long)]
    accounts_token_account_state: bool,

    /// Filter by lamports, format: `eq:42` / `ne:42` / `lt:42` / `gt:42`
    #[clap(long)]
    accounts_lamports: Vec<String>,

    /// Receive only part of updated data account, format: `offset,size`
    #[clap(long)]
    accounts_data_slice: Vec<String>,

    /// Subscribe on slots updates
    #[clap(long)]
    slots: bool,

    /// Filter slots by commitment
    #[clap(long)]
    slots_filter_by_commitment: bool,

    /// Subscribe on interslot slot updates
    #[clap(long)]
    slots_interslot_updates: bool,

    /// Subscribe on transactions updates
    #[clap(long)]
    transactions: bool,

    /// Filter vote transactions
    #[clap(long)]
    transactions_vote: Option<bool>,

    /// Filter failed transactions
    #[clap(long)]
    transactions_failed: Option<bool>,

    /// Filter by transaction signature
    #[clap(long)]
    transactions_signature: Option<String>,

    /// Filter included account in transactions
    #[clap(long)]
    transactions_account_include: Vec<String>,

    /// Filter excluded account in transactions
    #[clap(long)]
    transactions_account_exclude: Vec<String>,

    /// Filter required account in transactions
    #[clap(long)]
    transactions_account_required: Vec<String>,

    /// Subscribe on transactions_status updates
    #[clap(long)]
    transactions_status: bool,

    /// Filter vote transactions for transactions_status
    #[clap(long)]
    transactions_status_vote: Option<bool>,

    /// Filter failed transactions for transactions_status
    #[clap(long)]
    transactions_status_failed: Option<bool>,

    /// Filter by transaction signature for transactions_status
    #[clap(long)]
    transactions_status_signature: Option<String>,

    /// Filter included account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_include: Vec<String>,

    /// Filter excluded account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_exclude: Vec<String>,

    /// Filter required account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_required: Vec<String>,

    #[clap(long)]
    entries: bool,

    /// Subscribe on block updates
    #[clap(long)]
    blocks: bool,

    /// Filter included account in transactions
    #[clap(long)]
    blocks_account_include: Vec<String>,

    /// Include transactions to block message
    #[clap(long)]
    blocks_include_transactions: Option<bool>,

    /// Include accounts to block message
    #[clap(long)]
    blocks_include_accounts: Option<bool>,

    /// Include entries to block message
    #[clap(long)]
    blocks_include_entries: Option<bool>,

    /// Subscribe on block meta updates (without transactions)
    #[clap(long)]
    blocks_meta: bool,

    /// Re-send message from slot
    #[clap(long)]
    from_slot: Option<u64>,

    /// Send ping in subscribe request
    #[clap(long)]
    ping: Option<i32>,

    /// Resubscribe (only to slots) after
    #[clap(long)]
    resub: Option<usize>,

    /// Show total stat instead of messages
    #[clap(long, default_value_t = false)]
    stats: bool,

    /// Verify manually implemented encoding against prost
    #[clap(long, default_value_t = false)]
    verify_encoding: bool,
}

impl Action {
    async fn get_subscribe_request(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> anyhow::Result<Option<(SubscribeRequest, usize, bool, bool)>> {
        Ok(match self {
            Self::Subscribe(args) => {
                let mut accounts: AccountFilterMap = HashMap::new();
                if args.accounts {
                    let mut accounts_account = args.accounts_account.clone();
                    if let Some(path) = args.accounts_account_path.clone() {
                        let accounts = tokio::task::block_in_place(move || {
                            let file = File::open(path)?;
                            Ok::<Vec<String>, anyhow::Error>(serde_json::from_reader(file)?)
                        })?;
                        accounts_account.extend(accounts);
                    }

                    let mut filters = vec![];
                    for filter in args.accounts_memcmp.iter() {
                        match filter.split_once(',') {
                            Some((offset, data)) => {
                                filters.push(SubscribeRequestFilterAccountsFilter {
                                    filter: Some(AccountsFilterOneof::Memcmp(
                                        SubscribeRequestFilterAccountsFilterMemcmp {
                                            offset: offset
                                                .parse()
                                                .map_err(|_| anyhow::anyhow!("invalid offset"))?,
                                            data: Some(AccountsFilterMemcmpOneof::Base58(
                                                data.trim().to_string(),
                                            )),
                                        },
                                    )),
                                });
                            }
                            _ => anyhow::bail!("invalid memcmp"),
                        }
                    }
                    if let Some(datasize) = args.accounts_datasize {
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterOneof::Datasize(datasize)),
                        });
                    }
                    if args.accounts_token_account_state {
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterOneof::TokenAccountState(true)),
                        });
                    }
                    for filter in args.accounts_lamports.iter() {
                        match filter.split_once(':') {
                            Some((cmp, value)) => {
                                let Ok(value) = value.parse() else {
                                    anyhow::bail!("invalid lamports value: {value}");
                                };
                                filters.push(SubscribeRequestFilterAccountsFilter {
                                    filter: Some(AccountsFilterOneof::Lamports(
                                        SubscribeRequestFilterAccountsFilterLamports {
                                            cmp: Some(match cmp {
                                                "eq" => AccountsFilterLamports::Eq(value),
                                                "ne" => AccountsFilterLamports::Ne(value),
                                                "lt" => AccountsFilterLamports::Lt(value),
                                                "gt" => AccountsFilterLamports::Gt(value),
                                                _ => {
                                                    anyhow::bail!("invalid lamports filter: {cmp}")
                                                }
                                            }),
                                        },
                                    )),
                                });
                            }
                            _ => anyhow::bail!("invalid lamports"),
                        }
                    }

                    accounts.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterAccounts {
                            nonempty_txn_signature: args.accounts_nonempty_txn_signature,
                            account: accounts_account,
                            owner: args.accounts_owner.clone(),
                            filters,
                        },
                    );
                }

                let mut slots: SlotsFilterMap = HashMap::new();
                if args.slots {
                    slots.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterSlots {
                            filter_by_commitment: Some(args.slots_filter_by_commitment),
                            interslot_updates: Some(args.slots_interslot_updates),
                        },
                    );
                }

                let mut transactions: TransactionsFilterMap = HashMap::new();
                if args.transactions {
                    transactions.insert(
                        "client".to_string(),
                        SubscribeRequestFilterTransactions {
                            vote: args.transactions_vote,
                            failed: args.transactions_failed,
                            signature: args.transactions_signature.clone(),
                            account_include: args.transactions_account_include.clone(),
                            account_exclude: args.transactions_account_exclude.clone(),
                            account_required: args.transactions_account_required.clone(),
                        },
                    );
                }

                let mut transactions_status: TransactionsStatusFilterMap = HashMap::new();
                if args.transactions_status {
                    transactions_status.insert(
                        "client".to_string(),
                        SubscribeRequestFilterTransactions {
                            vote: args.transactions_status_vote,
                            failed: args.transactions_status_failed,
                            signature: args.transactions_status_signature.clone(),
                            account_include: args.transactions_status_account_include.clone(),
                            account_exclude: args.transactions_status_account_exclude.clone(),
                            account_required: args.transactions_status_account_required.clone(),
                        },
                    );
                }

                let mut entries: EntryFilterMap = HashMap::new();
                if args.entries {
                    entries.insert("client".to_owned(), SubscribeRequestFilterEntry {});
                }

                let mut blocks: BlocksFilterMap = HashMap::new();
                if args.blocks {
                    blocks.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterBlocks {
                            account_include: args.blocks_account_include.clone(),
                            include_transactions: args.blocks_include_transactions,
                            include_accounts: args.blocks_include_accounts,
                            include_entries: args.blocks_include_entries,
                        },
                    );
                }

                let mut blocks_meta: BlocksMetaFilterMap = HashMap::new();
                if args.blocks_meta {
                    blocks_meta.insert("client".to_owned(), SubscribeRequestFilterBlocksMeta {});
                }

                let mut accounts_data_slice = Vec::new();
                for data_slice in args.accounts_data_slice.iter() {
                    match data_slice.split_once(',') {
                        Some((offset, length)) => match (offset.parse(), length.parse()) {
                            (Ok(offset), Ok(length)) => {
                                accounts_data_slice
                                    .push(SubscribeRequestAccountsDataSlice { offset, length });
                            }
                            _ => anyhow::bail!("invalid data_slice"),
                        },
                        _ => anyhow::bail!("invalid data_slice"),
                    }
                }

                let ping = args.ping.map(|id| SubscribeRequestPing { id });

                Some((
                    SubscribeRequest {
                        slots,
                        accounts,
                        transactions,
                        transactions_status,
                        entry: entries,
                        blocks,
                        blocks_meta,
                        commitment: commitment.map(|x| x as i32),
                        accounts_data_slice,
                        ping,
                        from_slot: args.from_slot,
                    },
                    args.resub.unwrap_or(0),
                    args.stats,
                    args.verify_encoding,
                ))
            }
            _ => None,
        })
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
    let zero_attempts = Arc::new(Mutex::new(true));

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let args = args.clone();
        let zero_attempts = Arc::clone(&zero_attempts);

        async move {
            let mut zero_attempts = zero_attempts.lock().await;
            if *zero_attempts {
                *zero_attempts = false;
            } else {
                info!("Retry to connect to the server");
            }
            drop(zero_attempts);

            let commitment = args.get_commitment();
            let mut client = args.connect().await.map_err(backoff::Error::transient)?;
            info!("Connected");

            match &args.action {
                Action::HealthCheck => client
                    .health_check()
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::HealthWatch => geyser_health_watch(client).await,
                Action::Subscribe(_) => {
                    let (request, resub, stats, verify_encoding) = args
                        .action
                        .get_subscribe_request(commitment)
                        .await
                        .map_err(backoff::Error::Permanent)?
                        .ok_or(backoff::Error::Permanent(anyhow::anyhow!(
                            "expect subscribe action"
                        )))?;

                    geyser_subscribe(client, request, resub, stats, verify_encoding).await
                }
                Action::SubscribeReplayInfo => client
                    .subscribe_replay_info()
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::Ping { count } => client
                    .ping(*count)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetLatestBlockhash => client
                    .get_latest_blockhash(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetBlockHeight => client
                    .get_block_height(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetSlot => client
                    .get_slot(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::IsBlockhashValid { blockhash } => client
                    .is_blockhash_valid(blockhash.clone(), commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetVersion => client
                    .get_version()
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
            }
            .map_err(backoff::Error::transient)?;

            Ok::<(), backoff::Error<anyhow::Error>>(())
        }
        .inspect_err(|error| error!("failed to connect: {error}"))
    })
    .await
    .map_err(Into::into)
}

async fn geyser_health_watch(mut client: GeyserGrpcClient<impl Interceptor>) -> anyhow::Result<()> {
    let mut stream = client.health_watch().await?;
    info!("stream opened");
    while let Some(message) = stream.next().await {
        info!("new message: {message:?}");
    }
    info!("stream closed");
    Ok(())
}

async fn geyser_subscribe(
    mut client: GeyserGrpcClient<impl Interceptor>,
    request: SubscribeRequest,
    resub: usize,
    stats: bool,
    verify_encoding: bool,
) -> anyhow::Result<()> {
    let pb_multi = MultiProgress::new();
    let mut pb_accounts_c = 0;
    let pb_accounts = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("accounts"))?;
    let mut pb_slots_c = 0;
    let pb_slots = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("slots"))?;
    let mut pb_txs_c = 0;
    let pb_txs = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("transactions"))?;
    let mut pb_txs_st_c = 0;
    let pb_txs_st = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("transactions statuses"))?;
    let mut pb_entries_c = 0;
    let pb_entries = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("entries"))?;
    let mut pb_blocks_mt_c = 0;
    let pb_blocks_mt = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("blocks meta"))?;
    let mut pb_blocks_c = 0;
    let pb_blocks = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("blocks"))?;
    let mut pb_pp_c = 0;
    let pb_pp = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("ping/pong"))?;
    let mut pb_total_c = 0;
    let pb_total = crate_progress_bar(&pb_multi, ProgressBarTpl::Total)?;
    let mut pb_verify_c = verify_encoding.then_some((0, 0));
    let pb_verify = crate_progress_bar(&pb_multi, ProgressBarTpl::Verify)?;

    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    info!("stream opened");
    let mut counter = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if stats {
                    let encoded_len = msg.encoded_len() as u64;
                    let (pb_c, pb) = match msg.update_oneof {
                        Some(UpdateOneof::Account(_)) => (&mut pb_accounts_c, &pb_accounts),
                        Some(UpdateOneof::Slot(_)) => (&mut pb_slots_c, &pb_slots),
                        Some(UpdateOneof::Transaction(_)) => (&mut pb_txs_c, &pb_txs),
                        Some(UpdateOneof::TransactionStatus(_)) => (&mut pb_txs_st_c, &pb_txs_st),
                        Some(UpdateOneof::Entry(_)) => (&mut pb_entries_c, &pb_entries),
                        Some(UpdateOneof::BlockMeta(_)) => (&mut pb_blocks_mt_c, &pb_blocks_mt),
                        Some(UpdateOneof::Block(_)) => (&mut pb_blocks_c, &pb_blocks),
                        Some(UpdateOneof::Ping(_)) => (&mut pb_pp_c, &pb_pp),
                        Some(UpdateOneof::Pong(_)) => (&mut pb_pp_c, &pb_pp),
                        None => {
                            pb_multi.println("update not found in the message")?;
                            break;
                        }
                    };
                    *pb_c += 1;
                    pb.set_message(format_thousands(*pb_c));
                    pb.inc(encoded_len);
                    pb_total_c += 1;
                    pb_total.set_message(format_thousands(pb_total_c));
                    pb_total.inc(encoded_len);

                    if let Some((prost_c, ref_c)) = &mut pb_verify_c {
                        let encoded_len_prost0 = msg.encoded_len();
                        let encoded_prost0 = msg.encode_to_vec();

                        let update = FilteredUpdate::from_subscribe_update(msg)
                            .map_err(|error| anyhow::anyhow!(error))
                            .context("failed to convert update message to filtered update")?;

                        let ts = Instant::now();
                        let msg2 = update.as_subscribe_update();
                        let encoded_len_prost = msg2.encoded_len();
                        let encoded_prost = msg2.encode_to_vec();
                        *prost_c += ts.elapsed().as_nanos();

                        let ts = Instant::now();
                        let encoded_len_ref = update.encoded_len();
                        let encoded_ref = update.encode_to_vec();
                        *ref_c += ts.elapsed().as_nanos();

                        pb_verify.set_message(format!(
                            "{:.2?}%",
                            100f64 * (*ref_c as f64) / (*prost_c as f64)
                        ));

                        if encoded_len_prost0 != encoded_len_prost
                            || encoded_len_prost != encoded_len_ref
                            || encoded_prost0 != encoded_prost
                            || encoded_prost != encoded_ref
                        {
                            let dir = "grpc-client-verify";
                            let name = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
                            let path = format!("{dir}/{name}");
                            pb_multi
                                .println(format!("found unmached message, save to `{path}`"))?;
                            fs::create_dir_all(dir)
                                .await
                                .context("failed to create dir for unmached")?;
                            fs::write(path, encoded_prost)
                                .await
                                .context("failed to save unmached")?;
                        }
                    }

                    continue;
                }

                let filters = msg.filters;
                let created_at: SystemTime = msg
                    .created_at
                    .ok_or(anyhow::anyhow!("no created_at in the message"))?
                    .try_into()
                    .context("failed to parse created_at")?;
                match msg.update_oneof {
                    Some(UpdateOneof::Account(msg)) => {
                        let account = msg
                            .account
                            .ok_or(anyhow::anyhow!("no account in the message"))?;
                        let mut value = create_pretty_account(account)?;
                        value["isStartup"] = json!(msg.is_startup);
                        value["slot"] = json!(msg.slot);
                        print_update("account", created_at, &filters, value);
                    }
                    Some(UpdateOneof::Slot(msg)) => {
                        let status = SlotStatus::try_from(msg.status)
                            .context("failed to decode commitment")?;
                        print_update(
                            "slot",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "parent": msg.parent,
                                "status": status.as_str_name(),
                                "deadError": msg.dead_error,
                            }),
                        );
                    }
                    Some(UpdateOneof::Transaction(msg)) => {
                        let tx = msg
                            .transaction
                            .ok_or(anyhow::anyhow!("no transaction in the message"))?;
                        let mut value = create_pretty_transaction(tx)?;
                        value["slot"] = json!(msg.slot);
                        print_update("transaction", created_at, &filters, value);
                    }
                    Some(UpdateOneof::TransactionStatus(msg)) => {
                        print_update(
                            "transactionStatus",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "signature": Signature::try_from(msg.signature.as_slice()).context("invalid signature")?.to_string(),
                                "isVote": msg.is_vote,
                                "index": msg.index,
                                "err": convert_from::create_tx_error(msg.err.as_ref())
                                    .map_err(|error| anyhow::anyhow!(error))
                                    .context("invalid error")?,
                            }),
                        );
                    }
                    Some(UpdateOneof::Entry(msg)) => {
                        print_update("entry", created_at, &filters, create_pretty_entry(msg)?);
                    }
                    Some(UpdateOneof::BlockMeta(msg)) => {
                        print_update(
                            "blockmeta",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "blockhash": msg.blockhash,
                                "rewards": if let Some(rewards) = msg.rewards {
                                    Some(convert_from::create_rewards_obj(rewards).map_err(|error| anyhow::anyhow!(error))?)
                                } else {
                                    None
                                },
                                "blockTime": msg.block_time.map(|obj| obj.timestamp),
                                "blockHeight": msg.block_height.map(|obj| obj.block_height),
                                "parentSlot": msg.parent_slot,
                                "parentBlockhash": msg.parent_blockhash,
                                "executedTransactionCount": msg.executed_transaction_count,
                                "entriesCount": msg.entries_count,
                            }),
                        );
                    }
                    Some(UpdateOneof::Block(msg)) => {
                        print_update(
                            "block",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "blockhash": msg.blockhash,
                                "rewards": if let Some(rewards) = msg.rewards {
                                    Some(convert_from::create_rewards_obj(rewards).map_err(|error| anyhow::anyhow!(error))?)
                                } else {
                                    None
                                },
                                "blockTime": msg.block_time.map(|obj| obj.timestamp),
                                "blockHeight": msg.block_height.map(|obj| obj.block_height),
                                "parentSlot": msg.parent_slot,
                                "parentBlockhash": msg.parent_blockhash,
                                "executedTransactionCount": msg.executed_transaction_count,
                                "transactions": msg.transactions.into_iter().map(create_pretty_transaction).collect::<Result<Value, _>>()?,
                                "updatedAccountCount": msg.updated_account_count,
                                "accounts": msg.accounts.into_iter().map(create_pretty_account).collect::<Result<Value, _>>()?,
                                "entriesCount": msg.entries_count,
                                "entries": msg.entries.into_iter().map(create_pretty_entry).collect::<Result<Value, _>>()?,
                            }),
                        );
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        // This is necessary to keep load balancers that expect client pings alive. If your load balancer doesn't
                        // require periodic client pings then this is unnecessary
                        subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await?;
                    }
                    Some(UpdateOneof::Pong(_)) => {}
                    None => {
                        error!("update not found in the message");
                        break;
                    }
                }
            }
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }

        // Example to illustrate how to resubscribe/update the subscription
        counter += 1;
        if counter == resub {
            let mut new_slots: SlotsFilterMap = HashMap::new();
            new_slots.insert("client".to_owned(), SubscribeRequestFilterSlots::default());

            subscribe_tx
                .send(SubscribeRequest {
                    slots: new_slots.clone(),
                    accounts: HashMap::default(),
                    transactions: HashMap::default(),
                    transactions_status: HashMap::default(),
                    entry: HashMap::default(),
                    blocks: HashMap::default(),
                    blocks_meta: HashMap::default(),
                    commitment: None,
                    accounts_data_slice: Vec::default(),
                    ping: None,
                    from_slot: None,
                })
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        }
    }
    info!("stream closed");
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressBarTpl {
    Msg(&'static str),
    Total,
    Verify,
}

fn crate_progress_bar(
    pb: &MultiProgress,
    pb_t: ProgressBarTpl,
) -> Result<ProgressBar, indicatif::style::TemplateError> {
    let pb = pb.add(ProgressBar::no_length());
    let tpl = match pb_t {
        ProgressBarTpl::Msg(kind) => {
            format!("{{spinner}} {kind}: {{msg}} / ~{{bytes}} (~{{bytes_per_sec}})")
        }
        ProgressBarTpl::Total => {
            "{spinner} total: {msg} / ~{bytes} (~{bytes_per_sec}) in {elapsed_precise}".to_owned()
        }
        ProgressBarTpl::Verify => {
            "{spinner} verify: {msg} (elapsed time, compare to prost)".to_owned()
        }
    };
    pb.set_style(ProgressStyle::with_template(&tpl)?);
    Ok(pb)
}

fn format_thousands(value: u64) -> String {
    value
        .to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .expect("invalid number")
        .join(",")
}

fn create_pretty_account(account: SubscribeUpdateAccountInfo) -> anyhow::Result<Value> {
    Ok(json!({
        "pubkey": Pubkey::try_from(account.pubkey).map_err(|_| anyhow::anyhow!("invalid account pubkey"))?.to_string(),
        "lamports": account.lamports,
        "owner": Pubkey::try_from(account.owner).map_err(|_| anyhow::anyhow!("invalid account owner"))?.to_string(),
        "executable": account.executable,
        "rentEpoch": account.rent_epoch,
        "data": hex::encode(account.data),
        "writeVersion": account.write_version,
        "txnSignature": account.txn_signature.map(|sig| bs58::encode(sig).into_string()),
    }))
}

fn create_pretty_transaction(tx: SubscribeUpdateTransactionInfo) -> anyhow::Result<Value> {
    Ok(json!({
        "signature": Signature::try_from(tx.signature.as_slice()).context("invalid signature")?.to_string(),
        "isVote": tx.is_vote,
        "tx": convert_from::create_tx_with_meta(tx)
            .map_err(|error| anyhow::anyhow!(error))
            .context("invalid tx with meta")?
            .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
            .context("failed to encode transaction")?,
    }))
}

fn create_pretty_entry(msg: SubscribeUpdateEntry) -> anyhow::Result<Value> {
    Ok(json!({
        "slot": msg.slot,
        "index": msg.index,
        "numHashes": msg.num_hashes,
        "hash": Hash::new_from_array(<[u8; 32]>::try_from(msg.hash.as_slice()).context("invalid entry hash")?).to_string(),
        "executedTransactionCount": msg.executed_transaction_count,
        "startingTransactionIndex": msg.starting_transaction_index,
    }))
}

fn print_update(kind: &str, created_at: SystemTime, filters: &[String], value: Value) {
    let unix_since = created_at
        .duration_since(UNIX_EPOCH)
        .expect("valid system time");
    info!(
        "{kind} ({}) at {}.{:0>6}: {}",
        filters.join(","),
        unix_since.as_secs(),
        unix_since.subsec_micros(),
        serde_json::to_string(&value).expect("json serialization failed")
    );
}
