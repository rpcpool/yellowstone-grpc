use {
    anyhow::Ok,
    clap::{Parser, Subcommand},
    futures::{future::BoxFuture, stream::StreamExt},
    scylla::{frame::Compression, Session, SessionBuilder},
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::time::Instant,
    tracing::{info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        prelude::subscribe_update::UpdateOneof, yellowstone::log::EventSubscriptionPolicy,
    },
    yellowstone_grpc_tools::{
        config::{load as config_load, GrpcRequestToProto},
        create_shutdown,
        prom::run_server as prometheus_run_server,
        scylladb::{
            config::{Config, ConfigGrpc2ScyllaDB, ScyllaDbConnectionInfo},
            consumer::{
                common::InitialOffsetPolicy,
                grpc::{get_or_register_consumer, spawn_grpc_consumer, SpawnGrpcConsumerReq},
            },
            sink::ScyllaSink,
            types::Transaction,
        },
        setup_tracing,
    },
};

// 512MB
const MAX_DECODING_MESSAGE_SIZE_BYTES: usize = 512_000_000;

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC ScyllaDB Tool")]
struct Args {
    /// Path to config file
    #[clap(short, long)]
    config: String,

    /// Prometheus listen address
    #[clap(long)]
    prometheus: Option<SocketAddr>,

    #[command(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsAction {
    /// Receive data from gRPC and send them to the Kafka
    #[command(name = "grpc2scylla")]
    Grpc2Scylla,
    /// Receive data from Kafka and send them over gRPC
    #[command(name = "scylla2grpc")]
    Scylla2Grpc,
    #[command(name = "test")]
    Test,
}

impl ArgsAction {
    async fn run(self, config: Config) -> anyhow::Result<()> {
        let shutdown = create_shutdown()?;
        match self {
            ArgsAction::Grpc2Scylla => {
                let config2 = config.grpc2scylladb.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2scylladb` section in config should be defined")
                })?;
                Self::grpc2scylladb(config2, config.scylladb, shutdown).await
            }
            ArgsAction::Scylla2Grpc => {
                unimplemented!();
            }
            ArgsAction::Test => {
                let config2 = config.grpc2scylladb.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2scylladb` section in config should be defined")
                })?;
                Self::test(config2, config.scylladb, shutdown).await
            }
        }
    }

    async fn test(
        config: ConfigGrpc2ScyllaDB,
        scylladb_conn_config: ScyllaDbConnectionInfo,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        let session: Session = SessionBuilder::new()
            .known_node(scylladb_conn_config.hostname)
            .user(scylladb_conn_config.username, scylladb_conn_config.password)
            .compression(Some(Compression::Lz4))
            .use_keyspace(config.keyspace.clone(), false)
            .build()
            .await?;
        let session = Arc::new(session);
        let ci = get_or_register_consumer(
            Arc::clone(&session),
            "test",
            InitialOffsetPolicy::Earliest,
            EventSubscriptionPolicy::TransactionOnly,
        )
        .await?;

        // let hexstr = "16daf15e85d893b89d83a8ca7d7f86416f134905d1d79e4f62e3da70a3a20a7d";
        // let _pubkey = (0..hexstr.len())
        //     .step_by(2)
        //     .map(|i| u8::from_str_radix(&hexstr[i..i + 2], 16))
        //     .collect::<Result<Vec<_>, _>>()?;
        let req = SpawnGrpcConsumerReq {
            session: Arc::clone(&session),
            consumer_info: ci,
            account_update_event_filter: None,
            tx_event_filter: None,
            buffer_capacity: None,
            offset_commit_interval: None,
        };
        let mut rx = spawn_grpc_consumer(req).await?;

        let mut print_tx_secs = Instant::now() + Duration::from_secs(1);
        let mut num_events = 0;
        loop {
            if print_tx_secs.elapsed() > Duration::ZERO {
                println!("event/second {}", num_events);
                num_events = 0;
                print_tx_secs = Instant::now() + Duration::from_secs(1);
            }
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                Some(result) = rx.recv() => {
                    if result.is_err() {
                        anyhow::bail!("fail!!!")
                    }
                    let _x = result?.update_oneof.expect("got none");
                    // match x {
                    //     UpdateOneof::Account(acc) => println!("acc, slot {:?}", acc.slot),
                    //     UpdateOneof::Transaction(tx) => panic!("got tx"),
                    //     _ => unimplemented!()
                    // }
                    num_events += 1;
                },
                _ = tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)) => {
                    warn!("received no event")
                }
            }
        }
    }

    async fn grpc2scylladb(
        config: ConfigGrpc2ScyllaDB,
        scylladb_conn_config: ScyllaDbConnectionInfo,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        let sink_config = config.get_scylladb_sink_config();
        info!("sink configuration {:?}", sink_config);

        // Create gRPC client & subscribe
        let mut client = GeyserGrpcClient::build_from_shared(config.endpoint)?
            .x_token(config.x_token)?
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE_BYTES)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(5))
            .connect()
            .await?;

        let mut geyser = client.subscribe_once(config.request.to_proto()).await?;
        info!("Grpc subscription is successful .");

        let mut sink = ScyllaSink::new(
            sink_config,
            scylladb_conn_config.hostname,
            scylladb_conn_config.username,
            scylladb_conn_config.password,
        )
        .await?;

        info!("ScyllaSink is ready.");
        // Receive-send loop
        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                message = geyser.next() => message,
            }
            .transpose()?;

            if let Some(message) = message {
                let message = match message.update_oneof {
                    Some(value) => value,
                    None => unreachable!("Expect valid message"),
                };

                match message {
                    UpdateOneof::Account(msg) => {
                        let acc_update = msg.clone().try_into();
                        if acc_update.is_err() {
                            // Drop the message if invalid
                            warn!(
                                "failed to parse account update: {:?}",
                                acc_update.err().unwrap()
                            );
                            continue;
                        }
                        // If the sink is close, let it crash...
                        sink.log_account_update(acc_update.unwrap()).await.unwrap();
                    }
                    UpdateOneof::Transaction(msg) => {
                        let tx: Result<Transaction, anyhow::Error> = msg.try_into();
                        if tx.is_err() {
                            warn!("failed to convert update tx: {:?}", tx.err().unwrap());
                            continue;
                        }
                        sink.log_transaction(tx.unwrap()).await.unwrap();
                    }
                    _ => continue,
                };
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;

    // Parse args
    let args = Args::parse();
    let config = config_load::<Config>(&args.config).await?;

    // Run prometheus server
    if let Some(address) = args.prometheus.or(config.prometheus) {
        prometheus_run_server(address)?;
    }

    args.action.run(config).await.unwrap();

    Ok(())
}
