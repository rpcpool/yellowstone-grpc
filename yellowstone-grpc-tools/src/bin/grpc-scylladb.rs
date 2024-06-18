use {
    anyhow::Ok,
    clap::{Parser, Subcommand},
    futures::{future::BoxFuture, stream::StreamExt, TryFutureExt},
    scylla::{frame::Compression, Session, SessionBuilder},
    std::{convert::identity, net::SocketAddr, sync::Arc, time::Duration},
    tonic::transport::Server,
    tracing::{error, info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        prelude::subscribe_update::UpdateOneof,
        yellowstone::log::yellowstone_log_server::YellowstoneLogServer,
    },
    yellowstone_grpc_tools::{
        config::{load as config_load, GrpcRequestToProto},
        create_shutdown,
        prom::run_server as prometheus_run_server,
        scylladb::{
            config::{
                Config, ConfigGrpc2ScyllaDB, ConfigYellowstoneLogServer, ScyllaDbConnectionInfo,
            },
            sink::ScyllaSink,
            types::{Slot, Transaction},
            yellowstone_log::{
                consumer_group::{
                    consumer_group_store::ScyllaConsumerGroupStore,
                    coordinator::ConsumerGroupCoordinatorBackend,
                    producer::{EtcdProducerMonitor, ProducerMonitor, ScyllaProducerStore},
                },
                grpc2::ScyllaYsLog,
            },
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
    #[command(name = "yellowstone-log-server")]
    YellowstoneLogServer,

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
            ArgsAction::YellowstoneLogServer => {
                let config2 = config.yellowstone_log_server.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2scylladb` section in config should be defined")
                })?;
                Self::yellowstone_log_server(config2, config.scylladb, shutdown).await
            }
            ArgsAction::Test => {
                let config2 = config.grpc2scylladb.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2scylladb` section in config should be defined")
                })?;
                Self::test(config2, config.scylladb, shutdown).await
            }
        }
    }

    async fn yellowstone_log_server(
        config: ConfigYellowstoneLogServer,
        scylladb_conn_config: ScyllaDbConnectionInfo,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        let addr = config.listen.parse().unwrap();

        let session: Session = SessionBuilder::new()
            .known_node(scylladb_conn_config.hostname)
            .user(scylladb_conn_config.username, scylladb_conn_config.password)
            .compression(Some(Compression::Lz4))
            .use_keyspace(config.keyspace.clone(), false)
            .build()
            .await?;

        let etcd_endpoints = config.etcd_endpoints;
        let etcd_client = etcd_client::Client::connect(etcd_endpoints, None).await?;
        let session = Arc::new(session);
        let producer_monitor: Arc<dyn ProducerMonitor> =
            Arc::new(EtcdProducerMonitor::new(etcd_client.clone()));
        let producer_queries =
            ScyllaProducerStore::new(Arc::clone(&session), Arc::clone(&producer_monitor)).await?;
        let consumer_group_store =
            ScyllaConsumerGroupStore::new(Arc::clone(&session), producer_queries.clone()).await?;

        let (coordinator, coordinator_backend_handle) = ConsumerGroupCoordinatorBackend::spawn(
            etcd_client.clone(),
            Arc::clone(&session),
            consumer_group_store,
            producer_queries,
            Arc::clone(&producer_monitor),
            String::from("rpcpool"),
        );
        let scylla_ys_log = ScyllaYsLog::new(coordinator).await?;
        let ys_log_server = YellowstoneLogServer::new(scylla_ys_log);

        println!("YellowstoneLogServer listening on {}", addr);

        let server_fut = Server::builder()
            // GrpcWeb is over http1 so we must enable it.
            .add_service(ys_log_server)
            .serve(addr)
            .map_err(anyhow::Error::new);

        tokio::select! {
            _ = &mut shutdown => Ok(()),
            result = server_fut => result,
            result = coordinator_backend_handle => result.map_err(anyhow::Error::new).and_then(identity),
        }
    }

    async fn test(
        _config: ConfigGrpc2ScyllaDB,
        _scylladb_conn_config: ScyllaDbConnectionInfo,
        _shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        unimplemented!();
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
        let etcd = etcd_client::Client::connect(config.etcd_endpoints, None).await?;
        let mut geyser = client.subscribe_once(config.request.to_proto()).await?.peekable();
        tokio::pin!(geyser);
        info!("Grpc subscription is successful .");

        let first_slot = loop {
            let peek = geyser.as_mut().peek()
                .await
                .map(|inner| inner.to_owned())
                .transpose()?
                .expect("an error occurred during peek lookup");
            let slot = match peek.update_oneof {
                Some(UpdateOneof::Account(acc)) => acc.slot,
                Some(UpdateOneof::Transaction(tx)) => tx.slot,
                _ => { 
                    geyser.next().await.transpose()?;
                    continue;
                }
            };
            break slot;
        };

        let mut sink = ScyllaSink::new(
            etcd,
            sink_config,
            first_slot as Slot,
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
            .transpose();

            if let Err(error) = &message {
                error!("geyser plugin disconnected: {error:?}");
                break;
            }

            if let Some(message) = message? {
                let message = match message.update_oneof {
                    Some(value) => value,
                    None => unreachable!("Expect valid message"),
                };

                let result = match message {
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
                        sink.log_account_update(acc_update.unwrap()).await
                    }
                    UpdateOneof::Transaction(msg) => {
                        let tx: Result<Transaction, anyhow::Error> = msg.try_into();
                        if tx.is_err() {
                            warn!("failed to convert update tx: {:?}", tx.err().unwrap());
                            continue;
                        }
                        sink.log_transaction(tx.unwrap()).await
                    }
                    _ => continue,
                };

                if let Err(e) = result {
                    error!("error detected in sink: {e}");
                    break;
                }
            }
        }
        sink.shutdown().await
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
