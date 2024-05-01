use {
    anyhow::Ok,
    clap::{Parser, Subcommand},
    futures::{future::BoxFuture, stream::StreamExt},
    std::{net::SocketAddr, time::Duration},
    tracing::{info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
    yellowstone_grpc_tools::{
        config::{load as config_load, GrpcRequestToProto},
        create_shutdown,
        prom::run_server as prometheus_run_server,
        scylladb::{
            config::{Config, ConfigGrpc2ScyllaDB, ScyllaDbConnectionInfo},
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
                unimplemented!();
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

            match message {
                Some(message) => {
                    let message = match message.update_oneof {
                        Some(value) => value,
                        None => unreachable!("Expect valid message"),
                    };

                    match message {
                        UpdateOneof::Account(msg) => {
                            // let pubkey_opt: &Option<&[u8]> =
                            //     &msg.account.as_ref().map(|acc| acc.pubkey.as_ref());
                            // trace!(
                            //     "Received an account update slot={:?}, pubkey={:?}",
                            //     msg.slot, pubkey_opt
                            // );
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
                _ => (),
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
