use {
    clap::{Parser, Subcommand},
    futures::{future::BoxFuture, stream::StreamExt},
    google_cloud_googleapis::pubsub::v1::PubsubMessage,
    google_cloud_pubsub::{client::Client, subscription::SubscriptionConfig},
    std::{net::SocketAddr, time::Duration},
    tokio::{task::JoinSet, time::sleep},
    tracing::{info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
        prost::Message as _,
    },
    yellowstone_grpc_tools::{
        config::{load as config_load, GrpcRequestToProto},
        create_shutdown,
        google_pubsub::{
            config::{Config, ConfigGrpc2PubSub},
            prom,
        },
        prom::{run_server as prometheus_run_server, GprcMessageKind},
        setup_tracing,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC Google Pub/Sub Tool")]
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
    /// Receive data from gRPC and send them to the Pub/Sub
    #[command(name = "grpc2pubsub")]
    Grpc2PubSub,
    /// Dev: subscribe to message from Pub/Sub and print them to Stdout
    #[command(name = "pubsub2stdout")]
    PubSub2Stdout {
        #[clap(long)]
        topic: String,
        #[clap(long)]
        subscription: String,
    },
    /// Dev: create Pub/Sub topic
    #[command(name = "pubsubTopicCreate")]
    PubSubTopicCreate { topic: String },
    /// Dev: delete Pub/Sub topic
    #[command(name = "pubsubTopicDelete")]
    PubSubTopicDelete { topic: String },
}

impl ArgsAction {
    async fn run(self, config: Config) -> anyhow::Result<()> {
        let shutdown = create_shutdown()?;
        let client = config.create_client().await?;

        match self {
            ArgsAction::Grpc2PubSub => {
                let config = config.grpc2pubsub.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2pubsub` section in config should be defined")
                })?;
                Self::grpc2pubsub(client, config, shutdown).await
            }
            ArgsAction::PubSub2Stdout {
                topic,
                subscription,
            } => Self::pubsub2stdout(client, topic, subscription, shutdown).await,
            ArgsAction::PubSubTopicCreate { topic } => {
                let topic = client.topic(&topic);
                if !topic.exists(None).await? {
                    topic.create(None, None).await?;
                }
                Ok(())
            }
            ArgsAction::PubSubTopicDelete { topic } => {
                client.topic(&topic).delete(None).await.map_err(Into::into)
            }
        }
    }

    async fn grpc2pubsub(
        client: Client,
        config: ConfigGrpc2PubSub,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        // Connect to Pub/Sub and create topic if not exists
        let topic = client.topic(&config.topic);
        if !topic.exists(None).await? {
            anyhow::ensure!(
                config.create_if_not_exists,
                "topic {} doesn't exists",
                config.topic
            );
            topic.create(None, None).await?;
        }
        let publisher = topic.new_publisher(Some(config.get_publisher_config()));

        // Create gRPC client & subscribe
        let mut client = GeyserGrpcClient::connect_with_timeout(
            config.endpoint,
            config.x_token,
            None,
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(5)),
            false,
        )
        .await?;
        let mut geyser = client.subscribe_once2(config.request.to_proto()).await?;

        // Receive-send loop
        let mut send_tasks = JoinSet::new();
        'outer: loop {
            let sleep = sleep(Duration::from_millis(config.bulk_max_wait_ms as u64));
            tokio::pin!(sleep);
            let mut messages = vec![];
            let mut prom_kind = vec![];
            while messages.len() < config.bulk_max_size {
                let message = tokio::select! {
                    _ = &mut shutdown => break 'outer,
                    _ = &mut sleep => break,
                    maybe_result = send_tasks.join_next() => match maybe_result {
                        Some(result) => {
                            result??;
                            continue;
                        }
                        None => tokio::select! {
                            _ = &mut shutdown => break 'outer,
                            _ = &mut sleep => break,
                            message = geyser.next() => message,
                        }
                    },
                    message = geyser.next() => message,
                }
                .transpose()?;
                let message = match message {
                    Some(message) => message,
                    None => break 'outer,
                };

                let payload = message.encode_to_vec();
                let message = match &message.update_oneof {
                    Some(value) => value,
                    None => unreachable!("Expect valid message"),
                };

                messages.push(PubsubMessage {
                    data: payload,
                    ..Default::default()
                });
                prom_kind.push(GprcMessageKind::from(message));
            }
            if messages.is_empty() {
                continue;
            }

            for (awaiter, prom_kind) in publisher
                .publish_bulk(messages)
                .await
                .into_iter()
                .zip(prom_kind.into_iter())
            {
                send_tasks.spawn(async move {
                    awaiter.get().await?;
                    prom::sent_inc(prom_kind);
                    Ok::<(), anyhow::Error>(())
                });
            }
        }
        warn!("shutdown received...");
        while let Some(result) = send_tasks.join_next().await {
            result??;
        }
        Ok(())
    }

    async fn pubsub2stdout(
        client: Client,
        topic: String,
        subscription: String,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        let topic = client.topic(&topic);
        if !topic.exists(None).await? {
            topic.create(None, None).await?;
        }
        let subscription = client.subscription(&subscription);
        if !subscription.exists(None).await? {
            let fqtn = topic.fully_qualified_name();
            let config = SubscriptionConfig::default();
            subscription.create(fqtn, config, None).await?;
        }

        let mut stream = subscription.subscribe(None).await?;
        loop {
            let msg = tokio::select! {
                _ = &mut shutdown => break,
                msg = stream.next() => match msg {
                    Some(msg) => msg,
                    None => break,
                }
            };

            msg.ack().await?;
            match SubscribeUpdate::decode(msg.message.data.as_ref()) {
                Ok(msg) => match msg.update_oneof {
                    Some(UpdateOneof::Account(msg)) => info!("#{}, account", msg.slot),
                    Some(UpdateOneof::Slot(msg)) => info!("#{}, slot", msg.slot),
                    Some(UpdateOneof::Transaction(msg)) => {
                        info!("#{}, transaction", msg.slot)
                    }
                    Some(UpdateOneof::Block(msg)) => info!("#{}, block", msg.slot),
                    Some(UpdateOneof::Ping(_)) => {}
                    Some(UpdateOneof::Pong(_)) => {}
                    Some(UpdateOneof::BlockMeta(msg)) => info!("#{}, blockmeta", msg.slot),
                    Some(UpdateOneof::Entry(msg)) => info!("#{}, entry", msg.slot),
                    None => {}
                },
                Err(error) => {
                    warn!("failed to decode message: {error}");
                }
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

    args.action.run(config).await
}
