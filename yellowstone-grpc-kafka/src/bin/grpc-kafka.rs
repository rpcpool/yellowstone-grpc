use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::{
        future::{BoxFuture, FutureExt},
        stream::StreamExt,
    },
    rdkafka::{
        config::ClientConfig,
        consumer::{Consumer, StreamConsumer},
        message::Message,
        producer::{FutureProducer, FutureRecord},
    },
    sha2::{Digest, Sha256},
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::{
        signal::unix::{signal, SignalKind},
        task::JoinSet,
    },
    tracing::{debug, trace, warn},
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_kafka::{
        config::{Config, ConfigDedup, ConfigGrpc2Kafka, ConfigKafka2Grpc, GrpcRequestToProto},
        dedup::KafkaDedup,
        grpc::GrpcService,
        prom,
    },
    yellowstone_grpc_proto::{
        prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
        prost::Message as _,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
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
    /// Receive data from Kafka, deduplicate and send them back to Kafka
    Dedup,
    /// Receive data from gRPC and send them to the Kafka
    #[command(name = "grpc2kafka")]
    Grpc2Kafka,
    /// Receive data from Kafka and send them over gRPC
    #[command(name = "kafka2grpc")]
    Kafka2Grpc,
}

impl ArgsAction {
    async fn run(
        self,
        config: Config,
        kafka_config: ClientConfig,
        shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        match self {
            ArgsAction::Dedup => {
                let config = config.dedup.ok_or_else(|| {
                    anyhow::anyhow!("`dedup` section in config should be defined")
                })?;
                Self::dedup(kafka_config, config, shutdown).await
            }
            ArgsAction::Grpc2Kafka => {
                let config = config.grpc2kafka.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2kafka` section in config should be defined")
                })?;
                Self::grpc2kafka(kafka_config, config, shutdown).await
            }
            ArgsAction::Kafka2Grpc => {
                let config = config.kafka2grpc.ok_or_else(|| {
                    anyhow::anyhow!("`kafka2grpc` section in config should be defined")
                })?;
                Self::kafka2grpc(kafka_config, config, shutdown).await
            }
        }
    }

    async fn dedup(
        mut kafka_config: ClientConfig,
        config: ConfigDedup,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        // input
        let consumer: StreamConsumer = kafka_config.create()?;
        consumer.subscribe(&[&config.kafka_input])?;

        // output
        let kafka: FutureProducer = kafka_config
            .create()
            .context("failed to create kafka producer")?;

        // dedup
        let dedup = config.backend.create().await?;

        // input -> output loop
        let kafka_output = Arc::new(config.kafka_output);
        let mut send_tasks = JoinSet::new();
        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                maybe_result = send_tasks.join_next() => match maybe_result {
                    Some(result) => {
                        result??;
                        continue;
                    }
                    None => tokio::select! {
                        _ = &mut shutdown => break,
                        message = consumer.recv() => message,
                    }
                },
                message = consumer.recv() => message,
            }?;
            trace!(
                "received message with key: {:?}",
                message.key().and_then(|k| std::str::from_utf8(k).ok())
            );

            let (key, payload) = match (
                message
                    .key()
                    .and_then(|k| String::from_utf8(k.to_vec()).ok()),
                message.payload(),
            ) {
                (Some(key), Some(payload)) => (key, payload.to_vec()),
                _ => continue,
            };
            let (slot, hash, bytes) = match key
                .split_once('_')
                .and_then(|(slot, hash)| slot.parse::<u64>().ok().map(|slot| (slot, hash)))
                .and_then(|(slot, hash)| {
                    let mut bytes: [u8; 32] = [0u8; 32];
                    const_hex::decode_to_slice(hash, &mut bytes)
                        .ok()
                        .map(|()| (slot, hash, bytes))
                }) {
                Some((slot, hash, bytes)) => (slot, hash, bytes),
                _ => continue,
            };
            debug!("received message slot #{slot} with hash {hash}");

            let kafka = kafka.clone();
            let dedup = dedup.clone();
            let kafka_output = Arc::clone(&kafka_output);
            send_tasks.spawn(async move {
                if dedup.allowed(slot, bytes).await {
                    let record = FutureRecord::to(&kafka_output).key(&key).payload(&payload);
                    match kafka.send_result(record) {
                        Ok(future) => {
                            let result = future.await;
                            debug!("kafka send message with key: {key}, result: {result:?}");

                            result?.map_err(|(error, _message)| error)?;
                            Ok::<(), anyhow::Error>(())
                        }
                        Err(error) => Err(error.0.into()),
                    }
                } else {
                    Ok(())
                }
            });
            if send_tasks.len() >= config.kafka_queue_size {
                tokio::select! {
                    _ = &mut shutdown => break,
                    result = send_tasks.join_next() => {
                        if let Some(result) = result {
                            result??;
                        }
                    }
                }
            }
        }
        warn!("shutdown received...");
        while let Some(result) = send_tasks.join_next().await {
            result??;
        }
        Ok(())
    }

    async fn grpc2kafka(
        mut kafka_config: ClientConfig,
        config: ConfigGrpc2Kafka,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        // Connect to kafka
        let kafka: FutureProducer = kafka_config
            .create()
            .context("failed to create kafka producer")?;

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
        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                maybe_result = send_tasks.join_next() => match maybe_result {
                    Some(result) => {
                        let _ = result??;
                        continue;
                    }
                    None => tokio::select! {
                        _ = &mut shutdown => break,
                        message = geyser.next() => message,
                    }
                },
                message = geyser.next() => message,
            }
            .transpose()?;

            match message {
                Some(message) => {
                    if matches!(message.update_oneof, Some(UpdateOneof::Ping(_))) {
                        continue;
                    }

                    let slot = match &message.update_oneof {
                        Some(UpdateOneof::Account(msg)) => msg.slot,
                        Some(UpdateOneof::Slot(msg)) => msg.slot,
                        Some(UpdateOneof::Transaction(msg)) => msg.slot,
                        Some(UpdateOneof::Block(msg)) => msg.slot,
                        Some(UpdateOneof::Ping(_)) => unreachable!("Ping message not expected"),
                        Some(UpdateOneof::BlockMeta(msg)) => msg.slot,
                        Some(UpdateOneof::Entry(msg)) => msg.slot,
                        None => unreachable!("Expect valid message"),
                    };
                    let payload = message.encode_to_vec();
                    let hash = Sha256::digest(&payload);
                    let key = format!("{slot}_{}", const_hex::encode(hash));

                    let record = FutureRecord::to(&config.kafka_topic)
                        .key(&key)
                        .payload(&payload);

                    match kafka.send_result(record) {
                        Ok(future) => {
                            let _ = send_tasks.spawn(async move {
                                let result = future.await;
                                debug!("kafka send message with key: {key}, result: {result:?}");

                                Ok::<(i32, i64), anyhow::Error>(
                                    result?.map_err(|(error, _message)| error)?,
                                )
                            });
                            if send_tasks.len() >= config.kafka_queue_size {
                                tokio::select! {
                                    _ = &mut shutdown => break,
                                    result = send_tasks.join_next() => {
                                        if let Some(result) = result {
                                            result??;
                                        }
                                    }
                                }
                            }
                        }
                        Err(error) => return Err(error.0.into()),
                    }
                }
                None => break,
            }
        }
        warn!("shutdown received...");
        while let Some(result) = send_tasks.join_next().await {
            let _ = result??;
        }
        Ok(())
    }

    async fn kafka2grpc(
        mut kafka_config: ClientConfig,
        config: ConfigKafka2Grpc,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        let (grpc_tx, grpc_shutdown) = GrpcService::run(config.listen, config.channel_capacity)?;

        let consumer: StreamConsumer = kafka_config.create()?;
        consumer.subscribe(&[&config.kafka_topic])?;

        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                message = consumer.recv() => message?,
            };
            debug!(
                "received message with key: {:?}",
                message.key().and_then(|k| std::str::from_utf8(k).ok())
            );

            if let Some(payload) = message.payload() {
                match SubscribeUpdate::decode(payload) {
                    Ok(message) => {
                        let _ = grpc_tx.send(message);
                    }
                    Err(error) => {
                        warn!("failed to decode message: {error}");
                    }
                }
            }
        }

        warn!("shutdown received...");
        Ok(grpc_shutdown.await??)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup tracing
    let is_atty = atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr);
    let io_layer = tracing_subscriber::fmt::layer().with_ansi(is_atty);
    let level_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(io_layer)
        .with(level_layer)
        .try_init()?;

    // Parse args
    let args = Args::parse();
    let config = Config::load(&args.config).await?;

    // Run prometheus server
    prom::run_server(args.prometheus)?;

    // Create kafka config
    let mut kafka_config = ClientConfig::new();
    for (key, value) in config.kafka.iter() {
        kafka_config.set(key, value);
    }

    // Create shutdown signal
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {}
        };
    }
    .boxed();

    args.action.run(config, kafka_config, shutdown).await
}
