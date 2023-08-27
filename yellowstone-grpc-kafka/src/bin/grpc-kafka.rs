use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::{channel::mpsc, sink::SinkExt, stream::StreamExt},
    rdkafka::{
        config::ClientConfig,
        consumer::{Consumer, StreamConsumer},
        message::Message,
        producer::{FutureProducer, FutureRecord},
    },
    std::collections::BTreeMap,
    tokio::task::JoinSet,
    tonic::{
        codec::Streaming,
        metadata::AsciiMetadataValue,
        transport::{Channel, ClientTlsConfig},
        Request, Response,
    },
    tracing::{debug, warn},
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
    yellowstone_grpc_kafka::{
        config::{Config, ConfigGrpc2Kafka, ConfigKafka2Grpc, GrpcRequestToProto},
        grpc::GrpcService,
    },
    yellowstone_grpc_proto::{
        prelude::{geyser_client::GeyserClient, subscribe_update::UpdateOneof, SubscribeUpdate},
        prost::Message as _,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    /// Path to config file
    #[clap(short, long)]
    config: String,

    #[command(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsAction {
    // Receive data from Kafka, deduplicate and send them back to Kafka
    // TODO: Dedup
    /// Receive data from gRPC and send them to the Kafka
    #[command(name = "grpc2kafka")]
    Grpc2Kafka,
    /// Receive data from Kafka and send them over gRPC
    #[command(name = "kafka2grpc")]
    Kafka2Grpc,
}

impl ArgsAction {
    async fn run(self, config: Config, kafka_config: ClientConfig) -> anyhow::Result<()> {
        match self {
            ArgsAction::Grpc2Kafka => {
                let config = config.grpc2kafka.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2kafka` section in config should be defined")
                })?;
                Self::grpc2kafka(kafka_config, config).await
            }
            ArgsAction::Kafka2Grpc => {
                let config = config.kafka2grpc.ok_or_else(|| {
                    anyhow::anyhow!("`kafka2grpc` section in config should be defined")
                })?;
                Self::kafka2grpc(kafka_config, config).await
            }
        }
    }

    async fn grpc2kafka(
        mut kafka_config: ClientConfig,
        config: ConfigGrpc2Kafka,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        // Connect to kafka
        let kafka: FutureProducer = kafka_config
            .create()
            .context("failed to create kafka producer")?;

        // Create gRPC client
        let mut endpoint = Channel::from_shared(config.endpoint)?;
        if endpoint.uri().scheme_str() == Some("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
        }
        let channel = endpoint.connect().await?;
        let x_token: Option<AsciiMetadataValue> = match config.x_token {
            Some(x_token) => Some(x_token.try_into()?),
            None => None,
        };
        let mut client = GeyserClient::with_interceptor(channel, move |mut req: Request<()>| {
            if let Some(x_token) = x_token.clone() {
                req.metadata_mut().insert("x-token", x_token);
            }
            Ok(req)
        });

        // Subscribe on Geyser events
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        subscribe_tx.send(config.request.to_proto()).await?;
        let response: Response<Streaming<SubscribeUpdate>> = client.subscribe(subscribe_rx).await?;
        let mut geyser = response.into_inner().boxed();

        // Receive-send loop
        let mut kid = Grpc2KafkaKey::default();
        let mut send_tasks = JoinSet::new();
        loop {
            let message = tokio::select! {
                maybe_result = send_tasks.join_next() => match maybe_result {
                    Some(result) => {
                        let _ = result??;
                        continue;
                    }
                    None => geyser.next().await
                },
                message = geyser.next() => message,
            }
            .transpose()?;

            match message {
                Some(message) => {
                    if matches!(message.update_oneof, Some(UpdateOneof::Ping(_))) {
                        continue;
                    }

                    let key = kid.get_key(&message);
                    let payload = message.encode_to_vec();
                    let record = FutureRecord::to(&config.kafka_topic)
                        .key(&key)
                        .payload(&payload);

                    match kafka.send_result(record) {
                        Ok(future) => {
                            let _ = send_tasks.spawn(async move {
                                let result = future.await;
                                tracing::debug!(
                                    "kafka send message with key: {key:?}, result: {result:?}"
                                );

                                Ok::<(i32, i64), anyhow::Error>(
                                    result?.map_err(|(error, _message)| error)?,
                                )
                            });
                        }
                        Err(error) => return Err(error.0.into()),
                    }
                }
                None => {
                    while let Some(result) = send_tasks.join_next().await {
                        let _ = result??;
                    }
                    return Ok(());
                }
            }
        }
    }

    async fn kafka2grpc(
        mut kafka_config: ClientConfig,
        config: ConfigKafka2Grpc,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        let grpc_tx = GrpcService::run(config.listen, config.channel_capacity)?;

        let consumer: StreamConsumer = kafka_config.create()?;
        consumer.subscribe(&[&config.kafka_topic])?;

        loop {
            let message = consumer.recv().await?;
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
    }
}

#[derive(Debug, Default)]
struct Grpc2KafkaKey {
    slots: BTreeMap<u64, u64>,
}

impl Grpc2KafkaKey {
    fn get_key(&mut self, message: &SubscribeUpdate) -> String {
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

        // remove oudated
        loop {
            match self.slots.keys().next().cloned() {
                Some(kslot) if kslot < slot - 32 => self.slots.remove(&kslot),
                _ => break,
            };
        }

        let id = self.slots.entry(slot).or_default();
        *id = id.wrapping_add(1);
        format!("{slot}_{id}")
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

    // Create kafka config
    let mut kafka_config = ClientConfig::new();
    for (key, value) in config.kafka.iter() {
        kafka_config.set(key, value);
    }

    args.action.run(config, kafka_config).await
}
