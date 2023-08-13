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
        config::{Config, ConfigInput, ConfigOutput, GrpcRequestToProto},
        grpc::GrpcService,
    },
    yellowstone_grpc_proto::{
        prelude::{geyser_client::GeyserClient, SubscribeUpdate},
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
    /// Receive data from Kafka and send them over gRPC
    Read,
    /// Receive data from gRPC and send them to the Kafka
    Write,
}

impl ArgsAction {
    async fn run(self, config: Config, kafka_config: ClientConfig) -> anyhow::Result<()> {
        match self {
            ArgsAction::Read => {
                let output_config = match config.output {
                    Some(config) => config,
                    None => anyhow::bail!("`output` section in config should be defined"),
                };
                Self::read(kafka_config, output_config).await
            }
            ArgsAction::Write => {
                let input_config = match config.input {
                    Some(config) => config,
                    None => anyhow::bail!("`input` section in config should be defined"),
                };
                Self::write(kafka_config, input_config).await
            }
        }
    }

    async fn read(
        mut kafka_config: ClientConfig,
        output_config: ConfigOutput,
    ) -> anyhow::Result<()> {
        for (key, value) in output_config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        let grpc_tx = GrpcService::run(output_config.listen, output_config.channel_capacity)?;

        let consumer: StreamConsumer = kafka_config.create()?;
        consumer.subscribe(&[&output_config.kafka_topic])?;

        loop {
            let message = consumer.recv().await?;
            debug!(
                "received message with key: {:?}",
                message
                    .key()
                    .and_then(|k| k.try_into().ok())
                    .map(u64::from_be_bytes)
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

    async fn write(
        mut kafka_config: ClientConfig,
        input_config: ConfigInput,
    ) -> anyhow::Result<()> {
        for (key, value) in input_config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        // Connect to kafka
        let kafka: FutureProducer = kafka_config
            .create()
            .context("failed to create kafka producer")?;

        // Create gRPC client
        let mut endpoint = Channel::from_shared(input_config.endpoint)?;
        if endpoint.uri().scheme_str() == Some("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
        }
        let channel = endpoint.connect().await?;
        let x_token: Option<AsciiMetadataValue> = match input_config.x_token {
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
        subscribe_tx.send(input_config.request.to_proto()).await?;
        let response: Response<Streaming<SubscribeUpdate>> = client.subscribe(subscribe_rx).await?;
        let mut geyser = response.into_inner().boxed();

        // Receive-send loop
        let mut kid = 0u64;
        let mut send_tasks = JoinSet::new();
        loop {
            let message = if send_tasks.is_empty() {
                geyser.next().await
            } else {
                tokio::select! {
                    // always should be Some(x)
                    maybe_result = send_tasks.join_next() => {
                        if let Some(result) = maybe_result {
                            let _ = result??;
                        }
                        continue;
                    }
                    message = geyser.next() => message
                }
            }
            .transpose()?;

            match message {
                Some(message) => {
                    kid = kid.wrapping_add(1);

                    let key = kid.to_be_bytes();
                    let payload = message.encode_to_vec();
                    let record = FutureRecord::to(&input_config.kafka_topic)
                        .key(&key)
                        .payload(&payload);

                    match kafka.send_result(record) {
                        Ok(future) => {
                            let _ = send_tasks.spawn(async move {
                                let result = future.await;
                                tracing::debug!("kafka send result: {result:?}");

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
