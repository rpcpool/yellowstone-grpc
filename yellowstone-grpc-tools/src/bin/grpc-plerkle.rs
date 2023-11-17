use {
    anyhow::Context,
    clap::Parser,
    flatbuffers::FlatBufferBuilder,
    futures::{future::try_join_all, stream::StreamExt},
    plerkle_messenger::select_messenger,
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::sync::{mpsc, Mutex},
    tracing::error,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
    yellowstone_grpc_tools::{
        config::load as config_load,
        create_shutdown,
        plerkle::{
            config::Config,
            prom,
            ser::{serialize_account, serialize_block, serialize_transaction},
            PlerkleStream,
        },
        prom::run_server as prometheus_run_server,
        setup_tracing,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC Plerkle Tool")]
struct Args {
    /// Path to config file
    #[clap(short, long)]
    config: String,

    /// Prometheus listen address
    #[clap(long)]
    prometheus: Option<SocketAddr>,
}

#[derive(Debug)]
struct MessageToSend<'a> {
    stream: PlerkleStream,
    builder: FlatBufferBuilder<'a>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;

    // Parse args
    let args = Args::parse();
    let config = config_load::<Config>(&args.config)
        .await
        .with_context(|| format!("failed to parse config from: {}", args.config))?;

    // Run prometheus server
    if let Some(address) = args.prometheus.or(config.prometheus) {
        prometheus_run_server(address)?;
    }

    // Spawn messenger workers
    let (tx, rx) = mpsc::unbounded_channel::<MessageToSend>();
    let rx = Arc::new(Mutex::new(rx));
    let mut workers = vec![];
    for _ in 0..config.num_workers {
        let mut messenger = select_messenger(config.messenger_config.clone())
            .await
            .context("failed to create a Messenger")?;
        messenger
            .add_streams(&[
                (PlerkleStream::Account.as_str(), config.account_stream_size),
                (
                    PlerkleStream::Transaction.as_str(),
                    config.transaction_stream_size,
                ),
                (PlerkleStream::Block.as_str(), config.block_stream_size),
                (PlerkleStream::Slot.as_str(), config.slot_stream_size),
            ])
            .await?;
        let rx = Arc::clone(&rx);
        workers.push(tokio::spawn(async move {
            loop {
                let message = {
                    let mut rx_lock = rx.lock().await;
                    match rx_lock.recv().await {
                        Some(msg) => msg,
                        None => return,
                    }
                };
                prom::message_queue_size_dec();

                let bytes = message.builder.finished_data();
                if let Err(error) = messenger.send(message.stream.as_str(), bytes).await {
                    error!(
                        "stream: `{:?}`, failed to send a message: {:?}",
                        message.stream, error
                    );
                    prom::sent_inc(message.stream, Err(()));
                } else {
                    prom::sent_inc(message.stream, Ok(()));
                }
            }
        }));
    }

    // Create gRPC client, subscribe and handle messages
    let mut client = GeyserGrpcClient::connect_with_timeout(
        config.endpoint.clone(),
        config.x_token.clone(),
        None,
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(5)),
        false,
    )
    .await
    .context("failed to connect go gRPC")?;
    let mut geyser = client.subscribe_once2(config.create_request()).await?;
    let mut shutdown = create_shutdown()?;
    loop {
        let message = tokio::select! {
            _ = &mut shutdown => break,
            message = geyser.next() => match message {
                Some(message) => message,
                None => break,
            }
        };

        let mut builder = FlatBufferBuilder::new();
        let stream = match message?
            .update_oneof
            .ok_or_else(|| anyhow::anyhow!("message is not valid"))?
        {
            UpdateOneof::Account(account) => {
                serialize_account(&mut builder, account);
                PlerkleStream::Account
            }
            UpdateOneof::Slot(_) => continue,
            UpdateOneof::Transaction(transaction) => {
                serialize_transaction(&mut builder, transaction);
                PlerkleStream::Transaction
            }
            UpdateOneof::Block(_) => continue,
            UpdateOneof::Ping(_) => continue,
            UpdateOneof::Pong(_) => continue,
            UpdateOneof::BlockMeta(block_meta) => {
                serialize_block(&mut builder, block_meta);
                PlerkleStream::Block
            }
            UpdateOneof::Entry(_) => continue,
        };

        let message = MessageToSend { stream, builder };
        tx.send(message)
            .context("failed to send message to workers")?;
        prom::message_queue_size_inc();
    }
    drop(tx);

    // Wait messenger woekers
    try_join_all(workers)
        .await
        .context("failed to wait workers")?;

    Ok(())
}
