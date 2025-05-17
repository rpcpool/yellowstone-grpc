use {
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    log::info,
    solana_sdk::pubkey::Pubkey,
    std::{env, str::FromStr},
    tokio::time::{interval, Duration},
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeAccountRequest,
            SubscribeRequest, SubscribeRequestFilterSlots, SubscribeRequestPing,
            SubscribeUpdatePong, SubscribeUpdateSlot,
        },
        GeyserGrpcClient, SubscribeAccountRequestAction, SubscribeAccountUpdateMessage,
    },
    yellowstone_grpc_proto::geyser::SubscribeAccountRequestActionInsert,
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    /// Service endpoint
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long)]
    accounts: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    let (mut subscribe_tx, mut stream) = client.subscribe_account().await?;

    let accounts = args.accounts.clone();

    futures::try_join!(
        async move {
            subscribe_tx
                .send(SubscribeAccountRequest {
                    action: Some(SubscribeAccountRequestAction::Insert(
                        SubscribeAccountRequestActionInsert { accounts: accounts },
                    )),
                })
                .await?;
            anyhow::Ok(())
        },
        async move {
            while let Some(Ok(message)) = stream.next().await {
                if let Some(message) = message.message {
                    match message {
                        SubscribeAccountUpdateMessage::Ping(_) => {}
                        SubscribeAccountUpdateMessage::Account(message) => {
                            if let Some(account) = message.account {
                                info!(
                                    "on_account_update, address: {:?}, owner: {:?}, slot: {:?}",
                                    Pubkey::new_from_array(*arrayref::array_ref![
                                        account.pubkey,
                                        0,
                                        32
                                    ]),
                                    Pubkey::new_from_array(*arrayref::array_ref![
                                        account.owner,
                                        0,
                                        32
                                    ]),
                                    message.slot,
                                );
                            };
                        }
                        SubscribeAccountUpdateMessage::Pong(_) => {}
                    }
                }
            }
            anyhow::Ok(())
        }
    )?;

    Ok(())
}
