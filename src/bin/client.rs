use {
    clap::Parser,
    futures::stream::StreamExt,
    solana_geyser_grpc::grpc::proto::{geyser_client::GeyserClient, SubscribeRequest},
    tonic::Request,
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(short, long)]
    /// Stream all accounts
    any: bool,

    #[clap(short, long, conflicts_with = "any")]
    /// Filter by Account Pubkey
    accounts: Vec<String>,

    #[clap(short, long, conflicts_with = "any")]
    /// Filter by Owner Pubkey
    owner: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut client = GeyserClient::connect(args.endpoint).await?;
    let request = Request::new(SubscribeRequest {
        any: args.any,
        accounts: args.accounts,
        owners: args.owner,
    });
    let response = client.subscribe(request).await?;
    let mut stream = response.into_inner();

    println!("stream opened");
    while let Some(message) = stream.next().await {
        println!("new message: {:?}", message);
    }
    println!("stream closed");

    Ok(())
}
