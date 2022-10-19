use {
    clap::Parser,
    futures::stream::StreamExt,
    solana_geyser_grpc::grpc::proto::{
        geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestAccounts,
        SubscribeRequestSlots,
    },
    tonic::Request,
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(short, long)]
    /// Subscribe on slots updates
    slots: bool,

    #[clap(short, long)]
    /// Filter by Account Pubkey
    account: Vec<String>,

    #[clap(short, long)]
    /// Filter by Owner Pubkey
    owner: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut client = GeyserClient::connect(args.endpoint).await?;
    let request = Request::new(SubscribeRequest {
        slots: Some(SubscribeRequestSlots {
            enabled: args.slots,
        }),
        accounts: vec![SubscribeRequestAccounts {
            filter: "client".to_owned(),
            account: args.account,
            owner: args.owner,
        }],
    });
    let response = client.subscribe(request).await?;
    let mut stream = response.into_inner();

    println!("stream opened");
    while let Some(message) = stream.next().await {
        match message {
            Ok(message) => println!("new message: {:?}", message),
            Err(error) => eprintln!("error: {:?}", error),
        }
    }
    println!("stream closed");

    Ok(())
}
