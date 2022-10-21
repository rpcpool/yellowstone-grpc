use {
    clap::Parser,
    futures::stream::StreamExt,
    solana_geyser_grpc::proto::{
        geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
    std::collections::HashMap,
    tonic::Request,
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(long)]
    /// Subscribe on accounts updates
    accounts: bool,

    #[clap(short, long)]
    /// Filter by Account Pubkey
    account: Vec<String>,

    #[clap(short, long)]
    /// Filter by Owner Pubkey
    owner: Vec<String>,

    #[clap(long)]
    /// Subscribe on slots updates
    slots: bool,

    #[clap(long)]
    /// Subscribe on transactions updates
    transactions: bool,

    #[clap(short, long)]
    /// Filter vote transactions
    vote: bool,

    #[clap(short, long)]
    /// Filter failed transactions
    failed: bool,

    #[clap(long)]
    /// Subscribe on block updates
    blocks: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut accounts = HashMap::new();
    if args.accounts {
        accounts.insert(
            "client".to_owned(),
            SubscribeRequestFilterAccounts {
                any: args.account.is_empty() && args.owner.is_empty(),
                account: args.account,
                owner: args.owner,
            },
        );
    }

    let mut slots = HashMap::new();
    if args.slots {
        slots.insert(
            "client".to_owned(),
            SubscribeRequestFilterSlots { any: true },
        );
    }

    let mut transactions = HashMap::new();
    if args.transactions {
        transactions.insert(
            "client".to_string(),
            SubscribeRequestFilterTransactions {
                any: true,
                vote: args.vote,
                failed: args.failed,
            },
        );
    }

    let mut blocks = HashMap::new();
    if args.blocks {
        blocks.insert(
            "client".to_owned(),
            SubscribeRequestFilterBlocks { any: true },
        );
    }

    let mut client = GeyserClient::connect(args.endpoint).await?;
    let request = Request::new(SubscribeRequest {
        slots,
        accounts,
        transactions,
        blocks,
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
