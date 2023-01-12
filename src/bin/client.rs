use {
    clap::Parser,
    futures::stream::{once, StreamExt},
    solana_geyser_grpc::proto::{
        geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
    std::collections::HashMap,
    tonic::transport::{channel::ClientTlsConfig, Endpoint, Uri},
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

    #[clap(long)]
    /// Filter by Account Pubkey
    accounts_account: Vec<String>,

    #[clap(long)]
    /// Filter by Owner Pubkey
    accounts_owner: Vec<String>,

    #[clap(long)]
    /// Subscribe on slots updates
    slots: bool,

    #[clap(long)]
    /// Subscribe on transactions updates
    transactions: bool,

    #[clap(long)]
    /// Filter vote transactions
    transactions_vote: Option<bool>,

    #[clap(long)]
    /// Filter failed transactions
    transactions_failed: Option<bool>,

    #[clap(long)]
    /// Filter included account in transactions
    transactions_account_include: Vec<String>,

    #[clap(long)]
    /// Filter excluded account in transactions
    transactions_account_exclude: Vec<String>,

    #[clap(long)]
    /// Subscribe on block updates
    blocks: bool,

    #[clap(long)]
    /// Subscribe on block meta updates (without transactions)
    blocks_meta: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut accounts = HashMap::new();
    if args.accounts {
        accounts.insert(
            "client".to_owned(),
            SubscribeRequestFilterAccounts {
                account: args.accounts_account,
                owner: args.accounts_owner,
            },
        );
    }

    let mut slots = HashMap::new();
    if args.slots {
        slots.insert("client".to_owned(), SubscribeRequestFilterSlots {});
    }

    let mut transactions = HashMap::new();
    if args.transactions {
        transactions.insert(
            "client".to_string(),
            SubscribeRequestFilterTransactions {
                vote: args.transactions_vote,
                failed: args.transactions_failed,
                account_include: args.transactions_account_include,
                account_exclude: args.transactions_account_exclude,
            },
        );
    }

    let mut blocks = HashMap::new();
    if args.blocks {
        blocks.insert("client".to_owned(), SubscribeRequestFilterBlocks {});
    }

    let mut blocks_meta = HashMap::new();
    if args.blocks_meta {
        blocks_meta.insert("client".to_owned(), SubscribeRequestFilterBlocksMeta {});
    }

    let mut endpoint = Endpoint::from_shared(args.endpoint.clone())?;
    let uri: Uri = args.endpoint.parse()?;
    if uri.scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
    }
    let mut client = GeyserClient::connect(endpoint).await?;

    let request = SubscribeRequest {
        slots,
        accounts,
        transactions,
        blocks,
        blocks_meta,
    };
    println!("Going to send request: {:?}", request);

    let response = client.subscribe(once(async move { request })).await?;
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
