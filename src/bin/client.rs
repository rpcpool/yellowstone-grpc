use {
    clap::Parser,
    futures::stream::{once, StreamExt},
    solana_geyser_grpc::proto::{
        geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
    std::collections::HashMap,
    tonic::{
        metadata::MetadataValue,
        transport::{channel::ClientTlsConfig, Endpoint, Uri},
        Request,
    },
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    /// Subscribe on accounts updates
    #[clap(long)]
    accounts: bool,

    /// Filter by Account Pubkey
    #[clap(long)]
    accounts_account: Vec<String>,

    /// Filter by Owner Pubkey
    #[clap(long)]
    accounts_owner: Vec<String>,

    /// Subscribe on slots updates
    #[clap(long)]
    slots: bool,

    /// Subscribe on transactions updates
    #[clap(long)]
    transactions: bool,

    /// Filter vote transactions
    #[clap(long)]
    transactions_vote: Option<bool>,

    /// Filter failed transactions
    #[clap(long)]
    transactions_failed: Option<bool>,

    /// Filter by transaction signature
    #[clap(long)]
    transactions_signature: Option<String>,

    /// Filter included account in transactions
    #[clap(long)]
    transactions_account_include: Vec<String>,

    /// Filter excluded account in transactions
    #[clap(long)]
    transactions_account_exclude: Vec<String>,

    /// Subscribe on block updates
    #[clap(long)]
    blocks: bool,

    /// Subscribe on block meta updates (without transactions)
    #[clap(long)]
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
                signature: args.transactions_signature,
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
    // let mut client = GeyserClient::connect(endpoint).await?;
    let token: Option<MetadataValue<_>> = args.x_token.map(|token| token.parse()).transpose()?;
    let conn = endpoint.connect().await?;
    let mut client = GeyserClient::with_interceptor(conn, move |mut req: Request<()>| {
        if let Some(token) = token.clone() {
            req.metadata_mut().insert("x-token", token);
        }
        Ok(req)
    });

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
