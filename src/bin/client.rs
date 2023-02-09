use {
    backoff::{future::retry, ExponentialBackoff},
    clap::Parser,
    futures::stream::{once, StreamExt},
    solana_geyser_grpc::proto::{
        geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate,
    },
    std::collections::HashMap,
    tonic::{
        codec::Streaming,
        metadata::{Ascii, MetadataValue},
        service::interceptor::InterceptedService,
        transport::{channel::ClientTlsConfig, Channel, Uri},
        Request, Response, Status,
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

struct RetryChannel {
    x_token: Option<MetadataValue<Ascii>>,
    channel: Channel,
}

impl RetryChannel {
    /// Establish a channel to tonic endpoint
    /// The channel does not attempt to connect to the endpoint until first use
    pub fn new(endpoint: String, is_https: bool, x_token: Option<String>) -> anyhow::Result<Self> {
        let mut endpoint = Channel::from_shared(endpoint)?;
        if is_https {
            endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
        }
        let channel = endpoint.connect_lazy();
        let x_token_inner: Option<MetadataValue<_>> =
            x_token.map(|token| token.parse()).transpose()?;

        Ok(Self {
            x_token: x_token_inner,
            channel,
        })
    }

    /// Create a new GeyserClient client with Auth interceptor
    /// Clients require `&mut self`, due to `Tonic::transport::Channel` limitations, however
    /// creating new clients is cheap and thus can be used as a work around for ease of use.
    pub fn client(&self) -> RetryClient<impl FnMut(Request<()>) -> InterceptedRequestResult + '_> {
        let client = GeyserClient::with_interceptor(
            self.channel.clone(),
            move |mut req: tonic::Request<()>| {
                if let Some(x_token) = self.x_token.clone() {
                    req.metadata_mut().insert("x-token", x_token);
                }
                Ok(req)
            },
        );
        RetryClient { client }
    }

    pub async fn subscribe_retry(
        &self,
        slots: &SlotsFilterMap,
        accounts: &AccountFilterMap,
        transactions: &TransactionsFilterMap,
        blocks: &BlocksFilterMap,
        blocks_meta: &BlocksMetaFilterMap,
    ) -> anyhow::Result<()> {
        // The default exponential backoff strategy intervals:
        // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
        // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
        retry(ExponentialBackoff::default(), move || async {
            println!("Retry to connect to the server");
            let mut client = self.client();
            client
                .subscribe(slots, accounts, transactions, blocks, blocks_meta)
                .await?;
            Ok(())
        })
        .await
    }
}

type InterceptedRequestResult = std::result::Result<Request<()>, Status>;

pub struct RetryClient<F: FnMut(Request<()>) -> InterceptedRequestResult> {
    client: GeyserClient<InterceptedService<Channel, F>>,
}

type SlotsFilterMap = HashMap<String, SubscribeRequestFilterSlots>;
type AccountFilterMap = HashMap<String, SubscribeRequestFilterAccounts>;
type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type BlocksFilterMap = HashMap<String, SubscribeRequestFilterBlocks>;
type BlocksMetaFilterMap = HashMap<String, SubscribeRequestFilterBlocksMeta>;

impl<F: FnMut(Request<()>) -> InterceptedRequestResult> RetryClient<F> {
    async fn subscribe(
        &mut self,
        slots: &SlotsFilterMap,
        accounts: &AccountFilterMap,
        transactions: &TransactionsFilterMap,
        blocks: &BlocksFilterMap,
        blocks_meta: &BlocksMetaFilterMap,
    ) -> anyhow::Result<()> {
        let request = SubscribeRequest {
            slots: slots.clone(),
            accounts: accounts.clone(),
            transactions: transactions.clone(),
            blocks: blocks.clone(),
            blocks_meta: blocks_meta.clone(),
        };
        println!("Going to send request: {:?}", request);

        let response: Response<Streaming<SubscribeUpdate>> =
            self.client.subscribe(once(async move { request })).await?;
        let mut stream: Streaming<SubscribeUpdate> = response.into_inner();

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

    let mut slots: SlotsFilterMap = HashMap::new();
    if args.slots {
        slots.insert("client".to_owned(), SubscribeRequestFilterSlots {});
    }

    let mut transactions: TransactionsFilterMap = HashMap::new();
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

    let mut blocks: BlocksFilterMap = HashMap::new();
    if args.blocks {
        blocks.insert("client".to_owned(), SubscribeRequestFilterBlocks {});
    }

    let mut blocks_meta: BlocksMetaFilterMap = HashMap::new();
    if args.blocks_meta {
        blocks_meta.insert("client".to_owned(), SubscribeRequestFilterBlocksMeta {});
    }

    let is_https = args.endpoint.parse::<Uri>()?.scheme_str() == Some("https");

    // Client with retry policy
    let retry_channel: RetryChannel =
        RetryChannel::new(args.endpoint.clone(), is_https, args.x_token.clone())?;
    retry_channel
        .subscribe_retry(&slots, &accounts, &transactions, &blocks, &blocks_meta)
        .await?;

    Ok(())
}
