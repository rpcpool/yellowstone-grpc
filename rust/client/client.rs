use {
    backoff::{future::retry, ExponentialBackoff},
    clap::Parser,
    env_logger,
    futures::stream::{once, StreamExt},
    log::{error, info, warn},
    solana_geyser_grpc::proto::{
        geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate,
    },
    std::collections::HashMap,
    thiserror::Error,
    tonic::{
        codec::Streaming,
        metadata::{Ascii, MetadataValue},
        service::interceptor::InterceptedService,
        transport::{channel::ClientTlsConfig, Channel, Endpoint},
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

const XTOKEN_LENGTH: usize = 28;

#[derive(Debug, Error)]
pub enum Error {
    #[error("XToken: {0}")]
    XToken(String),

    #[error("Invalid URI {0}")]
    InvalidUri(String),

    #[error("RetrySubscribe")]
    RetrySubscribe(anyhow::Error),
}

#[derive(Debug)]
struct RetryChannel {
    x_token: Option<MetadataValue<Ascii>>,
    channel: Channel,
}

impl RetryChannel {
    /// Establish a channel to tonic endpoint
    /// The channel does not attempt to connect to the endpoint until first use
    pub fn new(endpoint_str: String, x_token_str: Option<String>) -> Result<Self, Error> {
        let endpoint: Endpoint;
        let x_token: Option<MetadataValue<Ascii>>;

        // the client should fail immediately if the x-token is invalid
        match x_token_str {
            // x-token length is 28
            Some(token_str) if token_str.len() == XTOKEN_LENGTH => {
                match token_str.parse::<MetadataValue<Ascii>>() {
                    Ok(metadata) => x_token = Some(metadata),
                    Err(_) => return Err(Error::XToken(token_str)),
                }
            }
            Some(token_str) => return Err(Error::XToken(token_str)),
            None => {
                x_token = None;
                warn!("x_token is None");
            }
        }

        let res = Channel::from_shared(endpoint_str.clone());
        match res {
            Err(e) => {
                error!("{}", e);
                return Err(Error::InvalidUri(endpoint_str));
            }
            Ok(_endpoint) => {
                if _endpoint.uri().scheme_str() == Some("https") {
                    match _endpoint.tls_config(ClientTlsConfig::new()) {
                        Err(e) => {
                            error!("{}", e);
                            return Err(Error::InvalidUri(endpoint_str));
                        }
                        Ok(e) => endpoint = e,
                    }
                } else {
                    endpoint = _endpoint;
                }
            }
        }
        let channel = endpoint.connect_lazy();

        Ok(Self { x_token, channel })
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
            info!("Retry to connect to the server");
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
        info!("Going to send request: {:?}", request);

        let response: Response<Streaming<SubscribeUpdate>> =
            self.client.subscribe(once(async move { request })).await?;
        let mut stream: Streaming<SubscribeUpdate> = response.into_inner();

        info!("stream opened");
        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => info!("new message: {:?}", message),
                Err(error) => error!("error: {:?}", error),
            }
        }
        info!("stream closed");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    ::std::env::set_var("RUST_LOG", "info");
    env_logger::init();

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

    // Client with retry policy
    let res: Result<RetryChannel, Error> =
        RetryChannel::new(args.endpoint.clone(), args.x_token.clone());
    if let Err(e) = res {
        error!("Error: {}", e);
        return Err(e);
    }

    let res: anyhow::Result<()> = res
        .unwrap()
        .subscribe_retry(&slots, &accounts, &transactions, &blocks, &blocks_meta)
        .await;
    if let Err(e) = res {
        error!("Error: {}", e);
        return Err(Error::RetrySubscribe(e));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Error, RetryChannel};

    #[tokio::test]
    async fn test_channel_https_success() {
        let endpoint = "https://ams17.rpcpool.com:443".to_owned();
        let x_token = "1000000000000000000000000007".to_owned();
        let res: Result<RetryChannel, Error> = RetryChannel::new(endpoint, Some(x_token));
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_channel_http_success() {
        let endpoint = "http://127.0.0.1:10000".to_owned();
        let x_token = "1234567891012141618202224268".to_owned();
        let res: Result<RetryChannel, Error> = RetryChannel::new(endpoint, Some(x_token));
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_channel_invalid_token_some() {
        let endpoint = "http://127.0.0.1:10000".to_owned();
        let x_token = "123".to_owned();
        let res: Result<RetryChannel, Error> = RetryChannel::new(endpoint, Some(x_token.clone()));
        assert!(res.is_err());

        if let Err(Error::XToken(_)) = res {
            assert!(true);
        } else {
            assert!(false);
        }
    }

    #[tokio::test]
    async fn test_channel_invalid_token_none() {
        let endpoint = "http://127.0.0.1:10000".to_owned();
        let x_token = None;
        // only show warning in log
        let res: Result<RetryChannel, Error> = RetryChannel::new(endpoint, x_token);
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_channel_invalid_uri() {
        let endpoint = "sites/files/images/picture.png".to_owned();
        let x_token = "1234567891012141618202224268".to_owned();
        assert!(matches!(
            RetryChannel::new(endpoint, Some(x_token)),
            Err(Error::InvalidUri(_))
        ));
    }
}
