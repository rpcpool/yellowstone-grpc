use {
    backoff::{future::retry, ExponentialBackoff},
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    log::{error, info},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError},
    yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdateAccount,
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

    /// Filter by Offset and Data, format: `offset,data in base58`
    #[clap(long)]
    accounts_memcmp: Vec<String>,

    /// Filter by Data size
    #[clap(long)]
    accounts_datasize: Option<u64>,

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

    // Resubscribe (only to slots) after
    #[clap(long)]
    resub: Option<u16>,
}

type SlotsFilterMap = HashMap<String, SubscribeRequestFilterSlots>;
type AccountFilterMap = HashMap<String, SubscribeRequestFilterAccounts>;
type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type BlocksFilterMap = HashMap<String, SubscribeRequestFilterBlocks>;
type BlocksMetaFilterMap = HashMap<String, SubscribeRequestFilterBlocksMeta>;

#[derive(Debug)]
#[allow(dead_code)]
pub struct AccountPretty {
    is_startup: bool,
    slot: u64,
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
    data: String,
    write_version: u64,
    txn_signature: String,
}

impl From<SubscribeUpdateAccount> for AccountPretty {
    fn from(
        SubscribeUpdateAccount {
            is_startup,
            slot,
            account,
        }: SubscribeUpdateAccount,
    ) -> Self {
        let account = account.expect("should be defined");
        Self {
            is_startup,
            slot,
            pubkey: Pubkey::try_from(account.pubkey).expect("valid pubkey"),
            lamports: account.lamports,
            owner: Pubkey::try_from(account.owner).expect("valid pubkey"),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: hex::encode(account.data),
            write_version: account.write_version,
            txn_signature: bs58::encode(account.txn_signature.unwrap_or_default()).into_string(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    let args = Args::parse();

    let mut accounts: AccountFilterMap = HashMap::new();
    if args.accounts {
        let mut filters = vec![];
        for filter in args.accounts_memcmp {
            match filter.split_once(',') {
                Some((offset, data)) => {
                    filters.push(SubscribeRequestFilterAccountsFilter {
                        filter: Some(AccountsFilterDataOneof::Memcmp(
                            SubscribeRequestFilterAccountsFilterMemcmp {
                                offset: offset
                                    .parse()
                                    .map_err(|_| anyhow::anyhow!("invalid offset"))?,
                                data: Some(AccountsFilterMemcmpOneof::Base58(
                                    data.trim().to_string(),
                                )),
                            },
                        )),
                    });
                }
                _ => anyhow::bail!("invalid memcmp"),
            }
        }
        if let Some(datasize) = args.accounts_datasize {
            filters.push(SubscribeRequestFilterAccountsFilter {
                filter: Some(AccountsFilterDataOneof::Datasize(datasize)),
            });
        }

        accounts.insert(
            "client".to_owned(),
            SubscribeRequestFilterAccounts {
                account: args.accounts_account,
                owner: args.accounts_owner,
                filters,
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

    let resub: u16 = args.resub.unwrap_or(0);

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let (endpoint, x_token) = (args.endpoint.clone(), args.x_token.clone());
        let (slots, accounts, transactions, blocks, blocks_meta) = (
            slots.clone(),
            accounts.clone(),
            transactions.clone(),
            blocks.clone(),
            blocks_meta.clone(),
        );

        async move {
            info!("Retry to connect to the server");
            let mut client = GeyserGrpcClient::connect(endpoint, x_token, None)?;
            let (mut subscribe_tx, mut stream) = client.subscribe().await?;
            subscribe_tx
                .send(SubscribeRequest {
                    slots,
                    accounts,
                    transactions,
                    blocks,
                    blocks_meta,
                })
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;

            info!("stream opened");
            let mut counter = 0;
            while let Some(message) = stream.next().await {
                match message {
                    Ok(msg) => {
                        #[allow(clippy::single_match)]
                        match msg.update_oneof {
                            Some(UpdateOneof::Account(account)) => {
                                let account: AccountPretty = account.into();
                                info!(
                                    "new account update: filters {:?}, account: {:#?}",
                                    msg.filters, account
                                );
                                continue;
                            }
                            _ => {}
                        }
                        info!("new message: {:?}", msg)
                    }
                    Err(error) => error!("error: {:?}", error),
                }

                // Example to illustrate how to resubscribe/update the subscription
                counter += 1;
                if counter == resub {
                    let mut new_slots: SlotsFilterMap = HashMap::new();
                    new_slots.insert("client".to_owned(), SubscribeRequestFilterSlots {});

                    subscribe_tx
                        .send(SubscribeRequest {
                            slots: new_slots.clone(),
                            accounts: HashMap::default(),
                            transactions: HashMap::default(),
                            blocks: HashMap::default(),
                            blocks_meta: HashMap::default(),
                        })
                        .await
                        .map_err(GeyserGrpcClientError::SubscribeSendError)?;
                }
            }
            info!("stream closed");
            Ok(())
        }
    })
    .await
    .map_err(Into::into)
}
