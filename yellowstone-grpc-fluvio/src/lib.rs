use std::{collections::HashMap, time::{Duration, Instant}};

use bytes::{BytesMut, Bytes};
use config::GeyserConfig;
use log::info;
use tokio_stream::wrappers::ReceiverStream;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder};
use yellowstone_grpc_proto::{geyser::{subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions, SubscribeRequestPing}, prelude::SubscribeUpdate, prost::Message, tonic};
use futures_util::{sink::SinkExt, Stream, StreamExt};
pub mod config;



#[derive(Debug, thiserror::Error)]
#[error("Geyser Error: {0}")]
pub struct GeyserError(tonic::Status);

pub async fn spawn_geyser_source(config: &GeyserConfig) -> impl Stream<Item = Result<SubscribeUpdate, GeyserError>> {
    let mut geyser = GeyserGrpcBuilder::from_shared(config.endpoint.clone())
        .expect("geyser builder")
        .x_token(config.x_token.clone())
        .expect("geyser x_token")
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("geyser tls_config")
        .connect()
        .await
        .expect("geyser connect");
    let request = SubscribeRequest {
        accounts: HashMap::from([("f1".to_owned(), SubscribeRequestFilterAccounts::default())]),
        transactions: HashMap::from([("f1".to_owned(), SubscribeRequestFilterTransactions::default())]),
        ..Default::default()
    };
    let (mut sink, mut source) = geyser.subscribe_with_request(Some(request)).await.expect("geyser subscribe");
    let (data_tx, data_rx) = tokio::sync::mpsc::channel(10000);
    tokio::task::spawn(async move {
        let mut ping_cnt = 0;
        while let Some(update) = source.next().await {
            match update {
                Ok(update) => {
                    match &update.update_oneof {
                        Some(oneof) => {
                            match oneof {
                                UpdateOneof::Account(_) | UpdateOneof::Transaction(_) => {}
                                UpdateOneof::Ping(_) => {
                                    log::info!("Received ping, sending pong");
                                    if sink
                                        .send(SubscribeRequest {
                                            ping: Some(SubscribeRequestPing { id: ping_cnt }),
                                            ..Default::default()
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                    ping_cnt += 1;
                                }
                                _ => continue,
                            }   
                        }
                        None => continue,
                    }
                    if data_tx.send(Ok(update)).await.is_err() {
                        break;
                    }
                },
                Err(err) => {
                    let _ = data_tx.send(Err(GeyserError(err))).await;
                    break;
                }
            }
        }
    });

    ReceiverStream::new(data_rx)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Account {
        pubkey: [u8; 32],
        tx_sig: [u8; 64],
    },
    Transaction {
        sig: [u8; 64] 
    }
}

impl Key {
    pub fn as_bytes(&self) -> Bytes {
        match self {
            Key::Account { pubkey, tx_sig } => {
                let mut buf = BytesMut::with_capacity(32 + 64);
                buf.extend_from_slice(pubkey);
                buf.extend_from_slice(tx_sig);
                buf.freeze()
            }
            Key::Transaction { sig } => {
                let mut buf = BytesMut::with_capacity(64);
                buf.extend_from_slice(sig);
                buf.freeze()
            }
        }
    }
}

fn extract_key_from_subscribe_update(update: &SubscribeUpdate) -> Option<Key> {
    let oneof = update.update_oneof.as_ref()?;
    match oneof {
        UpdateOneof::Account(account) => {
            account
                .account
                .as_ref()
                .and_then(|account| {
                    account.txn_signature.as_ref().map(|sig| (&account.pubkey, sig))
                })
                .map(|(pubkey, txn_sig)| {
                    let mut stack_pubkey: [u8; 32] = [0; 32];
                    stack_pubkey.copy_from_slice(&pubkey);
                    let mut tx_sig: [u8; 64] = [0; 64];
                    tx_sig.copy_from_slice(&txn_sig);
                    Key::Account { pubkey: stack_pubkey, tx_sig }
                })
        }
        UpdateOneof::Transaction(tx) => {
            tx.transaction.as_ref().map(|tx| {
                let mut sig: [u8; 64] = [0; 64];
                sig.copy_from_slice(&tx.signature);
                Key::Transaction { sig }
            })
        }
        _ => None,
    }
}

pub fn setup_logger() {
    env_logger::Builder::from_default_env()
        .init();
}

pub async fn fluvio_geyser_producer<G>(
    producer: fluvio::TopicProducerPool,
    mut geyser_stream: G,
) 
    where G: Stream<Item = Result<SubscribeUpdate, GeyserError>> + Unpin + Send
{

    let mut last_print = Instant::now();
    let mut data_sent_per_seconds = 0;
    while let Some(data) = geyser_stream.next().await {
        match data {
            Ok(update) => {
                let key = match extract_key_from_subscribe_update(&update) {
                    Some(key) => key,
                    None => continue,
                };
                // log::info!("Received update: {:?}", key);
                let mut buf = BytesMut::with_capacity(update.encoded_len());
                update.encode(&mut buf).expect("encode");
                let key_bytes = key.as_bytes();
                producer.send(key_bytes, buf).await.expect("send");
                data_sent_per_seconds += 1;
            }
            Err(err) => {
                log::error!("Error receiving update: {:?}", err);
                break;
            }
        }
        if last_print.elapsed() >= Duration::from_secs(1) {
            last_print = Instant::now();
            info!("Data sent per second: {}", data_sent_per_seconds);
            data_sent_per_seconds = 0;
        }
    }
}