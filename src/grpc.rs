use {
    crate::{
        config::ConfigGrpc,
        filters::AccountsFilter,
        grpc::proto::{
            geyser_server::{Geyser, GeyserServer},
            subscribe_update::UpdateOneof,
            SubscribeRequest, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
        },
        prom::CONNECTIONS_TOTAL,
    },
    log::*,
    std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    },
    tokio::sync::{mpsc, oneshot},
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        codec::CompressionEncoding,
        transport::server::{Server, TcpIncoming},
        Request, Response, Result as TonicResult, Status,
    },
};

pub mod proto {
    tonic::include_proto!("geyser");
}

#[derive(Debug)]
struct ClientConnection {
    id: usize,
    accounts_filter: AccountsFilter,
    stream_tx: mpsc::Sender<TonicResult<SubscribeUpdate>>,
}

#[derive(Debug)]
pub struct GrpcService {
    config: ConfigGrpc,
    subscribe_id: AtomicUsize,
    new_clients_tx: mpsc::UnboundedSender<ClientConnection>,
}

impl GrpcService {
    pub fn create(
        config: ConfigGrpc,
    ) -> Result<
        (mpsc::UnboundedSender<SubscribeUpdate>, oneshot::Sender<()>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // Bind service address
        let incoming = TcpIncoming::new(
            config.address,
            true, // tcp_nodelay
            None, // tcp_keepalive
        )?;

        // Create Server
        let (new_clients_tx, new_clients_rx) = mpsc::unbounded_channel();
        let service = GeyserServer::new(Self {
            config,
            subscribe_id: AtomicUsize::new(0),
            new_clients_tx,
        })
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

        // Run filter and send loop
        let (update_channel_tx, update_channel_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move { Self::send_loop(update_channel_rx, new_clients_rx).await });

        // Run Server
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(5)))
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        Ok((update_channel_tx, shutdown_tx))
    }

    async fn send_loop(
        mut update_channel_rx: mpsc::UnboundedReceiver<SubscribeUpdate>,
        mut new_clients_rx: mpsc::UnboundedReceiver<ClientConnection>,
    ) {
        let mut clients: HashMap<usize, ClientConnection> = HashMap::new();
        loop {
            tokio::select! {
                Some(message) = update_channel_rx.recv() => {
                    let mut ids_full = vec![];
                    let mut ids_closed = vec![];

                    for client in clients.values() {
                        let message = match &message.update_oneof {
                            Some(UpdateOneof::Account(SubscribeUpdateAccount {
                                account: Some(SubscribeUpdateAccountInfo { pubkey, owner,.. }),
                                ..
                            })) if !client.accounts_filter.is_account_selected(pubkey, owner) => {
                                continue;
                            }
                            _ => message.clone(),
                        };

                        match client.stream_tx.try_send(Ok(message)) {
                            Ok(()) => {},
                            Err(mpsc::error::TrySendError::Full(_)) => ids_full.push(client.id),
                            Err(mpsc::error::TrySendError::Closed(_)) => ids_closed.push(client.id),
                        }
                    }

                    for id in ids_full {
                        if let Some(client) = clients.remove(&id) {
                            tokio::spawn(async move {
                                CONNECTIONS_TOTAL.dec();
                                error!("{}, lagged, close stream", client.id);
                                let _ = client.stream_tx.send(Err(Status::internal("lagged"))).await;
                            });
                        }
                    }
                    for id in ids_closed {
                        if let Some(client) = clients.remove(&id) {
                            CONNECTIONS_TOTAL.dec();
                            error!("{}, client closed stream", client.id);
                        }
                    }
                },
                Some(client) = new_clients_rx.recv() => {
                    info!("{}, add client to receivers", client.id);
                    clients.insert(client.id, client);
                    CONNECTIONS_TOTAL.inc();
                }
                else => break,
            };
        }
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<SubscribeUpdate>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::SeqCst);
        info!("{}, new subscriber", id);

        let data = request.get_ref();
        let accounts_filter = match AccountsFilter::new(data.any, &data.accounts, &data.owners) {
            Ok(filter) => filter,
            Err(error) => {
                let message = format!("failed to create filter: {:?}", error);
                error!("{}, {}", id, message);
                return Err(Status::invalid_argument(message));
            }
        };

        let (stream_tx, stream_rx) = mpsc::channel(self.config.channel_capacity);
        if let Err(_error) = self.new_clients_tx.send(ClientConnection {
            id,
            accounts_filter,
            stream_tx,
        }) {
            return Err(Status::internal(""));
        }

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }
}
