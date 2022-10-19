use {
    crate::{
        config::ConfigGrpc,
        filters::Filter,
        grpc::proto::{
            geyser_server::{Geyser, GeyserServer},
            subscribe_update::UpdateOneof,
            SubscribeRequest, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
            SubscribeUpdateSlot, SubscribeUpdateSlotStatus,
        },
        prom::CONNECTIONS_TOTAL,
    },
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoVersions, SlotStatus,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
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
pub struct UpdateAccountMessageAccount {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
}

#[derive(Debug)]
pub struct UpdateAccountMessage {
    pub account: UpdateAccountMessageAccount,
    pub slot: u64,
    pub is_startup: bool,
}

impl<'a> From<(ReplicaAccountInfoVersions<'a>, u64, bool)> for UpdateAccountMessage {
    fn from((account, slot, is_startup): (ReplicaAccountInfoVersions<'a>, u64, bool)) -> Self {
        Self {
            account: match account {
                ReplicaAccountInfoVersions::V0_0_1(info) => UpdateAccountMessageAccount {
                    pubkey: Pubkey::new(info.pubkey),
                    lamports: info.lamports,
                    owner: Pubkey::new(info.owner),
                    executable: info.executable,
                    rent_epoch: info.rent_epoch,
                    data: info.data.into(),
                    write_version: info.write_version,
                    txn_signature: None,
                },
            },
            slot,
            is_startup,
        }
    }
}

impl UpdateAccountMessage {
    fn with_filters(&self, filters: Vec<String>) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: self.account.pubkey.as_ref().into(),
                    lamports: self.account.lamports,
                    owner: self.account.owner.as_ref().into(),
                    executable: self.account.executable,
                    rent_epoch: self.account.rent_epoch,
                    data: self.account.data.clone(),
                    write_version: self.account.write_version,
                    txn_signature: self.account.txn_signature.map(|sig| sig.as_ref().into()),
                }),
                slot: self.slot,
                is_startup: self.is_startup,
                filters,
            })),
        }
    }
}

#[derive(Debug)]
pub struct UpdateSlotMessage {
    slot: u64,
    parent: Option<u64>,
    status: SubscribeUpdateSlotStatus,
}

impl From<(u64, Option<u64>, SlotStatus)> for UpdateSlotMessage {
    fn from((slot, parent, status): (u64, Option<u64>, SlotStatus)) -> Self {
        Self {
            slot,
            parent,
            status: match status {
                SlotStatus::Processed => SubscribeUpdateSlotStatus::Processed,
                SlotStatus::Confirmed => SubscribeUpdateSlotStatus::Confirmed,
                SlotStatus::Rooted => SubscribeUpdateSlotStatus::Rooted,
            },
        }
    }
}

impl From<&UpdateSlotMessage> for SubscribeUpdate {
    fn from(msg: &UpdateSlotMessage) -> Self {
        Self {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: msg.slot,
                parent: msg.parent,
                status: msg.status as i32,
            })),
        }
    }
}

#[derive(Debug)]
pub enum Message {
    UpdateAccount(UpdateAccountMessage),
    UpdateSlot(UpdateSlotMessage),
}

#[derive(Debug)]
struct ClientConnection {
    id: usize,
    filter: Filter,
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
        (mpsc::UnboundedSender<Message>, oneshot::Sender<()>),
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
        mut update_channel_rx: mpsc::UnboundedReceiver<Message>,
        mut new_clients_rx: mpsc::UnboundedReceiver<ClientConnection>,
    ) {
        let mut clients: HashMap<usize, ClientConnection> = HashMap::new();
        loop {
            tokio::select! {
                Some(message) = update_channel_rx.recv() => {
                    let mut ids_full = vec![];
                    let mut ids_closed = vec![];

                    for client in clients.values() {
                        if let Some(message) = match &message {
                            Message::UpdateAccount(message) => {
                                let mut filter = client.filter.create_accounts_match();
                                filter.match_account(&message.account.pubkey);
                                filter.match_owner(&message.account.owner);

                                let filters = filter.get_filters();
                                if !filters.is_empty() {
                                    Some(message.with_filters(filters))
                                } else {
                                    None
                                }
                            }
                            Message::UpdateSlot(message) => {
                                if client.filter.is_slots_enabled() {
                                    Some(message.into())
                                } else {
                                    None
                                }
                            }
                        } {
                            match client.stream_tx.try_send(Ok(message)) {
                                Ok(()) => {},
                                Err(mpsc::error::TrySendError::Full(_)) => ids_full.push(client.id),
                                Err(mpsc::error::TrySendError::Closed(_)) => ids_closed.push(client.id),
                            }
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

        let filter = match Filter::try_from(request.get_ref()) {
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
            filter,
            stream_tx,
        }) {
            return Err(Status::internal(""));
        }

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }
}
