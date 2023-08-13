use {
    crate::version::VERSION,
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::{
        sync::{broadcast, mpsc},
        time::{sleep, Duration},
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        codec::{CompressionEncoding, Streaming},
        transport::server::{Server, TcpIncoming},
        Request, Response, Result as TonicResult, Status,
    },
    tracing::{error, info},
    yellowstone_grpc_proto::prelude::{
        geyser_server::{Geyser, GeyserServer},
        subscribe_update::UpdateOneof,
        GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdatePing,
    },
};

#[derive(Debug)]
pub struct GrpcService {
    subscribe_id: AtomicUsize,
    channel_capacity: usize,
    broadcast_tx: broadcast::Sender<SubscribeUpdate>,
}

impl GrpcService {
    pub fn run(
        listen: SocketAddr,
        channel_capacity: usize,
    ) -> anyhow::Result<broadcast::Sender<SubscribeUpdate>> {
        // Bind service address
        let incoming = TcpIncoming::new(
            listen,
            true,                          // tcp_nodelay
            Some(Duration::from_secs(20)), // tcp_keepalive
        )
        .map_err(|error| anyhow::anyhow!(format!("{error:?}")))?;

        // Messages to clients combined by commitment
        let (broadcast_tx, _) = broadcast::channel(channel_capacity);

        // Run Server
        let service = GeyserServer::new(Self {
            subscribe_id: AtomicUsize::new(0),
            channel_capacity,
            broadcast_tx: broadcast_tx.clone(),
        })
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);
        tokio::spawn(async move {
            Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(5)))
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
        });

        Ok(broadcast_tx)
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<SubscribeUpdate>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::SeqCst);
        let (stream_tx, stream_rx) = mpsc::channel(self.channel_capacity);
        let (client_tx, mut client_rx) = mpsc::unbounded_channel();
        let exit = Arc::new(AtomicBool::new(false));

        let ping_stream_tx = stream_tx.clone();
        let ping_client_tx = client_tx.clone();
        let ping_exit = Arc::clone(&exit);
        tokio::spawn(async move {
            while !ping_exit.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(10)).await;
                match ping_stream_tx.try_send(Ok(SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
                })) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        let _ = ping_client_tx.send(());
                        break;
                    }
                }
            }
        });

        let incoming_client_tx = client_tx;
        let incoming_exit = Arc::clone(&exit);
        tokio::spawn(async move {
            while !incoming_exit.load(Ordering::Relaxed) {
                match request.get_mut().message().await {
                    Ok(Some(_request)) => {}
                    Ok(None) => {
                        break;
                    }
                    Err(_error) => {
                        let _ = incoming_client_tx.send(());
                        break;
                    }
                }
            }
        });

        let mut messages_rx = self.broadcast_tx.subscribe();
        tokio::spawn(async move {
            info!("client #{id}: new");
            loop {
                tokio::select! {
                    _ = client_rx.recv() => {
                        break;
                    }
                    message = messages_rx.recv() => {
                        match message {
                            Ok(message) => {
                                match stream_tx.try_send(Ok(message)) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        error!("client #{id}: lagged to send update");
                                        tokio::spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("lagged"))).await;
                                        });
                                        break;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        error!("client #{id}: stream closed");
                                        break;
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            },
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                info!("client #{id}: lagged to receive geyser messages");
                                tokio::spawn(async move {
                                    let _ = stream_tx.send(Err(Status::internal("lagged"))).await;
                                });
                                break;
                            }
                        }
                    }
                }
            }
            info!("client #{id}: removed");
            exit.store(true, Ordering::Relaxed);
        });

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("not implemented in kafka reader"))
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("not implemented in kafka reader"))
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented("not implemented in kafka reader"))
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("not implemented in kafka reader"))
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&VERSION).unwrap(),
        }))
    }
}
