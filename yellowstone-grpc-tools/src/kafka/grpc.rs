use {
    crate::version::VERSION,
    futures::future::{BoxFuture, FutureExt},
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::{
        sync::{broadcast, mpsc, Notify},
        task::JoinError,
        time::{sleep, Duration},
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        codec::{CompressionEncoding, Streaming},
        transport::{
            server::{Server, TcpIncoming},
            Error as TransportError,
        },
        Request, Response, Result as TonicResult, Status,
    },
    tonic_health::server::health_reporter,
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
    #[allow(clippy::type_complexity)]
    pub fn run(
        listen: SocketAddr,
        channel_capacity: usize,
    ) -> anyhow::Result<(
        broadcast::Sender<SubscribeUpdate>,
        BoxFuture<'static, Result<Result<(), TransportError>, JoinError>>,
    )> {
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
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Zstd);

        let shutdown = Arc::new(Notify::new());
        let shutdown_grpc = Arc::clone(&shutdown);

        let server = tokio::spawn(async move {
            // gRPC Health check service
            let (mut health_reporter, health_service) = health_reporter();
            health_reporter.set_serving::<GeyserServer<Self>>().await;

            Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(5)))
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown_grpc.notified())
                .await
        });
        let shutdown = async move {
            shutdown.notify_one();
            server.await
        }
        .boxed();

        Ok((broadcast_tx, shutdown))
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<SubscribeUpdate>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        let (stream_tx, stream_rx) = mpsc::channel(self.channel_capacity);
        let notify_client = Arc::new(Notify::new());
        let notify_exit1 = Arc::new(Notify::new());
        let notify_exit2 = Arc::new(Notify::new());

        let ping_stream_tx = stream_tx.clone();
        let ping_client = Arc::clone(&notify_client);
        let ping_exit = Arc::clone(&notify_exit1);
        tokio::spawn(async move {
            let exit = ping_exit.notified();
            tokio::pin!(exit);

            let ping_msg = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            };

            loop {
                tokio::select! {
                    _ = &mut exit => break,
                    _ = sleep(Duration::from_secs(10)) => {
                        match ping_stream_tx.try_send(Ok(ping_msg.clone())) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {}
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                ping_client.notify_one();
                                break;
                            }
                        }
                    }
                }
            }
        });

        let incoming_client = Arc::clone(&notify_client);
        let incoming_exit = Arc::clone(&notify_exit2);
        tokio::spawn(async move {
            let exit = incoming_exit.notified();
            tokio::pin!(exit);

            loop {
                tokio::select! {
                    _ = &mut exit => break,
                    message = request.get_mut().message() => match message {
                        Ok(Some(_request)) => {}
                        Ok(None) => break,
                        Err(_error) => {
                            let _ = incoming_client.notify_one();
                            break;
                        }
                    }
                }
            }
        });

        let mut messages_rx = self.broadcast_tx.subscribe();
        tokio::spawn(async move {
            info!("client #{id}: new");
            loop {
                tokio::select! {
                    _ = notify_client.notified() => break,
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
                            Err(broadcast::error::RecvError::Closed) => break,
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
            notify_exit1.notify_one();
            notify_exit2.notify_one();
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
