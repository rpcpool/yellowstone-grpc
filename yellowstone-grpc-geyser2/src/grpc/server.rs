use {
    crate::{
        config::ConfigGrpc,
        grpc::{
            client_task::{ClientSinkError, ClientTask, ClientTaskError},
            proto::ZeroCopySubscribeUpdate,
        },
        proto::{
            gen::geyser_server::Geyser,
            geyser::{
                subscribe_update::UpdateOneof, GetVersionRequest, GetVersionResponse,
                SubscribeRequest,
            },
            r#gen::geyser_server::GeyserServer,
        },
    },
    futures::{stream::BoxStream, Sink, StreamExt},
    std::{collections::HashMap, fs, io, pin::Pin, sync::Arc, time::Duration},
    tokio::{
        sync::{broadcast, mpsc, oneshot},
        task::{Id, JoinError, JoinHandle, JoinSet},
    },
    tokio_stream::wrappers::ReceiverStream,
    tokio_util::{
        sync::{CancellationToken, PollSender},
        task::TaskTracker,
    },
    tonic::{
        async_trait,
        service::interceptor,
        transport::{server::TcpIncoming, Identity, Server, ServerTlsConfig},
        Request, Status,
    },
    tonic_health::server::health_reporter,
};

pub struct GeyserV2GrpcServer {
    pub geyser_broadcast: broadcast::Sender<Arc<UpdateOneof>>,
    pub ev_loop_cnc_tx: mpsc::Sender<EvLoopCommand>,
    pub client_channel_capacity: usize,
    pub cancellation_token: CancellationToken,
}

///
/// Adapter to [`ZeroCopySubscribeUpdate`] to [`Result<ZeroCopySubscribeUpdate, tonic::Status>`]
///
struct GrpcClientSinkAdapter {
    inner: PollSender<Result<ZeroCopySubscribeUpdate, tonic::Status>>,
}

impl Sink<ZeroCopySubscribeUpdate> for GrpcClientSinkAdapter {
    type Error = ClientSinkError;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_ready(cx)
            .map_err(|_| ClientSinkError)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: ZeroCopySubscribeUpdate,
    ) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner)
            .start_send(Ok(item))
            .map_err(|_| ClientSinkError)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_flush(cx)
            .map_err(|_| ClientSinkError)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_close(cx)
            .map_err(|_| ClientSinkError)
    }
}

#[async_trait]
impl Geyser for GeyserV2GrpcServer {
    type SubscribeStream = BoxStream<'static, Result<ZeroCopySubscribeUpdate, tonic::Status>>;

    async fn subscribe(
        &self,
        request: tonic::Request<tonic::Streaming<SubscribeRequest>>,
    ) -> std::result::Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let x_subscribe_id = request
            .metadata()
            .get("x-subscribe-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let remote_addr = request.remote_addr();
        log::trace!(
            "Received subscribe request with x-subscribe-id: {:?}",
            x_subscribe_id
        );

        let (client_tx, client_rx) = mpsc::channel(self.client_channel_capacity);
        let client_sink = PollSender::new(client_tx.clone());
        let client_task_sink_adapter = GrpcClientSinkAdapter { inner: client_sink };
        let grpc_in = request.into_inner();
        let client_task = ClientTask {
            x_subscribe_id: x_subscribe_id.unwrap_or("unknown".to_string()),
            remote_addr: remote_addr
                .map(|addr| addr.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            compiled_filters: Default::default(),
            geyser_source: self.geyser_broadcast.subscribe(),
            grpc_in,
            grpc_out: std::pin::Pin::new(Box::new(client_task_sink_adapter)),
        };

        if self.cancellation_token.is_cancelled() {
            return Err(tonic::Status::unavailable("server is shutting down"));
        }

        let (callback_tx, callback_rx) = oneshot::channel();
        let command = EvLoopCommand::AddClientTask(AddClientTaskCommand {
            client_task,
            callback: callback_tx,
            grpc_out: client_tx,
        });
        if self.ev_loop_cnc_tx.send(command).await.is_err() {
            return Err(tonic::Status::internal("failed to spawn client session"));
        }
        log::trace!("Waiting for client task to be added to the event loop");
        if callback_rx.await.is_err() {
            return Err(tonic::Status::internal(
                "client task was canceled before it could be added",
            ));
        }
        log::trace!("Client task added to the event loop");

        Ok(tonic::Response::new(ReceiverStream::new(client_rx).boxed()))
    }

    async fn get_version(
        &self,
        _request: tonic::Request<GetVersionRequest>,
    ) -> std::result::Result<tonic::Response<GetVersionResponse>, tonic::Status> {
        let version = crate::version::VERSION;
        let response = GetVersionResponse {
            version: version.version.to_string(),
        };
        Ok(tonic::Response::new(response))
    }
}

pub struct GeyserV2GrpcServerEvLoop {
    client_joinset: JoinSet<Result<(), ClientTaskError>>,
    client_task_map: HashMap<Id, ClientTaskMeta>,
    cnc_rx: mpsc::Receiver<EvLoopCommand>,
    cancellation_token: CancellationToken,
    shutdown_grace_period: Duration,
}

pub struct AddClientTaskCommand {
    client_task: ClientTask,
    grpc_out: mpsc::Sender<Result<ZeroCopySubscribeUpdate, tonic::Status>>,
    callback: oneshot::Sender<()>,
}

struct ClientTaskMeta {
    x_subscription_id: String,
    grpc_out: mpsc::Sender<Result<ZeroCopySubscribeUpdate, tonic::Status>>,
}

pub enum EvLoopCommand {
    AddClientTask(AddClientTaskCommand),
}

#[derive(Debug, Clone)]
pub struct GeyserV2GrpcServerEvLoopConfig {
    pub shutdown_grace_period: Duration,
}

impl Default for GeyserV2GrpcServerEvLoopConfig {
    fn default() -> Self {
        Self {
            shutdown_grace_period: Duration::from_secs(5),
        }
    }
}

impl GeyserV2GrpcServerEvLoop {
    pub fn spawn(
        config: GeyserV2GrpcServerEvLoopConfig,
        task_tracker: TaskTracker,
        cancellation_token: CancellationToken,
    ) -> (mpsc::Sender<EvLoopCommand>, JoinHandle<()>) {
        let (cnc_tx, cnc_rx) = mpsc::channel(100);
        let ev_loop = Self {
            client_joinset: JoinSet::new(),
            client_task_map: Default::default(),
            cnc_rx,
            cancellation_token,
            shutdown_grace_period: config.shutdown_grace_period,
        };

        let jh = task_tracker.spawn(async move {
            ev_loop.run().await;
        });

        (cnc_tx, jh)
    }

    fn handle_command(&mut self, cmd: EvLoopCommand) {
        match cmd {
            EvLoopCommand::AddClientTask(command) => {
                log::trace!("Handling AddClientTaskCommand");
                let AddClientTaskCommand {
                    client_task,
                    callback,
                    grpc_out,
                } = command;
                let x_subscription_id = client_task.x_subscribe_id.clone();
                let client_session_cancellation_token = self.cancellation_token.child_token();
                let ah = self
                    .client_joinset
                    .spawn(async move { client_task.run(client_session_cancellation_token).await });

                if callback.send(()).is_err() {
                    log::error!("client canceled the task before it could be added");
                    ah.abort();
                    return;
                };

                log::trace!(
                    "Client task {x_subscription_id:?} added to joinset with id: {:?}",
                    ah.id()
                );

                self.client_task_map.insert(
                    ah.id(),
                    ClientTaskMeta {
                        x_subscription_id,
                        grpc_out,
                    },
                );
            }
        }
    }

    async fn handle_client_task_result(
        &mut self,
        result: Result<(Id, Result<(), ClientTaskError>), JoinError>,
    ) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(e) => e.id(),
        };

        let Some(meta) = self.client_task_map.remove(&id) else {
            log::warn!("Received result for unknown client task with id: {:?}", id);
            return;
        };
        log::debug!(
            "Handling result for client task with id: {:?}, x_subscription_id: {:?}",
            id,
            meta.x_subscription_id
        );
        match result {
            Ok((id, result)) => match result {
                Ok(_) => {
                    log::trace!("Client task with id: {:?} completed successfully", id);
                }
                Err(e) => {
                    match e {
                        ClientTaskError::ClientOutletDisconnected => {
                            // Nothing to do...
                        }
                        ClientTaskError::InvalidSubscribeRequest(e2) => {
                            let _ = meta.grpc_out.send(Err(e2.into())).await;
                        }
                        ClientTaskError::GeyserDisconnected => {
                            let _ = meta
                                .grpc_out
                                .send(Err(Status::unavailable("server is shutting down")));
                        }
                        ClientTaskError::ClientLagged => {
                            let _ = meta
                                .grpc_out
                                .send(Err(Status::resource_exhausted(
                                    "client is too slow and has been disconnected",
                                )))
                                .await;
                        }
                        ClientTaskError::GrpcInputStreamError(status) => {
                            log::info!(
                                "Client task with id: {:?} input stream error: {:?}",
                                id,
                                status
                            );
                            let _ = meta
                                .grpc_out
                                .send(Err(Status::cancelled("canceled by client")))
                                .await;
                        }
                    }
                }
            },
            Err(e) => {
                let _ = meta
                    .grpc_out
                    .send(Err(Status::internal("internal server error")))
                    .await;
                log::error!("Client task with id: {:?} failed with error: {:?}", id, e);
            }
        }
        super::metrics::inc_total_disconnection_count();
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    log::info!("Event loop cancellation requested, shutting down");
                    break;
                }
                Some(result) = self.client_joinset.join_next_with_id() => {
                    self.handle_client_task_result(result).await;
                },
                maybe = self.cnc_rx.recv() => {
                    match maybe {
                        Some(cmd) => {
                            self.handle_command(cmd);
                        }
                        None => {
                            log::warn!("Command channel closed, exiting event loop");
                            break;
                        }
                    }
                },
            }
            super::metrics::set_total_client_connected(self.client_joinset.len());
        }
        self.cancellation_token.cancel();
        log::info!("Event loop is shutting down, aborting all client tasks");
        shutdown_joinset(self.shutdown_grace_period, &mut self.client_joinset).await;
        self.client_joinset.abort_all();
        log::info!("Event loop exited, all client tasks aborted");
    }
}

async fn shutdown_joinset(timeout: Duration, joinset: &mut JoinSet<Result<(), ClientTaskError>>) {
    let shutdown_deadline = tokio::time::Instant::now() + timeout;
    loop {
        tokio::select! {
            biased;

            _ = tokio::time::sleep_until(shutdown_deadline) => {
                log::warn!("Timeout reached while waiting for client tasks to shut down, aborting remaining tasks");
                joinset.abort_all();
                break;
            }

            maybe = joinset.join_next() => {
                match maybe {
                    Some(_) => {
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TrySpawnGrpcServerError {
    #[error(transparent)]
    InvalidCertFile(std::io::Error),
    #[error(transparent)]
    InvalidKeyFile(std::io::Error),
    #[error(transparent)]
    InvalidTlsConffig(tonic::transport::Error),
    #[error(transparent)]
    TcpBindError(#[from] io::Error),
}

pub async fn spawn_grpc_server(
    geyser_broadcast: broadcast::Sender<Arc<UpdateOneof>>,
    config: ConfigGrpc,
    task_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) -> Result<JoinHandle<Result<(), tonic::transport::Error>>, TrySpawnGrpcServerError> {
    let mut server_builder = Server::builder();

    if let Some(tls_config) = config.tls_config {
        let cert_path = tls_config.cert_path;
        let key_path = tls_config.key_path;
        let cert = fs::read(cert_path).map_err(|e| TrySpawnGrpcServerError::InvalidCertFile(e))?;
        let key = fs::read(key_path).map_err(|e| TrySpawnGrpcServerError::InvalidKeyFile(e))?;

        let server_tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));

        server_builder = server_builder
            .tls_config(server_tls)
            .map_err(|e| TrySpawnGrpcServerError::InvalidTlsConffig(e))?;
    }

    server_builder = server_builder
        .http2_keepalive_interval(config.server_http2_keepalive_interval)
        .http2_keepalive_timeout(config.server_http2_keepalive_timeout)
        .http2_adaptive_window(config.server_http2_adaptive_window)
        .initial_connection_window_size(config.server_initial_connection_window_size)
        .initial_stream_window_size(config.server_initial_stream_window_size)
        .tcp_nodelay(true);

    let (ev_loop, _) = GeyserV2GrpcServerEvLoop::spawn(
        Default::default(),
        task_tracker.clone(),
        cancellation_token.child_token(),
    );

    let service = GeyserV2GrpcServer {
        geyser_broadcast,
        ev_loop_cnc_tx: ev_loop,
        client_channel_capacity: config.channel_capacity,
        cancellation_token: cancellation_token.clone(),
    };

    let mut svc =
        GeyserServer::new(service).max_decoding_message_size(config.max_decoding_message_size);

    for accept in config.compression.accept {
        svc = svc.accept_compressed(accept);
    }

    for send in config.compression.send {
        svc = svc.send_compressed(send);
    }

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<GeyserServer<GeyserV2GrpcServer>>()
        .await;

    let incoming = TcpIncoming::bind(config.address)?
        .with_nodelay(Some(true))
        .with_keepalive_interval(config.server_http2_keepalive_interval);

    let fut = server_builder
        .layer(interceptor::InterceptorLayer::new(
            move |request: Request<()>| {
                if let Some(x_token) = &config.x_token {
                    match request.metadata().get("x-token") {
                        Some(token) if x_token == token => Ok(request),
                        _ => Err(Status::unauthenticated("No valid auth token")),
                    }
                } else {
                    Ok(request)
                }
            },
        ))
        .add_service(health_service)
        .add_service(svc)
        .serve_with_incoming_shutdown(incoming, cancellation_token.cancelled_owned());

    Ok(task_tracker.spawn(fut))
}
