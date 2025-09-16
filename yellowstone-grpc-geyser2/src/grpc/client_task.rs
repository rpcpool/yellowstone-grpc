use {
    crate::{
        grpc::{
            filter_impl::{CompiledFilters, TryFromSubscribeRequestError},
            proto::{SharedUpdateOneof, ZeroCopySubscribeUpdate},
        },
        proto::geyser::{subscribe_update::UpdateOneof, SubscribeRequest},
    },
    futures::stream::BoxStream,
    std::{collections::HashSet, sync::Arc},
    tokio::sync::{broadcast, mpsc},
    tokio_stream::StreamExt,
    tokio_util::sync::CancellationToken,
};

pub struct ClientTask {
    pub x_subscribe_id: String,
    pub remote_addr: String,
    pub compiled_filters: CompiledFilters,
    pub geyser_source: broadcast::Receiver<Arc<UpdateOneof>>,
    ///
    /// Channel for receiving subscribe requests from the client.
    ///
    pub grpc_in: BoxStream<'static, Result<SubscribeRequest, tonic::Status>>,
    ///
    /// Channel for sending updates to the client.
    ///
    pub grpc_out: mpsc::Sender<Result<ZeroCopySubscribeUpdate, tonic::Status>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientTaskError {
    #[error("client gRPC channel outlet disconnected")]
    ClientOutletDisconnected,
    #[error("invalid subscribe request received from client")]
    InvalidSubscribeRequest,
    #[error("Got disconnected from geyser source")]
    GeyserDisconnected,
    #[error("client lagged behind too much")]
    ClientLagged,
    #[error(transparent)]
    GrpcInputStreamError(#[from] tonic::Status),
}

impl From<TryFromSubscribeRequestError> for tonic::Status {
    fn from(e: TryFromSubscribeRequestError) -> Self {
        match e {
            TryFromSubscribeRequestError::CompileAccountFilter(e) => {
                tonic::Status::invalid_argument(format!("Failed to compile account filter: {}", e))
            }
            TryFromSubscribeRequestError::CompileTxFilter(e) => tonic::Status::invalid_argument(
                format!("Failed to compile transaction filter: {}", e),
            ),
        }
    }
}

impl CompiledFilters {
    pub fn get_updateoneof_matching(&self, update: &UpdateOneof, matches: &mut HashSet<String>) {
        match update {
            UpdateOneof::Account(account) => self.get_account_matching(account, matches),
            UpdateOneof::Slot(slot) => self.get_slot_matching(slot, matches),
            UpdateOneof::Transaction(tx) => self.get_tx_matching(tx, matches),
            UpdateOneof::Entry(entry) => self.get_entry_matching(entry, matches),
            UpdateOneof::BlockMeta(block_meta) => self.get_blockmeta_matching(block_meta, matches),
            _ => {}
        }
    }
}

impl ClientTask {
    pub async fn handle_geyser_source_event(
        &mut self,
        result: Result<Arc<UpdateOneof>, broadcast::error::RecvError>,
    ) -> Result<(), ClientTaskError> {
        match result {
            Ok(message) => {
                let mut matches = HashSet::new();

                self.compiled_filters
                    .get_updateoneof_matching(&message, &mut matches);
                if matches.is_empty() {
                    return Ok(());
                }

                let zc_update = ZeroCopySubscribeUpdate {
                    filters: matches.into_iter().collect(),
                    created_at: prost_types::Timestamp::default(),
                    shared: SharedUpdateOneof(Arc::clone(&message)),
                };

                self.grpc_out
                    .send(Ok(zc_update))
                    .await
                    .map_err(|_| ClientTaskError::ClientOutletDisconnected)?;

                super::metrics::inc_message_send_counter_per_client(
                    self.x_subscribe_id.as_str(),
                    self.remote_addr.as_str(),
                );
                log::trace!("Sent update to client: {:?}", self.remote_addr);

                Ok(())
            }
            Err(e) => match e {
                broadcast::error::RecvError::Closed => {
                    let _ = self
                        .grpc_out
                        .send(Err(tonic::Status::unavailable(
                            "geyser source disconnected",
                        )))
                        .await;

                    return Err(ClientTaskError::GeyserDisconnected);
                }
                broadcast::error::RecvError::Lagged(_) => {
                    let _ = self
                        .grpc_out
                        .send(Err(tonic::Status::unauthenticated(
                            "client lagged behind too much",
                        )))
                        .await;

                    return Err(ClientTaskError::ClientLagged);
                }
            },
        }
    }

    pub async fn handle_subscribe_request(
        &mut self,
        subscribe_request: SubscribeRequest,
    ) -> Result<(), ClientTaskError> {
        match CompiledFilters::try_from(&subscribe_request) {
            Ok(compiled_filters) => {
                log::trace!("Compiled filters from subscribe request");
                self.compiled_filters = compiled_filters;
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to compile filters from subscribe request: {:?}", e);
                let _ = self.grpc_out.send(Err(e.into())).await;
                Err(ClientTaskError::InvalidSubscribeRequest)
            }
        }
    }

    pub async fn run(
        mut self,
        cancellation_token: CancellationToken,
    ) -> Result<(), ClientTaskError> {
        let initial_subscribe_request = self
            .grpc_in
            .next()
            .await
            .ok_or(ClientTaskError::ClientOutletDisconnected)??;

        let result = CompiledFilters::try_from(&initial_subscribe_request);

        match result {
            Ok(compiled_filters) => {
                log::trace!("Compiled filters");
                self.compiled_filters = compiled_filters;
            }
            Err(e) => {
                self.grpc_out
                    .send(Err(e.into()))
                    .await
                    .map_err(|_| ClientTaskError::ClientOutletDisconnected)?;

                return Err(ClientTaskError::InvalidSubscribeRequest);
            }
        };

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    log::info!("Client task cancelled: {}", self.remote_addr);
                    let _ = self.grpc_out.try_send(Err(tonic::Status::internal("server is shutting down")));
                    return Ok(());
                }
                result = self.geyser_source.recv() => {
                    self.handle_geyser_source_event(result).await?;
                }
                Some(subscribe_request) = self.grpc_in.next() => {
                    log::trace!("Received new subscribe request: {:?}", subscribe_request);
                    self.handle_subscribe_request(subscribe_request?).await?;
                }
            }
        }
    }
}
