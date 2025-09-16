use {
    crate::{
        grpc::{
            filter_impl::{CompiledFilters, TryFromSubscribeRequestError},
            proto::{SharedUpdateOneof, ZeroCopySubscribeUpdate},
        },
        proto::geyser::{subscribe_update::UpdateOneof, SubscribeRequest},
    },
    futures::{Sink, SinkExt},
    std::{collections::HashSet, pin::Pin, sync::Arc},
    tokio::sync::broadcast,
    tokio_stream::StreamExt,
    tokio_util::sync::CancellationToken,
    tonic::Streaming,
};

#[derive(Debug, thiserror::Error)]
#[error("disconnected")]
pub struct ClientSinkError;

pub struct ClientTask {
    pub x_subscribe_id: String,
    pub remote_addr: String,
    pub compiled_filters: CompiledFilters,
    pub geyser_source: broadcast::Receiver<Arc<UpdateOneof>>,
    ///
    /// Channel for receiving subscribe requests from the client.
    ///
    pub grpc_in: Streaming<SubscribeRequest>,
    ///
    /// Channel for sending updates to the client.
    ///
    pub grpc_out:
        Pin<Box<dyn Sink<ZeroCopySubscribeUpdate, Error = ClientSinkError> + Send + Unpin>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientTaskError {
    #[error("client gRPC channel outlet disconnected")]
    ClientOutletDisconnected,
    #[error("invalid subscribe request received from client")]
    InvalidSubscribeRequest(#[from] TryFromSubscribeRequestError),
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
            _ => {
                log::warn!(
                    "Received unsupported update type in gRPC client: {:?}",
                    update
                );
            }
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
                    .send(zc_update)
                    .await
                    .map_err(|_| ClientTaskError::ClientOutletDisconnected)?;

                super::metrics::inc_message_send_counter_per_client(
                    self.x_subscribe_id.as_str(),
                    self.remote_addr.as_str(),
                );
                Ok(())
            }
            Err(e) => match e {
                broadcast::error::RecvError::Closed => {
                    return Err(ClientTaskError::GeyserDisconnected);
                }
                broadcast::error::RecvError::Lagged(_) => {
                    return Err(ClientTaskError::ClientLagged);
                }
            },
        }
    }

    pub fn handle_subscribe_request(
        &mut self,
        subscribe_request: SubscribeRequest,
    ) -> Result<(), ClientTaskError> {
        self.compiled_filters = CompiledFilters::try_from(&subscribe_request)?;
        log::debug!(
            "Client {} updated filters -- {} complexity",
            self.remote_addr,
            self.compiled_filters.complexity_score()
        );
        Ok(())
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

        self.handle_subscribe_request(initial_subscribe_request)?;

        let mut grpc_in_is_dead = false;
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    return Ok(());
                }
                result = self.geyser_source.recv() => {
                    self.handle_geyser_source_event(result).await.inspect_err(|e| {
                        log::warn!("Error handling geyser source event for client {}: {}", self.remote_addr, e);
                    })?;
                }
                // It safe to ignore other branch since the grpc_input stream is not mandatory to keep alive by the client.
                maybe = self.grpc_in.next(), if !grpc_in_is_dead  => {
                    let Some(result) = maybe else {
                        grpc_in_is_dead = true;
                        log::debug!("gRPC input stream closed by client: {}", self.remote_addr);
                        continue;
                    };
                    let Ok(filters) = result else {
                        grpc_in_is_dead = true;
                        log::warn!("gRPC input stream error from client: {}", self.remote_addr);
                        continue;
                    };
                    self.handle_subscribe_request(filters)?;
                }
            }
        }
    }
}
