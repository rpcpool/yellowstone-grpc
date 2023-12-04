use {
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    http::uri::InvalidUri,
    std::{collections::HashMap, time::Duration},
    tonic::{
        codec::Streaming,
        metadata::{errors::InvalidMetadataValue, AsciiMetadataValue},
        service::{interceptor::InterceptedService, Interceptor},
        transport::channel::{Channel, ClientTlsConfig, Endpoint},
        Request, Response, Status,
    },
    tonic_health::pb::{health_client::HealthClient, HealthCheckRequest, HealthCheckResponse},
    yellowstone_grpc_proto::prelude::{
        geyser_client::GeyserClient, CommitmentLevel, GetBlockHeightRequest,
        GetBlockHeightResponse, GetLatestBlockhashRequest, GetLatestBlockhashResponse,
        GetSlotRequest, GetSlotResponse, GetVersionRequest, GetVersionResponse,
        IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest, PongResponse,
        SubscribeRequest, SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate,
    },
};

#[derive(Debug, Clone)]
struct InterceptorFn {
    x_token: Option<AsciiMetadataValue>,
}

impl Interceptor for InterceptorFn {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(x_token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", x_token);
        }
        Ok(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcClientError {
    #[error("Invalid URI: {0}")]
    InvalidUri(#[from] InvalidUri),
    #[error("Failed to parse x-token: {0}")]
    MetadataValueError(#[from] InvalidMetadataValue),
    #[error("Invalid X-Token length: {0}, expected 28")]
    InvalidXTokenLength(usize),
    #[error("gRPC transport error: {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("gRPC status: {0}")]
    TonicStatus(#[from] Status),
    #[error("Failed to send subscribe request: {0}")]
    SubscribeSendError(#[from] mpsc::SendError),
}

pub type GeyserGrpcClientResult<T> = Result<T, GeyserGrpcClientError>;

pub struct GeyserGrpcClient<F> {
    health: HealthClient<InterceptedService<Channel, F>>,
    geyser: GeyserClient<InterceptedService<Channel, F>>,
}

impl GeyserGrpcClient<()> {
    fn connect2<E, T>(
        endpoint: E,
        tls_config: Option<ClientTlsConfig>,
        x_token: Option<T>,
    ) -> GeyserGrpcClientResult<(Endpoint, InterceptorFn)>
    where
        E: Into<Bytes>,
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        let mut endpoint = Channel::from_shared(endpoint)?;
        if let Some(tls_config) = tls_config {
            endpoint = endpoint.tls_config(tls_config)?;
        } else if endpoint.uri().scheme_str() == Some("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
        }

        let x_token: Option<AsciiMetadataValue> = match x_token {
            Some(x_token) => Some(x_token.try_into()?),
            None => None,
        };
        match x_token {
            Some(token) if token.is_empty() => {
                return Err(GeyserGrpcClientError::InvalidXTokenLength(token.len()));
            }
            _ => {}
        }
        let interceptor = InterceptorFn { x_token };

        Ok((endpoint, interceptor))
    }

    pub fn connect<E, T>(
        endpoint: E,
        x_token: Option<T>,
        tls_config: Option<ClientTlsConfig>,
    ) -> GeyserGrpcClientResult<GeyserGrpcClient<impl Interceptor>>
    where
        E: Into<Bytes>,
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        let (endpoint, interceptor) = Self::connect2(endpoint, tls_config, x_token)?;
        let channel = endpoint.connect_lazy();
        Ok(GeyserGrpcClient {
            health: HealthClient::with_interceptor(channel.clone(), interceptor.clone()),
            geyser: GeyserClient::with_interceptor(channel, interceptor)
                .max_decoding_message_size(64 * 1024 * 1024), // 64 MiB
        })
    }

    pub async fn connect_with_timeout<E, T>(
        endpoint: E,
        x_token: Option<T>,
        tls_config: Option<ClientTlsConfig>,
        connect_timeout: Option<Duration>,
        request_timeout: Option<Duration>,
        connect_lazy: bool,
    ) -> GeyserGrpcClientResult<GeyserGrpcClient<impl Interceptor>>
    where
        E: Into<Bytes>,
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        let (mut endpoint, interceptor) = Self::connect2(endpoint, tls_config, x_token)?;

        if let Some(timeout) = connect_timeout {
            endpoint = endpoint.connect_timeout(timeout);
        }
        if let Some(timeout) = request_timeout {
            endpoint = endpoint.timeout(timeout);
        }
        let channel = if connect_lazy {
            endpoint.connect_lazy()
        } else {
            endpoint.connect().await?
        };

        Ok(GeyserGrpcClient {
            health: HealthClient::with_interceptor(channel.clone(), interceptor.clone()),
            geyser: GeyserClient::with_interceptor(channel, interceptor)
                .max_decoding_message_size(64 * 1024 * 1024), // 64 MiB
        })
    }
}

impl<F: Interceptor> GeyserGrpcClient<F> {
    pub async fn health_check(&mut self) -> GeyserGrpcClientResult<HealthCheckResponse> {
        let request = HealthCheckRequest {
            service: "geyser.Geyser".to_owned(),
        };
        let response = self.health.check(request).await?;
        Ok(response.into_inner())
    }

    pub async fn health_watch(
        &mut self,
    ) -> GeyserGrpcClientResult<impl Stream<Item = Result<HealthCheckResponse, Status>>> {
        let request = HealthCheckRequest {
            service: "geyser.Geyser".to_owned(),
        };
        let response = self.health.watch(request).await?;
        Ok(response.into_inner())
    }

    pub async fn subscribe(
        &mut self,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        self.subscribe_with_request(None).await
    }

    pub async fn subscribe_with_request(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            subscribe_tx
                .send(request)
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        }
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe_once(
        &mut self,
        slots: HashMap<String, SubscribeRequestFilterSlots>,
        accounts: HashMap<String, SubscribeRequestFilterAccounts>,
        transactions: HashMap<String, SubscribeRequestFilterTransactions>,
        entry: HashMap<String, SubscribeRequestFilterEntry>,
        blocks: HashMap<String, SubscribeRequestFilterBlocks>,
        blocks_meta: HashMap<String, SubscribeRequestFilterBlocksMeta>,
        commitment: Option<CommitmentLevel>,
        accounts_data_slice: Vec<SubscribeRequestAccountsDataSlice>,
        ping: Option<SubscribeRequestPing>,
    ) -> GeyserGrpcClientResult<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        self.subscribe_once2(SubscribeRequest {
            slots,
            accounts,
            transactions,
            entry,
            blocks,
            blocks_meta,
            commitment: commitment.map(|value| value as i32),
            accounts_data_slice,
            ping,
        })
        .await
    }

    pub async fn subscribe_once2(
        &mut self,
        request: SubscribeRequest,
    ) -> GeyserGrpcClientResult<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        self.subscribe_with_request(Some(request))
            .await
            .map(|(_sink, stream)| stream)
    }

    pub async fn ping(&mut self, count: i32) -> GeyserGrpcClientResult<PongResponse> {
        let message = PingRequest { count };
        let request = tonic::Request::new(message);
        let response = self.geyser.ping(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_latest_blockhash(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<GetLatestBlockhashResponse> {
        let request = tonic::Request::new(GetLatestBlockhashRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_latest_blockhash(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_block_height(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<GetBlockHeightResponse> {
        let request = tonic::Request::new(GetBlockHeightRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_block_height(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_slot(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<GetSlotResponse> {
        let request = tonic::Request::new(GetSlotRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_slot(request).await?;
        Ok(response.into_inner())
    }

    pub async fn is_blockhash_valid(
        &mut self,
        blockhash: String,
        commitment: Option<CommitmentLevel>,
    ) -> GeyserGrpcClientResult<IsBlockhashValidResponse> {
        let request = tonic::Request::new(IsBlockhashValidRequest {
            blockhash,
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.is_blockhash_valid(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_version(&mut self) -> GeyserGrpcClientResult<GetVersionResponse> {
        let request = tonic::Request::new(GetVersionRequest {});
        let response = self.geyser.get_version(request).await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::{GeyserGrpcClient, GeyserGrpcClientError};

    #[tokio::test]
    async fn test_channel_https_success() {
        let endpoint = "https://ams17.rpcpool.com:443";
        let x_token = "1000000000000000000000000007";
        let res = GeyserGrpcClient::connect(endpoint, Some(x_token), None);
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_channel_http_success() {
        let endpoint = "http://127.0.0.1:10000";
        let x_token = "1234567891012141618202224268";
        let res = GeyserGrpcClient::connect(endpoint, Some(x_token), None);
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_channel_invalid_token_some() {
        let endpoint = "http://127.0.0.1:10000";
        let x_token = "";
        let res = GeyserGrpcClient::connect(endpoint, Some(x_token), None);
        assert!(matches!(
            res,
            Err(GeyserGrpcClientError::InvalidXTokenLength(_))
        ));
    }

    #[tokio::test]
    async fn test_channel_invalid_token_none() {
        let endpoint = "http://127.0.0.1:10000";
        let res = GeyserGrpcClient::connect::<_, String>(endpoint, None, None);
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_channel_invalid_uri() {
        let endpoint = "sites/files/images/picture.png";
        let x_token = "1234567891012141618202224268";
        let res = GeyserGrpcClient::connect(endpoint, Some(x_token), None);
        assert!(matches!(res, Err(GeyserGrpcClientError::InvalidUri(_))));
    }
}
