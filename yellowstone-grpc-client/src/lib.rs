use {
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    http::uri::InvalidUri,
    std::collections::HashMap,
    tonic::{
        codec::Streaming,
        metadata::{errors::InvalidMetadataValue, AsciiMetadataValue},
        service::{interceptor::InterceptedService, Interceptor},
        transport::channel::{Channel, ClientTlsConfig},
        Request, Response, Status,
    },
    yellowstone_grpc_proto::prelude::{
        geyser_client::GeyserClient, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate,
    },
};

pub const XTOKEN_LENGTH: usize = 28;

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
    client: GeyserClient<InterceptedService<Channel, F>>,
}

impl GeyserGrpcClient<()> {
    pub fn connect<E, T>(
        endpoint: E,
        x_token: Option<T>,
        tls_config: Option<ClientTlsConfig>,
    ) -> GeyserGrpcClientResult<GeyserGrpcClient<impl Interceptor>>
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
        let channel = endpoint.connect_lazy();

        let x_token: Option<AsciiMetadataValue> = match x_token {
            Some(x_token) => Some(x_token.try_into()?),
            None => None,
        };
        match x_token {
            Some(token) if token.len() != XTOKEN_LENGTH => {
                return Err(GeyserGrpcClientError::InvalidXTokenLength(token.len()));
            }
            _ => {}
        }

        let client = GeyserClient::with_interceptor(channel, move |mut req: Request<()>| {
            if let Some(x_token) = x_token.clone() {
                req.metadata_mut().insert("x-token", x_token);
            }
            Ok(req)
        });

        Ok(GeyserGrpcClient { client })
    }
}

impl<F: Interceptor> GeyserGrpcClient<F> {
    pub async fn subscribe(
        &mut self,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        let (subscribe_tx, subscribe_rx) = mpsc::unbounded();
        let response: Response<Streaming<SubscribeUpdate>> =
            self.client.subscribe(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    pub async fn subscribe_once(
        &mut self,
        slots: HashMap<String, SubscribeRequestFilterSlots>,
        accounts: HashMap<String, SubscribeRequestFilterAccounts>,
        transactions: HashMap<String, SubscribeRequestFilterTransactions>,
        blocks: HashMap<String, SubscribeRequestFilterBlocks>,
        blocks_meta: HashMap<String, SubscribeRequestFilterBlocksMeta>,
    ) -> GeyserGrpcClientResult<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        let (mut subscribe_tx, response) = self.subscribe().await?;
        subscribe_tx
            .send(SubscribeRequest {
                slots,
                accounts,
                transactions,
                blocks,
                blocks_meta,
            })
            .await?;
        Ok(response)
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
        let x_token = "123";
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
