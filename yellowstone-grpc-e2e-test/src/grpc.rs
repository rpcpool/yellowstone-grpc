use {
    crate::scenarios::RunConfig,
    anyhow::{Context, Result},
    tokio::net::TcpStream,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient},
};

pub async fn new_client(config: &RunConfig) -> Result<GeyserGrpcClient> {
    let mut builder = GeyserGrpcClient::build_from_shared(config.endpoint.clone())
        .context("endpoint should be a valid URI")?;

    if config.endpoint.starts_with("https://") {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .context("failed to configure TLS for HTTPS endpoint")?;
    }

    let builder = builder
        .x_token(config.x_token.clone())
        .context("x-token should be valid ASCII metadata if provided")?;

    let builder = builder
        .max_decoding_message_size(100_000_000)
        .http2_adaptive_window(true)
        .accept_compressed(yellowstone_grpc_proto::tonic::codec::CompressionEncoding::Zstd);

    if let Some(dial) = config.dial.clone() {
        builder
            .connect_with_connector(tower::service_fn(move |_uri| {
                let dial = dial.clone();
                async move {
                    let stream = TcpStream::connect(dial).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .context("client should build from endpoint, dial and token")
    } else {
        builder
            .connect()
            .await
            .context("client should build from endpoint and token")
    }
}
