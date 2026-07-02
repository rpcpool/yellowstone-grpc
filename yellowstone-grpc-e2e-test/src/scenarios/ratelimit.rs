use {
    crate::scenarios::RunConfig,
    anyhow::{bail, Result},
    futures::{stream::FuturesUnordered, StreamExt},
    serde::Deserialize,
    std::time::Duration,
    yellowstone_grpc_client::GeyserGrpcClientError,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::tonic::Code,
};

/// Module-level config loaded from `[scenarios.ratelimit]` in the TOML config file.
#[derive(Debug, Deserialize)]
pub struct GetVersionConfig {
    #[serde(
        deserialize_with = "crate::serde::deserialize_int_str",
        default = "GetVersionConfig::default_max_requests"
    )]
    pub max_requests: u32,
    #[serde(with = "humantime_serde", default = "GetVersionConfig::default_window")]
    pub window: Duration,
}

impl GetVersionConfig {
    pub const DEFAULT_MAX_REQUESTS: u32 = 1000;
    pub const DEFAULT_WINDOW: Duration = Duration::from_secs(10);

    pub const fn default_max_requests() -> u32 {
        Self::DEFAULT_MAX_REQUESTS
    }

    pub const fn default_window() -> Duration {
        Self::DEFAULT_WINDOW
    }
}

impl Default for GetVersionConfig {
    fn default() -> Self {
        Self {
            max_requests: Self::DEFAULT_MAX_REQUESTS,
            window: Self::DEFAULT_WINDOW,
        }
    }
}

/// Ensure the ratelimit guards are in-place for GetVersion gRPC requests, and that the retry-after header is returned when the limit is exceeded.
#[test_helper(name = "get-version", config = GetVersionConfig)]
pub async fn test_get_version_ratelimit(config: &RunConfig, cfg: &GetVersionConfig) -> Result<()> {
    // Use short concurrent bursts to avoid token-bucket refill masking in slow serial loops.
    const BURST_SIZE: usize = 128;

    let total = cfg.max_requests.saturating_add(1) as usize;
    let mut sent = 0usize;
    let mut first_resource_exhausted_at: Option<usize> = None;

    // Make sure we it ratelimited within 10% of the expected request count, to avoid false positives in case of a misconfigured ratelimit.
    let limit_to_reach_ratelimited = (cfg.max_requests as f64 * 1.1).ceil() as usize;

    'spam: while sent < limit_to_reach_ratelimited {
        let mut futures = FuturesUnordered::new();

        for _ in 0..BURST_SIZE {
            let run_cfg = config.clone();
            futures.push(async move {
                let mut client = crate::grpc::new_client(&run_cfg).await?;
                client.get_version().await.map_err(anyhow::Error::from)
            });
        }

        let mut idx_in_batch = 0usize;
        while let Some(result) = futures.next().await {
            idx_in_batch += 1;
            let absolute_idx = sent + idx_in_batch;

            match result {
                Ok(_) => {}
                Err(e) => match e.downcast::<GeyserGrpcClientError>() {
                    Ok(GeyserGrpcClientError::TonicStatus(status))
                        if status.code() == Code::ResourceExhausted =>
                    {
                        first_resource_exhausted_at.get_or_insert(absolute_idx);
                        break 'spam;
                    }
                    Ok(other) => {
                        bail!("request {} failed unexpectedly: {other:#}", absolute_idx);
                    }
                    Err(other) => {
                        bail!("request {} failed unexpectedly: {other:#}", absolute_idx);
                    }
                },
            }
        }

        sent += BURST_SIZE;
    }

    let Some(first_denied_at) = first_resource_exhausted_at else {
        bail!(
            "expected at least one ResourceExhausted within {} requests (configured max_requests={})",
            total,
            cfg.max_requests
        );
    };

    if first_denied_at <= cfg.max_requests as usize {
        bail!(
            "rate limit triggered before exhausting quota (request {} of {}), window={:?}",
            first_denied_at,
            cfg.max_requests,
            cfg.window
        );
    }

    if first_denied_at > limit_to_reach_ratelimited {
        bail!(
            "rate limit not triggered within {} requests (configured max_requests={}), window={:?}",
            limit_to_reach_ratelimited,
            cfg.max_requests,
            cfg.window
        );
    }
    Ok(())
}
