use {
    crate::{grpc::new_client, scenarios::RunConfig},
    anyhow::{ensure, Context, Result},
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
};

/// Verifies that the health check route returns a status of "ok".
#[test_helper(name = "health-routes")]
pub async fn test_health_routes(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;
    let resp = client
        .health_check()
        .await
        .context("health_check should succeed")?;
    ensure!(
        resp.status == 1,
        "health check should return status ok, got {}",
        resp.status
    );

    let mut watch_st = client
        .health_watch()
        .await
        .context("health_watch should succeed")?;
    let resp = watch_st
        .next()
        .await
        .ok_or(anyhow::anyhow!("none health check response"))??;

    ensure!(
        resp.status == 1,
        "health watch should return status ok, got {}",
        resp.status
    );
    Ok(())
}
