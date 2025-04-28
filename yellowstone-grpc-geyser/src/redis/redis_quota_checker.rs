use tokio::time::{interval, Duration};
use std::sync::Arc;
use crate::connection_manager::ConnectionManager;
use crate::redis::refreshing_fallback_cache::RefreshingFallbackCache;

pub async fn start_redis_quota_checker(
    manager: Arc<ConnectionManager>,
    quota_cache: Arc<RefreshingFallbackCache<bool>>,
    check_interval: Duration,
) {
    let mut ticker = interval(check_interval);

    loop {
        ticker.tick().await;

        let teams = manager.list_active_teams();
        for team_id in teams {
            match quota_cache.get_or_refresh(&team_id).await {
                Ok(true) => {
                    log::info!("Team {} is capped, shutting down connection", team_id);
                    manager.shutdown_client(&team_id);
                }
                Ok(false) => {
                    // Team is fine, do nothing
                }
                Err(e) => {
                    log::error!("Failed to check quota for team {}: {:?}", team_id, e);
                }
            }
        }
    }
}
