use deadpool_redis::{Config, Pool, Connection, Runtime};
use moka::future::Cache;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use log::{error};

#[derive(Clone)]
pub struct RefreshingFallbackCache<V>
where
    V: Clone + Send + Sync + 'static,
{
    redis_pool: Arc<Pool>,
    cache: Cache<String, Arc<CachedValue<V>>>,
    redis_prefix: String,
    ttl: Duration,
    background_buffer: Duration,
    value_parser: Arc<dyn Fn(Option<String>) -> V + Send + Sync>,
}

#[derive(Debug)]
struct CachedValue<V> {
    value: V,
    fetched_at: Instant,
}

impl<V> RefreshingFallbackCache<V>
where
    V: Clone + Send + Sync + 'static,
{
    pub async fn new(
        redis_url: &str,
        redis_prefix: String,
        ttl: Duration,
        capacity: usize,
        background_buffer: Duration,
        value_parser: Arc<dyn Fn(Option<String>) -> V + Send + Sync>,
    ) -> Result<Self> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;

        let cache = Cache::new(capacity);

        Ok(Self {
            redis_pool: Arc::new(pool),
            cache,
            redis_prefix,
            ttl,
            background_buffer,
            value_parser,
        })
    }

    pub async fn get_or_refresh(&self, key_suffix: &str) -> Result<V> {
        let now = Instant::now();
        let key = key_suffix.to_string();

        if let Some(cached) = self.cache.get(&key) {
            let age = now.duration_since(cached.fetched_at);

            if age < self.ttl {
                return Ok(cached.value.clone());
            }
            if age < self.ttl + self.background_buffer {
                self.trigger_background_refresh(key.clone());
                return Ok(cached.value.clone());
            }
        }

        let value = self.fetch_and_update(&key).await?;
        Ok(value)
    }

    fn trigger_background_refresh(&self, key: String) {
        let redis_pool = Arc::clone(&self.redis_pool);
        let redis_key = format!("{}:{}", self.redis_prefix, key);
        let cache = self.cache.clone();
        let parser = Arc::clone(&self.value_parser);

        tokio::spawn(async move {
            if let Ok(mut conn) = redis_pool.get().await {
                match conn.get::<_, Option<String>>(&redis_key).await {
                    Ok(result) => {
                        let parsed_value = parser(result);
                        let cached = Arc::new(CachedValue {
                            value: parsed_value,
                            fetched_at: Instant::now(),
                        });
                        let _ = cache.insert(key, cached).await;
                    }
                    Err(e) => {
                        error!("Background Redis refresh error for key {}: {:?}", redis_key, e);
                    }
                }
            }
        });
    }

    async fn fetch_and_update(&self, key: &str) -> Result<V> {
        let redis_key = format!("{}:{}", self.redis_prefix, key);
        let mut conn: Connection = self.redis_pool.get().await?;

        let result: Option<String> = conn.get(&redis_key).await.ok();

        let parsed_value = (self.value_parser)(result);
        let cached = Arc::new(CachedValue {
            value: parsed_value.clone(),
            fetched_at: Instant::now(),
        });

        self.cache.insert(key.to_string(), cached).await;

        Ok(parsed_value)
    }
}
