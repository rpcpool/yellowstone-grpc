use {
    futures::future::BoxFuture,
    moka::future::Cache,
    std::{
        hash::Hash,
        sync::{atomic::AtomicBool, Arc, Mutex},
    },
};

/// Extension for `moka::future::Cache` that behaves like `optionally_get_with`
/// but also propagates initializer errors.
///
/// Why this exists:
/// - `optionally_get_with` coalesces concurrent calls for the same key.
/// - If the coalesced initializer returns `None`, waiting callers may also receive
///   `None` without having run their own initializer future.
///
/// This extension adds deterministic behavior for that case:
/// - If this caller's initializer ran and returned `Err(E)`, return that `Err(E)`.
/// - If this caller's initializer ran and returned `Ok(None)`, return `Ok(None)`.
/// - If this caller only waited on another initializer that returned `None`, retry
///   so one of the waiters can attempt initialization again.
pub(crate) trait CacheTryOptionallyGetWithExt<K, V> {
    /// Resolves a value for `key` using `future_factory`, with `None` remaining
    /// non-cached exactly like `optionally_get_with`.
    ///
    /// Returns:
    /// - `Ok(Some(V))` when initialization succeeds with a value.
    /// - `Ok(None)` when this caller's initializer successfully returns `None`.
    /// - `Err(E)` when this caller's initializer fails.
    fn try_optionally_get_with<'a, E, F, Fut>(
        &'a self,
        key: K,
        max_loop_iterations: usize,
        future_factory: F,
    ) -> BoxFuture<'a, Result<Option<V>, E>>
    where
        K: Eq + Hash + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
        E: Send + 'a,
        F: FnMut() -> Fut + Send + 'a,
        Fut: std::future::Future<Output = Result<Option<V>, E>> + Send + 'a;
}

impl<K, V> CacheTryOptionallyGetWithExt<K, V> for Cache<K, V> {
    fn try_optionally_get_with<'a, E, F, Fut>(
        &'a self,
        key: K,
        max_loop_iterations: usize,
        mut future_factory: F,
    ) -> BoxFuture<'a, Result<Option<V>, E>>
    where
        K: Eq + Hash + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
        E: Send + 'a,
        F: FnMut() -> Fut + Send + 'a,
        Fut: std::future::Future<Output = Result<Option<V>, E>> + Send + 'a,
    {
        struct CacheCallTracker<E> {
            called: AtomicBool,
            error: Mutex<Option<E>>,
        }

        impl<E> Default for CacheCallTracker<E> {
            fn default() -> Self {
                Self {
                    called: AtomicBool::new(false),
                    error: Mutex::new(None),
                }
            }
        }

        Box::pin(async move {
            let max_loop_iterations = max_loop_iterations.max(1);
            let mut loop_iteration = 0usize;

            loop {
                loop_iteration += 1;
                let cache_call_tracker = Arc::new(CacheCallTracker::default());
                let get_fut = future_factory();
                let tracker = Arc::clone(&cache_call_tracker);

                let maybe = self
                    .optionally_get_with(key.clone(), async move {
                        tracker
                            .called
                            .store(true, std::sync::atomic::Ordering::Release);
                        match get_fut.await {
                            Ok(value) => value,
                            Err(e) => {
                                *tracker.error.lock().expect("cache call tracker lock") = Some(e);
                                None
                            }
                        }
                    })
                    .await;

                if let Some(value) = maybe {
                    return Ok(Some(value));
                }

                if cache_call_tracker
                    .called
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    if let Some(error) = cache_call_tracker
                        .error
                        .lock()
                        .expect("cache call tracker lock")
                        .take()
                    {
                        return Err(error);
                    }

                    return Ok(None);
                }

                if loop_iteration >= max_loop_iterations {
                    return Ok(None);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::CacheTryOptionallyGetWithExt,
        moka::future::Cache,
        std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn try_optionally_get_with_retries_waiters_after_none_result() {
        let cache: Cache<String, u32> = Cache::new(16);
        let key = "retry-key".to_string();
        let attempts = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let key = key.clone();
            let attempts = Arc::clone(&attempts);
            tasks.push(tokio::spawn(async move {
                cache
                    .try_optionally_get_with(key, 8, move || {
                        let attempts = Arc::clone(&attempts);
                        async move {
                            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                            if attempt == 0 {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                Ok::<Option<u32>, ()>(None)
                            } else {
                                Ok::<Option<u32>, ()>(Some(7))
                            }
                        }
                    })
                    .await
                    .expect("try_optionally_get_with should not fail")
            }));
        }

        let mut none_count = 0usize;
        let mut some_count = 0usize;
        for task in tasks {
            let result = task.await.expect("task join");
            if result.is_some() {
                some_count += 1;
            } else {
                none_count += 1;
            }
        }

        assert_eq!(
            none_count, 1,
            "only the first initializer caller should return None"
        );
        assert_eq!(
            some_count, 7,
            "all waiters should retry and eventually get Some"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "one None attempt then one Some attempt"
        );
    }
}
