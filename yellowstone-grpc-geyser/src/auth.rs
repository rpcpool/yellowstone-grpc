use {
    crate::{
        cache_ext::CacheTryOptionallyGetWithExt,
        config::{FileBackedAuthConfig, HttpBackedAuthConfig},
        metrics,
    },
    futures::future::{self, BoxFuture},
    http::{request::Parts, HeaderMap, HeaderName, HeaderValue, StatusCode},
    moka::future::{Cache, CacheBuilder},
    reqwest::{IntoUrl, Url},
    std::{collections::HashMap, convert::Infallible, str::FromStr, sync::Arc},
    yellowstone_grpc_tools::server::tonic::auth::service::SharedAuthenticator,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionKey {
    pub token: String,
    pub host: String,
}

impl From<(&str, &str)> for SubscriptionKey {
    fn from((host, token): (&str, &str)) -> Self {
        Self {
            host: host.to_owned(),
            token: token.to_owned(),
        }
    }
}

///
/// An HTTP-based implementation of [`SubscriptionRepository`] that fetches subscription info a REST Endpoint.
///
/// The expected REST endpoint should accept `host` and `token` query parameters and return a JSON object representing the subscription info, or a 404 status code if the subscription is not found.
#[derive(Clone)]
pub struct HttpSubscriptionRepository {
    client: reqwest::Client,
    url: Url,
    ///
    /// Optional cache for subscription info. If provided, the repository will cache subscription info.
    /// TTL is controlled by the `Cache` configuration.
    cache: Option<Cache<String, SubscriptionInfo>>,
    forwarded_headers: Vec<String>,
}

impl HttpSubscriptionRepository {
    #[cfg(test)]
    pub fn new<U: IntoUrl>(url: U, forwarded_headers: Vec<String>) -> Result<Self, reqwest::Error> {
        Self::with_client(reqwest::Client::new(), url, forwarded_headers)
    }

    #[cfg(test)]
    pub fn with_client<U: IntoUrl>(
        client: reqwest::Client,
        url: U,
        forwarded_headers: Vec<String>,
    ) -> Result<Self, reqwest::Error> {
        Self::with_client_and_cache(client, url, None, forwarded_headers)
    }

    pub fn with_client_and_cache<U: IntoUrl>(
        client: reqwest::Client,
        url: U,
        cache: Option<Cache<String, SubscriptionInfo>>,
        forwarded_headers: Vec<String>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client,
            url: url.into_url()?,
            cache,
            forwarded_headers,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpTritonSubscriptionRepositoryError {
    ///
    /// This error occurs when the HTTP request to the subscription repository fails, such as due to network issues, timeouts, or receiving an unexpected response. It indicates that the request could not be completed successfully.
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),
    ///
    /// This error occurs when the URL provided to the repository is invalid or cannot be parsed. It indicates a configuration issue that should be addressed by the developer or operator.
    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),
}

impl HttpSubscriptionRepository {
    fn get_subscription_info(
        &self,
        host: &str,
        headers: &HeaderMap<HeaderValue>,
    ) -> BoxFuture<'static, Result<Option<SubscriptionInfo>, HttpTritonSubscriptionRepositoryError>>
    {
        // To share cache across task, clone it, its cheap.
        let maybe_cache = self.cache.clone();
        let mut url = self.url.clone();
        url.query_pairs_mut().append_pair("host", host);

        let mut header_map = HeaderMap::new();
        let mut caching_key_parts = Vec::with_capacity(self.forwarded_headers.len() + 1);
        caching_key_parts.push(host);
        // Make sure we build the cache key following the forwarded_headers order, so that the same headers in different order don't produce different cache keys.
        for header_to_forward in &self.forwarded_headers {
            if let Some(value) = headers.get(header_to_forward) {
                caching_key_parts.push(header_to_forward);
                caching_key_parts.push(
                    value
                        .to_str()
                        .expect("forwarder header value should be valid UTF-8"),
                );
                header_map.insert(
                    HeaderName::from_str(header_to_forward)
                        .expect("forwarder header name should be valid"),
                    value.clone(),
                );
            }
        }
        const SEPARATOR: &str = "\0";
        let caching_key = caching_key_parts.join(SEPARATOR);

        let request_builder = self.client.get(url).headers(header_map);

        let future_factory = move || {
            let request = request_builder
                .try_clone()
                .expect("request should be clonable");
            async move {
                let response = request.send().await.and_then(|res| res.error_for_status());
                match response {
                    Ok(response) => {
                        let subscription_info = response.json::<SubscriptionInfo>().await?;
                        Ok::<Option<SubscriptionInfo>, reqwest::Error>(Some(subscription_info))
                    }
                    Err(e) => {
                        if e.is_status() {
                            log::warn!(
                                "Subscription repository returned error status: {}",
                                e.status().unwrap()
                            );
                        }
                        if let Some(StatusCode::NOT_FOUND) = e.status() {
                            Ok(None)
                        } else {
                            Err(e)
                        }
                    }
                }
            }
        };

        Box::pin(async move {
            if let Some(cache) = maybe_cache.as_ref() {
                const MAX_ATTEMPTS: usize = 10;
                cache
                    .try_optionally_get_with(caching_key, MAX_ATTEMPTS, future_factory)
                    .await
                    .map_err(HttpTritonSubscriptionRepositoryError::RequestError)
            } else {
                future_factory()
                    .await
                    .map_err(HttpTritonSubscriptionRepositoryError::RequestError)
            }
        })
    }
}

impl HttpBackedAuthConfig {
    ///
    /// Factory-method to build a subscription repository from the configuration. This method constructs an `HttpSubscriptionRepository` using the provided URL and optional cache settings.
    /// This is a convenience method that encapsulates the creation logic and ensures that the repository is configured correctly based on the configuration parameters.
    pub fn build_repository(&self) -> HttpSubscriptionRepository {
        let maybe_cache = self
            .subscription_resolution_cache_ttl
            .map(|ttl| CacheBuilder::default().time_to_live(ttl).build());

        HttpSubscriptionRepository::with_client_and_cache(
            reqwest::Client::new(),
            self.subscription_resolver_url.clone(),
            maybe_cache,
            self.forwarded_headers.clone(),
        )
        .expect("url parsing should be infallible at this point")
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SubscriptionRateLimits {
    #[serde(default = "SubscriptionRateLimits::default_rate_limit")]
    pub default: i32,
    #[serde(default)]
    pub methods: HashMap<String, i32>,
}

impl SubscriptionRateLimits {
    pub const fn default_rate_limit() -> i32 {
        1000
    }
}

impl Default for SubscriptionRateLimits {
    fn default() -> Self {
        Self {
            default: Self::default_rate_limit(),
            methods: HashMap::from([
                ("/geyser.Geyser/Subscribe".to_string(), 1000),
                ("/geyser.Geyser/SubscribeReplayInfo".to_string(), 1000),
                ("/geyser.Geyser/Ping".to_string(), 1000),
                ("/geyser.Geyser/GetLatestBlockhash".to_string(), 1000),
                ("/geyser.Geyser/GetBlockHeight".to_string(), 1000),
                ("/geyser.Geyser/GetSlot".to_string(), 1000),
                ("/geyser.Geyser/IsBlockhashValid".to_string(), 1000),
                ("/geyser.Geyser/GetVersion".to_string(), 1000),
            ]),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SubscriptionInfo {
    pub subscription_id: String,
    #[serde(alias = "rate_limits")]
    pub ratelimits: Option<SubscriptionRateLimits>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TokenSubscriptionInfoMapping {
    pub token: String,
    pub host: String,
    pub subscription_info: SubscriptionInfo,
}

impl SharedAuthenticator for HttpSubscriptionRepository {
    type AuthError = HttpTritonSubscriptionRepositoryError;

    type AuthExtension = SubscriptionInfo;

    type AuthFut = BoxFuture<'static, Result<Option<Self::AuthExtension>, Self::AuthError>>;

    fn authenticate(&self, parts: &Parts) -> Self::AuthFut {
        let host = parts.uri.host();
        let Some(host) = host else {
            log::warn!("Missing host in request to subscription repository");
            return Box::pin(future::ready(Ok(None)));
        };

        let fut = self.get_subscription_info(host, &parts.headers);

        Box::pin(async move {
            fut.await.inspect(|maybe| {
                if maybe.is_some() {
                    metrics::incr_authorized_count();
                } else {
                    metrics::incr_unauthorized_count();
                }
            })
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildRepositoryFromFileError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    JsonParse(#[from] serde_json::Error),
}

impl FileBackedAuthConfig {
    pub fn build_repository(
        &self,
    ) -> Result<ConstantSubscriptionRepository, BuildRepositoryFromFileError> {
        let file_content = std::fs::read_to_string(&self.subscription_resolver_path)
            .map_err(BuildRepositoryFromFileError::Io)?;

        let mappings = serde_json::from_str::<Vec<TokenSubscriptionInfoMapping>>(&file_content)?;
        let it = mappings.into_iter().map(|mapping| {
            let key = SubscriptionKey {
                host: mapping.host,
                token: mapping.token,
            };
            (key, mapping.subscription_info)
        });
        Ok(ConstantSubscriptionRepository::from_iter(it))
    }
}

///
/// A subscription repository that is backed by a constant in-memory mapping of host and token to subscription info.
///
/// This is useful for testing or for scenarios where the subscription info is known ahead of time and does not change.
#[derive(Debug, Clone)]
pub struct ConstantSubscriptionRepository {
    hashmap: Arc<HashMap<String, HashMap<String, SubscriptionInfo>>>,
}

impl ConstantSubscriptionRepository {
    ///
    /// Creates a new `ConstantSubscriptionRepository` from an iterator of `(SubscriptionKey, SubscriptionInfo)` pairs.
    ///
    pub fn from_iter<IT>(hashmap: IT) -> Self
    where
        IT: IntoIterator<Item = (SubscriptionKey, SubscriptionInfo)>,
    {
        let mut nested: HashMap<String, HashMap<String, SubscriptionInfo>> = HashMap::new();

        for (SubscriptionKey { host, token }, info) in hashmap {
            nested.entry(host).or_default().insert(token, info);
        }

        Self {
            hashmap: Arc::new(nested),
        }
    }

    pub fn get(&self, host: &str, token: &str) -> Option<&SubscriptionInfo> {
        let hashmap = &self.hashmap;
        hashmap.get(host).and_then(|by_token| by_token.get(token))
    }
}

impl SharedAuthenticator for ConstantSubscriptionRepository {
    type AuthError = std::convert::Infallible;

    type AuthExtension = SubscriptionInfo;

    type AuthFut = BoxFuture<'static, Result<Option<Self::AuthExtension>, Self::AuthError>>;

    fn authenticate(&self, parts: &Parts) -> Self::AuthFut {
        let host = parts.uri.host();
        let token = parts
            .headers
            .get("x-token")
            .and_then(|value| value.to_str().ok());

        let Some(host) = host else {
            log::warn!("Missing host in request to subscription repository");
            return Box::pin(future::ready(Ok(None)));
        };
        let Some(token) = token else {
            log::warn!("Missing x-token header in request to subscription repository");
            return Box::pin(future::ready(Ok(None)));
        };

        let maybe_info = self.get(host, token).cloned();
        let fut = Box::pin(async move { Ok::<_, Infallible>(maybe_info) });

        Box::pin(async move {
            fut.await
                .map_err(|_infallible| unreachable!())
                .inspect(|maybe| {
                    if maybe.is_some() {
                        metrics::incr_authorized_count();
                    } else {
                        metrics::incr_unauthorized_count();
                    }
                })
        })
    }
}

///
/// An authenticator that assumes x-subscription-id is embedded in the request and does not perform any external validation.
///
/// This is useful for testing or scenarios where the subscription-id is trusted and does not need to be verified against an external service.
///
#[derive(Clone)]
pub struct TrustedMetadataAuthenticator {
    default_ratelimits: Arc<SubscriptionRateLimits>,
}

impl TrustedMetadataAuthenticator {
    pub const X_SUBSCRIPTION_ID_HEADER: &'static str = "x-subscription-id";

    pub fn new<IT>(default_ratelimits: IT) -> Self
    where
        IT: IntoIterator<Item = (String, i32)>,
    {
        Self {
            default_ratelimits: Arc::new(SubscriptionRateLimits {
                methods: HashMap::from_iter(default_ratelimits),
                ..Default::default()
            }),
        }
    }
}

impl SharedAuthenticator for TrustedMetadataAuthenticator {
    type AuthError = std::convert::Infallible;

    type AuthExtension = SubscriptionInfo;

    type AuthFut = BoxFuture<'static, Result<Option<Self::AuthExtension>, Self::AuthError>>;

    fn authenticate(&self, parts: &Parts) -> Self::AuthFut {
        let subscription_id = parts
            .headers
            .get(Self::X_SUBSCRIPTION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string());

        let maybe_info = subscription_id.map(|subscription_id| SubscriptionInfo {
            subscription_id,
            ratelimits: Some((*self.default_ratelimits).clone()),
        });

        Box::pin(async move { Ok(maybe_info) })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        http::StatusCode,
        http_body_util::Full,
        hyper::{
            body::Incoming, server::conn::http1, service::service_fn, Request as HyperRequest,
            Response as HyperResponse,
        },
        hyper_util::rt::TokioIo,
        moka::future::Cache,
        std::{
            convert::Infallible,
            net::SocketAddr,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc, Mutex,
            },
        },
        tokio::net::TcpListener,
    };

    #[derive(Debug, Clone)]
    struct CapturedRequest {
        method: http::Method,
        uri: String,
        headers: HeaderMap<HeaderValue>,
    }

    async fn spawn_mock_subscription_server(
    ) -> (Url, Arc<AtomicUsize>, Arc<Mutex<Vec<CapturedRequest>>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr: SocketAddr = listener.local_addr().expect("local addr");
        let request_count = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_count_for_task = Arc::clone(&request_count);
        let requests_for_task = Arc::clone(&requests);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let request_count = Arc::clone(&request_count_for_task);
            let requests = Arc::clone(&requests_for_task);

            let service = service_fn(move |req: HyperRequest<Incoming>| {
                let request_count = Arc::clone(&request_count);
                let requests = Arc::clone(&requests);
                async move {
                    request_count.fetch_add(1, Ordering::Relaxed);
                    requests
                        .lock()
                        .expect("request capture lock")
                        .push(CapturedRequest {
                            method: req.method().clone(),
                            uri: req.uri().to_string(),
                            headers: req.headers().clone(),
                        });

                    let body = Full::new(Bytes::from_static(br#"{"subscription_id":"sub-123"}"#));
                    let response = HyperResponse::builder()
                        .status(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(body)
                        .expect("response build");

                    Ok::<_, Infallible>(response)
                }
            });

            let io = TokioIo::new(stream);
            http1::Builder::new()
                .serve_connection(io, service)
                .await
                .expect("serve connection");
        });

        (
            Url::parse(&format!("http://{addr}/")).expect("test url"),
            request_count,
            requests,
        )
    }

    #[tokio::test]
    async fn repository_fetches_subscription_info_from_mock_server() {
        let (url, request_count, requests) = spawn_mock_subscription_server().await;
        let repo = HttpSubscriptionRepository::new(url, vec![]).expect("repo init");
        let headers = HeaderMap::new();

        let info = repo
            .get_subscription_info("example.com", &headers)
            .await
            .expect("subscription info");

        let info = info.expect("subscription should exist");
        assert_eq!(info.subscription_id, "sub-123");

        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        let captured = requests.lock().expect("capture lock");
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].method, http::Method::GET);
        assert_eq!(captured[0].uri, "/?host=example.com");
    }

    #[tokio::test]
    async fn authenticate_reads_host_and_xtoken_from_request_parts() {
        let (url, request_count, requests) = spawn_mock_subscription_server().await;
        let repo =
            HttpSubscriptionRepository::new(url, vec!["x-token".to_string()]).expect("repo init");

        let request = http::Request::builder()
            .uri("http://example.com/some/path")
            .header("x-token", "token-abc")
            .body(())
            .expect("request build");
        let (parts, _) = request.into_parts();

        let info = repo
            .authenticate(&parts)
            .await
            .expect("authenticated subscription info");

        let info = info.expect("subscription should exist");
        assert_eq!(info.subscription_id, "sub-123");

        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        let captured = requests.lock().expect("capture lock");
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].method, http::Method::GET);
        assert_eq!(captured[0].uri, "/?host=example.com");
        assert_eq!(
            captured[0]
                .headers
                .get("x-token")
                .and_then(|v| v.to_str().ok()),
            Some("token-abc")
        );
    }

    #[tokio::test]
    async fn repository_without_cache_hits_server_every_time() {
        let (url, request_count, requests) = spawn_mock_subscription_server().await;
        let repo = HttpSubscriptionRepository::new(url, vec![]).expect("repo init");
        let headers = HeaderMap::new();

        let first = repo
            .get_subscription_info("example.com", &headers)
            .await
            .expect("first subscription info");
        let second = repo
            .get_subscription_info("example.com", &headers)
            .await
            .expect("second subscription info");

        assert_eq!(
            first.as_ref().map(|s| s.subscription_id.as_str()),
            Some("sub-123")
        );
        assert_eq!(
            second.as_ref().map(|s| s.subscription_id.as_str()),
            Some("sub-123")
        );
        assert_eq!(request_count.load(Ordering::Relaxed), 2);
        assert_eq!(requests.lock().expect("capture lock").len(), 2);
    }

    #[tokio::test]
    async fn repository_with_cache_reuses_first_result_for_same_key() {
        let (url, request_count, requests) = spawn_mock_subscription_server().await;
        let cache: Cache<String, SubscriptionInfo> = Cache::new(16);
        let repo = HttpSubscriptionRepository::with_client_and_cache(
            reqwest::Client::new(),
            url,
            Some(cache),
            vec![],
        )
        .expect("repo init");
        let headers = HeaderMap::new();

        let first = repo
            .get_subscription_info("example.com", &headers)
            .await
            .expect("first subscription info");
        let second = repo
            .get_subscription_info("example.com", &headers)
            .await
            .expect("second subscription info");

        assert_eq!(
            first.as_ref().map(|s| s.subscription_id.as_str()),
            Some("sub-123")
        );
        assert_eq!(
            second.as_ref().map(|s| s.subscription_id.as_str()),
            Some("sub-123")
        );
        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        assert_eq!(requests.lock().expect("capture lock").len(), 1);
    }
}
