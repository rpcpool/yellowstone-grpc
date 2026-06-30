use {
    bytes::Bytes,
    http::{request::Parts, Request, Response, StatusCode},
    pin_project::pin_project,
    std::{
        future::Future,
        sync::{Arc, Mutex},
        task::{ready, Context, Poll},
    },
    tokio::sync::{OwnedSemaphorePermit, Semaphore},
    tokio_util::sync::PollSemaphore,
    tonic::{
        body::Body as TonicBody,
        codegen::{Body as HttpBody, Service, StdError},
        Code,
    },
    tower_layer::Layer,
};

///
/// Helper function to create a gRPC error response with the specified gRPC status code and HTTP status code.
/// This is important to use both the gRPC status code and the HTTP status code to ensure that clients can correctly interpret the error response.
/// Filling both will ensure any client can properly interpret and print nicer error message to the user.
fn grpc_error_response(code: Code, status: StatusCode) -> Response<TonicBody> {
    let mut response = Response::new(TonicBody::empty());
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(tonic::Status::GRPC_STATUS, (code as i32).into());
    headers.insert(
        http::header::CONTENT_TYPE,
        tonic::metadata::GRPC_CONTENT_TYPE,
    );
    response
}

///
/// A trait representing a shared authentication mechanism that can be applied to incoming requests.
///
/// The authenticator is expected to be cloneable and thread-safe, allowing it to be shared across multiple service instances in a concurrent server environment.
///
/// A SharedAuthenticator takes an incoming request, performs asynchronous authentication logic (e.g. validating tokens, checking credentials), and either produces an authenticated request (potentially with additional extensions attached) or an authentication error.
///
pub trait SharedAuthenticator: Send + Sync + Clone + 'static {
    type AuthError: std::error::Error + Sync + Send + 'static;

    ///
    /// Information about the user being authenticated, which can be attached to the request as an extension for use by downstream services after successful authentication.
    ///
    type AuthExtension: Clone + Sync + Send + 'static;

    ///
    /// The future returned by the `authenticate` method.
    ///
    /// This future will resolve to either an authenticated request (potentially with extensions attached) or an authentication error. The service will wait for this future to complete before deciding whether to forward the request to the inner service or return an unauthorized response.
    /// The future should not do anything unless polled.
    type AuthFut: Future<Output = Result<Option<Self::AuthExtension>, Self::AuthError>>
        + Send
        + 'static;

    ///
    /// Authenticate an incoming request.
    ///
    /// This method takes ownership of the request and returns a future that will resolve to either an authenticated request (potentially with extensions attached) or an authentication error. The service will wait for this future to complete before deciding whether to forward the request to the inner service or return an unauthorized response.
    fn authenticate(&self, parts: &Parts) -> Self::AuthFut;
}

///
/// Tower service layer that applies a shared authenticator to incoming requests, returning 401 Unauthorized for requests that fail authentication and forwarding authenticated requests to the inner service.
///
/// # Generic Parameters
/// - `Auth`: The type of the shared authenticator. Must implement [`SharedAuthenticator`].
/// - `S`: The type of the inner service to which authenticated requests will be forwarded.
///
pub struct AuthGuardedService<S, Auth> {
    inner: Arc<Mutex<S>>,
    authenticator: Auth,
    poll_semaphore: PollSemaphore,
    already_acquired_permit: Option<OwnedSemaphorePermit>,
}

impl<S, Auth> Clone for AuthGuardedService<S, Auth>
where
    Auth: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            authenticator: self.authenticator.clone(),
            poll_semaphore: self.poll_semaphore.clone(),
            already_acquired_permit: None,
        }
    }
}

///
/// Tower layer that wraps a service into [`AuthGuardedService`] with a provided shared authenticator. This allows you to easily apply authentication logic to any Tower service by layering it with `AuthLayer`.
///
/// # Generic Parameters
///
/// - `Auth`: The type of the shared authenticator. Must implement [`SharedAuthenticator`].
///
pub struct AuthLayer<Auth> {
    authenticator: Auth,
    concurrent_auth_limit: usize,
}

impl<Auth> AuthLayer<Auth> {
    pub fn new(authenticator: Auth, concurrent_auth_limit: usize) -> Self {
        Self {
            authenticator,
            concurrent_auth_limit,
        }
    }
}

impl<S, Auth> Layer<S> for AuthLayer<Auth>
where
    Auth: Clone,
{
    type Service = AuthGuardedService<S, Auth>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthGuardedService::new(
            inner,
            self.authenticator.clone(),
            self.concurrent_auth_limit,
        )
    }
}

impl<S, Auth> AuthGuardedService<S, Auth> {
    ///
    /// Create a new `AuthGuardedService` wrapping the provided inner service with the provided shared authenticator.
    ///
    /// # Arguments
    ///
    /// - `inner`: The inner service to wrap with authentication logic.
    /// - `authenticator`: The shared authenticator to use for authenticating incoming requests.
    /// - `concurrent_auth_limit`: The maximum number of concurrent authentications to allow before backpressuring new requests.
    ///   This limits the number of requests that can be in the process of authenticating simultaneously.
    pub fn new(inner: S, authenticator: Auth, concurrent_auth_limit: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            authenticator,
            poll_semaphore: PollSemaphore::new(Arc::new(Semaphore::new(concurrent_auth_limit))),
            already_acquired_permit: None,
        }
    }
}

#[pin_project(project = AuthGuardFutureProj)]
pub enum AuthGuardFuture<S, AF, SF, ReqBody, ResBody, E, AuthX, AuthE>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>, Error = E, Future = SF>,
    AF: Future<Output = Result<Option<AuthX>, AuthE>>,
    SF: Future<Output = Result<Response<ResBody>, E>>,
{
    /// Ongoing authentication future. We stay in this state until the authentication future completes, at which point we either transition to PollingService (if auth succeeded but inner service wasn't ready) or CallingInner (if auth succeeded and inner service was ready), or Denied (if auth failed).
    Authenticating {
        #[pin]
        auth_fut: AF,
        request: Option<Request<ReqBody>>,
        permit: Option<OwnedSemaphorePermit>,
        inner_service: Arc<Mutex<S>>,
    },
    /// We've completed authentication and are now waiting for the inner service to become ready. We stay in this state until the inner service is ready, at which point we transition to CallingInner, or until the inner service returns an error from poll_ready, at which point we transition to Denied.
    ///
    /// # Note
    ///
    /// This state requires the permit to be held while we wait for the inner service to become ready, to ensure that we don't allow more than the configured number of concurrent authentications to proceed to the point of calling poll_ready on the inner service.
    PollingService {
        inner_service: Arc<Mutex<S>>,
        permit: Option<OwnedSemaphorePermit>,
        request: Option<Request<ReqBody>>,
    },
    ///
    /// We've completed authentication and the inner service was ready, so we're now calling the inner service's future.
    /// We stay in this state until the inner future completes, at which point we return the inner future's result.
    CallingInner {
        #[pin]
        inner_fut: SF,
    },
    Denied {
        result: Option<Result<Response<TonicBody>, E>>,
    },
}

/// The future returned by `AuthGuardedService::call`. This future manages the state of the authentication process,
/// including waiting for the authentication future to complete, checking if the inner service is ready,
/// and calling the inner service if authentication succeeds.
///
/// It also handles the case where authentication fails or the inner service is not ready,
/// returning appropriate HTTP responses in those cases.
impl<S, AF, SF, ReqBody, ResBody, E, AuthX, AuthE> Future
    for AuthGuardFuture<S, AF, SF, ReqBody, ResBody, E, AuthX, AuthE>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>, Error = E, Future = SF>,
    AF: Future<Output = Result<Option<AuthX>, AuthE>>,
    SF: Future<Output = Result<Response<ResBody>, E>>,
    ResBody: HttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<StdError>,
    AuthE: Into<StdError>,
    AuthX: Clone + Sync + Send + 'static,
    E: Into<StdError>,
{
    type Output = Result<Response<TonicBody>, E>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().project() {
                AuthGuardFutureProj::Authenticating {
                    auth_fut,
                    inner_service,
                    permit,
                    request,
                } => match auth_fut.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(maybe_auth_extensnion)) => {
                        let Some(auth_extensnion) = maybe_auth_extensnion else {
                            let denied = grpc_error_response(
                                Code::Unauthenticated,
                                StatusCode::UNAUTHORIZED,
                            );
                            self.set(AuthGuardFuture::Denied {
                                result: Some(Ok(denied)),
                            });
                            continue;
                        };

                        let Some(permit) = permit.take() else {
                            panic!("Authenticating state should always have a permit");
                        };
                        let Some(mut request) = request.take() else {
                            panic!("Authenticating state should always have a request");
                        };
                        request.extensions_mut().insert(auth_extensnion);
                        let next_state = {
                            let mut guard =
                                inner_service.lock().expect("inner service mutex poisoned");
                            match guard.poll_ready(cx) {
                                Poll::Pending => {
                                    // Move to PollingService state while we wait for the inner service to be ready
                                    AuthGuardFuture::PollingService {
                                        permit: Some(permit),
                                        request: Some(request),
                                        inner_service: Arc::clone(inner_service),
                                    }
                                }
                                Poll::Ready(Ok(())) => {
                                    // Inner service is ready, call it with the authenticated request
                                    let inner_fut = guard.call(request);
                                    AuthGuardFuture::CallingInner { inner_fut }
                                }
                                Poll::Ready(Err(e)) => {
                                    let denied = grpc_error_response(
                                        Code::Internal,
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                    );
                                    log::error!("Inner service not ready: {:?}", e.into());
                                    AuthGuardFuture::Denied {
                                        result: Some(Ok(denied)),
                                    }
                                }
                            }
                        };
                        self.set(next_state);
                    }
                    Poll::Ready(Err(auth_err)) => {
                        let denied =
                            grpc_error_response(Code::Internal, StatusCode::INTERNAL_SERVER_ERROR);
                        log::error!("Authentication failed: {:?}", auth_err.into());
                        self.set(AuthGuardFuture::Denied {
                            result: Some(Ok(denied)),
                        });
                    }
                },
                AuthGuardFutureProj::PollingService {
                    inner_service,
                    permit,
                    request,
                } => {
                    assert!(
                        permit.is_some(),
                        "PollingService state should always have a permit"
                    );
                    let next_state = {
                        let mut guard = inner_service.lock().expect("inner service mutex poisoned");
                        match guard.poll_ready(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Ok(())) => {
                                // Inner service is now ready, move to CallingInner state
                                let req = request
                                    .take()
                                    .expect("PollingService state should always have a request");
                                AuthGuardFuture::CallingInner {
                                    inner_fut: guard.call(req),
                                }
                            }
                            Poll::Ready(Err(e)) => {
                                let denied = grpc_error_response(
                                    Code::Internal,
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                );
                                log::error!("Inner service not ready: {:?}", e.into());
                                AuthGuardFuture::Denied {
                                    result: Some(Ok(denied)),
                                }
                            }
                        }
                    };
                    self.set(next_state);
                }
                AuthGuardFutureProj::CallingInner { inner_fut } => {
                    return match inner_fut.poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Ok(response)) => Poll::Ready(Ok(response.map(TonicBody::new))),
                        Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    }
                }
                AuthGuardFutureProj::Denied { result } => {
                    return Poll::Ready(
                        result
                            .take()
                            .expect("denied future polled after completion"),
                    )
                }
            }
        }
    }
}

impl<S, Auth, ReqBody, ResBody> Service<Request<ReqBody>> for AuthGuardedService<S, Auth>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: HttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<StdError>,
    Auth: SharedAuthenticator,
    S::Error: Into<StdError>,
{
    type Response = Response<TonicBody>;
    type Error = S::Error;
    type Future = AuthGuardFuture<
        S,
        Auth::AuthFut,
        S::Future,
        ReqBody,
        ResBody,
        S::Error,
        Auth::AuthExtension,
        Auth::AuthError,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.already_acquired_permit.is_some() {
            // If we've already acquired a permit, we're ready to go.
            Poll::Ready(Ok(()))
        } else {
            // Otherwise, we need to acquire a permit before we're ready.
            let permit = ready!(self.poll_semaphore.poll_acquire(cx));
            let permit = permit.expect("poll_semaphore should only return None if the semaphore is closed, but the semaphore is never closed in this service; qed");
            self.already_acquired_permit = Some(permit);
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        // The permit must be held until we are ready to call the inner service,
        // to ensure that we don't allow more than the configured number of concurrent authentications to proceed to the point of calling poll_ready on the inner service.
        let permit = self
            .already_acquired_permit
            .take()
            .expect("call should only be called after poll_ready has returned Ready, which guarantees we've acquired a permit; qed");

        let (parts, body) = request.into_parts();
        let auth_fut = self.authenticator.authenticate(&parts);
        let request = Request::from_parts(parts, body);

        AuthGuardFuture::Authenticating {
            auth_fut,
            permit: Some(permit),
            inner_service: Arc::clone(&self.inner),
            request: Some(request),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        futures::{executor::block_on, future::poll_fn},
        http_body_util::Empty,
        std::{
            convert::Infallible,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
        },
    };

    #[derive(Clone)]
    struct MockAuthenticator {
        allow: bool,
    }

    #[derive(Clone)]
    struct PendingOnceAuthenticator;

    #[derive(Clone)]
    struct NoneAuthenticator;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestAuthExtension {
        user_id: &'static str,
    }

    #[derive(Clone)]
    struct ExtensionAuthenticator;

    impl SharedAuthenticator for MockAuthenticator {
        type AuthError = Infallible;
        type AuthExtension = ();
        type AuthFut = std::future::Ready<Result<Option<()>, Self::AuthError>>;

        fn authenticate(&self, request: &Parts) -> Self::AuthFut {
            if self.allow {
                let _ = request;
                std::future::ready(Ok(Some(())))
            } else {
                std::future::ready(Ok(None))
            }
        }
    }

    impl SharedAuthenticator for ExtensionAuthenticator {
        type AuthError = Infallible;
        type AuthExtension = TestAuthExtension;
        type AuthFut = std::future::Ready<Result<Option<TestAuthExtension>, Self::AuthError>>;

        fn authenticate(&self, request: &Parts) -> Self::AuthFut {
            let _ = request;
            std::future::ready(Ok(Some(TestAuthExtension {
                user_id: "user-123",
            })))
        }
    }

    struct PendingOnceAuthFuture {
        pending_returned: bool,
    }

    impl Future for PendingOnceAuthFuture {
        type Output = Result<Option<()>, Infallible>;

        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.pending_returned {
                self.pending_returned = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            Poll::Ready(Ok(Some(())))
        }
    }

    impl SharedAuthenticator for PendingOnceAuthenticator {
        type AuthError = Infallible;
        type AuthExtension = ();
        type AuthFut = PendingOnceAuthFuture;

        fn authenticate(&self, request: &Parts) -> Self::AuthFut {
            let _ = request;
            PendingOnceAuthFuture {
                pending_returned: false,
            }
        }
    }

    impl SharedAuthenticator for NoneAuthenticator {
        type AuthError = Infallible;
        type AuthExtension = ();
        type AuthFut = std::future::Ready<Result<Option<()>, Self::AuthError>>;

        fn authenticate(&self, request: &Parts) -> Self::AuthFut {
            let _ = request;
            std::future::ready(Ok(None))
        }
    }

    #[derive(Clone)]
    struct TestService {
        call_count: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct ExtensionCheckingService {
        call_count: Arc<AtomicUsize>,
    }

    struct TestFuture {
        call_count: Arc<AtomicUsize>,
        polled: bool,
    }

    struct ExtensionCheckingFuture {
        call_count: Arc<AtomicUsize>,
        polled: bool,
    }

    impl Future for TestFuture {
        type Output = Result<Response<Empty<Bytes>>, Infallible>;

        fn poll(mut self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.polled {
                self.call_count.fetch_add(1, Ordering::Relaxed);
                self.polled = true;
            }

            Poll::Ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Empty::new())
                .expect("response build")))
        }
    }

    impl Future for ExtensionCheckingFuture {
        type Output = Result<Response<Empty<Bytes>>, Infallible>;

        fn poll(mut self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.polled {
                self.call_count.fetch_add(1, Ordering::Relaxed);
                self.polled = true;
            }

            Poll::Ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Empty::new())
                .expect("response build")))
        }
    }

    impl TestService {
        fn new(call_count: Arc<AtomicUsize>) -> Self {
            Self { call_count }
        }
    }

    impl Service<Request<()>> for TestService {
        type Response = Response<Empty<Bytes>>;
        type Error = Infallible;
        type Future = TestFuture;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<()>) -> Self::Future {
            TestFuture {
                call_count: Arc::clone(&self.call_count),
                polled: false,
            }
        }
    }

    impl Service<Request<()>> for ExtensionCheckingService {
        type Response = Response<Empty<Bytes>>;
        type Error = Infallible;
        type Future = ExtensionCheckingFuture;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<()>) -> Self::Future {
            let ext = request
                .extensions()
                .get::<TestAuthExtension>()
                .expect("authenticated extension should be present");
            assert_eq!(ext.user_id, "user-123");

            ExtensionCheckingFuture {
                call_count: Arc::clone(&self.call_count),
                polled: false,
            }
        }
    }

    #[derive(Clone)]
    struct PendingOnceReadyService {
        call_count: Arc<AtomicUsize>,
        ready_calls: Arc<AtomicUsize>,
    }

    impl Service<Request<()>> for PendingOnceReadyService {
        type Response = Response<Empty<Bytes>>;
        type Error = std::io::Error;
        type Future = std::future::Ready<Result<Response<Empty<Bytes>>, std::io::Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let seen = self.ready_calls.fetch_add(1, Ordering::Relaxed);
            if seen == 0 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn call(&mut self, _request: Request<()>) -> Self::Future {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Empty::new())
                .expect("response build")))
        }
    }

    #[derive(Clone)]
    struct AlwaysErrorReadyService;

    impl Service<Request<()>> for AlwaysErrorReadyService {
        type Response = Response<Empty<Bytes>>;
        type Error = std::io::Error;
        type Future = std::future::Ready<Result<Response<Empty<Bytes>>, std::io::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err(std::io::Error::other("downstream unavailable")))
        }

        fn call(&mut self, _request: Request<()>) -> Self::Future {
            std::future::ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Empty::new())
                .expect("response build")))
        }
    }

    #[test]
    fn auth_success_forwards_to_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            TestService::new(Arc::clone(&call_count)),
            MockAuthenticator { allow: true },
            16,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("service should become ready");
        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("inner response should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn auth_success_adds_extension_before_calling_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            ExtensionCheckingService {
                call_count: Arc::clone(&call_count),
            },
            ExtensionAuthenticator,
            16,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("service should become ready");
        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("inner response should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn auth_failure_returns_unauthorized_without_calling_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            TestService::new(Arc::clone(&call_count)),
            MockAuthenticator { allow: false },
            16,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("service should become ready");
        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("auth guard should produce response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn auth_pending_once_then_success_still_calls_inner_once() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            TestService::new(Arc::clone(&call_count)),
            PendingOnceAuthenticator,
            16,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("service should become ready");
        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("inner response should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn auth_none_returns_grpc_unauthenticated_without_calling_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            TestService::new(Arc::clone(&call_count)),
            NoneAuthenticator,
            16,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("service should become ready");
        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("auth guard should produce response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let headers = response.headers();
        assert_eq!(
            headers
                .get(tonic::Status::GRPC_STATUS)
                .and_then(|v| v.to_str().ok()),
            Some("16")
        );
        assert_eq!(
            headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/grpc")
        );
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn inner_poll_ready_pending_once_then_success_transitions_correctly() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let ready_calls = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            PendingOnceReadyService {
                call_count: Arc::clone(&call_count),
                ready_calls: Arc::clone(&ready_calls),
            },
            MockAuthenticator { allow: true },
            16,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("service should become ready");
        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("inner response should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
        assert!(ready_calls.load(Ordering::Relaxed) >= 2);
    }

    #[test]
    fn inner_poll_ready_error_returns_internal_server_error() {
        let mut svc = AuthGuardedService::new(
            AlwaysErrorReadyService,
            MockAuthenticator { allow: true },
            16,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("service should become ready");
        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("auth guard should produce response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn permit_released_after_request_completion() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            TestService::new(Arc::clone(&call_count)),
            MockAuthenticator { allow: true },
            1,
        );

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("first poll_ready should succeed");
        let req1 = Request::builder().body(()).expect("request build");
        let _ = block_on(svc.call(req1)).expect("first request should succeed");

        block_on(poll_fn(|cx| svc.poll_ready(cx))).expect("permit should be released");
        let req2 = Request::builder().body(()).expect("request build");
        let _ = block_on(svc.call(req2)).expect("second request should succeed");

        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    #[should_panic(expected = "call should only be called after poll_ready has returned Ready")]
    fn call_without_poll_ready_panics() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardedService::new(
            TestService::new(Arc::clone(&call_count)),
            MockAuthenticator { allow: true },
            16,
        );

        let request = Request::builder().body(()).expect("request build");
        block_on(svc.call(request)).expect("auth guard should produce response");
    }
}
