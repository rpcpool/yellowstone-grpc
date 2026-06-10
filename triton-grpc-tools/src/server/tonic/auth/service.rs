use std::{future::Future, task::{Context, Poll}};

use futures::{FutureExt, future::BoxFuture};
use http::{HeaderMap, Request, Response, StatusCode};
use pin_project::pin_project;
use tower_layer::Layer;
use tonic::{
    codegen::{Body as HttpBody, Service, StdError},
};

/// Async authenticator used by [`AuthGuardService`] and [`AuthGuardLayer`].
///
/// Implementations validate request metadata (typically headers/metadata copied
/// into an [`http::HeaderMap`]) and decide whether the request can proceed.
pub trait SharedAuthenticator: Send + Sync + Clone + 'static {

    type AuthError: std::error::Error + Sync + Send + 'static;
    type AuthFut: Future<Output = Result<(), Self::AuthError>> + Send + 'static;

    /// Validates an incoming request represented by cloned headers.
    ///
    /// Returning `Ok(())` allows the request to continue to the wrapped
    /// service. Returning `Err(())` causes [`AuthGuardFuture`] to synthesize a
    /// `401 UNAUTHORIZED` response.
    fn authenticate(&self, headers: &HeaderMap) -> Self::AuthFut;
}

/// Service middleware that authenticates each request before polling the inner
/// service future.
///
/// This wrapper defers the auth decision to [`SharedAuthenticator`] and uses a
/// custom future (`[`AuthGuardFuture`]`) to sequence authentication and service
/// execution.
pub struct AuthGuardService<S, Auth> {
    inner: S,
    authenticator: Auth,
}

/// Tower layer that wraps services in [`AuthGuardService`].
///
/// Useful for composing authentication into an existing service stack via
/// `.layer(...)` style APIs.
pub struct AuthGuardLayer<Auth> {
    authenticator: Auth,
}

impl<Auth> AuthGuardLayer<Auth> {
    /// Creates a new [`AuthGuardLayer`] with the provided authenticator.
    pub fn new(authenticator: Auth) -> Self {
        Self { authenticator }
    }
}

impl<S, Auth> Layer<S> for AuthGuardLayer<Auth>
where
    Auth: Clone,
{
    type Service = AuthGuardService<S, Auth>;

    /// Wraps `inner` in [`AuthGuardService`].
    fn layer(&self, inner: S) -> Self::Service {
        AuthGuardService::new(inner, self.authenticator.clone())
    }
}

impl<S, Auth> AuthGuardService<S, Auth> {
    /// Creates a new auth-guarding service wrapper.
    pub fn new(inner: S, authenticator: Auth) -> Self {
        Self { inner, authenticator }
    }
}

#[pin_project(project = AuthGuardFutureProj)]
/// Future driving auth + service execution for [`AuthGuardService`].
///
/// State flow:
/// - `Authenticating`: polls the async authenticator.
/// - `Allowed`: polls the inner service future.
/// - `Denied`: returns a synthetic unauthorized response exactly once.
pub enum AuthGuardFuture<F, ResBody, E, AuthE> {
    /// Authentication is in-flight and the inner future is held for later.
    Authenticating {
        #[pin]
        auth_fut: BoxFuture<'static, Result<(), AuthE>>,
        inner_fut: Option<F>,
        _marker: std::marker::PhantomData<(ResBody, E)>,
    },
    /// Authentication failed and an immediate response should be emitted.
    Denied {
        result: Option<Result<Response<ResBody>, E>>,
    },
    /// Authentication succeeded and we are polling the inner future.
    Allowed {
        #[pin]
        inner_fut: F,
    },
}

impl<F, ResBody, E, AuthE> std::future::Future for AuthGuardFuture<F, ResBody, E, AuthE>
where
    F: std::future::Future<Output = Result<Response<ResBody>, E>> + Unpin,
    ResBody: Default,
    AuthE: Into<StdError>,
{
    type Output = Result<Response<ResBody>, E>;

    /// Polls authentication first, then transitions into polling the inner
    /// service future when auth succeeds.
    ///
    /// On authentication failure, returns a single `401 UNAUTHORIZED`
    /// response with `ResBody::default()`.
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().project() {
                AuthGuardFutureProj::Authenticating {
                    auth_fut,
                    inner_fut,
                    ..
                } => match auth_fut.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let next_inner = inner_fut
                            .take()
                            .expect("auth future completed without inner future");
                        self.set(AuthGuardFuture::Allowed {
                            inner_fut: next_inner,
                        });
                    }
                    Poll::Ready(Err(_auth_e)) => {
                        let denied = Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(ResBody::default())
                            .expect("failed to build auth deny response");
                        self.set(AuthGuardFuture::Denied {
                            result: Some(Ok(denied)),
                        });
                    }
                },
                AuthGuardFutureProj::Denied { result } => {
                    return Poll::Ready(
                        result
                            .take()
                            .expect("denied future polled after completion"),
                    );
                }
                AuthGuardFutureProj::Allowed { inner_fut } => return inner_fut.poll(cx),
            }
        }
    }
}

impl<S, Auth, ReqBody, ResBody> Service<Request<ReqBody>> for AuthGuardService<S, Auth>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static + Unpin,
    ResBody: HttpBody + Send + 'static + Default,
    ResBody::Error: Into<StdError>,
    Auth: SharedAuthenticator,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = AuthGuardFuture<S::Future, ResBody, S::Error, Auth::AuthError>;

    /// Delegates readiness to the wrapped inner service.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Starts authentication and captures the inner future.
    ///
    /// The returned [`AuthGuardFuture`] will ensure authentication completes
    /// before any polling of the inner service future occurs.
    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let auth = self.authenticator.clone();
        let auth_fut = auth.authenticate(request.headers()).boxed();

        AuthGuardFuture::Authenticating {
            auth_fut,
            inner_fut: Some(self.inner.call(request)),
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        futures::executor::block_on,
        http_body_util::Empty,
        std::{
            convert::Infallible,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
        },
    };

    #[derive(Clone)]
    struct MockAuthenticator {
        allow: bool,
    }

    impl SharedAuthenticator for MockAuthenticator {
        type AuthError = std::io::Error;
        type AuthFut = std::future::Ready<Result<(), Self::AuthError>>;

        fn authenticate(&self, _headers: &HeaderMap) -> Self::AuthFut {
            if self.allow {
                std::future::ready(Ok(()))
            } else {
                std::future::ready(Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "mock auth denied",
                )))
            }
        }
    }

    struct TestService {
        call_count: Arc<AtomicUsize>,
    }

    struct TestFuture {
        call_count: Arc<AtomicUsize>,
        polled: bool,
    }

    impl std::future::Future for TestFuture {
        type Output = Result<Response<Empty<Bytes>>, Infallible>;

        fn poll(mut self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.polled {
                self.call_count.fetch_add(1, Ordering::Relaxed);
                self.polled = true;
            }

            Poll::Ready(Ok(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Empty::new())
                    .expect("response build"),
            ))
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

    #[test]
    fn auth_success_forwards_to_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardService::new(
            TestService::new(Arc::clone(&call_count)),
            MockAuthenticator { allow: true },
        );

        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("inner response should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn auth_failure_returns_unauthorized_without_calling_inner_service() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut svc = AuthGuardService::new(
            TestService::new(Arc::clone(&call_count)),
            MockAuthenticator { allow: false },
        );

        let request = Request::builder().body(()).expect("request build");
        let response = block_on(svc.call(request)).expect("auth guard should produce response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }
}
