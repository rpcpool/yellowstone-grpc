use {
    http::{Request, Response, StatusCode},
    pin_project::pin_project,
    std::{
        future::Future,
        task::{Context, Poll},
    },
    tonic::codegen::{Body as HttpBody, Service, StdError},
    tower_layer::Layer,
};

pub trait SharedAuthenticator<ReqBody>: Send + Sync + Clone + 'static {
    type AuthError: std::error::Error + Sync + Send + 'static;
    type AuthFut: Future<Output = Result<Request<ReqBody>, Self::AuthError>> + Send + 'static;

    fn authenticate(&self, request: Request<ReqBody>) -> Self::AuthFut;
}

pub struct AuthGuardService<S, Auth> {
    inner: S,
    authenticator: Auth,
}

pub struct AuthGuardLayer<Auth> {
    authenticator: Auth,
}

impl<Auth> AuthGuardLayer<Auth> {
    pub fn new(authenticator: Auth) -> Self {
        Self { authenticator }
    }
}

impl<S, Auth> Layer<S> for AuthGuardLayer<Auth>
where
    Auth: Clone,
{
    type Service = AuthGuardService<S, Auth>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthGuardService::new(inner, self.authenticator.clone())
    }
}

impl<S, Auth> AuthGuardService<S, Auth> {
    pub fn new(inner: S, authenticator: Auth) -> Self {
        Self {
            inner,
            authenticator,
        }
    }
}

#[pin_project(project = AuthGuardFutureProj)]
pub enum AuthGuardFuture<S, AF, SF, ReqBody, ResBody, E, AuthE>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>, Error = E, Future = SF>,
    AF: Future<Output = Result<Request<ReqBody>, AuthE>>,
    SF: Future<Output = Result<Response<ResBody>, E>>,
{
    Authenticating {
        #[pin]
        auth_fut: AF,
        inner_service: Option<S>,
    },
    CallingInner {
        #[pin]
        inner_fut: SF,
    },
    Denied {
        result: Option<Result<Response<ResBody>, E>>,
    },
}

impl<S, AF, SF, ReqBody, ResBody, E, AuthE> Future
    for AuthGuardFuture<S, AF, SF, ReqBody, ResBody, E, AuthE>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>, Error = E, Future = SF>,
    AF: Future<Output = Result<Request<ReqBody>, AuthE>>,
    SF: Future<Output = Result<Response<ResBody>, E>>,
    ResBody: Default,
    AuthE: Into<StdError>,
{
    type Output = Result<Response<ResBody>, E>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().project() {
                AuthGuardFutureProj::Authenticating {
                    auth_fut,
                    inner_service,
                } => match auth_fut.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(request)) => {
                        let mut inner = inner_service
                            .take()
                            .expect("auth future completed without inner service");
                        let inner_fut = inner.call(request);
                        self.set(AuthGuardFuture::CallingInner { inner_fut });
                    }
                    Poll::Ready(Err(_auth_err)) => {
                        let denied = Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(ResBody::default())
                            .expect("failed to build auth deny response");
                        self.set(AuthGuardFuture::Denied {
                            result: Some(Ok(denied)),
                        });
                    }
                },
                AuthGuardFutureProj::CallingInner { inner_fut } => return inner_fut.poll(cx),
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

impl<S, Auth, ReqBody, ResBody> Service<Request<ReqBody>> for AuthGuardService<S, Auth>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone,
    S::Future: Send + 'static,
    ResBody: HttpBody + Send + 'static + Default,
    ResBody::Error: Into<StdError>,
    Auth: SharedAuthenticator<ReqBody>,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future =
        AuthGuardFuture<S, Auth::AuthFut, S::Future, ReqBody, ResBody, S::Error, Auth::AuthError>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let auth_fut = self.authenticator.clone().authenticate(request);
        AuthGuardFuture::Authenticating {
            auth_fut,
            inner_service: Some(self.inner.clone()),
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
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
        },
    };

    #[derive(Clone)]
    struct MockAuthenticator {
        allow: bool,
    }

    impl SharedAuthenticator<()> for MockAuthenticator {
        type AuthError = std::io::Error;
        type AuthFut = std::future::Ready<Result<Request<()>, Self::AuthError>>;

        fn authenticate(&self, request: Request<()>) -> Self::AuthFut {
            if self.allow {
                std::future::ready(Ok(request))
            } else {
                std::future::ready(Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "mock auth denied",
                )))
            }
        }
    }

    #[derive(Clone)]
    struct TestService {
        call_count: Arc<AtomicUsize>,
    }

    struct TestFuture {
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
