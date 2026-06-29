//!
//! This module an InterceptorService that can be used as layer in a tonic server to provide a more powerful interception mechanism
//! that can handle `http::Request` insteand of `tonic::Request`. This allows for more advanced use cases such as rate limiting and metering,
//! which require access to the full HTTP request, including headers and URI paths.
//!
//!
//! ## Tonic's `Interceptor` limitations
//!
//! Interceptors are synchronous middleware that can only handled `tonic::Request` which renders them extremely limited in their capabilities.
//! Because of this limitation, interceptors cannot even introspect uri paths/methods, which is a common requirement for implementing rate limiting and metering.
//!

use {
    http::{Request, Response},
    pin_project::pin_project,
    std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    },
    tonic::server::NamedService,
    tower::Service,
};

pub struct HttpInterceptedService<S, I> {
    inner: S,
    interceptor: I,
}

/// Intercepts [`http::Request`] instead of [`tonic::Request`], while still being able to return [`tonic::Status`] errors.
///
pub trait HttpInterceptor {
    fn call(&mut self, request: http::Request<()>) -> Result<http::Request<()>, tonic::Status>;
}

impl<S, I, ReqBody, ResBody> Service<Request<ReqBody>> for HttpInterceptedService<S, I>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    I: HttpInterceptor,
{
    type Response = Response<ResponseBody<ResBody>>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let (parts, body) = request.into_parts();
        match self.interceptor.call(Request::from_parts(parts, ())) {
            Ok(req) => {
                let request = Request::from_parts(req.into_parts().0, body);
                ResponseFuture::future(self.inner.call(request))
            }
            Err(status) => ResponseFuture::status(status),
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct ResponseFuture<F> {
    #[pin]
    kind: Kind<F>,
}

impl<F> ResponseFuture<F> {
    fn future(future: F) -> Self {
        Self {
            kind: Kind::Future(future),
        }
    }

    fn status(status: tonic::Status) -> Self {
        Self {
            kind: Kind::Status(Some(status)),
        }
    }
}

#[pin_project(project = KindProj)]
#[derive(Debug)]
enum Kind<F> {
    Future(#[pin] F),
    Status(Option<tonic::Status>),
}

impl<F, E, B> Future for ResponseFuture<F>
where
    F: Future<Output = Result<http::Response<B>, E>>,
{
    type Output = Result<http::Response<ResponseBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::Future(future) => future.poll(cx).map_ok(|res| res.map(ResponseBody::wrap)),
            KindProj::Status(status) => {
                let (parts, ()) = status.take().unwrap().into_http::<()>().into_parts();
                let response = http::Response::from_parts(parts, ResponseBody::<B>::empty());
                Poll::Ready(Ok(response))
            }
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct ResponseBody<B> {
    #[pin]
    kind: ResponseBodyKind<B>,
}

#[pin_project(project = ResponseBodyKindProj)]
#[derive(Debug)]
enum ResponseBodyKind<B> {
    Empty,
    Wrap(#[pin] B),
}

impl<B> ResponseBody<B> {
    fn new(kind: ResponseBodyKind<B>) -> Self {
        Self { kind }
    }

    fn empty() -> Self {
        Self::new(ResponseBodyKind::Empty)
    }

    fn wrap(body: B) -> Self {
        Self::new(ResponseBodyKind::Wrap(body))
    }
}

impl<B: http_body::Body> http_body::Body for ResponseBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.project().kind.project() {
            ResponseBodyKindProj::Empty => Poll::Ready(None),
            ResponseBodyKindProj::Wrap(body) => body.poll_frame(cx),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match &self.kind {
            ResponseBodyKind::Empty => http_body::SizeHint::with_exact(0),
            ResponseBodyKind::Wrap(body) => body.size_hint(),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.kind {
            ResponseBodyKind::Empty => true,
            ResponseBodyKind::Wrap(body) => body.is_end_stream(),
        }
    }
}

impl<S, I> NamedService for HttpInterceptedService<S, I>
where
    S: NamedService,
    I: HttpInterceptor,
{
    const NAME: &'static str = S::NAME;
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        futures::executor::block_on,
        http_body::Body,
        http_body_util::{BodyExt, Full},
        std::{
            convert::Infallible,
            sync::{Arc, Mutex},
        },
    };

    #[derive(Clone)]
    struct TestInterceptor {
        deny: bool,
        rewrite_path_to: Option<&'static str>,
    }

    impl HttpInterceptor for TestInterceptor {
        fn call(
            &mut self,
            mut request: http::Request<()>,
        ) -> Result<http::Request<()>, tonic::Status> {
            if self.deny {
                return Err(tonic::Status::resource_exhausted("rate limit exceeded"));
            }

            if let Some(path) = self.rewrite_path_to {
                let uri = http::Uri::builder()
                    .path_and_query(path)
                    .build()
                    .expect("valid uri");
                *request.uri_mut() = uri;
            }

            Ok(request)
        }
    }

    #[derive(Clone)]
    struct TestInnerService {
        seen_paths: Arc<Mutex<Vec<String>>>,
    }

    impl Service<Request<Bytes>> for TestInnerService {
        type Response = Response<Full<Bytes>>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<Bytes>) -> Self::Future {
            self.seen_paths
                .lock()
                .expect("poisoned mutex")
                .push(request.uri().to_string());

            std::future::ready(Ok(Response::new(Full::new(request.into_body()))))
        }
    }

    impl NamedService for TestInnerService {
        const NAME: &'static str = "test.Service";
    }

    #[test]
    fn interceptor_passes_to_inner_and_wraps_response_body() {
        let seen_paths = Arc::new(Mutex::new(Vec::new()));
        let inner = TestInnerService {
            seen_paths: Arc::clone(&seen_paths),
        };
        let interceptor = TestInterceptor {
            deny: false,
            rewrite_path_to: Some("/rewritten"),
        };
        let mut service = HttpInterceptedService { inner, interceptor };

        let request = Request::builder()
            .uri("/original")
            .body(Bytes::from_static(b"payload"))
            .expect("request build");

        let response = block_on(service.call(request)).expect("service call should succeed");

        let captured_paths = seen_paths.lock().expect("poisoned mutex");
        assert_eq!(captured_paths.as_slice(), ["/rewritten"]);
        drop(captured_paths);

        let mut body = response.into_body();
        let frame = block_on(body.frame())
            .expect("expected frame")
            .expect("expected successful frame");
        assert_eq!(frame.data_ref().expect("data frame").as_ref(), b"payload");
    }

    #[test]
    fn interceptor_denial_returns_grpc_status_response() {
        let inner = TestInnerService {
            seen_paths: Arc::new(Mutex::new(Vec::new())),
        };
        let interceptor = TestInterceptor {
            deny: true,
            rewrite_path_to: None,
        };
        let mut service = HttpInterceptedService { inner, interceptor };

        let request = Request::builder()
            .uri("/original")
            .body(Bytes::from_static(b"payload"))
            .expect("request build");

        let response = block_on(service.call(request)).expect("service call should succeed");

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(tonic::Status::GRPC_STATUS)
                .and_then(|v| v.to_str().ok()),
            Some("8")
        );
        assert_eq!(
            response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/grpc")
        );
        assert!(response.into_body().is_end_stream());
    }

    #[test]
    fn named_service_delegates_name_to_inner() {
        type Wrapped = HttpInterceptedService<TestInnerService, TestInterceptor>;
        assert_eq!(<Wrapped as NamedService>::NAME, "test.Service");
    }
}
