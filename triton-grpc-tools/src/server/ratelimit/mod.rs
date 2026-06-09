pub mod config;
pub mod transport;

use {
    bytes::Buf,
    http::{HeaderMap, Request, Response},
    hyper::body::{Frame, SizeHint},
    pin_project::pin_project,
    std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{ready, Context, Poll},
        time::Instant,
    },
    tonic::codegen::{Body as HttpBody, Service, StdError},
    tower_layer::Layer,
};

/// Service wrapper that attaches per-request metering hooks to response bodies.
///
/// Each call creates hooks via [`MeteredManager::build_hooks`] and returns a
/// [`MeteredFuture`] that wraps the eventual response body with [`MeteredBody`].
#[derive(Debug, Clone)]
pub struct RatelimitedService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RatelimitedService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: HttpBody + Send + 'static,
    ResBody::Error: Into<StdError>,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = S::Future;

    /// Delegates readiness to the wrapped inner service.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Builds request hooks and delegates request execution to the inner service.
    ///
    /// The returned future will wrap successful responses with [`MeteredBody`]
    /// so byte emission can be observed frame by frame.
    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        todo!()
    }
}
