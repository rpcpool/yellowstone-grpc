use {
    crate::{GeyserGrpcClientError, GrpcConnector, TonicGrpcConnector},
    futures::stream::{Stream, StreamExt},
    std::{
        future::Future,
        pin::Pin,
        task::Poll,
        time::{Duration, Instant},
    },
    tonic::{codec::Streaming, Status},
    yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeUpdate},
};

pub struct Unstable<S> {
    inner: Option<S>,
    drop_interval: Duration,
    started_at: Instant,
}

impl<S> Stream for Unstable<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.started_at.elapsed() >= this.drop_interval {
            log::warn!(
                "unstable: simulated disconnect after {:?}",
                this.drop_interval
            );
            if let Some(stream) = this.inner.take() {
                drop(stream);
            }

            return Poll::Ready(Some(Err(Status::aborted("unstable: simulated disconnect"))));
        }
        if let Some(stream) = &mut this.inner {
            return stream.poll_next_unpin(cx);
        }
        {
            return Poll::Ready(None);
        }
    }
}

impl<S> Unstable<S> {
    pub fn new(inner: S, drop_interval: Duration) -> Self {
        Self {
            inner: Some(inner),
            drop_interval,
            started_at: Instant::now(),
        }
    }
}

#[derive(Clone)]
pub struct UnstableConnector {
    inner: TonicGrpcConnector,
    drop_interval: Duration,
}

impl UnstableConnector {
    pub fn new(inner: TonicGrpcConnector, drop_interval: Duration) -> Self {
        Self {
            inner,
            drop_interval,
        }
    }
}

impl GrpcConnector for UnstableConnector {
    type Stream = Unstable<Streaming<SubscribeUpdate>>;
    type ConnectError = GeyserGrpcClientError;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send>>;

    fn connect(
        &self,
        request: std::sync::Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture {
        let inner_fut = self.inner.connect(request, from_slot);
        let drop_interval = self.drop_interval;

        Box::pin(async move {
            let stream = inner_fut.await?;
            Ok(Unstable::new(stream, drop_interval))
        })
    }
}
