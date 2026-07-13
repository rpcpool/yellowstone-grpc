use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub trait BatchInto<Out> {
    fn batch_into(self, batch: &mut Vec<Out>, count: &mut usize);
}

pub trait BatchStream {
    type Item;

    fn poll_recv_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut Vec<Self::Item>,
    ) -> Poll<Option<usize>>;
}

pub struct NextBatch<'a, S, T> {
    stream: &'a mut S,
    batch: &'a mut Vec<T>,
}

impl<S, T> Future for NextBatch<'_, S, T>
where
    S: BatchStream<Item = T> + Unpin,
{
    type Output = Option<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        Pin::new(&mut *this.stream).poll_recv_batch(cx, this.batch)
    }
}

pub trait BatchStreamExt: BatchStream + Unpin {
    fn next_batch<'a>(
        &'a mut self,
        batch: &'a mut Vec<Self::Item>,
    ) -> NextBatch<'a, Self, Self::Item>
    where
        Self: Sized,
    {
        NextBatch {
            stream: self,
            batch,
        }
    }

    fn poll_next_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut Vec<Self::Item>,
    ) -> Poll<Option<usize>>
    where
        Self: Sized,
    {
        self.poll_recv_batch(cx, batch)
    }
}

impl<S> BatchStreamExt for S where S: BatchStream + Unpin {}

pub mod tokio {
    use {
        crate::stream::{BatchInto, BatchStream},
        std::{
            marker::PhantomData,
            pin::Pin,
            task::{Context, Poll},
        },
    };

    pub struct BatchStreamReceiver<T, Out> {
        inner: ::tokio::sync::mpsc::Receiver<T>,
        _out: PhantomData<fn() -> Out>,
    }

    impl<T, Out> BatchStreamReceiver<T, Out> {
        pub fn new(inner: ::tokio::sync::mpsc::Receiver<T>) -> BatchStreamReceiver<T, Out> {
            BatchStreamReceiver {
                inner,
                _out: PhantomData,
            }
        }
    }

    pub struct BatchStreamUnboundedReceiver<T, Out> {
        inner: ::tokio::sync::mpsc::UnboundedReceiver<T>,
        _out: PhantomData<fn() -> Out>,
    }

    impl<T, Out> BatchStreamUnboundedReceiver<T, Out> {
        pub fn new(
            inner: ::tokio::sync::mpsc::UnboundedReceiver<T>,
        ) -> BatchStreamUnboundedReceiver<T, Out> {
            BatchStreamUnboundedReceiver {
                inner,
                _out: PhantomData,
            }
        }
    }

    impl<T, Out> BatchStream for BatchStreamReceiver<T, Out>
    where
        T: BatchInto<Out>,
    {
        type Item = Out;

        fn poll_recv_batch(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            batch: &mut Vec<Self::Item>,
        ) -> Poll<Option<usize>> {
            if batch.len() >= batch.capacity() {
                return Poll::Ready(Some(0));
            }

            let this = self.get_mut();
            let mut i = 0;

            match Pin::new(&mut this.inner).poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    item.batch_into(batch, &mut i);

                    while let Ok(item) = this.inner.try_recv() {
                        item.batch_into(batch, &mut i);
                        if batch.len() >= batch.capacity() {
                            break;
                        }
                    }

                    Poll::Ready(Some(i))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<T, Out> BatchStream for BatchStreamUnboundedReceiver<T, Out>
    where
        T: BatchInto<Out>,
    {
        type Item = Out;

        fn poll_recv_batch(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            batch: &mut Vec<Self::Item>,
        ) -> Poll<Option<usize>> {
            if batch.len() >= batch.capacity() {
                return Poll::Ready(Some(0));
            }

            let this = self.get_mut();
            let mut i = 0;

            match Pin::new(&mut this.inner).poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    item.batch_into(batch, &mut i);

                    while let Ok(item) = this.inner.try_recv() {
                        item.batch_into(batch, &mut i);
                        if batch.len() >= batch.capacity() {
                            break;
                        }
                    }

                    Poll::Ready(Some(i))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
