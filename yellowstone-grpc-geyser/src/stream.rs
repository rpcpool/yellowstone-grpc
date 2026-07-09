use {
    futures::Stream,
    std::{
        future::Future,
        marker::PhantomData,
        pin::Pin,
        task::{Context, Poll},
    },
};

pub enum TryRecv<T> {
    Item(T),
    Empty,
    Closed,
}

pub trait PollReceiver {
    type Item;

    fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
    fn try_recv(&mut self) -> TryRecv<Self::Item>;
}

pub trait BatchInto<Out> {
    fn batch_into(self, batch: &mut Vec<Out>);
}

pub struct GeyserStream<R, Out> {
    receiver: R,
    batch_capacity: usize,
    _out: PhantomData<fn() -> Out>,
}

impl<R, Out> GeyserStream<R, Out> {
    pub fn new(receiver: R, batch_capacity: usize) -> Self {
        Self {
            receiver,
            batch_capacity,
            _out: PhantomData,
        }
    }
}

impl<R, Out> Stream for GeyserStream<R, Out>
where
    R: PollReceiver + Unpin,
    R::Item: BatchInto<Out>,
{
    type Item = Vec<Out>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let message = match Pin::new(&mut self.receiver).poll_recv(cx) {
            Poll::Ready(Some(item)) => item,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        let mut batch_messages = Vec::with_capacity(self.batch_capacity);
        message.batch_into(&mut batch_messages);

        while batch_messages.len() < self.batch_capacity {
            match self.receiver.try_recv() {
                TryRecv::Item(item) => item.batch_into(&mut batch_messages),
                TryRecv::Empty | TryRecv::Closed => break,
            }
        }

        Poll::Ready(Some(batch_messages))
    }
}

impl<T> PollReceiver for ::tokio::sync::mpsc::UnboundedReceiver<T> {
    type Item = T;

    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        ::tokio::sync::mpsc::UnboundedReceiver::poll_recv(&mut self, cx)
    }

    fn try_recv(&mut self) -> TryRecv<Self::Item> {
        use ::tokio::sync::mpsc::error::TryRecvError;
        match ::tokio::sync::mpsc::UnboundedReceiver::try_recv(self) {
            Ok(item) => TryRecv::Item(item),
            Err(TryRecvError::Empty) => TryRecv::Empty,
            Err(TryRecvError::Disconnected) => TryRecv::Closed,
        }
    }
}

impl<T> PollReceiver for ::tokio::sync::mpsc::Receiver<T> {
    type Item = T;

    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        ::tokio::sync::mpsc::Receiver::poll_recv(&mut self, cx)
    }

    fn try_recv(&mut self) -> TryRecv<Self::Item> {
        use ::tokio::sync::mpsc::error::TryRecvError;
        match ::tokio::sync::mpsc::Receiver::try_recv(self) {
            Ok(item) => TryRecv::Item(item),
            Err(TryRecvError::Empty) => TryRecv::Empty,
            Err(TryRecvError::Disconnected) => TryRecv::Closed,
        }
    }
}

pub trait BatchStream {
    type Item;

    fn poll_recv_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut Vec<Self::Item>,
    ) -> Poll<Option<usize>>;
    fn try_recv(&mut self) -> TryRecv<Self::Item>;
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
        crate::stream::{BatchStream, TryRecv},
        std::{
            pin::Pin,
            task::{Context, Poll},
        },
    };

    pub struct BatchStreamReceiver<T> {
        inner: ::tokio::sync::mpsc::Receiver<T>,
    }

    impl<T> BatchStreamReceiver<T> {
        pub fn new(inner: ::tokio::sync::mpsc::Receiver<T>) -> BatchStreamReceiver<T> {
            BatchStreamReceiver { inner }
        }
    }

    pub struct BatchStreamUnboundedReceiver<T> {
        inner: ::tokio::sync::mpsc::UnboundedReceiver<T>,
    }

    impl<T> BatchStreamUnboundedReceiver<T> {
        pub fn new(
            inner: ::tokio::sync::mpsc::UnboundedReceiver<T>,
        ) -> BatchStreamUnboundedReceiver<T> {
            BatchStreamUnboundedReceiver { inner }
        }
    }

    impl<T> BatchStream for BatchStreamReceiver<T> {
        type Item = T;

        fn poll_recv_batch(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            batch: &mut Vec<Self::Item>,
        ) -> Poll<Option<usize>> {
            if batch.len() == batch.capacity() {
                return Poll::Ready(Some(0));
            }

            let this = self.get_mut();
            let mut i = 0;
            match Pin::new(&mut this.inner).poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    i += 1;
                    batch.push(item);

                    // try drain loop here
                    while let Ok(item) = this.inner.try_recv() {
                        batch.push(item);
                        i += 1;
                        if batch.len() == batch.capacity() {
                            break;
                        }
                    }

                    Poll::Ready(Some(i))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }

        fn try_recv(&mut self) -> TryRecv<Self::Item> {
            use ::tokio::sync::mpsc::error::TryRecvError;
            match self.inner.try_recv() {
                Ok(item) => TryRecv::Item(item),
                Err(TryRecvError::Empty) => TryRecv::Empty,
                Err(TryRecvError::Disconnected) => TryRecv::Closed,
            }
        }
    }

    impl<T> BatchStream for BatchStreamUnboundedReceiver<T> {
        type Item = T;

        fn poll_recv_batch(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            batch: &mut Vec<Self::Item>,
        ) -> Poll<Option<usize>> {
            if batch.len() == batch.capacity() {
                return Poll::Ready(Some(0));
            }

            let this = self.get_mut();
            let mut i = 0;
            match Pin::new(&mut this.inner).poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    i += 1;
                    batch.push(item);

                    while let Ok(item) = this.inner.try_recv() {
                        batch.push(item);
                        i += 1;
                        if batch.len() == batch.capacity() {
                            break;
                        }
                    }

                    Poll::Ready(Some(i))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }

        fn try_recv(&mut self) -> TryRecv<Self::Item> {
            use ::tokio::sync::mpsc::error::TryRecvError;
            match self.inner.try_recv() {
                Ok(item) => TryRecv::Item(item),
                Err(TryRecvError::Empty) => TryRecv::Empty,
                Err(TryRecvError::Disconnected) => TryRecv::Closed,
            }
        }
    }
}
