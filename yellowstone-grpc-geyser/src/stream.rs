use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub trait BatchInto<Out> {
    fn batch_into(self, batch: &mut Vec<Out>) -> usize;
}

pub trait Buffer<T> {
    fn ready(&self) -> bool;

    fn accumulate(&mut self, item: T) -> Result<(), T>;
}

impl<T> Buffer<T> for Vec<T> {
    fn ready(&self) -> bool {
        self.len() < self.capacity()
    }

    fn accumulate(&mut self, item: T) -> Result<(), T> {
        if self.len() < self.capacity() {
            self.push(item);
            Ok(())
        } else {
            Err(item)
        }
    }
}

pub trait BatchStream {
    type Item;

    fn poll_recv_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut Vec<Self::Item>,
    ) -> Poll<Option<usize>> {
        self.poll_recv_batch_with_buffer(cx, batch)
    }

    fn poll_recv_batch_with_buffer<B>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut B,
    ) -> Poll<Option<usize>>
    where
        B: Buffer<Self::Item>;
}

pub struct NextBatch<'a, S, T, B> {
    stream: &'a mut S,
    batch: &'a mut B,
    _item: std::marker::PhantomData<T>,
}

impl<S, T, B> Future for NextBatch<'_, S, T, B>
where
    S: BatchStream<Item = T> + Unpin,
    B: Buffer<T> + Unpin,
    T: Unpin,
{
    type Output = Option<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        Pin::new(&mut *this.stream).poll_recv_batch_with_buffer(cx, this.batch)
    }
}

pub trait BatchStreamExt: BatchStream + Unpin {
    fn next_batch<'a, B>(&'a mut self, batch: &'a mut B) -> NextBatch<'a, Self, Self::Item, B>
    where
        B: Buffer<Self::Item> + Unpin,
        Self: Sized,
    {
        NextBatch {
            stream: self,
            batch,
            _item: Default::default(),
        }
    }

    fn poll_next_batch<B>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut B,
    ) -> Poll<Option<usize>>
    where
        B: Buffer<Self::Item> + Unpin,
        Self: Sized,
    {
        self.poll_recv_batch_with_buffer(cx, batch)
    }
}

impl<S> BatchStreamExt for S where S: BatchStream + Unpin {}

#[cfg(test)]
mod tests {
    use super::Buffer;

    #[test]
    fn vec_buffer_remaining_tracks_capacity() {
        let buffer: Vec<u32> = Vec::with_capacity(3);
        assert!(buffer.ready());
    }

    #[test]
    fn vec_buffer_accumulate_fills_then_rejects() {
        let mut buffer: Vec<u32> = Vec::with_capacity(2);

        assert_eq!(buffer.accumulate(1), Ok(()));
        assert!(buffer.ready());

        assert_eq!(buffer.accumulate(2), Ok(()));
        assert!(!buffer.ready());

        // Buffer is full: further items must be rejected (returned back to
        // the caller) rather than silently over-allocating.
        assert_eq!(buffer.accumulate(3), Err(3));
        assert_eq!(buffer, vec![1, 2]);
    }

    #[test]
    fn zero_capacity_vec_never_has_space() {
        let buffer: Vec<u32> = Vec::new();
        assert!(!buffer.ready());
    }
}

pub mod tokio {
    use {
        crate::stream::BatchStream,
        std::{
            pin::Pin,
            task::{Context, Poll},
        },
    };

    pub struct BatchStreamReceiver<T> {
        inner: ::tokio::sync::mpsc::Receiver<T>,
    }

    impl<T> BatchStreamReceiver<T> {
        pub const fn new(inner: ::tokio::sync::mpsc::Receiver<T>) -> BatchStreamReceiver<T> {
            BatchStreamReceiver { inner }
        }
    }

    pub struct BatchStreamUnboundedReceiver<T> {
        inner: ::tokio::sync::mpsc::UnboundedReceiver<T>,
    }

    impl<T> BatchStreamUnboundedReceiver<T> {
        pub const fn new(
            inner: ::tokio::sync::mpsc::UnboundedReceiver<T>,
        ) -> BatchStreamUnboundedReceiver<T> {
            BatchStreamUnboundedReceiver { inner }
        }
    }

    impl<T> BatchStream for BatchStreamReceiver<T> {
        type Item = T;

        fn poll_recv_batch_with_buffer<B>(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut B,
        ) -> Poll<Option<usize>>
        where
            B: super::Buffer<Self::Item>,
        {
            if !buffer.ready() {
                return Poll::Ready(Some(0));
            }

            let this = self.get_mut();

            match Pin::new(&mut this.inner).poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    if buffer.accumulate(item).is_err() {
                        unreachable!("Buffer should have remaining space");
                    }
                    let mut i = 1;
                    'drain: while buffer.ready() {
                        let Ok(item) = this.inner.try_recv() else {
                            break 'drain;
                        };
                        if buffer.accumulate(item).is_err() {
                            unreachable!("Buffer should have remaining space");
                        }
                        i += 1;
                    }
                    Poll::Ready(Some(i))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<T> BatchStream for BatchStreamUnboundedReceiver<T> {
        type Item = T;

        fn poll_recv_batch_with_buffer<B>(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut B,
        ) -> Poll<Option<usize>>
        where
            B: super::Buffer<Self::Item>,
        {
            if !buffer.ready() {
                return Poll::Ready(Some(0));
            }

            let this = self.get_mut();

            match Pin::new(&mut this.inner).poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    if buffer.accumulate(item).is_err() {
                        unreachable!("Buffer should have remaining space");
                    }
                    let mut i = 1;
                    'drain: while buffer.ready() {
                        let Ok(item) = this.inner.try_recv() else {
                            break 'drain;
                        };
                        if buffer.accumulate(item).is_err() {
                            unreachable!("Buffer should have remaining space");
                        }
                        i += 1;
                    }
                    Poll::Ready(Some(i))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use {
            super::{BatchStreamReceiver, BatchStreamUnboundedReceiver},
            crate::stream::BatchStreamExt,
        };

        #[tokio::test]
        async fn bounded_single_item_reports_correct_count() {
            let (tx, rx) = ::tokio::sync::mpsc::channel::<u32>(10);
            let mut stream = BatchStreamReceiver::new(rx);
            let mut batch = Vec::with_capacity(10);

            tx.send(1).await.unwrap();
            let count = stream.next_batch(&mut batch).await;

            // Regression test: the returned count must match the number of
            // items actually pushed into the batch (previously undercounted
            // by 1, since the first item pulled via `poll_recv` was not
            // included in the running total).
            assert_eq!(count, Some(1));
            assert_eq!(batch, vec![1]);
        }

        #[tokio::test]
        async fn bounded_drains_all_buffered_items_in_one_poll() {
            let (tx, rx) = ::tokio::sync::mpsc::channel::<u32>(10);
            let mut stream = BatchStreamReceiver::new(rx);
            let mut batch = Vec::with_capacity(10);

            tx.send(1).await.unwrap();
            tx.send(2).await.unwrap();
            tx.send(3).await.unwrap();

            let count = stream.next_batch(&mut batch).await;

            assert_eq!(count, Some(3));
            assert_eq!(batch, vec![1, 2, 3]);
        }

        #[tokio::test]
        async fn bounded_respects_buffer_capacity() {
            let (tx, rx) = ::tokio::sync::mpsc::channel::<u32>(10);
            let mut stream = BatchStreamReceiver::new(rx);
            let mut batch = Vec::with_capacity(2);

            tx.send(1).await.unwrap();
            tx.send(2).await.unwrap();
            tx.send(3).await.unwrap();

            let count = stream.next_batch(&mut batch).await;
            assert_eq!(count, Some(2));
            assert_eq!(batch, vec![1, 2]);

            // The buffer is now full: a further poll must not touch the
            // channel and must report 0 without blocking.
            let count = stream.next_batch(&mut batch).await;
            assert_eq!(count, Some(0));
            assert_eq!(batch, vec![1, 2]);

            batch.clear();
            let count = stream.next_batch(&mut batch).await;
            assert_eq!(count, Some(1));
            assert_eq!(batch, vec![3]);
        }

        #[tokio::test]
        async fn bounded_closed_channel_returns_none() {
            let (tx, rx) = ::tokio::sync::mpsc::channel::<u32>(10);
            let mut stream = BatchStreamReceiver::new(rx);
            let mut batch = Vec::with_capacity(10);

            drop(tx);
            let count = stream.next_batch(&mut batch).await;

            assert_eq!(count, None);
            assert!(batch.is_empty());
        }

        #[tokio::test]
        async fn unbounded_single_item_reports_correct_count() {
            let (tx, rx) = ::tokio::sync::mpsc::unbounded_channel::<u32>();
            let mut stream = BatchStreamUnboundedReceiver::new(rx);
            let mut batch = Vec::with_capacity(10);

            tx.send(1).unwrap();
            let count = stream.next_batch(&mut batch).await;

            assert_eq!(count, Some(1));
            assert_eq!(batch, vec![1]);
        }

        #[tokio::test]
        async fn unbounded_drains_all_buffered_items_in_one_poll() {
            let (tx, rx) = ::tokio::sync::mpsc::unbounded_channel::<u32>();
            let mut stream = BatchStreamUnboundedReceiver::new(rx);
            let mut batch = Vec::with_capacity(10);

            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();

            let count = stream.next_batch(&mut batch).await;

            assert_eq!(count, Some(3));
            assert_eq!(batch, vec![1, 2, 3]);
        }

        #[tokio::test]
        async fn unbounded_respects_buffer_capacity() {
            let (tx, rx) = ::tokio::sync::mpsc::unbounded_channel::<u32>();
            let mut stream = BatchStreamUnboundedReceiver::new(rx);
            let mut batch = Vec::with_capacity(2);

            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx.send(3).unwrap();

            let count = stream.next_batch(&mut batch).await;
            assert_eq!(count, Some(2));
            assert_eq!(batch, vec![1, 2]);

            let count = stream.next_batch(&mut batch).await;
            assert_eq!(count, Some(0));
            assert_eq!(batch, vec![1, 2]);
        }

        #[tokio::test]
        async fn unbounded_closed_channel_returns_none() {
            let (tx, rx) = ::tokio::sync::mpsc::unbounded_channel::<u32>();
            let mut stream = BatchStreamUnboundedReceiver::new(rx);
            let mut batch = Vec::with_capacity(10);

            drop(tx);
            let count = stream.next_batch(&mut batch).await;

            assert_eq!(count, None);
            assert!(batch.is_empty());
        }
    }
}
