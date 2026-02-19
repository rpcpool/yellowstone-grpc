use {
    futures::Stream,
    std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        task::{Context, Poll},
    },
    tokio::sync::mpsc::{
        error::{SendError, TrySendError},
        Receiver, Sender,
    },
};

#[derive(Debug)]
struct Shared {
    queue_size: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct LoadAwareSender<T> {
    shared: Arc<Shared>,
    inner: Sender<T>,
}

pub struct LoadAwareReceiver<T> {
    shared: Arc<Shared>,
    inner: Receiver<T>,
}

impl Shared {
    #[inline]
    fn add_load(&self) {
        self.queue_size.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn decr_load(&self) {
        self.queue_size.fetch_sub(1, Ordering::Relaxed);
    }
}

///
/// Creates a load-aware channel with the specified capacity and average weight rate window.
/// The sender and receiver can be used to send and receive items that implement the `Weighted`
/// trait, which provides a method to get the "traffic" weight of the item.
///
/// The word "traffic" is used here to indicate the load or weight of the item being sent.
///
pub fn load_aware_channel<T>(capacity: usize) -> (LoadAwareSender<T>, LoadAwareReceiver<T>) {
    let (inner_sender, inner_receiver) = tokio::sync::mpsc::channel(capacity);
    let shared = Arc::new(Shared {
        queue_size: AtomicU64::new(0), // Initialize queue size to 0
    });
    let sender = LoadAwareSender {
        shared: Arc::clone(&shared),
        inner: inner_sender,
    };

    let rx = LoadAwareReceiver {
        shared,
        inner: inner_receiver,
    };

    (sender, rx)
}

///
/// Sender end of the load-aware channel.
///
/// See [`load_aware_channel`] for more details.
///
impl<T> LoadAwareSender<T> {
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        self.inner.send(item).await?;
        self.shared.add_load();
        Ok(())
    }

    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(item)?;
        self.shared.add_load();
        Ok(())
    }

    pub fn queue_size(&self) -> u64 {
        self.shared.queue_size.load(Ordering::Relaxed)
    }
}

///
/// Receiving end of the load-aware channel.
///
/// See [`load_aware_channel`] for more details.
///
impl<T> LoadAwareReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        use std::future::poll_fn;
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let shared = Arc::clone(&self.shared);
        self.inner.poll_recv(cx).map(|maybe| {
            if maybe.is_some() {
                shared.decr_load();
            }
            maybe
        })
    }
}

impl<T> Stream for LoadAwareReceiver<T> {
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::util::testkit,
        log::LevelFilter,
        std::time::{Duration, Instant},
        tokio::task::yield_now,
        tokio_stream::StreamExt,
    };

    #[derive(Debug)]
    struct TestItem(u32);

    #[tokio::test]
    async fn test_basic_send_and_receive() {
        let (sender, mut receiver) = load_aware_channel(10);

        sender.send(TestItem(5)).await.unwrap();
        assert_eq!(sender.queue_size(), 1);
        let received = receiver.recv().await.unwrap();
        assert_eq!(sender.queue_size(), 0);
        assert_eq!(received.0, 5);
    }

    #[tokio::test]
    async fn test_stream_behavior() {
        let (sender, receiver) = load_aware_channel(10);

        sender.send(TestItem(1)).await.unwrap();
        sender.send(TestItem(2)).await.unwrap();
        sender.send(TestItem(3)).await.unwrap();

        assert_eq!(sender.queue_size(), 3);
        drop(sender);
        let mut stream = receiver;

        let mut results = vec![];
        while let Some(item) = stream.next().await {
            results.push(item.0);
        }

        assert_eq!(results, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_high_send_rate() {
        log::set_boxed_logger(Box::new(testkit::StdoutLogger))
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .unwrap();

        let (sender, mut receiver) = load_aware_channel(100000);
        let total_duration = Duration::from_secs(3);
        let item_weight = 1;

        let start_time = Instant::now();
        let mut _sent_count = 0; // Renamed to suppress unused variable warning

        let rx_task = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some(_item) = receiver.recv().await {
                cnt += 1;
            }
            cnt
        });

        let mut send_cnt = 0;
        while Instant::now().duration_since(start_time) < total_duration {
            sender.send(TestItem(item_weight)).await.unwrap();
            send_cnt += 1;
            let now = Instant::now();
            // Busy wait to simulate high send rate
            while now.elapsed() < Duration::from_micros(900) {
                yield_now().await;
            }
        }

        // Verify the send rate load

        drop(sender); // Close the sender to stop the receiver
        let received_count = rx_task.await.unwrap();
        log::trace!(
            "Total items sent: {}, received: {}",
            send_cnt,
            received_count
        );
        assert_eq!(
            send_cnt, received_count,
            "All sent items should be received"
        );
    }

    #[tokio::test]
    async fn sender_should_send_error_when_recv_drop() {
        let (sender, receiver) = load_aware_channel(10);
        drop(receiver);
        assert!(sender.send(TestItem(1)).await.is_err());
    }
}
