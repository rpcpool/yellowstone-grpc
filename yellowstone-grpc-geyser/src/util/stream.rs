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

pub trait Weighted {
    fn weight(&self) -> u64 {
        1
    }
}

#[derive(Debug)]
struct Shared {
    queue_size: AtomicU64,
    weight_capacity: u64,
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
    fn add_load(&self, weight: u64) -> u64 {
        self.queue_size.fetch_add(weight, Ordering::Relaxed)
    }

    #[inline]
    fn decr_load(&self, weight: u64) {
        self.queue_size.fetch_sub(weight, Ordering::Relaxed);
    }
}

///
/// Creates a load-aware channel with the specified capacity and average weight rate window.
/// The sender and receiver can be used to send and receive items that implement the `Weighted`
/// trait, which provides a method to get the "traffic" weight of the item.
///
/// The word "traffic" is used here to indicate the load or weight of the item being sent.
///
/// The channel holds at most `capacity` items and `capacity * weight_capacity_factor` weight.
/// `try_send` returns `Full` if the item would exceed either limit and the queue is not empty.
/// `send` waits only for item room and does not check the weight limit.
///
pub fn load_aware_channel<T>(
    capacity: usize,
    weight_capacity_factor: u64,
) -> (LoadAwareSender<T>, LoadAwareReceiver<T>) {
    let (inner_sender, inner_receiver) = tokio::sync::mpsc::channel(capacity);
    let shared = Arc::new(Shared {
        queue_size: AtomicU64::new(0), // Initialize queue size to 0
        weight_capacity: (capacity as u64).saturating_mul(weight_capacity_factor),
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
impl<T: Weighted> LoadAwareSender<T> {
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        let weight = item.weight();
        self.shared.add_load(weight);
        self.inner
            .send(item)
            .await
            .inspect_err(|_| self.shared.decr_load(weight))
    }

    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let item_weight = item.weight();
        let queued_weight = self.shared.add_load(item_weight);
        if queued_weight > 0 && queued_weight + item_weight > self.shared.weight_capacity {
            self.shared.decr_load(item_weight);
            return Err(TrySendError::Full(item));
        }

        self.inner
            .try_send(item)
            .inspect_err(|_| self.shared.decr_load(item_weight))
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
impl<T: Weighted> LoadAwareReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        use std::future::poll_fn;
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner.poll_recv(cx).map(|maybe| {
            if let Some(item) = &maybe {
                self.shared.decr_load(item.weight());
            }
            maybe
        })
    }
}

impl<T: Weighted> Stream for LoadAwareReceiver<T> {
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

    const WEIGHT_CAPACITY_FACTOR: u64 = 3;

    #[derive(Debug)]
    struct TestItem(u32);

    impl Weighted for TestItem {}

    #[derive(Debug)]
    struct HeavyItem(u64);

    impl Weighted for HeavyItem {
        fn weight(&self) -> u64 {
            self.0
        }
    }

    #[tokio::test]
    async fn try_send_counts_weight_against_capacity() {
        let (sender, mut receiver) = load_aware_channel(10, WEIGHT_CAPACITY_FACTOR);
        let budget = 10 * WEIGHT_CAPACITY_FACTOR;

        sender.try_send(HeavyItem(budget - 20)).unwrap();
        assert!(matches!(
            sender.try_send(HeavyItem(21)),
            Err(TrySendError::Full(HeavyItem(21)))
        ));
        assert_eq!(sender.queue_size(), budget - 20);
        sender.try_send(HeavyItem(20)).unwrap();
        assert_eq!(sender.queue_size(), budget);

        assert_eq!(receiver.recv().await.unwrap().0, budget - 20);
        assert_eq!(sender.queue_size(), 20);
    }

    #[tokio::test]
    async fn light_items_are_bounded_by_item_capacity() {
        let (sender, _receiver) = load_aware_channel(2, WEIGHT_CAPACITY_FACTOR);

        sender.try_send(TestItem(1)).unwrap();
        sender.try_send(TestItem(2)).unwrap();
        assert!(matches!(
            sender.try_send(TestItem(3)),
            Err(TrySendError::Full(TestItem(3)))
        ));
        assert_eq!(sender.queue_size(), 2);
    }

    #[tokio::test]
    async fn try_send_accepts_one_oversized_item_into_empty_queue() {
        let (sender, mut receiver) = load_aware_channel(10, WEIGHT_CAPACITY_FACTOR);

        sender
            .try_send(HeavyItem(10 * WEIGHT_CAPACITY_FACTOR + 15))
            .unwrap();
        assert!(matches!(
            sender.try_send(HeavyItem(1)),
            Err(TrySendError::Full(_))
        ));
        receiver.recv().await.unwrap();
        assert_eq!(sender.queue_size(), 0);
    }

    #[tokio::test]
    async fn test_basic_send_and_receive() {
        let (sender, mut receiver) = load_aware_channel(10, WEIGHT_CAPACITY_FACTOR);

        sender.send(TestItem(5)).await.unwrap();
        assert_eq!(sender.queue_size(), 1);
        let received = receiver.recv().await.unwrap();
        assert_eq!(sender.queue_size(), 0);
        assert_eq!(received.0, 5);
    }

    #[tokio::test]
    async fn test_stream_behavior() {
        let (sender, receiver) = load_aware_channel(10, WEIGHT_CAPACITY_FACTOR);

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

        let (sender, mut receiver) = load_aware_channel(100000, WEIGHT_CAPACITY_FACTOR);
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
        let (sender, receiver) = load_aware_channel(10, WEIGHT_CAPACITY_FACTOR);
        drop(receiver);
        assert!(sender.send(TestItem(1)).await.is_err());
    }
}
