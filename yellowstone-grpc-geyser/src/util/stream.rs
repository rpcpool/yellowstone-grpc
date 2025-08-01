use {
    crate::util::{
        ema::{Ema, EmaCurrentLoad, EmaReactivity, DEFAULT_EMA_WINDOW},
        rate::RateTracker,
    },
    futures::Stream,
    std::{
        sync::{
            atomic::AtomicU64, // Removed AtomicU32
            Arc,
        },
        task::{Context, Poll},
        time::{Duration, Instant},
    },
    tokio::sync::mpsc::{
        error::{SendError, TrySendError},
        Receiver, Sender,
    },
};

///
/// Basic trait for items that can be sent through a load-aware channel.
/// It requires the item to implement the `weight` method, which returns a `u32` representing the "traffic" weight of the item.
/// This weight is used to track the load on the channel.
///
/// The term "traffic" is used here to indicate the load or weight of the item being.
/// Its up to the application code to interpret the meaning of "traffic" in the context of the items being sent.
///
pub trait TrafficWeighted {
    fn weight(&self) -> u32;
}

#[derive(Debug)]
struct Shared {
    size: AtomicU64,
    weight: AtomicU64, // Reverted back to AtomicU64
    send_ema: Ema,
    rx_ema: Ema,
    avg_weight_rate: RateTracker,
}

#[derive(Debug, Clone)]
struct ChannelCapacities {
    threshold_size: u64,
    threshold_weight: u64,
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
    fn add_load(&self, weight: u32, now: Instant) {
        // Kept parameter type as u32
        self.size.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.weight
            .fetch_add(weight as u64, std::sync::atomic::Ordering::Relaxed); // Cast weight to u64
        self.avg_weight_rate.record(now, weight); // Cast weight to u64 for compatibility
        self.send_ema.record_load(now, weight); // Cast weight to u64 for compatibility
    }

    #[inline]
    fn decr_load(&self, weight: u32, now: Instant) {
        // Kept parameter type as u32
        self.size.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        self.weight
            .fetch_sub(weight as u64, std::sync::atomic::Ordering::Relaxed); // Cast weight to u64
        self.rx_ema.record_load(now, weight); // Cast weight to u64 for compatibility
    }
}

pub const DEFAULT_AVG_TRAFFIC_RATE_WINDOW: Duration = Duration::from_secs(10 * 60); // 10 minutes

///
/// Settings for the load-aware channel.
/// This struct defines the parameters for the average traffic rate window and the EMA settings.
/// It allows customization of the channel's behavior regarding load tracking and traffic estimation.
///
pub struct StatsSettings {
    avg_traffic_rate_window: Duration,
    tx_ema_window: Duration,
    tx_ema_reactivity: EmaReactivity,
    rx_ema_window: Duration,
    rx_ema_reactivity: EmaReactivity,
}

impl Default for StatsSettings {
    fn default() -> Self {
        Self {
            avg_traffic_rate_window: DEFAULT_AVG_TRAFFIC_RATE_WINDOW,
            tx_ema_window: DEFAULT_EMA_WINDOW,
            tx_ema_reactivity: EmaReactivity::Reactive,
            rx_ema_window: DEFAULT_EMA_WINDOW,
            rx_ema_reactivity: EmaReactivity::LessReactive, // Less reactive for receiving end -> closer to an all-time average
        }
    }
}

///
/// Creates a load-aware channel with the specified capacity and average weight rate window.
/// The sender and receiver can be used to send and receive items that implement the `Weighted`
/// trait, which provides a method to get the "traffic" weight of the item.
///
/// The word "traffic" is used here to indicate the load or weight of the item being sent.
///
pub fn load_aware_channel<T>(
    capacity: usize,
    stats_settings: StatsSettings,
) -> (LoadAwareSender<T>, LoadAwareReceiver<T>)
where
    T: TrafficWeighted,
{
    let (inner_sender, inner_receiver) = tokio::sync::mpsc::channel(capacity);

    let shared = Arc::new(Shared {
        size: AtomicU64::new(0),
        weight: AtomicU64::new(0),
        send_ema: Ema::new(
            stats_settings.tx_ema_window,
            stats_settings.tx_ema_reactivity,
        ),
        rx_ema: Ema::new(
            stats_settings.rx_ema_window,
            stats_settings.rx_ema_reactivity,
        ),
        avg_weight_rate: RateTracker::new(stats_settings.avg_traffic_rate_window),
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
impl<T> LoadAwareSender<T>
where
    T: TrafficWeighted,
{
    pub fn estimated_send_rate(&self) -> EmaCurrentLoad {
        self.shared.send_ema.current_load()
    }

    pub fn estimated_consuming_rate(&self) -> EmaCurrentLoad {
        self.shared.rx_ema.current_load()
    }

    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        let entry_weight = item.weight();
        let now = Instant::now();
        self.inner.send(item).await?;
        self.shared.add_load(entry_weight, now);
        Ok(())
    }

    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let entry_weight = item.weight();
        let now = Instant::now();
        self.inner.try_send(item)?;
        self.shared.add_load(entry_weight, now);
        Ok(())
    }
}

///
/// Receiving end of the load-aware channel.
///
/// See [`load_aware_channel`] for more details.
///
impl<T> LoadAwareReceiver<T>
where
    T: TrafficWeighted,
{
    pub async fn recv(&mut self) -> Option<T> {
        use std::future::poll_fn;
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let shared = Arc::clone(&self.shared);
        self.inner.poll_recv(cx).map(|maybe| {
            if let Some(entry) = &maybe {
                let entry_weight = entry.weight();
                shared.decr_load(entry_weight, Instant::now());
            }
            maybe
        })
    }

    pub fn estimated_rx_rate(&self) -> EmaCurrentLoad {
        self.shared.rx_ema.current_load()
    }
}

impl<T> Stream for LoadAwareReceiver<T>
where
    T: TrafficWeighted,
{
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
        std::sync::atomic::Ordering,
        tokio::{sync::mpsc, task::yield_now},
        tokio_stream::StreamExt,
    };

    #[derive(Debug)]
    struct TestItem(u32);

    impl TrafficWeighted for TestItem {
        fn weight(&self) -> u32 {
            self.0
        }
    }

    #[tokio::test]
    async fn test_basic_send_and_receive() {
        let (sender, mut receiver) = load_aware_channel(10, Default::default());

        sender.send(TestItem(5)).await.unwrap();
        let received = receiver.recv().await.unwrap();

        assert_eq!(received.0, 5);
    }

    #[tokio::test]
    async fn test_load_tracking() {
        let (sender, mut receiver) = load_aware_channel(10, Default::default());

        sender.send(TestItem(5)).await.unwrap();
        assert_eq!(sender.shared.size.load(Ordering::Relaxed), 1);
        assert_eq!(sender.shared.weight.load(Ordering::Relaxed), 5);

        receiver.recv().await.unwrap();
        assert_eq!(sender.shared.size.load(Ordering::Relaxed), 0);
        assert_eq!(sender.shared.weight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_stream_behavior() {
        let (sender, receiver) = load_aware_channel(10, Default::default());

        sender.send(TestItem(1)).await.unwrap();
        sender.send(TestItem(2)).await.unwrap();
        sender.send(TestItem(3)).await.unwrap();

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

        let (sender, mut receiver) = load_aware_channel(100000, Default::default());
        let target_rate_per_s = 1000;
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
        let estimated_send_rate = sender.estimated_send_rate();

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

        assert!(
            (estimated_send_rate.per_second() - target_rate_per_s as f64).abs() < 50.0,
            "Estimated rate: ~{}/s, target: {} +/- 50 / s",
            estimated_send_rate.per_second(),
            target_rate_per_s
        );
    }

    #[tokio::test]
    async fn sender_should_send_error_when_recv_drop() {
        let (sender, receiver) = load_aware_channel(10, Default::default());
        drop(receiver);
        assert!(sender.send(TestItem(1)).await.is_err());
    }
}
