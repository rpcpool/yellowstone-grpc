use {
    futures::Stream,
    std::{
        fmt,
        marker::PhantomData,
        sync::Arc,
        task::{Context, Poll},
    },
    thiserror::Error,
    tokio::sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        Semaphore, TryAcquireError,
    },
};

/// Largest capacity accepted by [`load_aware_channel`].
pub const MAX_CAPACITY: usize = Semaphore::MAX_PERMITS;

/// Error returned by [`LoadAwareSender::send`] when the [`LoadAwareReceiver`] is dropped.
///
/// It hands the item that could not be sent back to the caller.
#[derive(Error)]
#[error("load aware channel is closed")]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    /// Takes the item that could not be sent back out of the error.
    ///
    /// # Returns
    ///
    /// The item that was passed to [`LoadAwareSender::send`].
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

/// Error returned by [`LoadAwareSender::try_send`].
///
/// Both variants hand the item that could not be sent back to the caller.
#[derive(Error)]
pub enum TrySendError<T> {
    /// There is not enough free weight for the item right now, or another sender is already
    /// waiting for capacity.
    #[error("load aware channel is full")]
    Full(T),
    /// The [`LoadAwareReceiver`] is dropped.
    #[error("load aware channel is closed")]
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Takes the item that could not be sent back out of the error.
    ///
    /// # Returns
    ///
    /// The item that was passed to [`LoadAwareSender::try_send`].
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(item) | Self::Closed(item) => item,
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("Full(..)"),
            Self::Closed(_) => f.write_str("Closed(..)"),
        }
    }
}

/// Assigns the abstract "weight" a [`load_aware_channel`] uses for backpressure to each item.
///
/// The weight is an opaque score: the channel never interprets it, it only sums the weights of
/// the items currently queued and compares that sum against the channel capacity. Every
/// [`LoadAwareSender`] owns a copy of the weigher, so a weigher can carry configuration such as
/// per-kind scales.
pub trait Weigher {
    /// The type of item this weigher weighs.
    type Item;

    /// Returns the weight of `item`.
    ///
    /// # Arguments
    ///
    /// * `item` - The item about to be queued.
    ///
    /// # Returns
    ///
    /// The weight of the item. A weight of `0` is treated as `1` by the channel, and a weight
    /// above the channel capacity is treated as the full capacity.
    fn weight(&self, item: &Self::Item) -> u32;
}

/// A [`Weigher`] that weighs every item `1`, so the channel capacity bounds the item count.
pub struct UnitWeigher<T>(PhantomData<fn(&T)>);

impl<T> UnitWeigher<T> {
    /// Creates a [`UnitWeigher`].
    ///
    /// # Returns
    ///
    /// A [`UnitWeigher`] for items of type `T`.
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for UnitWeigher<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for UnitWeigher<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for UnitWeigher<T> {}

impl<T> fmt::Debug for UnitWeigher<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UnitWeigher")
    }
}

impl<T> Weigher for UnitWeigher<T> {
    type Item = T;

    fn weight(&self, _item: &T) -> u32 {
        1
    }
}

/// State shared between every [`LoadAwareSender`] and the [`LoadAwareReceiver`].
#[derive(Debug)]
struct Shared {
    /// One permit per unit of weight. A sender acquires the item weight before queueing and the
    /// receiver gives it back on dequeue. Waiting senders are served in strict FIFO order.
    semaphore: Semaphore,
    /// Total weight the channel can hold.
    capacity: usize,
}

impl Shared {
    /// Computes the weight an item is accounted for while it sits in the channel.
    ///
    /// A weight of `0` is floored to `1` so that every queued item consumes capacity, and a
    /// weight above `capacity` is clamped to `capacity` so an oversized item is admitted once
    /// the channel is empty instead of waiting forever.
    ///
    /// # Arguments
    ///
    /// * `weight` - The weight of the item about to be queued.
    ///
    /// # Returns
    ///
    /// The admitted weight, in `1..=capacity`. It always fits in a `u32` because the weight
    /// does.
    fn admitted_weight(&self, weight: u32) -> u32 {
        (weight.max(1) as usize).min(self.capacity) as u32
    }
}

/// Sender end of the channel created by [`load_aware_channel`].
///
/// It can be cloned freely, every clone shares the same capacity and weighs items with its own
/// clone of the [`Weigher`] `W`.
pub struct LoadAwareSender<T, W> {
    shared: Arc<Shared>,
    inner: UnboundedSender<(u32, T)>,
    weigher: W,
}

impl<T, W> fmt::Debug for LoadAwareSender<T, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadAwareSender")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl<T, W: Clone> Clone for LoadAwareSender<T, W> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            inner: self.inner.clone(),
            weigher: self.weigher.clone(),
        }
    }
}

/// Receiver end of the channel created by [`load_aware_channel`].
///
/// Dropping it closes the channel and wakes every sender waiting for capacity.
pub struct LoadAwareReceiver<T> {
    shared: Arc<Shared>,
    inner: UnboundedReceiver<(u32, T)>,
}

impl<T> Drop for LoadAwareReceiver<T> {
    fn drop(&mut self) {
        self.shared.semaphore.close();
    }
}

/// Creates an mpsc channel whose capacity is a total weight instead of an item count.
///
/// Each item is accounted for by the [`Weigher::weight`] of `weigher` from the moment a sender admits it
/// until the receiver dequeues it. The uncontended path is a single atomic operation plus a
/// lock-free enqueue, and a lock is only taken when a sender actually has to wait.
///
/// Semantics:
///
/// * Senders waiting for capacity are served in strict FIFO order, so a heavy item at the head
///   of the line blocks the lighter ones behind it even if they would fit.
/// * [`LoadAwareSender::try_send`] never jumps ahead of a waiting [`LoadAwareSender::send`].
/// * A weight of `0` counts as `1`, and a weight above `capacity` counts as `capacity`, meaning
///   such an item is only admitted into an empty channel.
///
/// # Arguments
///
/// * `weighted_capacity` - The total weight the channel can hold.
/// * `weigher` - The [`Weigher`] that weighs each item as a sender queues it.
///
/// # Returns
///
/// The [`LoadAwareSender`] and [`LoadAwareReceiver`] halves of the channel.
///
/// # Panics
///
/// Panics if `weighted_capacity` is `0` or exceeds [`MAX_CAPACITY`].
pub fn load_aware_channel<T, W>(
    weighted_capacity: usize,
    weigher: W,
) -> (LoadAwareSender<T, W>, LoadAwareReceiver<T>)
where
    W: Weigher<Item = T>,
{
    assert!(
        weighted_capacity > 0,
        "load aware channel weight capacity must be positive"
    );
    let (inner_sender, inner_receiver) = tokio::sync::mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        semaphore: Semaphore::new(weighted_capacity),
        capacity: weighted_capacity,
    });
    let sender = LoadAwareSender {
        shared: Arc::clone(&shared),
        inner: inner_sender,
        weigher,
    };

    let rx = LoadAwareReceiver {
        shared,
        inner: inner_receiver,
    };

    (sender, rx)
}

impl<T, W> LoadAwareSender<T, W>
where
    W: Weigher<Item = T>,
{
    /// Returns the weight `item` is accounted for while it sits in the channel.
    fn admitted_weight(&self, item: &T) -> u32 {
        self.shared.admitted_weight(self.weigher.weight(item))
    }

    /// Sends an item, waiting until the channel has enough free weight for it and every sender
    /// ahead of it in line.
    ///
    /// Cancel safe: dropping the returned future gives back any capacity it already reserved.
    ///
    /// # Arguments
    ///
    /// * `item` - The item to queue, weighed by the sender's [`Weigher`].
    ///
    /// # Returns
    ///
    /// `Ok(())` once the item is queued.
    ///
    /// # Errors
    ///
    /// Returns the `item` back in a [`SendError`] if the [`LoadAwareReceiver`] is dropped.
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        let weight = self.admitted_weight(&item);
        // Fast path: skip building the `Acquire` future when capacity is available. A failed try
        // never takes capacity ahead of a waiting sender, so falling through keeps FIFO order.
        match self.shared.semaphore.try_acquire_many(weight) {
            Ok(permit) => permit.forget(),
            Err(TryAcquireError::Closed) => return Err(SendError(item)),
            Err(TryAcquireError::NoPermits) => {
                match self.shared.semaphore.acquire_many(weight).await {
                    Ok(permit) => permit.forget(),
                    Err(_closed) => return Err(SendError(item)),
                }
            }
        }
        self.inner
            .send((weight, item))
            .map_err(|err| SendError(err.0 .1))
    }

    /// Sends an item without waiting.
    ///
    /// Fails with [`TrySendError::Full`] when the channel does not have enough free weight, or
    /// when another sender is already waiting for capacity, so it never jumps the queue.
    ///
    /// # Arguments
    ///
    /// * `item` - The item to queue, weighed by the sender's [`Weigher`].
    ///
    /// # Returns
    ///
    /// `Ok(())` once the item is queued.
    ///
    /// # Errors
    ///
    /// Returns the `item` back in a [`TrySendError`]: [`TrySendError::Full`] if there is no
    /// capacity for it right now, [`TrySendError::Closed`] if the [`LoadAwareReceiver`] is
    /// dropped.
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let weight = self.admitted_weight(&item);
        match self.shared.semaphore.try_acquire_many(weight) {
            Ok(permit) => permit.forget(),
            Err(TryAcquireError::NoPermits) => return Err(TrySendError::Full(item)),
            Err(TryAcquireError::Closed) => return Err(TrySendError::Closed(item)),
        }
        self.inner
            .send((weight, item))
            .map_err(|err| TrySendError::Closed(err.0 .1))
    }

    /// Returns the weight currently held by the channel.
    ///
    /// This includes capacity partially reserved by senders waiting for room, so it can be
    /// higher than the sum of the weights of the queued items while a sender is waiting.
    ///
    /// # Returns
    ///
    /// The held weight, in `0..=capacity`.
    pub fn current_weight(&self) -> usize {
        self.shared.capacity - self.shared.semaphore.available_permits()
    }
}

impl<T> LoadAwareReceiver<T> {
    /// Receives the next item, releasing its weight back to the channel.
    ///
    /// # Returns
    ///
    /// The next item, or [`None`] once the channel is empty and every [`LoadAwareSender`] is
    /// dropped.
    pub async fn recv(&mut self) -> Option<T> {
        use std::future::poll_fn;
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Polls for the next item, releasing its weight back to the channel.
    ///
    /// # Arguments
    ///
    /// * `cx` - The task [`Context`] to register for wake-up.
    ///
    /// # Returns
    ///
    /// [`Poll::Ready`] with the next item, or with [`None`] once the channel is empty and every
    /// [`LoadAwareSender`] is dropped. [`Poll::Pending`] otherwise.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner.poll_recv(cx).map(|maybe| {
            maybe.map(|(weight, item)| {
                self.shared.semaphore.add_permits(weight as usize);
                item
            })
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
        futures::poll,
        std::{pin::pin, task::Poll},
        tokio::task::yield_now,
        tokio_stream::StreamExt,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct TestItem(u32);

    /// Weighs a [`TestItem`] by the value it holds.
    #[derive(Debug, Clone, Copy)]
    struct TestWeigher;

    impl Weigher for TestWeigher {
        type Item = TestItem;

        fn weight(&self, item: &TestItem) -> u32 {
            item.0
        }
    }

    /// Weighs a [`TestItem`] by the value it holds times a configured factor.
    #[derive(Debug, Clone, Copy)]
    struct ScaledWeigher(u32);

    impl Weigher for ScaledWeigher {
        type Item = TestItem;

        fn weight(&self, item: &TestItem) -> u32 {
            item.0 * self.0
        }
    }

    #[tokio::test]
    async fn test_basic_send_and_receive() {
        let (sender, mut receiver) = load_aware_channel(10, TestWeigher);

        sender.send(TestItem(5)).await.unwrap();
        assert_eq!(sender.current_weight(), 5);
        let received = receiver.recv().await.unwrap();
        assert_eq!(sender.current_weight(), 0);
        assert_eq!(received.0, 5);
    }

    #[tokio::test]
    async fn test_stream_behavior() {
        let (sender, receiver) = load_aware_channel(10, TestWeigher);

        sender.send(TestItem(1)).await.unwrap();
        sender.send(TestItem(2)).await.unwrap();
        sender.send(TestItem(3)).await.unwrap();

        assert_eq!(sender.current_weight(), 6);
        drop(sender);
        let mut stream = receiver;

        let mut results = vec![];
        while let Some(item) = stream.next().await {
            results.push(item.0);
        }

        assert_eq!(results, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn waiting_senders_are_served_in_fifo_order() {
        let (sender, mut receiver) = load_aware_channel(10, TestWeigher);
        sender.send(TestItem(3)).await.unwrap();
        sender.send(TestItem(3)).await.unwrap();

        // Heavy item reserves the 4 free units, then waits for 4 more.
        let mut heavy = pin!(sender.send(TestItem(8)));
        assert!(poll!(&mut heavy).is_pending());
        // The light item would fit once a single light item leaves, but it is behind the heavy one.
        let mut light = pin!(sender.send(TestItem(1)));
        assert!(poll!(&mut light).is_pending());

        // Frees 3 units: the heavy item still needs 1, so the light item must keep waiting.
        assert_eq!(receiver.recv().await, Some(TestItem(3)));
        assert!(poll!(&mut heavy).is_pending());
        assert!(poll!(&mut light).is_pending());

        // Frees 3 more: the heavy item completes, then the light one gets the leftover.
        assert_eq!(receiver.recv().await, Some(TestItem(3)));
        assert!(matches!(poll!(&mut heavy), Poll::Ready(Ok(()))));
        assert!(matches!(poll!(&mut light), Poll::Ready(Ok(()))));

        assert_eq!(receiver.recv().await, Some(TestItem(8)));
        assert_eq!(receiver.recv().await, Some(TestItem(1)));
    }

    #[tokio::test]
    async fn try_send_does_not_jump_ahead_of_a_waiting_send() {
        let (sender, mut receiver) = load_aware_channel(10, TestWeigher);
        sender.send(TestItem(6)).await.unwrap();

        let mut heavy = pin!(sender.send(TestItem(8)));
        assert!(poll!(&mut heavy).is_pending());

        // 4 units are nominally free, but the waiting sender is first in line.
        assert!(matches!(
            sender.try_send(TestItem(1)),
            Err(TrySendError::Full(TestItem(1)))
        ));

        assert_eq!(receiver.recv().await, Some(TestItem(6)));
        assert!(matches!(poll!(&mut heavy), Poll::Ready(Ok(()))));
        // Nothing is waiting anymore, so try_send admits what fits.
        sender.try_send(TestItem(2)).unwrap();
    }

    #[tokio::test]
    async fn dropping_a_waiting_send_gives_its_capacity_back() {
        let (sender, mut receiver) = load_aware_channel(10, TestWeigher);
        sender.send(TestItem(6)).await.unwrap();

        {
            let mut heavy = pin!(sender.send(TestItem(8)));
            assert!(poll!(&mut heavy).is_pending());
            // It holds the 4 free units while it waits.
            assert_eq!(sender.current_weight(), 10);
        }

        assert_eq!(sender.current_weight(), 6);
        sender.try_send(TestItem(4)).unwrap();
        assert_eq!(receiver.recv().await, Some(TestItem(6)));
        assert_eq!(receiver.recv().await, Some(TestItem(4)));
        assert_eq!(sender.current_weight(), 0);
    }

    #[tokio::test]
    async fn dropping_the_receiver_wakes_waiting_senders() {
        let (sender, receiver) = load_aware_channel(2, TestWeigher);
        sender.send(TestItem(2)).await.unwrap();

        let mut waiting = pin!(sender.send(TestItem(1)));
        assert!(poll!(&mut waiting).is_pending());

        drop(receiver);

        match poll!(&mut waiting) {
            Poll::Ready(Err(SendError(item))) => assert_eq!(item, TestItem(1)),
            other => panic!("expected the waiting send to fail, got {other:?}"),
        }
        assert!(matches!(
            sender.try_send(TestItem(1)),
            Err(TrySendError::Closed(TestItem(1)))
        ));
        assert!(sender.send(TestItem(1)).await.is_err());
    }

    #[tokio::test]
    async fn oversized_item_is_admitted_only_into_an_empty_channel() {
        let (sender, mut receiver) = load_aware_channel(10, TestWeigher);

        // Empty channel: admitted immediately and accounted as the full capacity.
        sender.send(TestItem(100)).await.unwrap();
        assert_eq!(sender.current_weight(), 10);
        assert!(matches!(
            sender.try_send(TestItem(1)),
            Err(TrySendError::Full(_))
        ));
        assert_eq!(receiver.recv().await, Some(TestItem(100)));
        assert_eq!(sender.current_weight(), 0);

        // Non-empty channel: has to wait for it to drain.
        sender.send(TestItem(3)).await.unwrap();
        let mut oversized = pin!(sender.send(TestItem(100)));
        assert!(poll!(&mut oversized).is_pending());
        assert_eq!(receiver.recv().await, Some(TestItem(3)));
        assert!(matches!(poll!(&mut oversized), Poll::Ready(Ok(()))));
        assert_eq!(receiver.recv().await, Some(TestItem(100)));
        assert_eq!(sender.current_weight(), 0);
    }

    #[tokio::test]
    async fn weigher_state_sets_the_weight() {
        let (sender, mut receiver) = load_aware_channel(10, ScaledWeigher(3));
        let clone = sender.clone();

        sender.try_send(TestItem(2)).unwrap();
        assert_eq!(sender.current_weight(), 6);
        // A cloned sender weighs with the same scale.
        assert!(matches!(
            clone.try_send(TestItem(2)),
            Err(TrySendError::Full(TestItem(2)))
        ));
        assert_eq!(receiver.recv().await, Some(TestItem(2)));
        assert_eq!(sender.current_weight(), 0);
    }

    #[tokio::test]
    async fn zero_weight_items_still_consume_capacity() {
        let (sender, mut receiver) = load_aware_channel(2, TestWeigher);
        sender.try_send(TestItem(0)).unwrap();
        sender.try_send(TestItem(0)).unwrap();
        assert_eq!(sender.current_weight(), 2);
        assert!(matches!(
            sender.try_send(TestItem(0)),
            Err(TrySendError::Full(_))
        ));
        receiver.recv().await.unwrap();
        receiver.recv().await.unwrap();
        assert_eq!(sender.current_weight(), 0);
    }

    #[derive(Debug)]
    struct StressItem {
        sender_id: usize,
        seq: usize,
        weight: u32,
    }

    /// Weighs a [`StressItem`] by its `weight` field.
    #[derive(Debug, Clone, Copy)]
    struct StressWeigher;

    impl Weigher for StressWeigher {
        type Item = StressItem;

        fn weight(&self, item: &StressItem) -> u32 {
            item.weight
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_senders_deliver_everything_and_release_all_weight() {
        const CAPACITY: usize = 20;
        const SENDERS: usize = 4;
        const ITEMS_PER_SENDER: usize = 5_000;

        let (sender, mut receiver) = load_aware_channel(CAPACITY, StressWeigher);
        let probe = sender.clone();

        let mut tasks = Vec::new();
        for sender_id in 0..SENDERS {
            let sender = sender.clone();
            tasks.push(tokio::spawn(async move {
                for seq in 0..ITEMS_PER_SENDER {
                    let item = StressItem {
                        sender_id,
                        seq,
                        weight: ((seq + sender_id) % 7 + 1) as u32,
                    };
                    // Odd senders spin on try_send, even ones use the waiting send.
                    if sender_id % 2 == 1 {
                        let mut item = item;
                        loop {
                            match sender.try_send(item) {
                                Ok(()) => break,
                                Err(TrySendError::Full(back)) => {
                                    item = back;
                                    yield_now().await;
                                }
                                Err(TrySendError::Closed(_)) => panic!("receiver dropped"),
                            }
                        }
                    } else {
                        sender.send(item).await.unwrap();
                    }
                }
            }));
        }
        drop(sender);

        let mut next_seq = [0usize; SENDERS];
        for _ in 0..SENDERS * ITEMS_PER_SENDER {
            let item = receiver.recv().await.expect("channel closed early");
            assert_eq!(
                item.seq, next_seq[item.sender_id],
                "per sender order broken"
            );
            next_seq[item.sender_id] += 1;
            assert!(probe.current_weight() <= CAPACITY);
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(next_seq, [ITEMS_PER_SENDER; SENDERS]);
        assert_eq!(probe.current_weight(), 0);
    }
}
