//! geyser_loop's state as the processed owner, and the owner's work between two plugin
//! batches.

#![allow(rustdoc::private_intra_doc_links)]

#[cfg(doc)]
use crate::grpc::GrpcService;
use {
    crate::{
        grpc::{BroadcastedMessage, SubscriberChannels},
        plugin::message::{CommitmentLevel, Message},
        route::{OwnerRoutes, RelayedItem, RouteChange},
        stream::{BatchStream, BatchStreamExt, Buffer},
    },
    std::pin::Pin,
    tokio::{
        sync::mpsc,
        time::{sleep_until, Sleep},
    },
};

/// What [`GrpcService::geyser_loop`] does between two plugin batches as the processed owner: it
/// sends what block reconstruction relays, takes in the route changes of processed clients, and
/// runs index slices.
///
/// # Examples
///
/// ```ignore
/// let mut processed_owner = ProcessedOwner::new(broadcast, route_changes_rx, relay_rx);
/// while processed_owner.next_plugin_batch(&mut messages_rx, &mut buffer).await.is_some() {
///     processed_owner.send_batch(Arc::new(buffer.message_batch.drain(..).collect()));
/// }
/// ```
pub struct ProcessedOwner {
    /// The routes at processed. Every batch sent at processed goes through them.
    owner_routes: OwnerRoutes,
    /// The route changes of processed clients, and every client's last leave.
    route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
    /// What block reconstruction relays: its processed batches and its replays.
    relay_rx: mpsc::UnboundedReceiver<RelayedItem>,
    /// Fires at [`OwnerRoutes::next_index_slice_at`].
    index_slice_timer: Pin<Box<Sleep>>,
}

impl ProcessedOwner {
    /// Creates geyser_loop's state as the processed owner, with no route yet.
    ///
    /// # Arguments
    ///
    /// * `broadcast` - The broadcast channels its [`OwnerRoutes`] broadcast on.
    /// * `route_changes_rx` - The route changes of processed clients, and every client's last
    ///   leave.
    /// * `relay_rx` - What block reconstruction relays.
    ///
    /// # Returns
    ///
    /// The processed owner, whose index slice timer fires at once.
    ///
    /// # Panics
    ///
    /// Panics outside a tokio runtime, as [`sleep_until`] does.
    pub fn new(
        broadcast: SubscriberChannels,
        route_changes_rx: mpsc::UnboundedReceiver<RouteChange>,
        relay_rx: mpsc::UnboundedReceiver<RelayedItem>,
    ) -> Self {
        let owner_routes = OwnerRoutes::new(broadcast);
        let index_slice_timer = Box::pin(sleep_until(owner_routes.next_index_slice_at()));
        Self {
            owner_routes,
            route_changes_rx,
            relay_rx,
            index_slice_timer,
        }
    }

    /// Reads the plugin's next batch into `buffer`, and does the owner's other work until the
    /// batch is ready.
    ///
    /// Each pass starts with [`OwnerRoutes::take_queued_work`]. Then it takes the first ready arm
    /// of a `biased` `select!`, in this order: the relay, so a ready raw batch does not go ahead
    /// of a relayed item; the plugin's next batch, which ends the call; a route change that
    /// arrives while it waits; and the index slice timer, while index work waits. So a route
    /// change lands between two sends, and an idle slice runs only when the other arms read
    /// `Pending`, at most once per slice interval.
    ///
    /// The relay arm sends every item queued when it reads the first one, for one unit of
    /// tokio's cooperative budget, as a read of the plugin's next batch does. With one unit per
    /// item, a burst of replay answers spends the budget of 128 units per poll. The task then
    /// waits in its worker's defer list until the worker parks or runs its maintenance, and the
    /// plugin's batches wait with it.
    ///
    /// # Arguments
    ///
    /// * `messages_rx` - The plugin's messages.
    /// * `buffer` - Gets the plugin's next batch.
    ///
    /// # Returns
    ///
    /// What [`BatchStreamExt::next_batch`] gives: the number of messages read, or [`None`] once
    /// the plugin's channel has closed.
    pub async fn next_plugin_batch<St, B>(
        &mut self,
        messages_rx: &mut St,
        buffer: &mut B,
    ) -> Option<usize>
    where
        St: BatchStream<Item = Message> + Unpin,
        B: Buffer<Message> + Unpin,
    {
        loop {
            self.owner_routes
                .take_queued_work(&mut self.route_changes_rx, self.index_slice_timer.as_mut());
            tokio::select! {
                biased;
                Some(relayed_item) = self.relay_rx.recv() => {
                    relayed_item.send_or_answer(&mut self.owner_routes);
                    for _ in 0..self.relay_rx.len() {
                        let Ok(relayed_item) = self.relay_rx.try_recv() else {
                            break;
                        };
                        relayed_item.send_or_answer(&mut self.owner_routes);
                    }
                }
                batch_size_maybe = messages_rx.next_batch(buffer) => return batch_size_maybe,
                Some(route_change) = self.route_changes_rx.recv() => {
                    self.owner_routes.take_route_change(route_change);
                }
                () = &mut self.index_slice_timer, if self.owner_routes.has_index_work() => {
                    self.owner_routes
                        .run_index_slice_and_reset_timer(self.index_slice_timer.as_mut());
                }
            }
        }
    }

    /// Sends `message_batch` at processed through the owner's [`OwnerRoutes`].
    ///
    /// # Arguments
    ///
    /// * `message_batch` - The batch to send.
    #[inline]
    pub fn send_batch(&mut self, message_batch: BroadcastedMessage) {
        self.owner_routes
            .send_batch(CommitmentLevel::Processed, message_batch);
    }
}
