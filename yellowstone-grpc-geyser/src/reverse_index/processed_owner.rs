//! geyser_loop's state as the processed owner, and the owner's work between two plugin
//! batches.

#![allow(rustdoc::private_intra_doc_links)]

#[cfg(doc)]
use crate::grpc::GrpcService;
use {
    crate::{
        grpc::{BroadcastedMessage, SubscriberChannels},
        plugin::message::{CommitmentLevel, Message},
        reverse_index::{IndexChange, OwnerIndexes, RelayedItem},
        stream::{BatchStream, BatchStreamExt, Buffer},
    },
    std::pin::Pin,
    tokio::{
        sync::mpsc,
        time::{sleep_until, Sleep},
    },
};

/// What [`GrpcService::geyser_loop`] does between two plugin batches as the processed owner: it
/// sends what block reconstruction relays, takes in the index changes of processed clients, and
/// runs index slices.
pub struct ProcessedOwner {
    /// The owner's indexes. Every batch sent at processed goes through them.
    owner_indexes: OwnerIndexes,
    /// The index changes of processed clients, and every client's last leave.
    #[allow(clippy::disallowed_types, reason = "owner inputs on tokio channels")]
    index_changes_rx: mpsc::UnboundedReceiver<IndexChange>,
    /// What block reconstruction relays: its processed batches and its replays.
    #[allow(clippy::disallowed_types, reason = "owner inputs on tokio channels")]
    relay_rx: mpsc::UnboundedReceiver<RelayedItem>,
    /// Fires at [`OwnerIndexes::next_index_slice_at`].
    index_slice_timer: Pin<Box<Sleep>>,
}

impl ProcessedOwner {
    /// Creates geyser_loop's state as the processed owner, with no indexed subscriber yet.
    ///
    /// # Panics
    ///
    /// Panics outside a tokio runtime, as [`sleep_until`] does.
    #[allow(clippy::disallowed_types, reason = "owner inputs on tokio channels")]
    pub fn new(
        broadcast: SubscriberChannels,
        index_changes_rx: mpsc::UnboundedReceiver<IndexChange>,
        relay_rx: mpsc::UnboundedReceiver<RelayedItem>,
    ) -> Self {
        let owner_indexes = OwnerIndexes::new(broadcast);
        let index_slice_timer = Box::pin(sleep_until(owner_indexes.next_index_slice_at()));
        Self {
            owner_indexes,
            index_changes_rx,
            relay_rx,
            index_slice_timer,
        }
    }

    /// Reads the plugin's next batch, meanwhile sending relayed items (all queued ones for one budget
    /// unit), taking index changes and running idle index slices, in that biased order.
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
            self.owner_indexes
                .take_queued_work(&mut self.index_changes_rx, self.index_slice_timer.as_mut());
            tokio::select! {
                biased;
                Some(relayed_item) = self.relay_rx.recv() => {
                    relayed_item.send_or_answer(&mut self.owner_indexes);
                    for _ in 0..self.relay_rx.len() {
                        let Ok(relayed_item) = self.relay_rx.try_recv() else {
                            break;
                        };
                        relayed_item.send_or_answer(&mut self.owner_indexes);
                    }
                }
                batch_size_maybe = messages_rx.next_batch(buffer) => return batch_size_maybe,
                Some(index_change) = self.index_changes_rx.recv() => {
                    self.owner_indexes.take_index_change(index_change);
                }
                () = &mut self.index_slice_timer, if self.owner_indexes.has_index_work() => {
                    self.owner_indexes
                        .run_index_slice_and_reset_timer(self.index_slice_timer.as_mut());
                }
            }
        }
    }

    /// Sends `message_batch` at processed through the owner's [`OwnerIndexes`].
    #[inline]
    pub fn send_batch(&mut self, message_batch: BroadcastedMessage) {
        self.owner_indexes
            .send_batch(CommitmentLevel::Processed, message_batch);
    }
}
