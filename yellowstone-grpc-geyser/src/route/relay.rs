//! What block reconstruction relays to geyser_loop, the only sender at processed, and the
//! yield that lets a task it woke run first.

#![allow(rustdoc::private_intra_doc_links)]

#[cfg(doc)]
use crate::{geyser_subscriber::ReplayCut, grpc::GrpcService};
use {
    crate::{
        grpc::{BroadcastedMessage, ReplayedResponse},
        plugin::message::CommitmentLevel,
        route::OwnerRoutes,
    },
    std::task::Poll,
    tokio::sync::oneshot,
};

/// What block reconstruction hands [`GrpcService::geyser_loop`], the only sender at processed.
/// geyser_loop sends the items in the order it gets them.
pub enum RelayedItem {
    /// A batch block reconstruction built for the processed channel: a bank's Block, footer and
    /// block meta, or its commitment statuses.
    Batch(BroadcastedMessage),
    /// A replay request's answer, and the sender to answer on. It goes behind the batches
    /// relayed before it, so the replay's [`ReplayCut::last_sent`] at processed is out on the
    /// channel before the client gets the replay.
    Answer(oneshot::Sender<ReplayedResponse>, ReplayedResponse),
}

impl RelayedItem {
    /// Sends a relayed batch at processed, or answers a replay request.
    ///
    /// # Arguments
    ///
    /// * `owner_routes` - The [`OwnerRoutes`] of [`GrpcService::geyser_loop`], the sender at
    ///   processed.
    pub fn send_or_answer(self, owner_routes: &mut OwnerRoutes) {
        match self {
            Self::Batch(relayed_batch) => {
                owner_routes.send_batch(CommitmentLevel::Processed, relayed_batch)
            }
            Self::Answer(response_tx, replayed_response) => {
                let _ = response_tx.send(replayed_response);
            }
        }
    }
}

/// Ends the calling task's poll once, so a task it woke on the same worker runs before it.
///
/// A task woken from a tokio worker waits in that worker's LIFO slot, which no other worker
/// steals from, until the poll of the task that woke it ends. This wakes the calling task and
/// returns `Pending` once: tokio then puts the calling task at the back of the worker's run
/// queue, where an idle worker can steal it, and polls the LIFO slot next. Unlike
/// [`tokio::task::yield_now`], it does not wait for the worker to park first.
///
/// # Returns
///
/// A future that is ready at its second poll.
pub async fn let_woken_task_run_first() {
    let mut rescheduled = false;
    std::future::poll_fn(|task_context| {
        if std::mem::replace(&mut rescheduled, true) {
            return Poll::Ready(());
        }
        task_context.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}
