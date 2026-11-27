//! A stream's sends: live entries queue without waiting, while replay items, the missed batch,
//! the backlog and snapshot frames wait for room in the client's queue.

use {
    crate::{
        grpc::SubscribeSender,
        plugin::{
            filter::{message::WeightScales, Filter, MatchScratch},
            message::{CommitmentLevel, Message},
        },
        subscribe::{entry::SubscribeQueueEntry, group::MemberQueueEntries},
        util::stream::{SendError, TrySendError},
    },
    std::{sync::Arc, time::Duration},
};

/// Why a send that never waits did not queue an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveSendFailure {
    /// The entry did not fit, or another sender waits for room.
    Full,
    /// The client's queue is closed: its stream has ended.
    Closed,
}

/// An unrouted client's live step for one message: queues its entries without waiting and adds
/// each one queued to `sent_entry_count`. The first entry that does not fit ends the step.
pub fn try_send_member_entries(
    stream_tx: &SubscribeSender,
    member_filter: &Arc<Filter>,
    routed_message: &Message,
    commitment_level: Option<CommitmentLevel>,
    match_scratch: &mut MatchScratch,
    sent_entry_count: &mut u64,
) -> Result<(), LiveSendFailure> {
    let member_entries = MemberQueueEntries::of_message(
        member_filter,
        routed_message,
        commitment_level,
        match_scratch,
        stream_tx.weigher(),
    );
    for queue_entry in member_entries {
        match stream_tx.try_send(Ok(queue_entry)) {
            Ok(()) => *sent_entry_count += 1,
            Err(TrySendError::Full(_)) => return Err(LiveSendFailure::Full),
            Err(TrySendError::Closed(_)) => return Err(LiveSendFailure::Closed),
        }
    }
    Ok(())
}

/// Why a waiting send did not queue its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitingSendError {
    /// The client's queue is closed: its stream has ended.
    Closed,
    /// The entry waited past its limit and was withdrawn: the client never gets it.
    TimedOut,
}

/// One stream's sender for the producers that wait for room in its queue.
pub struct StreamUpdateSender<'stream> {
    stream_tx: &'stream SubscribeSender,
}

impl<'stream> StreamUpdateSender<'stream> {
    pub const fn new(stream_tx: &'stream SubscribeSender) -> Self {
        Self { stream_tx }
    }

    /// The scales the stream's queue weighs its entries with.
    pub const fn weight_scales(&self) -> &'stream WeightScales {
        self.stream_tx.weigher()
    }

    /// Queues `queue_entry` once the queue has room for it, behind every sender that waits
    /// already. Past `wait_limit` the entry is withdrawn and its waited weight goes back.
    pub async fn send_waiting(
        &self,
        queue_entry: SubscribeQueueEntry,
        wait_limit: Option<Duration>,
    ) -> Result<(), WaitingSendError> {
        let waiting_send = self.stream_tx.send(Ok(queue_entry));
        let send_result = match wait_limit {
            Some(wait_limit) => tokio::time::timeout(wait_limit, waiting_send)
                .await
                .map_err(|_elapsed| WaitingSendError::TimedOut)?,
            None => waiting_send.await,
        };
        send_result.map_err(|SendError(_)| WaitingSendError::Closed)
    }
}
