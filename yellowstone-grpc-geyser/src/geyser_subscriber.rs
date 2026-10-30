//! How a client catches up on a `from_slot` replay before its live stream.
//!
//! The client subscribes its live receiver at L, then asks block reconstruction for the replay,
//! which builds it at R. What went out between the two is in both:
//!
//! ```text
//! processed channel:   ... b1  b2  b3 │ b4  b5 │ b6  b7  b8 ...
//!                                     L       R
//!
//!               before L        L → R           after R
//!             ┌───────────┬───────────────┬──────────────┐
//! replay      │    yes    │     yes       │     no       │
//! live        │    no     │     yes       │     yes      │
//!             └───────────┴───────────────┴──────────────┘
//! ```
//!
//! [`ReplayPhase`] hands out the replay, then the batches queued on the receiver between L and
//! R less what the replay already sent, and then gives the receiver back: from there every
//! batch is new.
//!
//! Two tasks send on the processed channel, so R has two edges. geyser_loop broadcasts raw
//! batches and only then hands them to block reconstruction, so a raw batch went into the
//! replay if block reconstruction took it in: [`ReplayCut::last_taken_in`] is the last one.
//! Block reconstruction's own batches went out before R up to [`ReplayCut::last_sent`]. Both
//! are compared by pointer, since block reconstruction holds the very `Arc` that went out.
//!
//! Block reconstruction builds a replay only once its queue reads empty, on a read made after it
//! took the request, so every batch handed over before that read is in the replay. One raw
//! batch can still be in neither. geyser_loop
//! broadcasts a batch, then hands it over; if it pauses between the two while the client
//! subscribes and block reconstruction empties its queue and builds the replay, that batch is
//! missed by both:
//!
//! ```text
//! time ──────────────────────────────────────────────────────────────────────────────►
//!
//! geyser_loop:  broadcast b2  hand b2   broadcast b3 ······ pause ······ hand b3   broadcast b4
//!                                                     │              │
//! client:                                             L              │
//!                                                     │              │
//! block recon:                                        │  empty, R    │
//!
//!   b2  handed over before the queue emptied  → in the replay
//!   b3  broadcast before L, handed over after → in neither: sent through `next_taken_in`
//!   b4  broadcast after b3 was handed over    → on the live receiver
//! ```
//!
//! Only one batch can fall through: the next one is broadcast after this one is handed over,
//! so after the replay, so live. [`ReplayCut::next_taken_in`] is the first raw batch block
//! reconstruction takes in after the replay. If the live receiver does not have it, it is
//! the missed batch, and goes out right after the replay.

use {
    crate::{
        grpc::{BroadcastedMessage, Replay, ReplayResponseMessageType},
        plugin::message::{CommitmentLevel, Message, SlotStatus},
    },
    solana_clock::BankId,
    std::{collections::VecDeque, ops::Range, sync::Arc, vec},
    tokio::sync::{
        broadcast::{
            self,
            error::{RecvError, TryRecvError},
        },
        oneshot,
    },
};

/// Where block reconstruction stood when it built a replay.
pub struct ReplayCut {
    /// The last raw batch it took in. Only used at processed, the one channel geyser_loop
    /// broadcasts raw batches on.
    pub last_taken_in: Option<BroadcastedMessage>,
    /// The last batch it broadcast on the replay's commitment channel.
    pub last_sent: Option<BroadcastedMessage>,
    /// The first raw batch it takes in after the replay. Processed only.
    ///
    /// At most one raw batch can be in neither the replay nor the live receiver, and this is
    /// it. That holds while:
    /// 1. geyser_loop is the only task that broadcasts raw batches and hands them over;
    /// 2. it broadcasts each batch and then hands it over, with no `await` in between, so a
    ///    batch is handed over before the next one is broadcast;
    /// 3. the live receiver is subscribed before the replay is asked for;
    /// 4. block reconstruction builds the replay only once its queue reads empty, on a read made
    ///    after it took the request, so it took in every batch handed over before that read. A
    ///    read made before the request arrived is stale: two batches can be handed over, and
    ///    the client subscribe, between it and the request.
    ///
    /// A batch in neither was broadcast before the receiver existed and handed over after the
    /// replay, so the batch after it was broadcast after the replay, and is live.
    pub next_taken_in: Option<oneshot::Receiver<BroadcastedMessage>>,
}

/// The smallest and largest bank id in a replay, noted by block reconstruction as it pushes
/// each bank, so the replay's messages need no pass to find it.
///
/// # Examples
///
/// ```ignore
/// let mut banks: Option<BankSpan> = None;
/// for bank_id in [102, 100, 105] {
///     banks.get_or_insert(BankSpan::new(bank_id)).include(bank_id);
/// }
/// assert_eq!(banks, Some(BankSpan { min: 100, max: 105 }));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BankSpan {
    pub min: BankId,
    pub max: BankId,
}

impl BankSpan {
    /// The span of one bank.
    ///
    /// # Arguments
    ///
    /// * `bank_id` - The bank.
    ///
    /// # Returns
    ///
    /// A span from `bank_id` to `bank_id`.
    pub const fn new(bank_id: BankId) -> Self {
        Self {
            min: bank_id,
            max: bank_id,
        }
    }

    /// Widens the span to cover `bank_id`.
    ///
    /// # Arguments
    ///
    /// * `bank_id` - A bank the replay holds.
    pub fn include(&mut self, bank_id: BankId) {
        self.min = self.min.min(bank_id);
        self.max = self.max.max(bank_id);
    }

    /// The span of the banks `messages` name, found by a pass over them. Tests build their
    /// replays by hand, so they have no block reconstruction to note it.
    ///
    /// # Arguments
    ///
    /// * `messages` - A replay's messages.
    ///
    /// # Returns
    ///
    /// The span, or [`None`] if no message names a bank.
    #[cfg(test)]
    pub fn of(messages: &[ReplayResponseMessageType]) -> Option<Self> {
        let mut banks: Option<Self> = None;
        for (bank_id, _) in messages.iter().flatten().filter_map(bank_part) {
            banks.get_or_insert(Self::new(bank_id)).include(bank_id);
        }
        banks
    }
}

/// A `from_slot` replay being sent to one client, and the live receiver it joins.
///
/// It owns the replay, its [`ReplayCut`] and the live receiver, and hands out, in order,
/// everything the client needs before its live stream:
///
/// 1. the replay's messages;
/// 2. the raw batch that fell between the replay and the receiver, if one did;
/// 3. the batches queued on the receiver up to the cut, less what the replay already sent.
///
/// Then [`ReplayPhase::into_receiver`] gives the receiver back, where every batch is new. It is
/// the only way to the receiver, and it checks everything was handed out, so the live stream
/// cannot start before the replay is done.
///
/// # Examples
///
/// ```ignore
/// let mut phase = ReplayPhase::new(rx, commitment, replay)?;
/// while let Some(message) = phase.next().await? {
///     send_with_backpressure(message).await?;
/// }
/// let rx = phase.into_receiver();
/// ```
pub struct ReplayPhase {
    rx: broadcast::Receiver<BroadcastedMessage>,
    commitment: CommitmentLevel,
    replay: ReplayMessages,
    sent: ReplaySent,
    /// Every batch sent before R that the receiver got is in here: R came before the replay
    /// arrived.
    queued: VecDeque<BroadcastedMessage>,
    /// The queued batch being handed out, and where its next message is.
    batch: Option<(BroadcastedMessage, usize)>,
    /// Raw batches up to this one can repeat the replay. [`None`] once it has gone by, or if it
    /// went out before the receiver existed.
    last_taken_in: Option<BroadcastedMessage>,
    /// Block reconstruction's batches up to this one can repeat the replay. [`None`] as above.
    last_sent: Option<BroadcastedMessage>,
    /// See [`ReplayCut::next_taken_in`]. Nothing queued is handed out until it is in.
    next_taken_in: Option<oneshot::Receiver<BroadcastedMessage>>,
    /// Set once [`ReplayPhase::next`] returned [`None`].
    done: bool,
}

/// A replay's messages, in order, one at a time.
///
/// # Examples
///
/// ```ignore
/// let mut replay = ReplayMessages::new(messages);
/// while let Some(message) = replay.next() {
///     send(message);
/// }
/// ```
struct ReplayMessages {
    /// The items not started yet.
    items: vec::IntoIter<ReplayResponseMessageType>,
    /// The batch being handed out, and the indexes of its messages still to go.
    batch: Option<(Arc<Vec<Message>>, Range<usize>)>,
}

impl ReplayMessages {
    /// Starts at the first message of `items`.
    ///
    /// # Arguments
    ///
    /// * `items` - The replay, as block reconstruction built it.
    ///
    /// # Returns
    ///
    /// The iterator over the messages of `items`.
    fn new(items: Vec<ReplayResponseMessageType>) -> Self {
        Self {
            items: items.into_iter(),
            batch: None,
        }
    }
}

impl Iterator for ReplayMessages {
    type Item = Message;

    fn next(&mut self) -> Option<Message> {
        loop {
            if let Some((batch, range)) = &mut self.batch {
                if let Some(message) = range.find_map(|index| batch.get(index)) {
                    return Some(message.clone());
                }
                self.batch = None;
            }
            match self.items.next()? {
                ReplayResponseMessageType::Single(message) => return Some(message),
                ReplayResponseMessageType::Batch(batch, range) => self.batch = Some((batch, range)),
            }
        }
    }
}

/// What a replay sent, per bank: a table looked up by `bank_id & mask`, with no hashing.
///
/// ```text
/// replay banks 100 101 102 104 105: span 6, so 8 entries, mask 7
///
/// entry:   0      1      2   3   4        5      6      7
/// bank:    104    105    -   -   100      101    102    -
/// parts:   D      D              D P C    D P    D
///
/// 101 Processed → entry 5, bank 101, P set   → sent
/// 108 account   → entry 4, bank 100 ≠ 108    → not sent
/// ```
///
/// The table has a power-of-two length at least the span of the replay's bank ids, so two of
/// its banks never share an entry. A bank outside the replay can land on a used entry, but its
/// id does not match, so nothing of it is dropped. Bank ids come from one counter per validator
/// process, incremented per bank, so a replay's banks span about as many ids as it has banks.
#[derive(Default)]
struct ReplaySent {
    /// Bank `bank_id` is at `bank_id & mask`.
    entries: Vec<SentBank>,
    mask: u64,
}

/// One entry of [`ReplaySent`].
#[derive(Debug, Default, Clone, Copy)]
struct SentBank {
    bank_id: BankId,
    /// What the replay sent of `bank_id`. Empty if the entry is.
    sent: BankParts,
}

/// A part of a bank the replay can send, and the dedup tracks as one. Its value is its bit in
/// [`BankParts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum BankPart {
    /// Accounts, transactions, entries, CreatedBank, Block, footer and block meta.
    Data = 0,
    Processed = 1,
    Confirmed = 2,
    Finalized = 3,
}

/// A set of [`BankPart`]s, one bit each.
///
/// # Examples
///
/// ```ignore
/// let mut sent = BankParts::default();
/// sent.insert(BankPart::Data);
/// assert!(sent.contains(BankPart::Data));
/// assert!(!sent.contains(BankPart::Confirmed));
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct BankParts(u8);

impl BankParts {
    /// Adds `part`.
    ///
    /// # Arguments
    ///
    /// * `part` - The part to add.
    const fn insert(&mut self, part: BankPart) {
        self.0 |= Self::bit(part);
    }

    /// Whether `part` is in the set.
    ///
    /// # Arguments
    ///
    /// * `part` - The part to look for.
    ///
    /// # Returns
    ///
    /// `true` if `part` was inserted.
    const fn contains(self, part: BankPart) -> bool {
        self.0 & Self::bit(part) != 0
    }

    /// Whether the set has no part.
    ///
    /// # Returns
    ///
    /// `true` if nothing was inserted.
    const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The bit of `part`.
    ///
    /// # Arguments
    ///
    /// * `part` - The part.
    ///
    /// # Returns
    ///
    /// `1` shifted left by the value of `part`.
    const fn bit(part: BankPart) -> u8 {
        1 << part as u8
    }
}

impl ReplayPhase {
    /// Starts sending `replay`, joined to the live receiver subscribed before it was asked for.
    ///
    /// # Arguments
    ///
    /// * `rx` - The live receiver, subscribed before the replay request.
    /// * `commitment` - The commitment of `rx` and of the replay.
    /// * `replay` - The replay, and where block reconstruction stood when it built it.
    ///
    /// # Returns
    ///
    /// The phase, with the batches queued on `rx` now taken.
    ///
    /// # Errors
    ///
    /// The [`RecvError`] `rx` returned while its queue was being taken.
    pub fn new(
        mut rx: broadcast::Receiver<BroadcastedMessage>,
        commitment: CommitmentLevel,
        replay: Replay,
    ) -> Result<Self, RecvError> {
        let Replay {
            messages,
            bank_id_span: _,
            cut,
        } = replay;
        let mut queued = VecDeque::new();
        take_queued(&mut rx, &mut queued)?;
        // A cut batch not queued went out before `rx` existed: nothing queued can repeat it.
        let queued_cut = |cut: Option<BroadcastedMessage>| {
            cut.filter(|cut| queued.iter().any(|batch| Arc::ptr_eq(batch, cut)))
        };
        let last_taken_in = queued_cut(cut.last_taken_in);
        // A last batch taken in that went out after `rx` existed means the next one did too.
        let next_taken_in = cut.next_taken_in.filter(|_| last_taken_in.is_none());
        let last_sent = queued_cut(cut.last_sent);
        Ok(Self {
            rx,
            commitment,
            sent: ReplaySent::new(&messages),
            replay: ReplayMessages::new(messages),
            queued,
            batch: None,
            last_taken_in,
            last_sent,
            next_taken_in,
            done: false,
        })
    }

    /// The next message to filter and send to the client, in order: the replay, then the
    /// missed batch, then the queued batches less what the replay already sent.
    ///
    /// # Returns
    ///
    /// The next message, or [`None`] once everything was handed out: then call
    /// [`ReplayPhase::into_receiver`].
    ///
    /// # Errors
    ///
    /// The [`RecvError`] of the live receiver, or [`RecvError::Closed`] if block reconstruction
    /// stopped before sending [`ReplayCut::next_taken_in`].
    pub async fn next(&mut self) -> Result<Option<Message>, RecvError> {
        if let Some(message) = self.replay.next() {
            return Ok(Some(message));
        }
        // Awaited in place, so dropping this call does not lose it.
        if let Some(next_taken_in) = &mut self.next_taken_in {
            let next = next_taken_in.await.map_err(|_| RecvError::Closed)?;
            self.next_taken_in = None;
            self.take_missed(next)?;
        }
        loop {
            if let Some((batch, at)) = &mut self.batch {
                if let Some(message) = batch.get(*at) {
                    *at += 1;
                    return Ok(Some(message.clone()));
                }
                self.batch = None;
            }
            match self.queued.pop_front() {
                Some(batch) => self.batch = self.join(batch).map(|batch| (batch, 0)),
                None => {
                    self.done = true;
                    return Ok(None);
                }
            }
        }
    }

    /// Ends the phase.
    ///
    /// # Returns
    ///
    /// The live receiver, where every batch is new.
    ///
    /// # Panics
    ///
    /// If [`ReplayPhase::next`] has not returned [`None`] yet: the live stream would start
    /// before the replay and its backlog were sent.
    pub fn into_receiver(self) -> broadcast::Receiver<BroadcastedMessage> {
        assert!(
            self.done,
            "the live stream starts only after ReplayPhase::next returned None"
        );
        self.rx
    }

    /// Puts `next`, the first raw batch block reconstruction took in after the replay, first
    /// in the queue if the live receiver does not have it.
    fn take_missed(&mut self, next: BroadcastedMessage) -> Result<(), RecvError> {
        // `next` went out before block reconstruction took it in: if the receiver got it, it
        // is queued by now.
        take_queued(&mut self.rx, &mut self.queued)?;
        if !self.queued.iter().any(|batch| Arc::ptr_eq(batch, &next)) {
            self.queued.push_front(next);
        }
        Ok(())
    }

    /// Drops from `batch` what the replay already sent, if `batch` went out before R.
    fn join(&mut self, batch: BroadcastedMessage) -> Option<BroadcastedMessage> {
        let raw = self.commitment == CommitmentLevel::Processed && !is_reconstruction_batch(&batch);
        let cut = if raw {
            &mut self.last_taken_in
        } else {
            &mut self.last_sent
        };
        let Some(cut_batch) = cut else {
            return Some(batch);
        };
        if Arc::ptr_eq(cut_batch, &batch) {
            *cut = None;
        }

        if !batch.iter().any(|message| self.sent.contains(message)) {
            return Some(batch);
        }
        let rest: Vec<Message> = batch
            .iter()
            .filter(|message| !self.sent.contains(message))
            .cloned()
            .collect();
        (!rest.is_empty()).then(|| Arc::new(rest))
    }
}

/// Moves every batch queued on `rx` to `queued`.
fn take_queued(
    rx: &mut broadcast::Receiver<BroadcastedMessage>,
    queued: &mut VecDeque<BroadcastedMessage>,
) -> Result<(), RecvError> {
    loop {
        match rx.try_recv() {
            Ok(batch) => queued.push_back(batch),
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Closed) => return Err(RecvError::Closed),
            Err(TryRecvError::Lagged(skipped)) => return Err(RecvError::Lagged(skipped)),
        }
    }
}

/// Whether block reconstruction sent `batch`. geyser_loop never broadcasts a Block, and hands
/// block metas, footers and commitment statuses to block reconstruction only.
fn is_reconstruction_batch(batch: &[Message]) -> bool {
    match batch.first() {
        Some(Message::Block(_)) => true,
        Some(Message::Slot(slot)) => matches!(
            slot.status,
            SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Finalized
        ),
        _ => false,
    }
}

impl ReplaySent {
    /// Records what `replay` sent, in two passes: the first sizes the table from the span of
    /// bank ids, the second records each bank's parts.
    ///
    /// # Arguments
    ///
    /// * `replay` - The replay's messages.
    ///
    /// # Returns
    ///
    /// The table, empty if no replayed message names a bank.
    fn new(replay: &[ReplayResponseMessageType]) -> Self {
        let replayed = || replay.iter().flatten().filter_map(bank_part);
        let Some((min, max)) = replayed().fold(None, |span, (bank_id, _)| match span {
            None => Some((bank_id, bank_id)),
            Some((min, max)) => Some((BankId::min(min, bank_id), BankId::max(max, bank_id))),
        }) else {
            return Self::default();
        };
        let len = (max - min + 1).next_power_of_two();
        let mut sent = Self {
            entries: vec![SentBank::default(); len as usize],
            mask: len - 1,
        };
        for (bank_id, part) in replayed() {
            let index = sent.index(bank_id);
            let entry = &mut sent.entries[index];
            debug_assert!(entry.sent.is_empty() || entry.bank_id == bank_id);
            entry.bank_id = bank_id;
            entry.sent.insert(part);
        }
        sent
    }

    /// Whether the replay already sent `message`.
    ///
    /// # Arguments
    ///
    /// * `message` - A message queued on the live receiver.
    ///
    /// # Returns
    ///
    /// `true` if the replay sent it: its bank is in the table with its part.
    fn contains(&self, message: &Message) -> bool {
        bank_part(message).is_some_and(|(bank_id, part)| {
            self.entries
                .get(self.index(bank_id))
                .is_some_and(|entry| entry.bank_id == bank_id && entry.sent.contains(part))
        })
    }

    /// Where bank `bank_id` is in [`ReplaySent::entries`].
    ///
    /// # Arguments
    ///
    /// * `bank_id` - The bank.
    ///
    /// # Returns
    ///
    /// `bank_id & mask`.
    const fn index(&self, bank_id: BankId) -> usize {
        (bank_id & self.mask) as usize
    }
}

/// The bank `message` belongs to, and which of its parts it is.
///
/// # Arguments
///
/// * `message` - A replayed or queued message.
///
/// # Returns
///
/// [`None`] for a message that names no bank, or a FirstShredReceived, Completed or Dead
/// status: those are never dropped.
fn bank_part(message: &Message) -> Option<(BankId, BankPart)> {
    let Message::Slot(slot) = message else {
        return bank_of(message).map(|bank_id| (bank_id, BankPart::Data));
    };
    let part = match slot.status {
        SlotStatus::CreatedBank => BankPart::Data,
        SlotStatus::Processed => BankPart::Processed,
        SlotStatus::Confirmed => BankPart::Confirmed,
        SlotStatus::Finalized => BankPart::Finalized,
        SlotStatus::FirstShredReceived | SlotStatus::Completed | SlotStatus::Dead => return None,
    };
    slot.bank_id.map(|bank_id| (bank_id, part))
}

/// The bank whose data `message` is, other than a slot status.
fn bank_of(message: &Message) -> Option<BankId> {
    match message {
        Message::Account(account) => account.bank_id,
        Message::Transaction(transaction) => Some(transaction.bank_id),
        Message::Entry(entry) => Some(entry.bank_id),
        Message::BlockFooter(footer) => Some(footer.bank_id),
        Message::BlockMeta(block_meta) => Some(block_meta.bank_id),
        Message::Block(block) => Some(block.meta.bank_id),
        Message::Slot(_)
        | Message::DeshredUpdateParent(_)
        | Message::EntryUpdateParent(_)
        | Message::DeshredTransaction(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::{filter::fixtures, message::MessageBlock},
        std::time::Duration,
    };

    // Diagrams: time runs left to right.
    //   channel: the batches, in the order they were broadcast.
    //   L: the live receiver subscribes.   R: the replay is built.
    //   replay / live: the batches each one holds.
    //   overlap: in both, dropped.   gap: in neither.
    //   client: what `next` hands out after the replay's own messages.

    /// Bank ids are slots here, as in the fixtures.
    fn entry(bank: u64) -> Message {
        Message::Entry(fixtures::message_entry(bank, 0))
    }

    fn status(bank: u64, status: SlotStatus) -> Message {
        Message::Slot(Arc::new(fixtures::message_slot(bank, status)))
    }

    fn block(bank: u64) -> Message {
        let mut meta = fixtures::message_block_meta(bank);
        Arc::get_mut(&mut meta).unwrap().block_meta.bank_id = bank;
        Message::Block(Arc::new(MessageBlock::new(meta, vec![], vec![], vec![])))
    }

    /// A broadcast channel, and a receiver subscribed before anything is sent: L.
    fn channel() -> (
        broadcast::Sender<BroadcastedMessage>,
        broadcast::Receiver<BroadcastedMessage>,
    ) {
        broadcast::channel(16)
    }

    fn send(
        tx: &broadcast::Sender<BroadcastedMessage>,
        messages: Vec<Message>,
    ) -> BroadcastedMessage {
        let batch = Arc::new(messages);
        tx.send(Arc::clone(&batch)).unwrap();
        batch
    }

    fn no_cut() -> ReplayCut {
        ReplayCut {
            last_taken_in: None,
            last_sent: None,
            next_taken_in: None,
        }
    }

    fn start(
        rx: broadcast::Receiver<BroadcastedMessage>,
        commitment: CommitmentLevel,
        replayed: &[Message],
        cut: ReplayCut,
    ) -> Result<ReplayPhase, RecvError> {
        let messages: Vec<_> = replayed
            .iter()
            .cloned()
            .map(ReplayResponseMessageType::Single)
            .collect();
        let bank_id_span = BankSpan::of(&messages);
        ReplayPhase::new(
            rx,
            commitment,
            Replay {
                messages,
                bank_id_span,
                cut,
            },
        )
    }

    /// Short names for messages.
    fn names<'a>(messages: impl IntoIterator<Item = &'a Message>) -> Vec<String> {
        messages
            .into_iter()
            .map(|message| match message {
                Message::Entry(entry) => format!("entry {}", entry.bank_id),
                Message::Block(block) => format!("block {}", block.meta.bank_id),
                Message::Slot(slot) => format!("{:?} {}", slot.status, slot.slot),
                _ => "other".to_owned(),
            })
            .collect()
    }

    /// Everything [`ReplayPhase::next`] hands out, until [`None`].
    async fn catch_up(phase: &mut ReplayPhase) -> Vec<String> {
        let mut handed_out = Vec::new();
        while let Some(message) = tokio::time::timeout(Duration::from_secs(5), phase.next())
            .await
            .expect("no wait")
            .unwrap()
        {
            handed_out.extend(names([&message]));
        }
        handed_out
    }

    /// The replay's messages, then `client`.
    fn after_replay(replayed: &[Message], client: &[&str]) -> Vec<String> {
        let mut expected = names(replayed);
        expected.extend(client.iter().map(|name| name.to_string()));
        expected
    }

    /// Bank 1 sealed and went out before R; bank 2 was open at R. Up to their cuts, what the
    /// replay sent is dropped; after, everything passes. Then the receiver is live.
    ///
    /// ```text
    /// time ───────────────────────────────────────────►
    /// channel:   b1   b2   b3   b4   b5   b6   b7
    ///          L                   R
    /// replay:    b1   b2   b3   b4*
    /// live:      b1   b2   b3   b4   b5   b6   b7
    ///            └──── overlap ───┘
    /// client:                   b4'  b5   b6   b7
    ///
    /// b1 = entry 1   b2 = block 1   b3 = Processed 1 (last_sent)
    /// b4 = entry 2 + FirstShred 2 (last_taken_in)   * the replay has entry 2 only
    /// b5 = entry 2   b6 = block 2   b7 = Processed 2, sent after the replay phase
    /// b4' = FirstShred 2: the part of b4 the replay did not send
    /// ```
    #[tokio::test]
    async fn processed_drops_what_the_replay_sent_until_the_cuts() {
        let (tx, rx) = channel();
        send(&tx, vec![entry(1)]);
        send(&tx, vec![block(1)]);
        let processed_1 = send(&tx, vec![status(1, SlotStatus::Processed)]);
        let bank_2_head = send(
            &tx,
            vec![entry(2), status(2, SlotStatus::FirstShredReceived)],
        );
        // R: block reconstruction took in up to `bank_2_head`, and sent up to `processed_1`.
        send(&tx, vec![entry(2)]);
        send(&tx, vec![block(2)]);

        let replayed = [
            entry(1),
            block(1),
            status(1, SlotStatus::Processed),
            entry(2),
        ];
        let cut = ReplayCut {
            last_taken_in: Some(bank_2_head),
            last_sent: Some(processed_1),
            next_taken_in: None,
        };
        let mut phase = start(rx, CommitmentLevel::Processed, &replayed, cut).unwrap();

        // Bank 1's tail, block and status, and bank 2's head are in the replay. The
        // FirstShredReceived names no bank, so it passes. Bank 2's tail and block went out
        // after R.
        assert_eq!(
            catch_up(&mut phase).await,
            after_replay(&replayed, &["FirstShredReceived 2", "entry 2", "block 2"])
        );

        let mut rx = phase.into_receiver();
        send(&tx, vec![status(2, SlotStatus::Processed)]);
        assert_eq!(names(rx.recv().await.unwrap().iter()), ["Processed 2"]);
    }

    /// A cut batch that went out before the receiver existed leaves nothing queued to drop.
    ///
    /// ```text
    /// time ─────────────────────►
    /// channel:   b1   b2   b3
    ///               R L
    /// replay:    b1
    /// live:           b2   b3
    /// client:         b2   b3        R before L: no overlap, nothing dropped
    ///
    /// b1 = entry 1 (last_taken_in, last_sent)   b2 = entry 2   b3 = block 2
    /// ```
    #[tokio::test]
    async fn cuts_sent_before_the_receiver_drop_nothing() {
        // `send` needs a receiver.
        let (tx, _keep) = channel();
        let before_rx = send(&tx, vec![entry(1)]);
        let rx = tx.subscribe();
        send(&tx, vec![entry(2)]);
        send(&tx, vec![block(2)]);

        let replayed = [entry(1), block(1), entry(2), block(2)];
        let cut = ReplayCut {
            last_taken_in: Some(Arc::clone(&before_rx)),
            last_sent: Some(before_rx),
            next_taken_in: None,
        };
        let mut phase = start(rx, CommitmentLevel::Processed, &replayed, cut).unwrap();

        assert_eq!(
            catch_up(&mut phase).await,
            after_replay(&replayed, &["entry 2", "block 2"])
        );
    }

    /// At confirmed every batch is block reconstruction's, bank data included, so only
    /// [`ReplayCut::last_sent`] counts.
    ///
    /// ```text
    /// time ─────────────────────────►
    /// channel:   b1   b2   b3   b4
    ///          L              R
    /// replay:    b1   b2   b3
    /// live:      b1   b2   b3   b4
    ///            └─ overlap ──┘
    /// client:                   b4
    ///
    /// b1 = entry 1   b2 = block 1   b3 = Confirmed 1 (last_sent)   b4 = entry 2
    /// ```
    #[tokio::test]
    async fn confirmed_drops_bank_data_up_to_the_last_sent_batch() {
        let (tx, rx) = channel();
        send(&tx, vec![entry(1)]);
        send(&tx, vec![block(1)]);
        let confirmed_1 = send(&tx, vec![status(1, SlotStatus::Confirmed)]);
        send(&tx, vec![entry(2)]);

        let replayed = [entry(1), block(1), status(1, SlotStatus::Confirmed)];
        let cut = ReplayCut {
            last_sent: Some(confirmed_1),
            ..no_cut()
        };
        let mut phase = start(rx, CommitmentLevel::Confirmed, &replayed, cut).unwrap();

        assert_eq!(
            catch_up(&mut phase).await,
            after_replay(&replayed, &["entry 2"])
        );
    }

    /// A status the replay did not send passes, even for a replayed bank.
    ///
    /// ```text
    /// time ─────────────────►
    /// channel:   b1   b2
    ///          L         R
    /// replay:    b1
    /// live:      b1   b2
    ///            └┘ overlap
    /// client:         b2        b2 is not in the replay
    ///
    /// b1 = Processed 1   b2 = Confirmed 1 (last_sent)
    /// ```
    #[tokio::test]
    async fn later_status_of_a_replayed_bank_passes() {
        let (tx, rx) = channel();
        send(&tx, vec![status(1, SlotStatus::Processed)]);
        let confirmed_1 = send(&tx, vec![status(1, SlotStatus::Confirmed)]);

        let replayed = [entry(1), block(1), status(1, SlotStatus::Processed)];
        let cut = ReplayCut {
            last_sent: Some(confirmed_1),
            ..no_cut()
        };
        let mut phase = start(rx, CommitmentLevel::Processed, &replayed, cut).unwrap();

        assert_eq!(
            catch_up(&mut phase).await,
            after_replay(&replayed, &["Confirmed 1"])
        );
    }

    /// The next batch taken in went out before the receiver existed: it was missed, and goes
    /// out right after the replay.
    ///
    /// ```text
    /// time ─────────────────────►
    /// channel:   b1   b2   b3
    ///                    L
    /// replay:    b1
    /// live:                b3
    ///                 b2 ◄ gap: in neither, sent as next_taken_in
    /// client:         b2   b3
    ///
    /// b1 = entry 1 (last_taken_in)   b2 = entry 2   b3 = entry 3
    /// ```
    #[tokio::test]
    async fn missed_batch_goes_out_right_after_the_replay() {
        let (tx, _keep) = channel();
        let taken_in = send(&tx, vec![entry(1)]);
        let missed = send(&tx, vec![entry(2)]);
        let rx = tx.subscribe();
        send(&tx, vec![entry(3)]);

        let (next_tx, next_rx) = oneshot::channel();
        let replayed = [entry(1)];
        let cut = ReplayCut {
            last_taken_in: Some(taken_in),
            next_taken_in: Some(next_rx),
            ..no_cut()
        };
        let mut phase = start(rx, CommitmentLevel::Processed, &replayed, cut).unwrap();
        next_tx.send(missed).unwrap();

        assert_eq!(
            catch_up(&mut phase).await,
            after_replay(&replayed, &["entry 2", "entry 3"])
        );
    }

    /// The next batch taken in is live, queued or not yet: it goes out once.
    ///
    /// ```text
    /// time ─────────────────►
    /// channel:   b1   b2
    ///          L
    /// replay:    (none)
    /// next:      b1             next_taken_in
    /// live:      b1   b2
    ///            └┘ overlap: the same batch twice
    /// client:    b1   b2
    ///
    /// b1 = entry 2   b2 = entry 3
    /// Run twice: b1 broadcast before the replay arrives, and after.
    /// ```
    #[tokio::test]
    async fn live_next_batch_goes_out_once() {
        for queued_before_the_replay in [true, false] {
            let (tx, rx) = channel();
            let next = queued_before_the_replay.then(|| send(&tx, vec![entry(2)]));

            let (next_tx, next_rx) = oneshot::channel();
            let cut = ReplayCut {
                next_taken_in: Some(next_rx),
                ..no_cut()
            };
            let mut phase = start(rx, CommitmentLevel::Processed, &[], cut).unwrap();
            let next = next.unwrap_or_else(|| send(&tx, vec![entry(2)]));
            next_tx.send(next).unwrap();
            send(&tx, vec![entry(3)]);

            assert_eq!(catch_up(&mut phase).await, ["entry 2", "entry 3"]);
        }
    }

    /// Nothing queued goes out before the next batch taken in is known; the replay does.
    ///
    /// ```text
    /// time ─────────────────────────────────────────────►
    /// channel:   b1
    ///          L
    /// replay:    b0
    /// next:      ······ pending ······ sender dropped
    /// client:    b0, then nothing      Closed
    ///
    /// b0 = entry 1   b1 = entry 2
    /// ```
    #[tokio::test]
    async fn waits_for_the_next_batch_taken_in() {
        let (tx, rx) = channel();
        send(&tx, vec![entry(2)]);

        let (next_tx, next_rx) = oneshot::channel();
        let cut = ReplayCut {
            next_taken_in: Some(next_rx),
            ..no_cut()
        };
        let mut phase = start(rx, CommitmentLevel::Processed, &[entry(1)], cut).unwrap();
        let replayed = phase.next().await.unwrap().unwrap();
        assert_eq!(names([&replayed]), ["entry 1"]);

        let waiting = tokio::time::timeout(Duration::from_millis(50), phase.next()).await;
        assert!(waiting.is_err());

        drop(next_tx);
        assert!(matches!(phase.next().await, Err(RecvError::Closed)));
    }

    /// A last batch taken in that the receiver got means the next one is live: no wait.
    ///
    /// ```text
    /// time ─────────────────►
    /// channel:   b1   b2
    ///          L    R
    /// replay:    b1
    /// live:      b1   b2
    ///            └┘ overlap
    /// client:         b2        no wait: b1 is live, so b2 is too
    ///
    /// b1 = entry 1 (last_taken_in)   b2 = entry 2
    /// ```
    #[tokio::test]
    async fn no_wait_when_the_last_batch_taken_in_is_live() {
        let (tx, rx) = channel();
        let taken_in = send(&tx, vec![entry(1)]);
        send(&tx, vec![entry(2)]);

        let (_next_tx, next_rx) = oneshot::channel();
        let replayed = [entry(1)];
        let cut = ReplayCut {
            last_taken_in: Some(taken_in),
            next_taken_in: Some(next_rx),
            ..no_cut()
        };
        let mut phase = start(rx, CommitmentLevel::Processed, &replayed, cut).unwrap();

        // `catch_up` fails on a wait.
        assert_eq!(
            catch_up(&mut phase).await,
            after_replay(&replayed, &["entry 2"])
        );
    }

    /// A receiver that lost batches cannot join: the replay and the live stream would not
    /// overlap.
    ///
    /// ```text
    /// time ─────────────────►
    /// channel:   b1   b2        capacity 1
    ///          L
    /// live:      ✗    b2        b1 overwritten
    ///            ▲ gap
    /// join:      Err(Lagged(1))
    ///
    /// b1 = entry 1   b2 = entry 2
    /// ```
    #[tokio::test]
    async fn lagged_receiver_fails_the_join() {
        let (tx, rx) = broadcast::channel(1);
        send(&tx, vec![entry(1)]);
        send(&tx, vec![entry(2)]);

        assert!(matches!(
            start(rx, CommitmentLevel::Processed, &[], no_cut()),
            Err(RecvError::Lagged(1))
        ));
    }

    /// The live receiver comes back only once everything was handed out.
    #[tokio::test]
    #[should_panic(expected = "the live stream starts only after ReplayPhase::next returned None")]
    async fn receiver_before_the_replay_is_done_panics() {
        let (_tx, rx) = channel();
        let phase = start(rx, CommitmentLevel::Processed, &[entry(1)], no_cut()).unwrap();
        let _ = phase.into_receiver();
    }

    /// A replay split into batch ranges is handed out in order, once.
    #[tokio::test]
    async fn replay_ranges_go_out_in_order() {
        let (_tx, rx) = channel();
        let bank = Arc::new(vec![entry(1), entry(2), entry(3)]);
        let messages = vec![
            ReplayResponseMessageType::Batch(Arc::clone(&bank), 0..1),
            ReplayResponseMessageType::Single(status(1, SlotStatus::CreatedBank)),
            ReplayResponseMessageType::Batch(bank, 1..3),
        ];
        let replay = Replay {
            bank_id_span: BankSpan::of(&messages),
            messages,
            cut: no_cut(),
        };
        let mut phase = ReplayPhase::new(rx, CommitmentLevel::Processed, replay).unwrap();

        assert_eq!(
            catch_up(&mut phase).await,
            ["entry 1", "CreatedBank 1", "entry 2", "entry 3"]
        );
        let _ = phase.into_receiver();
    }

    /// Each part has its own bit: inserting one adds only it, and again changes nothing.
    #[test]
    fn bank_parts_hold_each_part_apart() {
        let all = [
            BankPart::Data,
            BankPart::Processed,
            BankPart::Confirmed,
            BankPart::Finalized,
        ];
        let mut parts = BankParts::default();
        assert!(parts.is_empty());
        assert!(all.iter().all(|&part| !parts.contains(part)));

        parts.insert(BankPart::Data);
        parts.insert(BankPart::Confirmed);
        parts.insert(BankPart::Confirmed);
        assert!(!parts.is_empty());
        assert_eq!(
            all.map(|part| parts.contains(part)),
            [true, false, true, false]
        );

        for part in all {
            parts.insert(part);
        }
        assert!(all.iter().all(|&part| parts.contains(part)));
    }

    fn sent(replayed: &[Message]) -> ReplaySent {
        let replay: Vec<_> = replayed
            .iter()
            .cloned()
            .map(ReplayResponseMessageType::Single)
            .collect();
        ReplaySent::new(&replay)
    }

    /// Each replayed bank has its own entry with what was sent of it, and a bank outside the
    /// replay never matches, even on a used entry.
    ///
    /// ```text
    /// replay banks 100 101 102 104 105: span 6, so 8 entries, mask 7
    ///
    /// entry:   0      1      2   3   4        5      6      7
    /// bank:    104    105    -   -   100      101    102    -
    /// parts:   D      D              D P C    D P    D
    /// ```
    #[test]
    fn sent_table_holds_each_bank_and_what_was_sent() {
        let sent = sent(&[
            entry(100),
            status(100, SlotStatus::CreatedBank),
            block(100),
            status(100, SlotStatus::Processed),
            status(100, SlotStatus::Confirmed),
            entry(101),
            status(101, SlotStatus::Processed),
            entry(102),
            entry(104),
            entry(105),
        ]);
        assert_eq!(sent.entries.len(), 8);
        let banks: Vec<_> = sent
            .entries
            .iter()
            .map(|entry| (!entry.sent.is_empty()).then_some(entry.bank_id))
            .collect();
        assert_eq!(
            banks,
            [
                Some(104),
                Some(105),
                None,
                None,
                Some(100),
                Some(101),
                Some(102),
                None
            ]
        );

        // Sent: data and the statuses the replay had.
        assert!(sent.contains(&entry(100)));
        assert!(sent.contains(&block(100)));
        assert!(sent.contains(&status(100, SlotStatus::CreatedBank)));
        assert!(sent.contains(&status(100, SlotStatus::Confirmed)));
        assert!(sent.contains(&status(101, SlotStatus::Processed)));
        assert!(sent.contains(&status(102, SlotStatus::CreatedBank)));
        // Not sent: a status reached after the replay.
        assert!(!sent.contains(&status(100, SlotStatus::Finalized)));
        assert!(!sent.contains(&status(101, SlotStatus::Confirmed)));
        // Not sent: banks outside the replay. 108 and 96 land on bank 100's entry, 103 on an
        // empty one.
        assert!(!sent.contains(&entry(108)));
        assert!(!sent.contains(&entry(96)));
        assert!(!sent.contains(&status(108, SlotStatus::Processed)));
        assert!(!sent.contains(&entry(103)));
        // Never dropped: statuses that name no bank.
        assert!(!sent.contains(&status(100, SlotStatus::FirstShredReceived)));
        assert!(!sent.contains(&status(100, SlotStatus::Completed)));
        assert!(!sent.contains(&status(100, SlotStatus::Dead)));
    }

    /// A span that is already a power of two fills the table exactly; one more bank doubles it.
    #[test]
    fn sent_table_covers_the_span_of_bank_ids() {
        let full = sent(&(8..16).map(entry).collect::<Vec<_>>());
        assert_eq!(full.entries.len(), 8);
        assert!((8..16).all(|bank| full.contains(&entry(bank))));
        assert!(!full.contains(&entry(16)));

        let wider = sent(&(8..17).map(entry).collect::<Vec<_>>());
        assert_eq!(wider.entries.len(), 16);
        assert!((8..17).all(|bank| wider.contains(&entry(bank))));
    }

    /// Singles and batch ranges come out in order, each message once, and a batch's messages
    /// outside its range never come out.
    #[test]
    fn replay_messages_go_out_in_order() {
        let bank = Arc::new(vec![entry(1), entry(2), entry(3), entry(4)]);
        let replay = ReplayMessages::new(vec![
            ReplayResponseMessageType::Batch(Arc::clone(&bank), 0..1),
            ReplayResponseMessageType::Single(status(1, SlotStatus::CreatedBank)),
            ReplayResponseMessageType::Batch(Arc::clone(&bank), 1..3),
            ReplayResponseMessageType::Single(block(1)),
        ]);
        assert_eq!(
            names(&replay.collect::<Vec<_>>()),
            ["entry 1", "CreatedBank 1", "entry 2", "entry 3", "block 1"]
        );
    }

    /// An empty range is skipped, and indexes past the batch's end are ignored.
    #[test]
    fn replay_messages_skip_empty_and_out_of_bounds_ranges() {
        let bank = Arc::new(vec![entry(1), entry(2)]);
        let replay = ReplayMessages::new(vec![
            ReplayResponseMessageType::Batch(Arc::clone(&bank), 0..0),
            ReplayResponseMessageType::Single(block(1)),
            ReplayResponseMessageType::Batch(Arc::clone(&bank), 1..5),
            ReplayResponseMessageType::Batch(bank, 2..2),
        ]);
        assert_eq!(names(&replay.collect::<Vec<_>>()), ["block 1", "entry 2"]);
    }

    /// An empty replay has nothing to hand out.
    #[test]
    fn replay_messages_of_an_empty_replay_is_empty() {
        assert!(ReplayMessages::new(vec![]).next().is_none());
    }

    /// A span starts at one bank and widens both ways, whatever order the banks come in.
    #[test]
    fn bank_span_covers_every_bank_included() {
        let mut banks: Option<BankSpan> = None;
        for bank_id in [102, 100, 105, 101] {
            banks.get_or_insert(BankSpan::new(bank_id)).include(bank_id);
        }
        assert_eq!(banks, Some(BankSpan { min: 100, max: 105 }));
    }

    /// The span of a replay's messages covers every bank they name, statuses included, and is
    /// [`None`] when they name none.
    #[test]
    fn bank_span_of_messages() {
        let messages = |replayed: Vec<Message>| -> Vec<_> {
            replayed
                .into_iter()
                .map(ReplayResponseMessageType::Single)
                .collect()
        };
        assert_eq!(
            BankSpan::of(&messages(vec![
                entry(7),
                status(3, SlotStatus::Confirmed),
                block(9),
                status(1, SlotStatus::FirstShredReceived),
            ])),
            Some(BankSpan { min: 3, max: 9 })
        );
        assert_eq!(
            BankSpan::of(&messages(vec![status(4, SlotStatus::Completed)])),
            None
        );
    }

    /// A replay with no bank in it holds nothing, and drops nothing.
    #[test]
    fn sent_table_without_banks_is_empty() {
        let sent = sent(&[status(5, SlotStatus::FirstShredReceived)]);
        assert!(sent.entries.is_empty());
        assert!(!sent.contains(&entry(5)));
        assert!(!sent.contains(&entry(0)));
    }
}
