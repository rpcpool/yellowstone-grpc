//! Bug: a `from_slot` replay at `processed` loses transactions broadcast before the client
//! subscribed.
//!
//! At `processed` the geyser loop sends a bank's transactions out live, one by one, as the
//! node executes them; when block reconstruction seals the bank only its `Block` and
//! `BlockMeta` follow. A `from_slot` replay is a snapshot of what block reconstruction has
//! sealed so far. A transaction broadcast before the client subscribed reaches it only if
//! that snapshot holds it, and it does not when:
//! - the bank is still in progress: the snapshot holds sealed banks only;
//! - block reconstruction lags the geyser loop: it has not taken the bank in yet.
//!
//! The server holds those transactions either way, and nothing tells the client a bank is
//! incomplete. The tests assert what a replay must deliver, so they fail while the bug stands.

use {
    super::{super::*, settle},
    crate::{
        block_reconstruction_v2::MUST_HAVE_SYSVAR_ACCOUNTS,
        plugin::{
            filter::Filter,
            message::{
                MessageAccount, MessageAccountInfo, MessageEntry, MessageTransaction,
                MessageTransactionInfo,
            },
        },
    },
    bytes::Bytes,
    foldhash::{HashSet as FoldHashSet, HashSetExt as _},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::sync::OnceLock,
    tokio::sync::watch,
    yellowstone_grpc_proto::prelude::{
        SubscribeRequest, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions,
        SubscribeUpdateBlockMeta,
    },
};

/// What the client saw, in order.
#[derive(Debug, PartialEq)]
enum Seen {
    Tx { slot: u64, name: u8 },
    BlockMeta { slot: u64 },
}

/// A subscribed client: its request channel, kept open for the session, and its updates.
struct Client {
    _requests: mpsc::UnboundedSender<Option<(Option<u64>, Filter)>>,
    updates: LoadAwareReceiver<TonicResult<FilteredUpdate>>,
}

/// The plugin side of the server: the geyser loop and block reconstruction, with replay.
/// Block reconstruction can be paused to make it lag the geyser loop.
struct Node {
    messages_tx: mpsc::UnboundedSender<Message>,
    reconstruction_paused: watch::Sender<bool>,
    broadcast: SubscriberChannels,
    replay_tx: mpsc::Sender<ReplayStoredSlotsRequest>,
    first_available: Arc<AtomicU64>,
}

impl Node {
    fn spawn() -> Self {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        let (reconstruction_tx, mut lagging_rx) = mpsc::unbounded_channel();
        let (lagging_tx, reconstruction_rx) = mpsc::unbounded_channel();
        let (replay_tx, replay_rx) = mpsc::channel(1);
        let broadcast = SubscriberChannels::new(1024, 1024, 1024);
        let first_available = Arc::new(AtomicU64::new(0));
        tokio::spawn(GrpcService::geyser_loop(
            BatchStreamUnboundedReceiver::new(messages_rx),
            broadcast.clone(),
            reconstruction_tx,
        ));
        // Passes the geyser loop's output on to block reconstruction, holding it while paused.
        let (reconstruction_paused, mut paused) = watch::channel(false);
        tokio::spawn(async move {
            while let Some(message) = lagging_rx.recv().await {
                if paused.wait_for(|paused| !paused).await.is_err()
                    || lagging_tx.send(message).is_err()
                {
                    return;
                }
            }
        });
        tokio::spawn(GrpcService::block_reconstruction_loop(
            BatchStreamUnboundedReceiver::new(reconstruction_rx),
            broadcast.clone(),
            Some(replay_rx),
            Some(Arc::clone(&first_available)),
            100,
        ));
        Self {
            messages_tx,
            reconstruction_paused,
            broadcast,
            replay_tx,
            first_available,
        }
    }

    fn send(&self, message: Message) {
        self.messages_tx.send(message).unwrap();
    }

    /// The node creates the bank of `slot` (its id is the slot number).
    fn open_bank(&self, slot: u64) {
        self.send(slot_status(slot, SlotStatus::CreatedBank));
    }

    /// The node executes a transaction, named by one byte, in the bank of `slot`.
    fn execute(&self, slot: u64, name: u8) {
        self.send(transaction(slot, name));
    }

    /// The node finishes the bank of `slot`: what block reconstruction needs to seal it.
    fn finish_bank(&self, slot: u64) {
        for pubkey in MUST_HAVE_SYSVAR_ACCOUNTS {
            self.send(sysvar_write(slot, pubkey));
        }
        self.send(entry(slot));
        self.send(block_meta(slot));
        self.send(slot_status(slot, SlotStatus::Processed));
    }

    fn pause_reconstruction(&self) {
        self.reconstruction_paused.send_replace(true);
    }

    fn resume_reconstruction(&self) {
        self.reconstruction_paused.send_replace(false);
    }

    /// Waits until `slot` is sealed and replayable.
    async fn wait_replayable(&self, slot: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.first_available.load(Ordering::Relaxed) != slot {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the slot never became replayable");
    }

    /// A client subscribes at `processed` for transactions and block metas, replaying
    /// from `from_slot`.
    fn subscribe(&self, from_slot: u64) -> Client {
        let request = SubscribeRequest {
            transactions: HashMap::from([(
                "txs".into(),
                SubscribeRequestFilterTransactions::default(),
            )]),
            blocks_meta: HashMap::from([(
                "metas".into(),
                SubscribeRequestFilterBlocksMeta::default(),
            )]),
            commitment: Some(CommitmentLevelProto::Processed as i32),
            ..Default::default()
        };
        let mut names = FilterNames::new(64, 1024, Duration::from_secs(1));
        let filter = Filter::new(&request, &FilterLimits::default(), &mut names).unwrap();

        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let (stream_tx, stream_rx) = load_aware_channel(1024);
        let session = ClientSession::new(
            0,
            Some("test".into()),
            "test".into(),
            CancellationToken::new(),
            None,
        );
        tokio::spawn(GrpcService::client_loop(
            session,
            stream_tx,
            client_rx,
            None,
            self.broadcast.clone(),
            Some(self.replay_tx.clone()),
            TaskTracker::new(),
        ));
        client_tx.send(Some((Some(from_slot), filter))).unwrap();
        Client {
            _requests: client_tx,
            updates: stream_rx,
        }
    }
}

/// Everything the client receives up to and including the block meta of `last_slot`.
async fn receive_through(client: &mut Client, last_slot: u64) -> Vec<Seen> {
    let mut seen = Vec::new();
    loop {
        let update = tokio::time::timeout(Duration::from_secs(2), client.updates.recv())
            .await
            .expect("timed out waiting for an update")
            .expect("stream closed")
            .expect("status error");
        match update.message {
            FilteredUpdateOneof::Transaction(tx) => seen.push(Seen::Tx {
                slot: tx.slot,
                name: tx.transaction.transaction.signature.as_ref()[0],
            }),
            FilteredUpdateOneof::BlockMeta(meta) => {
                seen.push(Seen::BlockMeta { slot: meta.slot });
                if meta.slot == last_slot {
                    return seen;
                }
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn replay_delivers_transactions_executed_before_subscribe_in_the_bank_in_progress() {
    let node = Node::spawn();

    // Slot 100 is sealed before the client subscribes.
    node.open_bank(100);
    node.execute(100, 1);
    node.finish_bank(100);
    node.wait_replayable(100).await;

    // Slot 101 is in progress: transaction 2 has executed, the bank is not sealed.
    node.open_bank(101);
    node.execute(101, 2);
    settle().await;

    // The client subscribes, replaying from slot 100.
    let mut client = node.subscribe(100);
    settle().await;

    // Slot 101 goes on: transaction 3 executes, then the bank seals.
    node.execute(101, 3);
    node.finish_bank(101);

    assert_eq!(
        receive_through(&mut client, 101).await,
        [
            Seen::Tx { slot: 100, name: 1 },
            Seen::BlockMeta { slot: 100 },
            // Executed before the client subscribed; today it never arrives.
            Seen::Tx { slot: 101, name: 2 },
            Seen::Tx { slot: 101, name: 3 },
            Seen::BlockMeta { slot: 101 },
        ]
    );
}

#[tokio::test]
async fn replay_delivers_transactions_block_reconstruction_has_not_taken_in_yet() {
    let node = Node::spawn();

    // Slot 99 is sealed and replayable.
    node.open_bank(99);
    node.execute(99, 1);
    node.finish_bank(99);
    node.wait_replayable(99).await;

    // Block reconstruction falls behind: slot 100 executes and finishes, but only the geyser
    // loop has seen it, and broadcast transaction 2 to nobody.
    node.pause_reconstruction();
    node.open_bank(100);
    node.execute(100, 2);
    node.finish_bank(100);
    settle().await;

    // The client subscribes, replaying from slot 99; then block reconstruction catches up.
    let mut client = node.subscribe(99);
    settle().await;
    node.resume_reconstruction();

    assert_eq!(
        receive_through(&mut client, 100).await,
        [
            Seen::Tx { slot: 99, name: 1 },
            Seen::BlockMeta { slot: 99 },
            // Broadcast before the client subscribed; today it never arrives.
            Seen::Tx { slot: 100, name: 2 },
            Seen::BlockMeta { slot: 100 },
        ]
    );
}

fn ts() -> Timestamp {
    Timestamp::from(SystemTime::now())
}

fn slot_status(slot: u64, status: SlotStatus) -> Message {
    Message::Slot(Arc::new(MessageSlot {
        slot,
        parent: Some(slot - 1),
        status,
        dead_error: None,
        created_at: ts(),
        bank_id: Some(slot),
    }))
}

fn transaction(slot: u64, name: u8) -> Message {
    Message::Transaction(Arc::new(MessageTransaction {
        transaction: MessageTransactionInfo {
            signature: Signature::from([name; 64]),
            is_vote: false,
            transaction: Default::default(),
            meta: Default::default(),
            index: usize::from(name),
            account_keys: FoldHashSet::new(),
            pre_encoded: OnceLock::new(),
            token_owners_all: OnceLock::new(),
            token_owners_changed: OnceLock::new(),
        },
        slot,
        created_at: ts(),
        bank_id: slot,
    }))
}

fn sysvar_write(slot: u64, pubkey: Pubkey) -> Message {
    Message::Account(Arc::new(MessageAccount {
        account: MessageAccountInfo {
            pubkey,
            lamports: 1,
            owner: Pubkey::default(),
            executable: false,
            rent_epoch: 0,
            data: Bytes::new(),
            write_version: 1,
            txn_signature: None,
            pre_encoded: OnceLock::new(),
        },
        slot,
        is_startup: false,
        bank_id: Some(slot),
        created_at: ts(),
    }))
}

fn entry(slot: u64) -> Message {
    Message::Entry(Arc::new(MessageEntry {
        slot,
        index: 0,
        num_hashes: 1,
        hash: Hash::default(),
        executed_transaction_count: 0,
        starting_transaction_index: 0,
        bank_id: slot,
        created_at: ts(),
    }))
}

fn block_meta(slot: u64) -> Message {
    Message::BlockMeta(Arc::new(MessageBlockMeta::from_update_oneof(
        SubscribeUpdateBlockMeta {
            slot,
            parent_slot: slot - 1,
            blockhash: Hash::new_unique().to_string(),
            bank_id: slot,
            ..Default::default()
        },
        ts(),
    )))
}
