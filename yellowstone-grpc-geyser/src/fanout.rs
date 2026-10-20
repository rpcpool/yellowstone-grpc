//! Processed fan-out through K shard tasks, enabled with `YELLOWSTONE_FANOUT_SHARDS=K`.
//! A shard runs each member's filter over a batch and wakes the member only when something matches.

use {
    crate::{
        grpc::BroadcastedMessage,
        plugin::{filter::Filter, message::CommitmentLevel},
    },
    arc_swap::ArcSwap,
    log::info,
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::sync::{broadcast, mpsc},
};

enum Control {
    Register { id: usize, member: Member },
    Unregister { id: usize },
}

struct Member {
    filter: Arc<ArcSwap<Filter>>,
    forward_all: Arc<AtomicBool>,
    tx: mpsc::UnboundedSender<Item>,
}

enum Item {
    Batch(BroadcastedMessage),
    Lagged,
}

#[derive(Debug)]
pub struct FanoutShards {
    controls: Vec<mpsc::UnboundedSender<Control>>,
    next_id: AtomicUsize,
}

impl FanoutShards {
    /// Starts the shards on the current runtime when YELLOWSTONE_FANOUT_SHARDS is a positive count.
    pub fn from_env(sender: &broadcast::Sender<BroadcastedMessage>) -> Option<Arc<Self>> {
        let count = std::env::var("YELLOWSTONE_FANOUT_SHARDS")
            .ok()?
            .parse::<usize>()
            .ok()
            .filter(|count| *count > 0)?;
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        let controls = (0..count)
            .map(|_| {
                let (tx, rx) = mpsc::unbounded_channel();
                runtime.spawn(run_shard(sender.subscribe(), rx));
                tx
            })
            .collect();
        info!("processed fan-out through {count} shards");
        Some(Arc::new(Self {
            controls,
            next_id: AtomicUsize::new(0),
        }))
    }

    pub fn register(&self, filter: Filter) -> ShardReceiver {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let control = self.controls[id % self.controls.len()].clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let filter = Arc::new(ArcSwap::from_pointee(filter));
        let forward_all = Arc::new(AtomicBool::new(false));
        let _ = control.send(Control::Register {
            id,
            member: Member {
                filter: Arc::clone(&filter),
                forward_all: Arc::clone(&forward_all),
                tx,
            },
        });
        ShardReceiver {
            id,
            rx,
            control,
            filter,
            forward_all,
        }
    }
}

pub struct ShardReceiver {
    id: usize,
    rx: mpsc::UnboundedReceiver<Item>,
    control: mpsc::UnboundedSender<Control>,
    filter: Arc<ArcSwap<Filter>>,
    forward_all: Arc<AtomicBool>,
}

impl Drop for ShardReceiver {
    fn drop(&mut self) {
        let _ = self.control.send(Control::Unregister { id: self.id });
    }
}

/// Live batches of one subscriber: straight from the broadcast, or pre-filtered by a shard.
pub enum LiveReceiver {
    Broadcast(broadcast::Receiver<BroadcastedMessage>),
    Sharded(ShardReceiver),
}

impl LiveReceiver {
    pub async fn recv(&mut self) -> Result<BroadcastedMessage, broadcast::error::RecvError> {
        match self {
            Self::Broadcast(rx) => rx.recv().await,
            Self::Sharded(rx) => match rx.rx.recv().await {
                Some(Item::Batch(batch)) => Ok(batch),
                Some(Item::Lagged) => Err(broadcast::error::RecvError::Lagged(0)),
                None => Err(broadcast::error::RecvError::Closed),
            },
        }
    }

    pub fn set_filter(&self, filter: &Filter) {
        if let Self::Sharded(rx) = self {
            rx.filter.store(Arc::new(filter.clone()));
        }
    }

    /// While a member holds banks after from_slot, it needs every batch, matched or not.
    pub fn set_forward_all(&self, forward_all: bool) {
        if let Self::Sharded(rx) = self {
            rx.forward_all.store(forward_all, Ordering::Relaxed);
        }
    }
}

async fn run_shard(
    mut batches: broadcast::Receiver<BroadcastedMessage>,
    mut control: mpsc::UnboundedReceiver<Control>,
) {
    let mut members: HashMap<usize, Member> = HashMap::new();
    loop {
        tokio::select! {
            biased;
            command = control.recv() => match command {
                Some(Control::Register { id, member }) => {
                    members.insert(id, member);
                }
                Some(Control::Unregister { id }) => {
                    members.remove(&id);
                }
                None => break,
            },
            batch = batches.recv() => match batch {
                Ok(batch) => members.retain(|_, member| {
                    let wanted = member.forward_all.load(Ordering::Relaxed) || {
                        let filter = member.filter.load();
                        batch.iter().any(|message| {
                            !filter
                                .get_updates(message, Some(CommitmentLevel::Processed))
                                .is_empty()
                        })
                    };
                    !wanted || member.tx.send(Item::Batch(Arc::clone(&batch))).is_ok()
                }),
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    for member in members.values() {
                        let _ = member.tx.send(Item::Lagged);
                    }
                    members.clear();
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
}
