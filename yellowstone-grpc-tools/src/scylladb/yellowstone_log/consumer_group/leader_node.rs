use {
    crate::scylladb::{
        etcd_utils::lease::ManagedLease, types::{BlockchainEventType, ConsumerGroupId, InstanceId, ProducerId}, yellowstone_log::{common::InitialOffset, consumer_group::etcd_path::get_producer_lock_path_v1}
    }, etcd_client::{WatchOptions, Watcher}, futures::Future, rdkafka::producer::Producer, serde::{Deserialize, Serialize}, std::{
        borrow::{Borrow, BorrowMut},
        collections::{HashMap, HashSet},
        sync::{Arc, RwLock},
    }, tokio::sync::{mpsc, oneshot::{self, error::{RecvError, TryRecvError}}, Mutex}, tracing::warn, yellowstone_grpc_proto::{
        geyser::CommitmentLevel,
        yellowstone::log::{AccountUpdateEventFilter, TransactionEventFilter},
    }
};

// enum ConsumerGroupLeaderLocation(
//     Local,
//     Remote()
// )

#[derive(Serialize, Deserialize)]
enum LeaderCommand {
    Join {
        lock_key: Vec<u8>,
    }
}



// enum ConsumerGroupLeaderState {
//     Idle,
//     HandlingJoinRequest {
//         lock_key: Vec<u8>,
//         instance_id: InstanceId,
//     },
//     HandlingTimelineTranslation {
//         from_producer_id: ProducerId,
//         target_producer_id: Produ
//     },
//     Poisoned,
// }


struct ProducerDeadSignal {
    // When this object is drop, the sender will drop too and cancel the watch automatically
    _cancel_watcher_tx: oneshot::Sender<()>,
    inner: oneshot::Receiver<()>,
}

impl Future for ProducerDeadSignal {
    type Output = Result<(), RecvError>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let inner = &mut self.inner;
        tokio::pin!(inner);
        inner.poll(cx)
    }
}

async fn get_producer_dead_signal(mut etcd: etcd_client::Client, producer_id: ProducerId) -> anyhow::Result<ProducerDeadSignal> {
    let producer_lock_path = get_producer_lock_path_v1(producer_id);
    let (mut watch_handle, mut stream) = etcd.watch(producer_lock_path.as_bytes(), Some(WatchOptions::new().with_prefix())).await?;

    let (tx, rx) = oneshot::channel();
    let get_resp = etcd.get(producer_lock_path.as_bytes(), None).await?;


    let (cancel_watch_tx, cancel_watch_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let _ = cancel_watch_rx.await;
        let _ = watch_handle.cancel().await;
    });

    // If the producer is already dead, we can quit early
    if get_resp.count() == 0 {
        tx.send(()).map_err(|_| anyhow::anyhow!("failed to early notify dead producer"));
        return Ok(ProducerDeadSignal {
            _cancel_watcher_tx: cancel_watch_tx,
            inner: rx,
        })
    }

    tokio::spawn(async move {
        while let Some(message) = stream.message().await.expect("watch stream was terminated early") {
            let ev_type = message.events().first().map(|ev| ev.event_type()).expect("watch received a none event");
            match ev_type {
            etcd_client::EventType::Put => panic!("corrupted system state, producer was created after dead signal"),
                etcd_client::EventType::Delete => { 
                    if tx.send(()).is_err() {
                        warn!("producer dead signal receiver half was terminated before signal was send");
                    }
                    break;
                }
            }
        }
    });
    Ok(ProducerDeadSignal {
        _cancel_watcher_tx: cancel_watch_tx,
        inner: rx,
    })
}


pub struct ConsumerGroupLeaderNode {
    consumer_group_id: ConsumerGroupId,
    etcd: etcd_client::Client,
    living_producers: HashSet<ProducerId>,
    etcd_leader_info: (Vec<u8>, ManagedLease),
    current_producer_id: ProducerId,
    timeline_id: Vec<u8>,
    producer_watcher: ProducerDeadSignal,
}

impl ConsumerGroupLeaderNode {


    pub async fn new(
        etcd: etcd_client::Client,
        consumer_group_id: ConsumerGroupId,
        producer_id: ProducerId,
    ) -> anyhow::Result<Self> {
        
        todo!()
    }


    pub fn leader_loop(&mut self) {

    }

}
