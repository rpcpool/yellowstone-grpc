use {
    super::{consumer_source::ConsumerSource, lock::InstanceLock},
    crate::scylladb::types::{ConsumerGroupId, InstanceId},
    chrono::Local,
    futures::channel::oneshot,
    std::collections::HashMap,
    tokio::sync::{mpsc, RwLock},
};

pub enum LocalCommand {
    StopInstanceOfConsumerGroup(ConsumerGroupId, oneshot::Sender<Vec<InstanceId>>),
}

struct ConsumerHandle {}

struct LocalConsumerGroupCoordinator {
    rx: mpsc::Receiver<LocalCommand>,
    cg_shutdown_signal_map:
        RwLock<HashMap<ConsumerGroupId, HashMap<InstanceId, oneshot::Sender<()>>>>,
}

impl LocalConsumerGroupCoordinator {
    // async fn stop_consumption(&mut self, cg_id: ConsumerGroupId, callback: oneshot::Sender<Vec<InstanceId>>) -> anyhow::Result<()> {

    //     let signal_map = self
    //         .cg_shutdown_signal_map
    //         .read()
    //         .await;

    //     if let Some(instance_map) = signal_map.get(&cg_id) {
    //         instance_map
    //             .iter()
    //             .for_each(|(instance_id, signal)| signal.send())
    //     }
    // }

    async fn step(&mut self) -> anyhow::Result<()> {
        let cmd = self.rx.recv().await.ok_or(anyhow::anyhow!(
            "local consumer group coordinator sender channel half is closed."
        ))?;

        // match cmd {
        //     LocalCommand::StopInstanceOfConsumerGroup(cg_id, cb) => self.stop_consumption(cg_id, cb).await
        // }
        todo!()
    }

    // async fn spawn_instance(&self, instance_lock: InstanceLock) {
    //     ConsumerSource::new(session, consumer_group_id, consumer_id, producer_id, subscribed_event_types, sender, offset_commit_interval, shard_iterators, instance_lock)
    // }
}
