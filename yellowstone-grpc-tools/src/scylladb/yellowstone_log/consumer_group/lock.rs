use {
    super::etcd_path::{self, get_instance_lock_name_path_v1, get_instance_revision_counter_key_path_v1}, crate::scylladb::{
        etcd_utils::{self, lock::{ManagedLock}},
        types::InstanceId,
    }, etcd_client::{Compare, CompareOp, Txn, TxnOp}, tokio::time::Instant, tracing::trace
};

pub struct InstanceLock {
    lock: ManagedLock,
    instance_id: InstanceId,
    fencing_token_key: Vec<u8>,
    consumer_group_id: Vec<u8>,
    etcd_client: etcd_client::Client,
}

impl InstanceLock {
    pub async fn get_fencing_token(&self) -> anyhow::Result<i64> {
        self.lock.fencing_token(self.fencing_token_key.clone()).await
    }
}

pub struct InstanceLocker(pub etcd_client::Client);

impl InstanceLocker {
    pub async fn try_lock_instance_id(
        &self,
        consumer_group_id: impl Into<Vec<u8>>,
        instance_id: InstanceId,
    ) -> anyhow::Result<InstanceLock> {
        let consumer_group_id = consumer_group_id.into();
        let client = self.0.clone();
        let lock_name = get_instance_lock_name_path_v1(consumer_group_id.clone(), instance_id.clone());
        let fencing_token_key = get_instance_revision_counter_key_path_v1(consumer_group_id.clone(), instance_id.clone());

        let lock = etcd_utils::lock::try_lock(client, lock_name.as_str()).await?;
        Ok(InstanceLock {
            lock,
            instance_id,
            fencing_token_key: fencing_token_key.as_bytes().to_vec(),
            etcd_client: self.0.clone(),
            consumer_group_id,
        })
    }
}
