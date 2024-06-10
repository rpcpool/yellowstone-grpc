use {
    super::etcd_path::{get_instance_fencing_token_key_path_v1, get_instance_lock_name_path_v1},
    crate::scylladb::{
        etcd_utils::{self, lock::ManagedLock},
        types::{ConsumerGroupId, ConsumerId},
    },
    etcd_client::{Compare, CompareOp, Txn, TxnOp},
    tokio::time::Instant,
    tracing::trace,
};

pub struct ConsumerLock {
    lock: ManagedLock,
    pub consumer_id: ConsumerId,
    fencing_token_key: Vec<u8>,
    pub consumer_group_id: ConsumerGroupId,
    etcd_client: etcd_client::Client,
}

#[derive(Clone)]
pub struct FencingTokenGenerator {
    etcd: etcd_client::Client,
    lock_key: Vec<u8>,
    fencing_token_key: Vec<u8>,
}

impl FencingTokenGenerator {
    pub async fn generate(&self) -> anyhow::Result<i64> {
        let t = Instant::now();

        let mut client = self.etcd.clone();

        let txn = Txn::new()
            .when(vec![Compare::version(
                self.lock_key.clone(),
                CompareOp::Greater,
                0,
            )])
            .and_then(vec![TxnOp::put(
                self.fencing_token_key.as_slice(),
                [0],
                None,
            )]);

        let txn_resp = client.txn(txn).await?;
        anyhow::ensure!(txn_resp.succeeded(), "failed to get fencing token");

        let op_resp = txn_resp
            .op_responses()
            .pop()
            .ok_or(anyhow::anyhow!("failed to get fencing token"))?;

        trace!("get fencing token from etcd latency: {t:?}");
        match op_resp {
            etcd_client::TxnOpResponse::Put(put_resp) => put_resp
                .header()
                .take()
                .ok_or(anyhow::anyhow!("put response empty"))
                .map(|header| header.revision()),
            _ => panic!("unexpected operation in etcd txn response"),
        }
    }
}

impl ConsumerLock {
    pub async fn get_fencing_token(&self) -> anyhow::Result<i64> {
        self.lock
            .fencing_token(self.fencing_token_key.clone())
            .await
    }

    pub fn get_fencing_token_gen(&self) -> FencingTokenGenerator {
        FencingTokenGenerator {
            etcd: self.etcd_client.clone(),
            lock_key: self.lock.lock_key.clone(),
            fencing_token_key: self.fencing_token_key.clone(),
        }
    }
}

pub struct ConsumerLocker(pub etcd_client::Client);

impl ConsumerLocker {
    pub async fn try_lock_instance_id(
        &self,
        consumer_group_id: ConsumerGroupId,
        consumer_id: &ConsumerId,
    ) -> anyhow::Result<ConsumerLock> {
        let client = self.0.clone();
        let lock_name = get_instance_lock_name_path_v1(consumer_group_id, consumer_id.clone());
        let fencing_token_key =
            get_instance_fencing_token_key_path_v1(consumer_group_id, consumer_id.clone());

        let lock = etcd_utils::lock::try_lock(client, lock_name.as_str()).await?;
        Ok(ConsumerLock {
            lock,
            consumer_id: consumer_id.clone(),
            fencing_token_key: fencing_token_key.as_bytes().to_vec(),
            etcd_client: self.0.clone(),
            consumer_group_id,
        })
    }
}
