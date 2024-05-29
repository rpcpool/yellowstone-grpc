
use etcd_client::{Compare, CompareOp, Txn, TxnOp};

use crate::scylladb::etcd_utils::{self, lock::Lock};

use super::types::{ConsumerGroupId, InstanceId};



pub struct InstanceLock {
    lock: Lock,
    instance_id: InstanceId,
    fencing_token_key: Vec<u8>,
    consumer_group_id: ConsumerGroupId,
    etcd_client: etcd_client::Client,
}


impl InstanceLock {

    async fn get_fencing_token(&self) -> anyhow::Result<i64> {
        let mut client = self.etcd_client.clone();

        let txn = Txn::new()
            .when(vec![
                Compare::version(self.fencing_token_key.clone(), CompareOp::Greater, 0),
            ])
            .and_then(vec![
                TxnOp::put(self.fencing_token_key.clone(), "", None)
            ]);

        let txn_resp = client.txn(txn).await?;
                
        let op_resp = txn_resp.op_responses()
            .pop()
            .expect("failed to get fencing token");

        match op_resp {
            etcd_client::TxnOpResponse::Put(put_resp) => {
                put_resp.header().take().ok_or(anyhow::anyhow!("put response empty")).map(|header| header.revision())
            },
            _ => panic!("unexpected operation in etcd txn response")
        }
    }
}


fn get_proper_instance_lock_name(consumer_group_id: ConsumerGroupId, instance_id: InstanceId) -> String {
    let uuid_str = consumer_group_id.to_string();
    format!("v1/lock/cg-{uuid_str}/i-{instance_id}")
}

fn get_instance_revision_counter_key(consumer_group_id: ConsumerGroupId, instance_id: InstanceId) -> String {
    let uuid_str = consumer_group_id.to_string();
    format!("v1/fencing-token/cg-{uuid_str}/i-{instance_id}")
}


pub struct InstanceLocker(pub etcd_client::Client);

impl InstanceLocker {

    pub async fn try_lock_instance_id(&self, consumer_group_id: ConsumerGroupId, instance_id: InstanceId) -> anyhow::Result<InstanceLock> {
        let client = self.0.clone();
        let lock_name = get_proper_instance_lock_name(consumer_group_id.clone(), instance_id.clone());
        let fencing_token_key = get_instance_revision_counter_key(consumer_group_id.clone(), instance_id.clone());

        let lock = etcd_utils::lock::try_lock(client, lock_name.as_str()).await?;
        Ok(
            InstanceLock {
                lock,
                instance_id,
                fencing_token_key: fencing_token_key.as_bytes().to_vec(),
                etcd_client: self.0.clone(),
                consumer_group_id
            }
        )
    }
}