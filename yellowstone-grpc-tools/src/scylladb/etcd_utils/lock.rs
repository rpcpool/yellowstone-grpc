use {
    crate::scylladb::etcd_utils::lease::ManagedLease,
    core::fmt,
    etcd_client::{Compare, CompareOp, GetOptions, LockOptions, Txn, TxnOp, TxnResponse},
    std::time::Duration,
    thiserror::Error,
    tokio::time::Instant,
    tracing::{info, trace},
};

const LOCK_LEASE_DURATION: Duration = Duration::from_secs(5);

///
/// A Lock instance with automatic lease refresh and lock revocation when dropped.
///
pub struct ManagedLock {
    pub(crate) lock_key: Vec<u8>,
    managed_lease: ManagedLease,
    etcd: etcd_client::Client,
}

impl ManagedLock {
    pub fn lease_id(&self) -> i64 {
        self.managed_lease.lease_id
    }

    fn last_keep_alive(&self) -> Instant {
        self.managed_lease.last_keep_alive()
    }

    pub async fn txn(&self, operations: impl Into<Vec<TxnOp>>) -> anyhow::Result<TxnResponse> {
        let mut client = self.etcd.clone();

        let txn = Txn::new()
            .when(vec![Compare::version(
                self.lock_key.clone(),
                CompareOp::Greater,
                0,
            )])
            .and_then(operations);

        let txn_resp = client.txn(txn).await?;
        Ok(txn_resp)
    }

    pub async fn fencing_token(
        &self,
        fencing_token_key: impl Into<Vec<u8>>,
    ) -> anyhow::Result<i64> {
        let t = Instant::now();
        let txn_ops = vec![TxnOp::put(fencing_token_key, [0], None)];

        let txn_resp = self.txn(txn_ops).await?;
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

    pub async fn revoke(self) -> anyhow::Result<()> {
        self.managed_lease.revoke().await
    }
}

#[derive(Debug, PartialEq, Eq, Error)]
pub enum TryLockError {
    InvalidLockName,
    AlreadyTaken,
    LockingDeadlineExceeded,
}

impl fmt::Display for TryLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLockName => f.write_str("InvalidLockName"),
            Self::AlreadyTaken => f.write_str("AlreadyTaken"),
            Self::LockingDeadlineExceeded => f.write_str("LockingDeadlineExceeded"),
        }
    }
}

pub async fn try_lock(mut client: etcd_client::Client, name: &str) -> anyhow::Result<ManagedLock> {
    const TRY_LOCKING_DURATION: Duration = Duration::from_millis(500);
    trace!("Trying to lock {name}...");
    let get_response = client
        .get(name, Some(GetOptions::new().with_prefix()))
        .await?;

    anyhow::ensure!(get_response.count() <= 1, TryLockError::InvalidLockName);
    anyhow::ensure!(get_response.count() == 0, TryLockError::AlreadyTaken);

    let managed_lease = ManagedLease::new(client.clone(), LOCK_LEASE_DURATION, None).await?;
    let lease_id = managed_lease.lease_id;

    let lock_key = tokio::select! {
        _ = tokio::time::sleep(TRY_LOCKING_DURATION) => {
            Err(anyhow::Error::new(TryLockError::LockingDeadlineExceeded))
        }
        result = client.lock(name, Some(LockOptions::new().with_lease(lease_id))) => {
            result
                .map(|lock_response| lock_response.key().to_vec())
                .map_err(anyhow::Error::new)
        }
    }?;

    Ok(ManagedLock {
        lock_key,
        managed_lease,
        etcd: client,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_should_acquire_lock() {
        let mut client = etcd_client::Client::connect(["localhost:2379"], None)
            .await
            .unwrap();
        let lock = try_lock(client.clone(), "test-lock").await.unwrap();
        let resp = client.get(lock.lock_key.clone(), None).await.unwrap();

        assert_eq!(resp.count(), 1);

        let actual_key = resp
            .kvs()
            .iter()
            .map(|kv| String::from_utf8(kv.key().to_vec()))
            .next()
            .unwrap()
            .unwrap();
        let expected_key = String::from_utf8(lock.lock_key.clone()).unwrap();
        assert_eq!(actual_key, expected_key);
    }

    #[tokio::test]
    async fn it_should_revoke_lock_on_drop() {
        let mut client = etcd_client::Client::connect(["localhost:2379"], None)
            .await
            .unwrap();

        let lock_key = {
            let lock = try_lock(client.clone(), "test-lock2").await.unwrap();
            lock.lock_key
        };

        let resp = client.get(lock_key, None).await.unwrap();
        assert_eq!(resp.count(), 0);
    }

    #[tokio::test]
    async fn it_should_receive_keep_alive() {
        let client = etcd_client::Client::connect(["localhost:2379"], None)
            .await
            .unwrap();

        let lock = try_lock(client.clone(), "test-lock3").await.unwrap();
        let last_keepalive = lock.last_keep_alive();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let new_keepalive = lock.last_keep_alive();

        assert!(last_keepalive < new_keepalive);
    }
}
