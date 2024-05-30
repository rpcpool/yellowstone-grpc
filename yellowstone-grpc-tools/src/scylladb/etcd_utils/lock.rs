use {
    crate::scylladb::etcd_utils::lease::ManagedLease,
    core::fmt,
    etcd_client::{Compare, GetOptions, LockOptions, LockResponse, Txn, WatchOptions},
    futures::{channel::oneshot, lock},
    std::time::Duration,
    thiserror::Error,
    tokio::{sync::watch, task::JoinHandle, time::Instant},
    tracing::{error, warn},
};

const LOCK_LEASE_DURATION: Duration = Duration::from_secs(5);

///
/// A Lock instance with automatic lease refresh and lock revocation when dropped.
///
pub struct Lock {
    pub(crate) lock_key: Vec<u8>,
    managed_lease: ManagedLease,
}

impl Lock {
    pub fn lease_id(&self) -> i64 {
        self.managed_lease.lease_id
    }

    fn last_keep_alive(&self) -> Instant {
        self.managed_lease.last_keep_alive()
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

pub async fn try_lock(mut client: etcd_client::Client, name: &str) -> anyhow::Result<Lock> {
    const TRY_LOCKING_DURATION: Duration = Duration::from_millis(500);

    let get_response = client
        .get(name, Some(GetOptions::new().with_from_key()))
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

    Ok(Lock {
        lock_key,
        managed_lease,
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
        let mut client = etcd_client::Client::connect(["localhost:2379"], None)
            .await
            .unwrap();

        let lock = try_lock(client.clone(), "test-lock3").await.unwrap();
        let last_keepalive = lock.last_keep_alive();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let new_keepalive = lock.last_keep_alive();

        assert!(last_keepalive < new_keepalive);
    }
}
