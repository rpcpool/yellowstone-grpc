use core::fmt;
use std::time::Duration;

use etcd_client::{Compare, GetOptions, LockOptions, LockResponse, Txn, WatchOptions};
use futures::{channel::oneshot, lock};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle, time::Instant};
use tracing::{error, warn};



const LOCK_LEASE_DURATION: Duration = Duration::from_secs(5);

///
/// A Lock instance with automatic lease refresh and lock revocation when dropped.
/// 
pub struct Lock {
    lock_key: Vec<u8>,
    lease_id: i64,
    // Dropping this sender will cause the etcd lock to be revoke.
    sender: oneshot::Sender<()>,
    lifecycle_handle: JoinHandle<()>,
    keep_alive_response_watch: watch::Receiver<Instant>,
}

impl Lock {

    fn last_keep_alive(&self) -> Instant {
        self.keep_alive_response_watch.borrow().to_owned()
    }
}


pub struct LockFactory {
    etcd_client: etcd_client::Client
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

    let get_response = client.get(name, Some(GetOptions::new().with_from_key())).await?;
    anyhow::ensure!(get_response.count() <= 1, TryLockError::InvalidLockName);
    anyhow::ensure!(get_response.count() == 0, TryLockError::AlreadyTaken);

    let lease_id = client.lease_grant(LOCK_LEASE_DURATION.as_secs() as i64, None).await?.id();


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

    let (sender, receiver) = oneshot::channel();

    let (mut keeper, mut keep_alive_resp_stream) = client.lease_keep_alive(lease_id).await?;

    let lock_id2 = lock_key.clone();
    let client2 = client.clone();
    let lifecycle_handle = tokio::spawn(async move {
        let lock_id = lock_id2;
        const RENEW_LEASE_INTERVAL: Duration = Duration::from_secs(1);
        let mut client = client2.clone();
        let mut receiver = receiver;
        let next_renewal = Instant::now() + RENEW_LEASE_INTERVAL;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(next_renewal) => {
                    if let Err(e) = keeper.keep_alive().await {
                        error!("failed to keep alive lease {lease_id:?}, got {e:?}");
                        break;
                    }
                }
                _ = &mut receiver => {
                    break;
                }
            }
        }
        
        let unlock_result = client.unlock(lock_id.clone()).await;
        if let Err(e) = unlock_result {
            error!("failed to unlock {lock_id:?}, got {e:?}");
        }
    });

    let (wsender, wreceiver) = watch::channel(Instant::now());
    tokio::spawn(async move {
        while let Ok(Some(_msg)) = keep_alive_resp_stream.message().await {
            if let Err(e) = wsender.send(Instant::now()) {
                warn!("lock watch closed its receiving half");
                break;
            }
        }
    });

    Ok(Lock {
        lock_key,
        lease_id,
        sender,
        lifecycle_handle,
        keep_alive_response_watch: wreceiver,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_should_acquire_lock() {
        let mut client = etcd_client::Client::connect(["localhost:2379"], None).await.unwrap();
        let lock = try_lock(client.clone(), "test-lock").await.unwrap();
        let resp = client.get(lock.lock_key.clone(), None).await.unwrap();


        assert_eq!(resp.count(), 1);

        let actual_key = resp.kvs().iter().map(|kv| String::from_utf8(kv.key().to_vec())).next().unwrap().unwrap();
        let expected_key = String::from_utf8(lock.lock_key.clone()).unwrap();
        assert_eq!(actual_key, expected_key);
    }


    #[tokio::test]
    async fn it_should_revoke_lock_on_drop() {
        let mut client = etcd_client::Client::connect(["localhost:2379"], None).await.unwrap();

        let (lock_key, lifecycle_handle) = {
            let lock = try_lock(client.clone(), "test-lock2").await.unwrap();
            (lock.lock_key, lock.lifecycle_handle)
        };

        lifecycle_handle.await.unwrap();

        let resp = client.get(lock_key, None).await.unwrap();
        assert_eq!(resp.count(), 0);
    }

    #[tokio::test]
    async fn it_should_receive_keep_alive() {
        let mut client = etcd_client::Client::connect(["localhost:2379"], None).await.unwrap();

        let lock = try_lock(client.clone(), "test-lock3").await.unwrap();
        let last_keepalive = lock.last_keep_alive();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let new_keepalive = lock.last_keep_alive();

        assert!(last_keepalive < new_keepalive);
    }
}