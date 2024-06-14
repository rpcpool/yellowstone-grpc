use {
    std::time::Duration,
    tokio::{
        sync::{oneshot, watch},
        task::JoinHandle,
        time::Instant,
    },
    tracing::{error, warn},
};

pub struct ManagedLease {
    pub lease_id: i64,
    keep_alive_response_watch: watch::Receiver<Instant>,
    tx_terminate: oneshot::Sender<()>,
    lifecycle_handle: JoinHandle<()>,
}

impl ManagedLease {
    pub async fn new(
        etcd_client: etcd_client::Client,
        ttl: Duration,
        keepalive_interval: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let mut client = etcd_client;
        let ttl: i64 = ttl.as_secs() as i64;
        anyhow::ensure!(ttl >= 2, "lease ttl must be at least two (2) seconds");
        let lease_id = client.lease_grant(ttl, None).await?.id();

        let (mut keeper, mut keep_alive_resp_stream) = client.lease_keep_alive(lease_id).await?;

        let (sender, receiver) = oneshot::channel();
        let lifecycle_handle = tokio::spawn(async move {
            let mut receiver = receiver;
            let keepalive_interval =
                keepalive_interval.unwrap_or(Duration::from_secs((ttl / 2) as u64));
            let next_renewal = Instant::now() + keepalive_interval;
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
            client
                .lease_revoke(lease_id)
                .await
                .expect("unable to pre-revoke lease");
        });

        let (wsender, wreceiver) = watch::channel(Instant::now());
        tokio::spawn(async move {
            while let Ok(Some(_msg)) = keep_alive_resp_stream.message().await {
                if let Err(_e) = wsender.send(Instant::now()) {
                    warn!("lock watch closed its receiving half");
                    break;
                }
            }
        });

        Ok(ManagedLease {
            lease_id,
            keep_alive_response_watch: wreceiver,
            tx_terminate: sender,
            lifecycle_handle: lifecycle_handle,
        })
    }

    pub fn last_keep_alive(&self) -> Instant {
        self.keep_alive_response_watch.borrow().to_owned()
    }

    pub async fn revoke(self) -> anyhow::Result<()> {
        drop(self.tx_terminate);
        self.lifecycle_handle.await.map_err(anyhow::Error::new)
    }
}
