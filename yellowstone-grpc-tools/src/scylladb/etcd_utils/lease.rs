use {
    etcd_client::LeaseGrantOptions,
    std::time::Duration,
    tokio::{
        sync::{oneshot, watch},
        task::JoinHandle,
        time::Instant,
    },
    tracing::{error, warn},
};

const RENEW_LEASE_INTERVAL: Duration = Duration::from_secs(1);

pub struct ManagedLease {
    pub(crate) lease_id: i64,
    keep_alive_response_watch: watch::Receiver<Instant>,
    _sender: oneshot::Sender<()>,
    _lifecycle_handle: JoinHandle<()>,
}

impl ManagedLease {
    pub async fn new(
        etcd_client: etcd_client::Client,
        ttl: Duration,
        keepalive_interval: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let mut client = etcd_client;
        let ttl = ttl.as_secs() as i64;
        let lease_id = client.lease_grant(ttl, None).await?.id();
        let (mut keeper, mut keep_alive_resp_stream) = client.lease_keep_alive(lease_id).await?;

        let (sender, receiver) = oneshot::channel();
        let lifecycle_handle = tokio::spawn(async move {
            let mut receiver = receiver;
            let keepalive_interval = keepalive_interval.unwrap_or(RENEW_LEASE_INTERVAL);
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
                if let Err(e) = wsender.send(Instant::now()) {
                    warn!("lock watch closed its receiving half");
                    break;
                }
            }
        });

        Ok(ManagedLease {
            lease_id,
            keep_alive_response_watch: wreceiver,
            _sender: sender,
            _lifecycle_handle: lifecycle_handle,
        })
    }

    pub fn last_keep_alive(&self) -> Instant {
        self.keep_alive_response_watch.borrow().to_owned()
    }
}
