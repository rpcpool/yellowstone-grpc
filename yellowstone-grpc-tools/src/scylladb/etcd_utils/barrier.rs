use {
    etcd_client::{GetOptions, KvClientPrefix, PutOptions, WatchFilterType, WatchOptions, Watcher},
    futures::channel::oneshot,
    tracing::info,
};

pub struct Barrier {
    key: Vec<u8>,
    watch_handle: Watcher,
    watch_stream_kill_signal: oneshot::Sender<()>,
    etcd_response: oneshot::Receiver<()>,
}

impl Barrier {
    pub async fn wait(self) {
        let _ = self.etcd_response.await;
    }
}

pub async fn get_barrier(
    mut etcd: etcd_client::Client,
    barrier_key: &[u8],
) -> anyhow::Result<Barrier> {
    let mut wc = etcd.watch_client();
    let (watch_handle, mut watch_stream) = wc
        .watch(
            barrier_key,
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_filters([WatchFilterType::NoPut]),
            ),
        )
        .await?;

    let total = etcd
        .get(barrier_key, Some(GetOptions::new().with_prefix()))
        .await?
        .kvs()
        .len();

    let (tx, mut rx) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    tokio::spawn(async move {
        let mut counter = total;

        while counter > 0 {
            tokio::select! {
                _ = &mut rx => (),
                result = watch_stream.message() => {
                    match result {
                        Ok(Some(_)) => (),
                        _ => panic!("watch stream closed unexpectedly"),
                    }
                    counter -= 1;
                }
            }
        }
        let _ = tx2.send(());
    });

    Ok(Barrier {
        key: barrier_key.to_vec(),
        watch_handle,
        watch_stream_kill_signal: tx,
        etcd_response: rx2,
    })
}

pub async fn release_child(
    etcd: etcd_client::Client,
    barrier_key: impl AsRef<[u8]>,
    child: impl AsRef<[u8]>,
) -> anyhow::Result<()> {
    info!("releasing child from barrier");
    let mut dir = KvClientPrefix::new(etcd.kv_client(), barrier_key.as_ref().to_vec());
    dir.delete(child.as_ref(), None).await?;
    Ok(())
}

pub async fn new_barrier<S>(
    etcd: etcd_client::Client,
    barrier_key: impl AsRef<[u8]>,
    children: &[S],
    lease_id: i64,
) -> anyhow::Result<Barrier>
where
    S: AsRef<[u8]>,
{
    let mut dir = KvClientPrefix::new(etcd.kv_client(), barrier_key.as_ref().to_vec());

    let mut revision_to_watch_from = 1;
    for child in children {
        let revision = dir
            .put(
                child.as_ref(),
                [],
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await?
            .take_header()
            .map(|h| h.revision())
            .ok_or(anyhow::anyhow!("failed to create barrier children"))?;
        revision_to_watch_from = revision + 1;
    }

    let mut wc = etcd.watch_client();

    let (watch_handle, mut watch_stream) = wc
        .watch(
            barrier_key.as_ref(),
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(revision_to_watch_from)
                    .with_filters([WatchFilterType::NoPut]),
            ),
        )
        .await?;

    let total = children.len();

    let (tx, mut rx) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    tokio::spawn(async move {
        let mut counter = total;

        while counter > 0 {
            tokio::select! {
                _ = &mut rx => (),
                result = watch_stream.message() => {
                    match result {
                        Ok(Some(_)) => (),
                        _ => panic!("watch stream closed unexpectedly"),
                    }
                    counter -= 1;
                }
            }
        }
        let _ = tx2.send(());
    });

    Ok(Barrier {
        key: barrier_key.as_ref().to_vec(),
        watch_handle,
        watch_stream_kill_signal: tx,
        etcd_response: rx2,
    })
}
