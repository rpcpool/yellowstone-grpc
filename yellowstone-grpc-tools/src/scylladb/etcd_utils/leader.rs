use {
    crate::scylladb::etcd_utils::lease::ManagedLease,
    etcd_client::CampaignResponse,
    futures::{FutureExt, Stream, StreamExt},
    std::time::Duration,
    tokio::sync::watch,
    tracing::trace,
};

const DEFAULT_LEADER_LEASE_DURATION: Duration = Duration::from_secs(60);

struct Campaign {}

pub enum LeaderRef {
    Leader {
        key: Vec<u8>,
        managed_lease: ManagedLease,
    },
    Follower {
        leader_watch: watch::Receiver<Vec<u8>>,
    },
}

async fn get_leader_ref(
    mut client: etcd_client::Client,
    name: &str,
    initial_value: impl Into<Vec<u8>>,
) -> anyhow::Result<LeaderRef> {
    // let lease = ManagedLease::new(client.clone(), DEFAULT_LEADER_LEASE_DURATION, None).await?;
    // let mut client2 = client.clone();

    // let campaign_fut = async move {
    //     let resp = client.campaign(name, initial_value.into(), lease.lease_id).await?;
    //     let leader_key = resp
    //         .take_leader()
    //         .map(|leader| leader.key().to_vec()).ok_or(anyhow::anyhow!("campaign failed"))?;
    //     Ok(LeaderRef::Leader { key: leader_key, managed_lease: lease })
    // };

    todo!()
}
