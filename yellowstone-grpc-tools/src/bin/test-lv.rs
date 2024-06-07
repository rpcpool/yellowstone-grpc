use {
    etcd_client::{Client, LeaderKey, ResignOptions, WatchOptions},
    std::time::Duration,
    tokio::sync::mpsc,
    uuid::Uuid,
    yellowstone_grpc_tools::scylladb::etcd_utils::lease::ManagedLease,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let s = "p-0000";
    let producer_id = &s[2..].parse::<u8>()?;
    println!("producer_id {producer_id}");


    let mut client = Client::connect(["localhost:2379"], None).await?;
    let mut client2 = client.clone();
    let lease1 = ManagedLease::new(
        client.clone(),
        Duration::from_secs(2),
        Some(Duration::from_secs(1)),
    )
    .await?;

    let uuid = Uuid::new_v4();
    println!("uuid: {uuid:?}");

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        let (_watcher, mut stream) = client2
            .watch("myleader", Some(WatchOptions::new().with_prefix()))
            .await
            .expect("fail");
        while let Some(msg) = stream.message().await.expect("") {
            let ev = &msg.events()[0];
            let kv = ev.kv().unwrap();

            if tx
                .send((ev.event_type(), kv.key().to_vec(), kv.value().to_vec()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let res = rx.try_recv();
    println!("try recv1 : {res:?}");

    let resp = client
        .campaign("myleader", uuid.to_string(), lease1.lease_id)
        .await?;
    let leader_key = String::from_utf8(resp.leader().unwrap().key().to_vec())?;
    println!("leader key: {leader_key:?}");

    // let (ev_type, k, v) = rx.recv().await.unwrap();
    // let v = String::from_utf8(v)?;
    // println!("rx: {ev_type:?} {v:?}");

    let lk = LeaderKey::new().with_key(leader_key).with_name("myleader");
    //client.lease_revoke(lease1).await?;

    client
        .resign(Some(ResignOptions::new().with_leader(lk.clone())))
        .await?;
    // client
    //     .resign(Some(ResignOptions::new().with_leader(lk)))
    //     .await?;

    let (ev_type, _k, v) = rx.recv().await.unwrap();
    println!("after resign recv1 : {ev_type:?} {v:?}");

    let lease2 = client.lease_grant(1, None).await?.id();
    let uuid = Uuid::new_v4();
    let resp = client.campaign("myleader", uuid.to_string(), lease2).await;
    println!("campaign result: {resp:?}");
    //let leader_key = String::from_utf8(resp.leader().unwrap().key().to_vec())?;
    //println!("leader key: {leader_key:?}");

    let (ev_type, _k, v) = rx.recv().await.unwrap();
    let v = String::from_utf8(v)?;
    println!("rx: {ev_type:?} {v:?}");

    Ok(())
}
