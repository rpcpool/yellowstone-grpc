use {
    etcd_client::{
        Client, GetOptions, LeaderKey, LockOptions, ProclaimOptions, ResignOptions, WatchOptions,
    },
    futures::{
        future::{join_all, select_all},
        try_join, FutureExt,
    },
    tokio::sync::mpsc,
    uuid::Uuid,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::connect(["localhost:2379"], None).await?;
    let mut client2 = client.clone();
    let lease1 = client.lease_grant(10, None).await?.id();

    let uuid = Uuid::new_v4();
    println!("uuid: {uuid:?}");

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        let (watcher, mut stream) = client2
            .watch("myleader", Some(WatchOptions::new().with_prefix()))
            .await
            .expect("fail");
        while let Some(mut msg) = stream.message().await.expect("") {
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
        .campaign("myleader", uuid.to_string(), lease1)
        .await?;
    let leader_key = String::from_utf8(resp.leader().unwrap().key().to_vec())?;
    println!("leader key: {leader_key:?}");

    let (ev_type, k, v) = rx.recv().await.unwrap();
    let v = String::from_utf8(v)?;
    println!("rx: {ev_type:?} {v:?}");

    let lk = LeaderKey::new().with_key(leader_key).with_name("myleader");
    //  client.lease_revoke(lease1).await?;
    client
        .resign(Some(ResignOptions::new().with_leader(lk.clone())))
        .await?;
    // client
    //     .resign(Some(ResignOptions::new().with_leader(lk)))
    //     .await?;

    let (ev_type, k, v) = rx.recv().await.unwrap();
    println!("after resign recv1 : {ev_type:?} {v:?}");

    let lease1 = client.lease_grant(1, None).await?.id();
    let uuid = Uuid::new_v4();
    let resp = client
        .campaign("myleader", uuid.to_string(), lease1)
        .await?;
    let leader_key = String::from_utf8(resp.leader().unwrap().key().to_vec())?;
    println!("leader key: {leader_key:?}");

    let (ev_type, k, v) = rx.recv().await.unwrap();
    let v = String::from_utf8(v)?;
    println!("rx: {ev_type:?} {v:?}");

    Ok(())
}
