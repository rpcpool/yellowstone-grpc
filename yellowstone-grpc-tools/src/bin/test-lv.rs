use {
    etcd_client::{Client, GetOptions, LeaderKey, LockOptions, ProclaimOptions, ResignOptions},
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
    let lease1 = client.lease_grant(1, None).await?.id();

    let uuid = Uuid::new_v4();
    println!("uuid: {uuid:?}");

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        let mut stream = client2.observe("myleader").await.expect("fail");
        while let Some(mut msg) = stream.message().await.expect("") {
            let kv = msg.take_kv().unwrap();
            if tx
                .send((kv.key().to_vec(), kv.value().to_vec()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let resp = client
        .campaign("myleader", uuid.to_string(), lease1)
        .await?;
    let leader_key = String::from_utf8(resp.leader().unwrap().key().to_vec())?;
    println!("{resp:?}");

    let (k, v) = rx.recv().await.unwrap();
    let v = String::from_utf8(v)?;
    println!("rx: {v:?}");

    let lk = LeaderKey::new().with_key(leader_key).with_name("myleader");
    client
        .resign(Some(ResignOptions::new().with_leader(lk)))
        .await?;

    let uuid = Uuid::new_v4();
    let resp = client
        .campaign("myleader", uuid.to_string(), lease1)
        .await?;
    println!("{resp:?}");

    let (k, v) = rx.recv().await.unwrap();
    let v = String::from_utf8(v)?;
    println!("rx: {v:?}");

    Ok(())
}
