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

    let (watcher, mut stream) = client
        .watch("test", Some(WatchOptions::new().with_prefix()))
        .await
        .expect("fail");
    drop(watcher);
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
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
    let mut i = 1;
    loop {
        client
            .put(format!("test/{i}"), 1_i32.to_be_bytes(), None)
            .await?;

        let val = rx.recv().await;
        println!("rx1 {val:?}");
        i += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}
