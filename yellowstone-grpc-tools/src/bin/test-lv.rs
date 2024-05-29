use etcd_client::{Client, GetOptions, LockOptions, ProclaimOptions, ResignOptions};
use futures::{future::join_all, try_join};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::connect(["localhost:2379"], None).await?;

    println!("try to lock with name \'lock-test\'");
    let resp = client.lock("lock-test", None).await?;
    let key = resp.key();
    let key_str = std::str::from_utf8(key)?;
    println!("the key is {:?}", key_str);

    let resp = client.get("lock-test", Some(GetOptions::new().with_from_key())).await?;
    let kvs = resp.kvs().iter().map(|kv| String::from_utf8_lossy(kv.value())).collect::<Vec<_>>();
    println!("get response: {kvs:?}");


    println!("try to unlock it");
    client.unlock(key).await?;
    println!("finish!");
    println!();

    // make a lease
    let resp = client.lease_grant(60, None).await?;
    println!(
        "grant a lease with id {:?}, ttl {:?}",
        resp.id(),
        resp.ttl()
    );
    let lease_id = resp.id();

    // lock with lease
    println!(
        "try to lock with name \'lock-test2\' and lease {:?}",
        lease_id
    );
    let lock_options = LockOptions::new().with_lease(lease_id);
    let resp = client.lock("lock-test2", Some(lock_options)).await?;
    let key = resp.key();
    let key_str = std::str::from_utf8(key);
    println!("the key is {:?}", key_str);

    println!("try to unlock it");
    client.unlock(key).await?;
    println!("finish!");

    Ok(())
}