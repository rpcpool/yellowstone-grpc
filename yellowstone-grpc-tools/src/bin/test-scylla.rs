use std::{any, borrow::Cow, path::Path, sync::Arc};

use futures::{lock, FutureExt};
use tokio::task::JoinSet;
use tokio_zookeeper::{Acl, CreateMode, WatchedEvent, WatchedEventType, ZooKeeper};

const MAX_SHARDS: u16 = 256;



const BASE_PATH: &str = "/log";
const SHARDS_ABS_PATH: &str = "/log/shards";

async fn create_path_if_not_exists<P: AsRef<str>>(zk: Arc<ZooKeeper>, path: P) -> anyhow::Result<()> {
    zk
        .create(path.as_ref(), &[0][..], Acl::open_unsafe(), CreateMode::Persistent)
        .await
        .map_err(|e| anyhow::Error::msg(e))?
        .map(|_| ())
        .or_else(|e| match e {
            tokio_zookeeper::error::Create::NodeExists => Ok(()),
            e => Err(anyhow::format_err!(e)),
        })
}


fn get_shard_path(shard_number: u16) -> String {
    let shard_name = format!("shard-{:0>4}", shard_number);
    vec![SHARDS_ABS_PATH, &shard_name].join("/")
}

async fn create_shards_if_not_exists(zk: Arc<ZooKeeper>) -> Result<(), anyhow::Error>{

    let mut js = JoinSet::new();

    for i in 0..MAX_SHARDS {
        let shard_path = get_shard_path(i);

        let zk = Arc::clone(&zk);
        js.spawn(async move {
            let shard_path = shard_path;
            create_path_if_not_exists(Arc::clone(&zk), shard_path).await
        });
    }

    while let Some(join_result) = js.join_next().await {
        let result = join_result
            .map_err(anyhow::Error::new)
            .and_then(|inner| inner);
        
        if result.is_err() {
            return result;
        }

    } 
    Ok(())
}


async fn try_create<P: AsRef<str>>(zk: Arc<ZooKeeper>, path: P, data: Vec<u8>) -> anyhow::Result<()> {
    zk
        .create(path.as_ref(), data, Acl::read_unsafe(), CreateMode::Ephemeral)
        .await
        .map_err(|e| anyhow::format_err!(e))?
        .map(|_| ())
        .map_err(anyhow::Error::msg)
}

async fn try_acquire_shard<P: AsRef<str>>(zk: Arc<ZooKeeper>, id: Vec<u8>, shard_number: u16, data: Vec<u8>) -> anyhow::Result<()> {

    let shard_path = get_shard_path(shard_number);
    let lock_path = format!("{:?}/{:?}", shard_path, "lock");
    
    // let get_node_fut = zk
    //     .get_data(&lock_path)
    //     .map(|result| result.map_err(anyhow::Error::msg))
    //     .map(|result| result.map(|opt| opt.map(|tupl| tupl.0)))
    //     .map(|result|  result.and_then(|opt| opt.ok_or(anyhow::anyhow!("Shard {:?} has no lock", shard_number))));


    let zk2 = Arc::clone(&zk);
    let lock_path2 = lock_path.clone();
    let id2 = id.clone();
    let watch_when_node_delete_fut = async move {
        let zk = zk2;
        let (receiver, _) = zk
            .with_watcher()
            .exists(lock_path2.as_ref())
            .await
            .map_err(anyhow::Error::msg)?;

        let event = receiver.await.map_err(anyhow::Error::new)?;

        if event.event_type == WatchedEventType::NodeDeleted {
            try_create(Arc::clone(&zk), lock_path2, id2).await
        } else {
            Err(anyhow::anyhow!("Node already locked by someone else"))
        }
    };

    tokio::select! {
        Ok(()) = try_create(Arc::clone(&zk), lock_path.clone(), id.clone()) => Ok(()),
        Ok(()) = watch_when_node_delete_fut => Ok(()),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {



    let (zk, _default_watcher) = ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .await
        .unwrap();

    let zk = Arc::new(zk);

    create_path_if_not_exists(Arc::clone(&zk), BASE_PATH).await?;
    create_path_if_not_exists(Arc::clone(&zk), SHARDS_ABS_PATH).await?;

    
    println!("base path supposed to exists");
    let children = zk.get_children(SHARDS_ABS_PATH).await.unwrap().unwrap_or(Vec::new());

    println!("base path supposed to exists2");
    if children.is_empty() {
        create_shards_if_not_exists(Arc::clone(&zk)).await?;
    }

    println!("ici: {:?}", children);

    Ok(())
}
