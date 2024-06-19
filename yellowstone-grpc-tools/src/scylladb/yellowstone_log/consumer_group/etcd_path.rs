use {
    crate::scylladb::types::{ConsumerGroupId, ConsumerId, ProducerId, ShardId},
    tracing::{info, trace},
    uuid::Uuid,
};

pub fn get_instance_lock_name_path_v1(
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
) -> String {
    let uuid = Uuid::from_bytes(consumer_group_id);
    let uuid_str = uuid.to_string();
    format!("v1#lock#cg-{uuid_str}#i-{consumer_id}")
}

pub fn get_instance_lock_prefix_v1(consumer_group_id: ConsumerGroupId) -> String {
    let uuid = Uuid::from_bytes(consumer_group_id);
    let uuid_str = uuid.to_string();
    format!("v1#lock#cg-{uuid_str}#i-")
}

pub fn parse_lock_key_v1(
    lock_key: impl AsRef<[u8]>,
) -> anyhow::Result<(ConsumerGroupId, ConsumerId)> {
    let s = String::from_utf8_lossy(lock_key.as_ref());
    let mut parts = s.split("#");
    let _ = parts.next(); // skip v1#
    let _ = parts.next(); // skip lock#
    let cg_id = parts
        .next()
        .ok_or(anyhow::anyhow!("invalid lock key format"))?;
    let consumer_id = parts
        .next()
        .ok_or(anyhow::anyhow!("invalid lock key format"))?;
    let cg_id = cg_id
        .strip_prefix("cg-")
        .ok_or(anyhow::anyhow!("invalid lock key format"))?;
    let consumer_id = consumer_id
        .strip_prefix("i-")
        .ok_or(anyhow::anyhow!("invalid lock key format"))?;

    let (consumer_id, _lock_id) = consumer_id
        .split_once("/")
        .ok_or(anyhow::anyhow!("invalid lock key format"))?;
    let cg_id = Uuid::parse_str(cg_id)?;
    let consumer_id = String::from(consumer_id);
    Ok((cg_id.into_bytes(), consumer_id))
}

pub fn get_instance_fencing_token_key_path_v1(
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
) -> String {
    let uuid = Uuid::from_bytes(consumer_group_id);
    let uuid_str = uuid.to_string();
    format!("v1#fencing-token#cg-{uuid_str}#i-{consumer_id}")
}

pub fn get_producer_lock_path_v1(producer_id: ProducerId) -> String {
    format!("v1#lock#producers#p-{producer_id}")
}

pub fn get_producer_id_from_lock_key_v1(lock_key: &[u8]) -> anyhow::Result<ProducerId> {
    let s = String::from_utf8_lossy(lock_key);
    let (producer_id, _) = s
        .split("#")
        .skip(3)
        .next()
        .and_then(|s| s.strip_prefix("p-"))
        .and_then(|s| s.split_once("/"))
        .ok_or(anyhow::anyhow!("invalid lock key format"))?;
    producer_id.try_into().map_err(anyhow::Error::new)
}

pub fn get_producer_lock_prefix_v1() -> String {
    String::from("v1#lock#producers#p-")
}

// pub fn get_producer_fencing_token_key_path_v1(producer_id: ProducerId) -> String {
//     let producer_id_num = u8::from_be_bytes(producer_id);
//     format!("v1#fencing-token#producers#p-{:0>4}", producer_id_num)
// }

// pub fn get_shard_fencing_token_key_path_v1(producer_id: ProducerId, shard_id: ShardId) -> String {
//     let producer_id_num = u8::from_be_bytes(producer_id);
//     format!(
//         "v1#fencing-token#shards#p-{:0>4}#s-{:0>5}",
//         producer_id_num, shard_id
//     )
// }
