use {
    super::{scylla_types::ConsumerGroupInfo, types::{ConsumerGroupId, InstanceId}}, crate::scylladb::{types::ShardId, yellowstone_log::consumer_group::scylla_types::ConsumerGroupType}, scylla::{
        prepared_statement::PreparedStatement,
        Session,
    }, std::{collections::BTreeMap, net::IpAddr, sync::Arc}, uuid::Uuid
};

const NUM_SHARDS: usize = 64;


const CREATE_STATIC_CONSUMER_GROUP: &str = r###"
    INSERT INTO consumer_groups (
        consumer_group_id, 
        group_type,
        instance_id_shard_assignments,
        last_access_ip_address,
        created_at,
        updated_at
    )
    VALUES (?, ?, ?, ?, currentTimestamp(), currentTimestamp())
"###;


const GET_STATIC_CONSUMER_GROUP: &str = r###"
    SELECT
        consumer_group_id,
        group_type,
        producer_id,
        fencing_token,
        instance_id_shard_assignments,
        last_access_ip_address
    FROM consume_groups
    WHERE consume_group_id = ?
"###;


pub(crate) struct ConsumerGroupRepo {
    session: Arc<Session>,
    create_static_consumer_group_ps: PreparedStatement,
    get_static_consumer_group_ps: PreparedStatement,
}

fn assign_shards(ids: &[InstanceId], num_shards: usize) -> BTreeMap<InstanceId, Vec<ShardId>> {
    let mut ids = ids.to_vec();
    ids.sort();

    let num_parts_per_id = num_shards / ids.len();
    let shard_vec = (0..num_shards).map(|x| x as ShardId).collect::<Vec<_>>();
    let chunk_it = shard_vec
        .chunks(num_parts_per_id)
        .into_iter()
        .map(|chunk| chunk.iter().cloned().collect());

    ids.into_iter().zip(chunk_it).collect()
}

pub(crate) struct StaticConsumerGroupInfo {
    pub(crate) consumer_group_id: ConsumerGroupId,
    pub(crate) instance_id_assignments: BTreeMap<InstanceId, Vec<ShardId>>,
}

impl ConsumerGroupRepo {
    pub async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        let create_static_consumer_group_ps = session.prepare(CREATE_STATIC_CONSUMER_GROUP).await?;
        let get_static_consumer_group_ps = session.prepare(GET_STATIC_CONSUMER_GROUP).await?;
        let this = ConsumerGroupRepo {
            session,
            create_static_consumer_group_ps,
            get_static_consumer_group_ps,
        };

        Ok(this)
    }

    pub async fn get_consumer_group_info(&self, consumer_group_id: ConsumerGroupId) -> anyhow::Result<Option<ConsumerGroupInfo>> {
        self.session
            .execute(&self.get_static_consumer_group_ps, (consumer_group_id,))
            .await?
            .maybe_first_row_typed::<ConsumerGroupInfo>()
            .map_err(anyhow::Error::new)
    }

    pub async fn create_static_consumer_group(
        &self,
        instance_ids: &[InstanceId],
        remote_ip_addr: Option<IpAddr>,
    ) -> anyhow::Result<StaticConsumerGroupInfo> {
        let consumer_group_id = Uuid::new_v4();
        let shard_assignments = assign_shards(&instance_ids, NUM_SHARDS);
        self.session
            .execute(
                &self.create_static_consumer_group_ps,
                (
                    consumer_group_id.as_bytes(),
                    ConsumerGroupType::Static,
                    &shard_assignments,
                    remote_ip_addr
                ),
            )
            .await?;

        let ret = StaticConsumerGroupInfo {
            consumer_group_id,
            instance_id_assignments: shard_assignments,
        };

        Ok(ret)
    }
}
