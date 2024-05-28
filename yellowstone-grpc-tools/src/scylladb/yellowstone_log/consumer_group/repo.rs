use {
    crate::scylladb::types::ShardId,
    scylla::{
        cql_to_rust::{FromCqlVal, FromCqlValError},
        frame::response::result::CqlValue,
        prepared_statement::PreparedStatement,
        serialize::value::SerializeCql,
        Session,
    },
    std::{collections::BTreeMap, net::IpAddr, sync::Arc},
    uuid::Uuid,
};

const NUM_SHARDS: usize = 64;

type ConsumerGroupId = Uuid;
type InstanceId = String;

const CREATE_STATIC_CONSUMER_GROUP: &str = r###"
    INSERT INTO consumer_groups (
        consumer_group_id, 
        group_type,
        last_access_ip_address,
        instance_id_shard_assignments,
        redundant_id_shard_assignments,
        created_at,
        updated_at
    )
    VALUES (?, ?, ?, ?, ?, currentTimestamp(), currentTimestamp())
"###;

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
enum ConsumerGroupType {
    Static = 0,
}

impl TryFrom<i16> for ConsumerGroupType {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ConsumerGroupType::Static),
            x => Err(anyhow::anyhow!(
                "Unknown ConsumerGroupType equivalent for {:?}",
                x
            )),
        }
    }
}

impl From<ConsumerGroupType> for i16 {
    fn from(val: ConsumerGroupType) -> Self {
        match val {
            ConsumerGroupType::Static => 0,
        }
    }
}

impl SerializeCql for ConsumerGroupType {
    fn serialize<'b>(
        &self,
        typ: &scylla::frame::response::result::ColumnType,
        writer: scylla::serialize::CellWriter<'b>,
    ) -> Result<
        scylla::serialize::writers::WrittenCellProof<'b>,
        scylla::serialize::SerializationError,
    > {
        let x: i16 = (*self).into();
        SerializeCql::serialize(&x, typ, writer)
    }
}

impl FromCqlVal<CqlValue> for ConsumerGroupType {
    fn from_cql(cql_val: CqlValue) -> Result<Self, scylla::cql_to_rust::FromCqlValError> {
        match cql_val {
            CqlValue::SmallInt(x) => x.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

pub(crate) struct ConsumerGroupRepo {
    session: Arc<Session>,
    create_static_consumer_group_ps: PreparedStatement,
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
    pub(crate) redundant_instance_id_assignments: BTreeMap<InstanceId, Vec<ShardId>>,
}

impl ConsumerGroupRepo {
    pub async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        let create_static_consumer_group_ps = session.prepare(CREATE_STATIC_CONSUMER_GROUP).await?;

        let this = ConsumerGroupRepo {
            session,
            create_static_consumer_group_ps,
        };

        Ok(this)
    }

    pub async fn create_static_consumer_group(
        &self,
        instance_ids: &[InstanceId],
        redundant_instance_ids: &[InstanceId],
        remote_ip_addr: Option<IpAddr>,
    ) -> anyhow::Result<StaticConsumerGroupInfo> {
        let consumer_group_id = Uuid::new_v4();
        anyhow::ensure!(
            instance_ids.len() == redundant_instance_ids.len(),
            "mismatch number if instance/redundant ids"
        );
        let shard_assignments = assign_shards(&instance_ids, NUM_SHARDS);
        let shard_assignments2 = assign_shards(&redundant_instance_ids, NUM_SHARDS);
        self.session
            .execute(
                &self.create_static_consumer_group_ps,
                (
                    consumer_group_id.as_bytes(),
                    ConsumerGroupType::Static,
                    remote_ip_addr.map(|ipaddr| ipaddr.to_string()),
                    &shard_assignments,
                    &shard_assignments2,
                ),
            )
            .await?;

        let ret = StaticConsumerGroupInfo {
            consumer_group_id,
            instance_id_assignments: shard_assignments,
            redundant_instance_id_assignments: shard_assignments2,
        };

        Ok(ret)
    }
}
