use {
    super::{etcd_path::get_producer_id_from_lock_key_v1, producer_queries::ProducerQueries},
    crate::scylladb::{
        etcd_utils,
        scylladb_utils::LwtResult,
        sink,
        types::{
            BlockchainEventType, CommitmentLevel, ConsumerGroupId, ConsumerGroupInfo,
            ConsumerGroupType, ConsumerId, ExecutionId, InstanceId, ProducerExecutionInfo,
            ProducerId, ShardId, ShardOffset, ShardOffsetMap, Slot,
        },
        yellowstone_log::{
            common::SeekLocation,
            consumer_group::{
                error::StaleRevision,
                etcd_path::{get_producer_lock_path_v1, get_producer_lock_prefix_v1},
            },
        },
    },
    anyhow::anyhow,
    etcd_client::GetOptions,
    rdkafka::{consumer::Consumer, producer},
    scylla::{
        batch::{Batch, BatchType},
        prepared_statement::PreparedStatement,
        statement::Consistency,
        Session,
    },
    serde::{Deserialize, Serialize},
    std::{
        cell::BorrowMutError,
        collections::{self, btree_map, BTreeMap},
        iter,
        net::IpAddr,
        sync::Arc,
        thread::current,
    },
    tracing::info,
    uuid::Uuid,
};

const NUM_SHARDS: usize = 64;

const INSERT_STATIC_GROUP_MEMBER_OFFSETS: &str = r###"
    INSERT INTO consumer_shard_offset_v2 (
        consumer_group_id, 
        consumer_id, 
        producer_id, 
        execution_id,
        acc_shard_offset_map,
        tx_shard_offset_map,
        revision, 
        created_at, 
        updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, currentTimestamp(), currentTimestamp())
    IF NOT EXISTS
"###;

const CREATE_STATIC_CONSUMER_GROUP: &str = r###"
    INSERT INTO consumer_groups (
        consumer_group_id,
        group_type,
        producer_id,
        execution_id,
        commitment_level,
        subscribed_event_types,
        instance_id_shard_assignments,
        last_access_ip_address,
        revision,
        created_at,
        updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, currentTimestamp(), currentTimestamp())
"###;

const GET_STATIC_CONSUMER_GROUP: &str = r###"
    SELECT
        consumer_group_id,
        group_type,
        producer_id,
        execution_id,
        revision,
        commitment_level,
        subscribed_event_types,
        instance_id_shard_assignments,
        last_access_ip_address
    FROM consumer_groups
    WHERE consumer_group_id = ?
"###;

const UPDATE_STATIC_CONSUMER_GROUP: &str = r###"
    UPDATE consumer_groups
    SET producer_id = ?,
        execution_id = ?,
        revision = ?
    WHERE consumer_group_id = ?
    IF revision < ?
"###;

const UPDATE_CONSUMER_SHARD_OFFSET_V2: &str = r###"
    UPDATE consumer_shard_offset_v2
    SET acc_shard_offset_map = ?, tx_shard_offset_map = ?, revision = ?
    WHERE 
        consumer_group_id = ?
        AND consumer_id = ?
        AND execution_id = ?
    IF revision < ?
"###;

pub(crate) struct ConsumerGroupStore {
    session: Arc<Session>,
    etcd: etcd_client::Client,
    producer_queries: ProducerQueries,
    create_static_consumer_group_ps: PreparedStatement,
    get_static_consumer_group_ps: PreparedStatement,
    update_static_consumer_group_ps: PreparedStatement,
    insert_consumer_shard_offset_ps_if_not_exists: PreparedStatement,
    update_consumer_shard_offset_ps: PreparedStatement,
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

impl ConsumerGroupStore {
    pub async fn new(session: Arc<Session>, etcd: etcd_client::Client) -> anyhow::Result<Self> {
        let create_static_consumer_group_ps = session.prepare(CREATE_STATIC_CONSUMER_GROUP).await?;

        let mut get_static_consumer_group_ps = session.prepare(GET_STATIC_CONSUMER_GROUP).await?;
        get_static_consumer_group_ps.set_consistency(Consistency::Serial);

        let update_static_consumer_group_ps = session.prepare(UPDATE_STATIC_CONSUMER_GROUP).await?;

        let insert_consumer_shard_offset_ps_if_not_exists =
            session.prepare(INSERT_STATIC_GROUP_MEMBER_OFFSETS).await?;
        let update_consumer_shard_offset_ps =
            session.prepare(UPDATE_CONSUMER_SHARD_OFFSET_V2).await?;
        let this = ConsumerGroupStore {
            session: Arc::clone(&session),
            create_static_consumer_group_ps,
            get_static_consumer_group_ps,
            producer_queries: ProducerQueries::new(session, etcd.clone()).await?,
            update_static_consumer_group_ps,
            etcd,
            insert_consumer_shard_offset_ps_if_not_exists,
            update_consumer_shard_offset_ps,
        };
        Ok(this)
    }

    pub async fn update_consumer_group_producer(
        &self,
        consumer_group_id: &ConsumerGroupId,
        producer_id: &ProducerId,
        execution_id: &ExecutionId,
        revision: i64,
    ) -> anyhow::Result<()> {
        let bind_values = (
            producer_id,
            execution_id,
            revision,
            consumer_group_id,
            revision,
        );
        let lwt_result = self
            .session
            .execute(&self.update_static_consumer_group_ps, bind_values)
            .await?
            .first_row_typed::<LwtResult>()?;
        anyhow::ensure!(
            lwt_result == LwtResult(true),
            "failed to update consumer group producer"
        );
        Ok(())
    }

    pub async fn get_consumer_group_info(
        &self,
        consumer_group_id: &ConsumerGroupId,
    ) -> anyhow::Result<Option<ConsumerGroupInfo>> {
        self.session
            .execute(&self.get_static_consumer_group_ps, (consumer_group_id,))
            .await?
            .maybe_first_row_typed::<ConsumerGroupInfo>()
            .map_err(anyhow::Error::new)
    }

    pub async fn get_lowest_common_slot_number(
        &self,
        consumer_group_id: &ConsumerGroupId,
        max_revision_opt: Option<i64>,
    ) -> anyhow::Result<(Slot, i64)> {
        let consumer_group_info = self
            .get_consumer_group_info(consumer_group_id)
            .await?
            .ok_or(anyhow::anyhow!("consumer group id not found"))?;
        if let Some(max_revision) = max_revision_opt {
            let remote_revision = consumer_group_info.revision;
            anyhow::ensure!(max_revision >= remote_revision, StaleRevision(max_revision));
        }
        let execution_id = consumer_group_info
            .execution_id
            .expect("cannot compute LCS of unused consumer group");
        let instance_id_in_clause = consumer_group_info
            .instance_id_shard_assignments
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        // TODO: handle the possible CQL injection here.
        // the rust driver support little-to know support for IN c
        let query = format!(
            r###"
            SELECT
                consumer_id,
                revision,
                acc_shard_offset_map,
                tx_shard_offset_map,
            FROM consumer_shard_offset_v2
            WHERE 
                consumer_group_id = ?
                AND consumer_id IN ?
                AND execution_id = ?
            "###
        );

        let subscribed_events = consumer_group_info.subscribed_event_types;
        let rows = self
            .session
            .query(
                query,
                (consumer_group_id, instance_id_in_clause, execution_id),
            )
            .await?
            .rows_typed::<(ConsumerGroupId, i64, ShardOffsetMap, ShardOffsetMap)>()?
            .collect::<Result<Vec<_>, _>>()?;
        let shard_max_revision = rows
            .iter()
            .map(|(_, revision, _, _)| revision)
            .cloned()
            .max()
            .unwrap_or(0);
        let min_slot = rows
            .iter()
            .map(|(_, _, acc_offset, tx_offset)| {
                let min1 = if subscribed_events.contains(&BlockchainEventType::AccountUpdate) {
                    acc_offset
                        .iter()
                        .map(|(_, (_, slot))| *slot)
                        .min()
                        .unwrap_or(i64::MAX)
                } else {
                    i64::MAX
                };
                let min2 = if subscribed_events.contains(&BlockchainEventType::NewTransaction) {
                    tx_offset
                        .iter()
                        .map(|(_, (_, slot))| *slot)
                        .min()
                        .unwrap_or(i64::MAX)
                } else {
                    i64::MAX
                };
                std::cmp::min(min1, min2)
            })
            .min()
            .unwrap_or(i64::MAX);

        if let Some(max_revision) = max_revision_opt {
            anyhow::ensure!(
                max_revision >= shard_max_revision,
                StaleRevision(max_revision)
            );
        }

        anyhow::ensure!(min_slot < i64::MAX, "found not shard offset map content");
        Ok((min_slot, shard_max_revision))
    }

    pub async fn set_static_group_members_shard_offset(
        &self,
        consumer_group_id: &ConsumerGroupId,
        producer_id: &ProducerId,
        execution_id: &ExecutionId,
        shard_offset_map: &BTreeMap<ShardId, (ShardOffset, Slot)>,
        current_revision: i64,
    ) -> anyhow::Result<()> {
        let cg_info = self
            .get_consumer_group_info(consumer_group_id)
            .await?
            .ok_or(anyhow::anyhow!("consumer group does not exists"))?;

        anyhow::ensure!(
            cg_info.revision < current_revision,
            "consumer group is more up to date then current operation"
        );
        anyhow::ensure!(
            cg_info.producer_id == Some(*producer_id),
            "producer id mismatch"
        );
        anyhow::ensure!(
            cg_info.execution_id == Some(execution_id.clone()),
            "execution id mismatch"
        );

        for consumer_id in cg_info.instance_id_shard_assignments.keys() {
            let values = (
                consumer_group_id.clone(),
                consumer_id,
                producer_id,
                &cg_info.execution_id,
                &shard_offset_map,
                &shard_offset_map,
                current_revision,
            );
            let lwt_result = self
                .session
                .execute(&self.insert_consumer_shard_offset_ps_if_not_exists, values)
                .await?
                .single_row_typed::<LwtResult>()?;
            if lwt_result == LwtResult(false) {
                let values2 = (
                    &shard_offset_map,
                    &shard_offset_map,
                    current_revision,
                    consumer_group_id.clone(),
                    consumer_id,
                    &cg_info.execution_id,
                    current_revision,
                );
                let lwt_result2 = self
                    .session
                    .execute(&self.update_consumer_shard_offset_ps, values2)
                    .await?
                    .single_row_typed::<LwtResult>()?;

                anyhow::ensure!(
                    lwt_result2 == LwtResult(true),
                    "failed to update {consumer_id} shard offset"
                );
            }
        }
        Ok(())
    }

    async fn create_static_group_members(
        &self,
        consumer_group_info: &ConsumerGroupInfo,
        seek_loc: SeekLocation,
    ) -> anyhow::Result<()> {
        let producer_id = consumer_group_info
            .producer_id
            .expect("missing producer id during static group membership registration");

        let execution_id = consumer_group_info
            .execution_id
            .clone()
            .expect("consumer group does not have any execution id assigned yet");

        let shard_offset_map = self
            .producer_queries
            .compute_offset(producer_id, seek_loc, None)
            .await?;

        info!("Shard offset has been computed successfully");
        self.set_static_group_members_shard_offset(
            &consumer_group_info.consumer_group_id,
            &producer_id,
            &execution_id,
            &shard_offset_map,
            0,
        )
        .await
    }

    pub async fn create_static_consumer_group(
        &self,
        instance_ids: &[InstanceId],
        commitment_level: CommitmentLevel,
        subscribed_blockchain_event_types: &[BlockchainEventType],
        initial_offset: SeekLocation,
        remote_ip_addr: Option<IpAddr>,
    ) -> anyhow::Result<ConsumerGroupInfo> {
        let consumer_group_id = Uuid::new_v4();
        let shard_assignments = assign_shards(&instance_ids, NUM_SHARDS);

        let maybe_slot_range = if let SeekLocation::SlotApprox {
            desired_slot,
            min_slot,
        } = initial_offset
        {
            Some(min_slot..=desired_slot)
        } else {
            None
        };

        let (producer_id, execution_id) = self
            .producer_queries
            .get_producer_id_with_least_assigned_consumer(maybe_slot_range, commitment_level)
            .await?;

        self.session
            .execute(
                &self.create_static_consumer_group_ps,
                (
                    consumer_group_id.as_bytes(),
                    ConsumerGroupType::Static,
                    producer_id,
                    execution_id.clone(),
                    commitment_level,
                    subscribed_blockchain_event_types,
                    &shard_assignments,
                    remote_ip_addr,
                    0_i64,
                ),
            )
            .await?;

        info!("created consumer group row -- {consumer_group_id:?}");
        let static_consumer_group_info = ConsumerGroupInfo {
            consumer_group_id: consumer_group_id.as_bytes().to_vec(),
            instance_id_shard_assignments: shard_assignments,
            producer_id: Some(producer_id),
            commitment_level,
            revision: 0,
            subscribed_event_types: subscribed_blockchain_event_types.to_vec(),
            group_type: ConsumerGroupType::Static,
            last_access_ip_address: remote_ip_addr,
            execution_id: Some(execution_id),
        };

        self.create_static_group_members(&static_consumer_group_info, initial_offset)
            .await?;

        info!("created consumer group static members -- {consumer_group_id:?}");
        Ok(static_consumer_group_info)
    }
}

pub struct ConsumerGroupManagerV2 {
    etcd: etcd_client::Client,
    session: Arc<Session>,
    producer_queries: ProducerQueries,
}
