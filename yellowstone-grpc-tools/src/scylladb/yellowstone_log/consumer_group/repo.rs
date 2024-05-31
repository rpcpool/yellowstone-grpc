use {
    super::producer_queries::ProducerQueries,
    crate::scylladb::{
        sink,
        types::{
            BlockchainEventType, CommitmentLevel, ConsumerGroupInfo, ConsumerGroupType, ConsumerId,
            InstanceId, ProducerId, ShardId, ShardOffset, Slot,
        },
        yellowstone_log::{
            common::InitialOffset, consumer_group::producer_queries::ImpossibleSlotOffset,
        },
    },
    scylla::{
        batch::{Batch, BatchType},
        prepared_statement::PreparedStatement,
        Session,
    },
    std::{collections::BTreeMap, net::IpAddr, sync::Arc},
    tracing::info,
    uuid::Uuid,
};

const NUM_SHARDS: usize = 64;

const INSERT_INITIAL_STATIC_GROUP_MEMBER_OFFSETS: &str = r###"
    INSERT INTO consumer_shard_offset_v2 (
        consumer_group_id, 
        consumer_id, 
        producer_id, 
        acc_shard_offset_map,
        tx_shard_offset_map,
        revision, 
        created_at, 
        updated_at
    )
    VALUES (?, ?, ?, ?, ?, 0, currentTimestamp(), currentTimestamp())
"###;

const CREATE_STATIC_CONSUMER_GROUP: &str = r###"
    INSERT INTO consumer_groups (
        consumer_group_id,
        group_type,
        producer_id,
        commitment_level,
        subscribed_event_types,
        instance_id_shard_assignments,
        last_access_ip_address,
        revision,
        created_at,
        updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, 0, currentTimestamp(), currentTimestamp())
"###;

const GET_STATIC_CONSUMER_GROUP: &str = r###"
    SELECT
        consumer_group_id,
        group_type,
        producer_id,
        revision,
        commitment_level,
        subscribed_event_types,
        instance_id_shard_assignments,
        last_access_ip_address
    FROM consumer_groups
    WHERE consumer_group_id = ?
"###;

pub(crate) struct ConsumerGroupRepo {
    session: Arc<Session>,
    producer_queries: ProducerQueries,
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
    pub(crate) consumer_group_id: Uuid,
    pub(crate) producer_id: Option<ProducerId>,
    pub(crate) commitment_level: CommitmentLevel,
    pub(crate) revision: i64,
    pub(crate) subscribed_blockchain_event_types: Vec<BlockchainEventType>,
    pub(crate) instance_id_assignments: BTreeMap<InstanceId, Vec<ShardId>>,
}

impl ConsumerGroupRepo {
    pub async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        let create_static_consumer_group_ps = session.prepare(CREATE_STATIC_CONSUMER_GROUP).await?;
        let get_static_consumer_group_ps = session.prepare(GET_STATIC_CONSUMER_GROUP).await?;
        let this = ConsumerGroupRepo {
            session: Arc::clone(&session),
            create_static_consumer_group_ps,
            get_static_consumer_group_ps,
            producer_queries: ProducerQueries::new(session).await?,
        };

        Ok(this)
    }

    pub async fn get_consumer_group_info(
        &self,
        consumer_group_id: impl Into<Vec<u8>>,
    ) -> anyhow::Result<Option<ConsumerGroupInfo>> {
        let consumer_group_id = consumer_group_id.into();
        self.session
            .execute(&self.get_static_consumer_group_ps, (consumer_group_id,))
            .await?
            .maybe_first_row_typed::<ConsumerGroupInfo>()
            .map_err(anyhow::Error::new)
    }

    async fn create_static_group_members(
        &self,
        static_cgroup_info: &StaticConsumerGroupInfo,
        initial_offset: InitialOffset,
    ) -> anyhow::Result<()> {
        let producer_id = static_cgroup_info
            .producer_id
            .expect("missing producer id during static group membership registration");
        let num_shards: usize = static_cgroup_info
            .instance_id_assignments
            .iter()
            .map(|(_, ranges)| ranges.len())
            .sum();

        let shard_offset_pairs: Vec<(ShardId, ShardOffset, Slot)> = match initial_offset {
            InitialOffset::Latest => {
                sink::get_max_shard_offsets_for_producer(
                    Arc::clone(&self.session),
                    producer_id,
                    num_shards,
                )
                .await?
            }
            InitialOffset::Earliest => {
                self.producer_queries
                    .get_min_offset_for_producer(producer_id)
                    .await?
            }
            InitialOffset::SlotApprox {
                desired_slot,
                min_slot,
            } => {
                let minium_producer_offsets = self
                    .producer_queries
                    .get_min_offset_for_producer(producer_id)
                    .await?
                    .into_iter()
                    .map(|(shard_id, shard_offset, slot)| (shard_id, (shard_offset, slot)))
                    .collect::<BTreeMap<_, _>>();

                let shard_offsets_contain_slot = self
                    .producer_queries
                    .get_slot_shard_offsets(
                        desired_slot,
                        min_slot,
                        producer_id,
                        num_shards as ShardId,
                    )
                    .await?
                    .ok_or(ImpossibleSlotOffset(desired_slot))?;

                let are_shard_offset_reachable =
                    shard_offsets_contain_slot
                        .iter()
                        .all(|(shard_id, offset1, _)| {
                            minium_producer_offsets
                                .get(shard_id)
                                .filter(|(offset2, _)| offset1 > offset2)
                                .is_some()
                        });

                if !are_shard_offset_reachable {
                    anyhow::bail!(ImpossibleSlotOffset(desired_slot))
                }
                shard_offsets_contain_slot
            }
        };

        if shard_offset_pairs.len() != (num_shards as usize) {
            anyhow::bail!("Producer {producer_id:?} shard offsets is incomplete");
        }

        let shard_offset_lookup_index = shard_offset_pairs
            .into_iter()
            .map(|(x, y, z)| (x, (y, z)))
            .collect::<BTreeMap<_, _>>();

        info!("Shard offset has been computed successfully");
        let adjustment = match initial_offset {
            InitialOffset::Earliest
            | InitialOffset::SlotApprox {
                desired_slot: _,
                min_slot: _,
            } => -1,
            InitialOffset::Latest => 0,
        };

        let mut batch = Batch::new(BatchType::Logged);
        let insert_initial_offset_ps = self
            .session
            .prepare(INSERT_INITIAL_STATIC_GROUP_MEMBER_OFFSETS)
            .await?;
        let mut values = Vec::with_capacity(static_cgroup_info.instance_id_assignments.len());
        for (consumer_id, assigned_shards) in static_cgroup_info.instance_id_assignments.iter() {
            let shard_offset_map = shard_offset_lookup_index
                .iter()
                .filter(|(k, _)| assigned_shards.contains(k))
                .map(|(k, v)| (*k, v.to_owned()))
                .collect::<BTreeMap<_, _>>();

            let acc_shard_offset_map = if static_cgroup_info
                .subscribed_blockchain_event_types
                .contains(&BlockchainEventType::AccountUpdate)
            {
                Some(shard_offset_map.clone())
            } else {
                None
            };
            let tx_shard_offset_map = if static_cgroup_info
                .subscribed_blockchain_event_types
                .contains(&BlockchainEventType::NewTransaction)
            {
                Some(shard_offset_map.clone())
            } else {
                None
            };
            let row = (
                static_cgroup_info.consumer_group_id.as_bytes().to_vec(),
                consumer_id,
                producer_id,
                acc_shard_offset_map,
                tx_shard_offset_map,
            );
            values.push(row);
            batch.append_statement(insert_initial_offset_ps.clone());
        }
        self.session.batch(&batch, values).await?;
        Ok(())
    }

    pub async fn create_static_consumer_group(
        &self,
        instance_ids: &[InstanceId],
        commitment_level: CommitmentLevel,
        subscribed_blockchain_event_types: &[BlockchainEventType],
        initial_offset: InitialOffset,
        remote_ip_addr: Option<IpAddr>,
    ) -> anyhow::Result<StaticConsumerGroupInfo> {
        let consumer_group_id = Uuid::new_v4();
        let shard_assignments = assign_shards(&instance_ids, NUM_SHARDS);

        let maybe_slot_range = if let InitialOffset::SlotApprox {
            desired_slot,
            min_slot,
        } = initial_offset
        {
            Some(min_slot..=desired_slot)
        } else {
            None
        };

        let producer_id = self
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
                    commitment_level,
                    subscribed_blockchain_event_types,
                    &shard_assignments,
                    remote_ip_addr,
                ),
            )
            .await?;

        info!("created consumer group row -- {consumer_group_id:?}");
        let static_consumer_group_info = StaticConsumerGroupInfo {
            consumer_group_id: consumer_group_id,
            instance_id_assignments: shard_assignments,
            producer_id: Some(producer_id),
            commitment_level,
            revision: 0,
            subscribed_blockchain_event_types: subscribed_blockchain_event_types.to_vec(),
        };

        self.create_static_group_members(&static_consumer_group_info, initial_offset)
            .await?;

        info!("created consumer group static members -- {consumer_group_id:?}");
        Ok(static_consumer_group_info)
    }
}
