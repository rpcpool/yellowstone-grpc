CREATE KEYSPACE solana WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}  AND durable_writes = true;
CREATE KEYSPACE solana2 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}  AND durable_writes = true;


drop materialized view if exists producer_consumer_mapping_mv;
drop materialized view if exists slot_map;

drop table if exists producer_slot_seen;
drop table if exists shard_statistics;
drop table if exists producer_info;
drop table if exists consumer_shard_offset;
drop table if exists consumer_producer_mapping;
drop table if exists log;
drop type if exists transaction_meta;
drop type if exists message_addr_table_lookup;
drop type if exists compiled_instr;
drop type if exists tx_token_balance;
drop type if exists reward;
drop type if exists inner_instrs;
drop type if exists inner_instr;
drop type if exists return_data;


create table if not exists consumer_shard_offset (
    consumer_id text,
    producer_id blob,
    shard_id smallint,
    event_type smallint,
    offset bigint,
    slot bigint,
    revision bigint,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((consumer_id, producer_id), shard_id, event_type)
)
with default_time_to_live = 3600;

create table if not exists consumer_shard_offset_v2 (
    consumer_group_id blob,
    consumer_id text,
    producer_id blob,
    execution_id blob,
    -- map<ShardId, <ShardOffset, Slot>>
    acc_shard_offset_map frozen<map<smallint, tuple<bigint, bigint>>>,
    tx_shard_offset_map frozen<map<smallint, tuple<bigint, bigint>>>,
    revision bigint,
    created_at timestamp,
    updated_at timestamp,
    -- LWT serialize access at the partition level,
    -- to avoid failing to concurrent LWT, we to match the partition key
    -- to our distributed locking semantic, so we make sure no two instance
    -- try to do LWT on the same partition.
    PRIMARY KEY ((consumer_group_id, consumer_id), execution_id)
);


create table if not exists consumer_groups (
    consumer_group_id blob,
    group_type smallint,
    producer_id blob,
    execution_id blob,
    revision bigint,
    commitment_level smallint,
    subscribed_event_types frozen<set<smallint>>,

    -- fields for static consumer group only
    instance_id_shard_assignments frozen<map<text, set<smallint>>>,
    -- instance_id_shard_assignments2 frozen<map<text, set<smallint>>>,
    
    -- acc_pubkeys_filter frozen<set<blob>>,
    -- acc_owners_filer frozen<set<blob>>,
    -- tx_account_keys_filter frozen<set<blob>>,

    last_access_ip_address inet,
    created_at timestamp,
    updated_at timestamp,

    primary key (consumer_group_id)
);

create materialized view if not exists consumer_group_member_mv
as
    select
        consumer_group_id,
        consumer_id
    from consumer_info
    where
        consumer_id is not null
        and consumer_group_id is not null
primary key (consumer_group_id, consumer_id);

create table if not exists consumer_info (
    consumer_id blob,
    consumer_group_id blob,
    producer_id blob,
    consumer_ip inet,
    subscribed_event_types frozen<set<smallint>>,
    last_connection timestamp,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY (consumer_id)
);

create materialized view if not exists producer_consumer_mapping_mv
as
select
    producer_id,
    consumer_id
from consumer_info
where 
    consumer_id is not null
    and producer_id is not null
primary key (producer_id, consumer_id);
    
create table if not exists producer_info (
    producer_id blob,
    commitment_level smallint,
    num_shards smallint,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY (producer_id)
);

create table if not exists producer_lock (
    producer_id blob,
    execution_id blob,
    revision bigint,
    ifname text,
    ipv4 inet,
    is_ready boolean,
    minimum_shard_offset frozen<map<smallint, tuple<bigint, bigint>>>,
    created_at timestamp,
    primary key (producer_id)
);


create type if not exists message_addr_table_lookup (
    account_key blob,
    writable_indexes blob,
    readonly_indexes blob
);

create type if not exists compiled_instr (
    program_id_index bigint,
    accounts blob,
    data blob
);

create type if not exists inner_instr (
    program_id_index bigint,
    accounts blob,
    data blob,
    stack_height bigint
);

create type if not exists inner_instrs (
    "index" bigint,
    instructions frozen<list<inner_instr>>
);

create type if not exists ui_token_amount (
    ui_amount double,
    decimals bigint,
    amount text,
    ui_amount_string text
);

create type if not exists tx_token_balance (
    account_index bigint,
    mint text,  --varchar(44)
    ui_token_amount frozen<ui_token_amount>,
    owner text, --varchar(44)
    program_id text,
);

create type if not exists reward (
    pubkey text,    -- varchar(44)
    lamports bigint,
    post_balance bigint,
    reward_type int,   --Fee,  Rent, Staking, Voting
    commission text
);

create type if not exists return_data (
    program_id blob,
    data blob
);

create type if not exists transaction_meta (
    error blob,
    fee bigint,
    pre_balances frozen<list<bigint>>,
    post_balances frozen<list<bigint>>,
    inner_instructions frozen<list<inner_instrs>>,
    log_messages frozen<list<text>>,
    pre_token_balances frozen<list<tx_token_balance>>,
    post_token_balances frozen<list<tx_token_balance>>,
    rewards frozen<list<reward>>,
    loaded_writable_addresses frozen<list<blob>>,
    loaded_readonly_addresses frozen<list<blob>>,
    return_data frozen<return_data>,
    compute_units_consumed bigint
);


-- ScyllaDB table can hold different kind of entities at the same time.
-- There is not performance advantage to have separate tables since ScyllaDB is wide-column family database.
-- ScyllaDB is built to have sparse columns (alot of unused columns)
-- On each query, the storage engine only retrieves what matters to the query.
create table if not exists log (

    -- commun columns
    shard_id smallint,
    period bigint,
    producer_id blob,
    offset bigint,
    slot bigint,
    event_type smallint,
        -- 0 = account update
        -- 1 = new transaction

    -- account columns
    pubkey blob,
    lamports bigint,
    owner blob,
    executable boolean,
    rent_epoch bigint,
    write_version bigint,
    data blob,
    txn_signature blob,


    -- transaction columns
    signature blob,
    signatures frozen<list<blob>>,
    num_required_signatures int,
    num_readonly_signed_accounts int,
    num_readonly_unsigned_accounts int,
    account_keys frozen<list<blob>>,
    recent_blockhash blob,
    instructions frozen<list<compiled_instr>>,
    versioned boolean,
    address_table_lookups frozen<list<message_addr_table_lookup>>,
    meta transaction_meta,
    is_vote boolean,
    tx_index bigint,


    -- meta data field for debugging purposes
    created_at timestamp,

    primary key ((shard_id, period, producer_id), offset)
)
WITH CLUSTERING ORDER BY (offset desc)
    AND default_time_to_live = 86400
    and compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'compaction_window_size' : 30}; 

create table if not exists producer_slot_seen (
    producer_id blob,
    slot bigint,
    revision bigint,
    shard_offset_map frozen<set<tuple<smallint, bigint>>>,
    created_at timestamp,
    primary key (producer_id, slot)
)
with clustering order by (slot DESC)
     AND default_time_to_live = 82800; -- 23 hours

CREATE materialized VIEW if not exists slot_producer_seen_mv
AS
SELECT slot, producer_id FROM producer_slot_seen
WHERE 
    producer_id is not null
    and slot is not null
PRIMARY KEY (slot, producer_id);


create table  if not exists last_committed_shard_period (
    producer_id blob,
    shard_id smallint,
    revision bigint,
    period bigint,
    updated_at timestamp,
    primary key ((producer_id, shard_id))
);


create table if not exists producer_period_commit_log (
    producer_id blob,
    shard_id smallint,
    period bigint,
    created_at timestamp,
    PRIMARY KEY((producer_id, shard_id), period)
) with clustering order by (period desc)
       AND default_time_to_live = 82800; -- 23 hours

-- clear all table

insert into producer_info

truncate log;
truncate producer_period_commit_log;
truncate producer_slot_seen;
truncate producer_lock;

truncate consumer_info;
truncate consumer_shard_offset;
truncate consumer_shard_offset_v2;
truncate shard_tip;