CREATE KEYSPACE solana WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}  AND durable_writes = true;

drop materialized view if exists solana.producer_consumer_mapping_mv;
drop materialized view if exists solana.shard_max_offset_mv;
drop materialized view if exists solana.slot_map;

drop table if exists solana.shard_statistics;
drop table if exists solana.producer_info;
drop table if exists solana.consumer_info;
drop table if exists solana.consumer_producer_mapping;
drop table if exists solana.log;
drop type if exists solana.transaction_meta;
drop type if exists solana.message_addr_table_lookup;
drop type if exists solana.compiled_instr;
drop type if exists solana.tx_token_balance;
drop type if exists solana.reward;
drop type if exists solana.inner_instrs;
drop type if exists solana.inner_instr;
drop type if exists solana.return_data;


create table if not exists solana.consumer_info (
    consumer_id text,
    producer_id blob,
    shard_id smallint,
    event_type smallint,
    offset bigint,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((consumer_id, producer_id), shard_id, event_type)
);


create table if not exists solana.consumer_producer_mapping (
    consumer_id text,
    producer_id blob,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY (consumer_id)
);

create materialized view if not exists solana.producer_consumer_mapping_mv
as
select
    producer_id,
    consumer_id
from solana.consumer_producer_mapping
where 
    consumer_id is not null
    and producer_id is not null
primary key (producer_id, consumer_id);
    
create table if not exists solana.producer_info (
    producer_id blob,
    num_shards smallint,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY (producer_id)
);

create table if not exists solana.producer_lock (
    producer_id blob,
    lock_id text,
    ifname text,
    ipv4 text,
    created_at timestamp,
    primary key (producer_id)
)

-- # example
insert into solana.producer_info (
    producer_id, 
    num_shards, 
    created_at,
    updated_at
)
values (0x00, 256, currentTimestamp(), currentTimestamp());



create table if not exists solana.producer_period_commit_log (
    producer_id blob,
    shard_id smallint,
    period bigint,
    updated_at timestamp,
    PRIMARY KEY(producer_id, shard_id)
);

create table if not exists solana.shard_statistics (
    shard_id smallint,
    period bigint,
    producer_id blob,
    offset bigint,
    min_slot bigint,
    max_slot bigint,
    total_events bigint,
    slot_event_counter map<bigint, int>,
    primary key(shard_id, period, offset)
) with CLUSTERING order by (period desc, offset desc);

create type if not exists solana.message_addr_table_lookup (
    account_key blob,
    writable_indexes blob,
    readonly_indexes blob
);

create type if not exists solana.compiled_instr (
    program_id_index bigint,
    accounts blob,
    data blob
);

create type if not exists solana.inner_instr (
    program_id_index bigint,
    accounts blob,
    data blob,
    stack_height bigint
);

create type if not exists solana.inner_instrs (
    "index" bigint,
    instructions frozen<list<solana.inner_instr>>
);

create type if not exists solana.ui_token_amount (
    ui_amount double,
    decimals bigint,
    amount text,
    ui_amount_string text
);

create type if not exists solana.tx_token_balance (
    account_index bigint,
    mint text,  --varchar(44)
    ui_token_amount frozen<solana.ui_token_amount>,
    owner text, --varchar(44)
    program_id text,
);

create type if not exists solana.reward (
    pubkey text,    -- varchar(44)
    lamports bigint,
    post_balance bigint,
    reward_type int,   --Fee,  Rent, Staking, Voting
    commission text
);

create type if not exists solana.return_data (
    program_id blob,
    data blob
);

create type if not exists solana.transaction_meta (
    error blob,
    fee bigint,
    pre_balances frozen<list<bigint>>,
    post_balances frozen<list<bigint>>,
    inner_instructions frozen<list<solana.inner_instrs>>,
    log_messages frozen<list<text>>,
    pre_token_balances frozen<list<solana.tx_token_balance>>,
    post_token_balances frozen<list<solana.tx_token_balance>>,
    rewards frozen<list<solana.reward>>,
    loaded_writable_addresses frozen<list<blob>>,
    loaded_readonly_addresses frozen<list<blob>>,
    return_data frozen<solana.return_data>,
    compute_units_consumed bigint
);


-- ScyllaDB table can hold different kind of entities at the same time.
-- There is not performance advantage to have separate tables since ScyllaDB is wide-column family database.
-- ScyllaDB is built to have sparse columns (alot of unused columns)
-- On each query, the storage engine only retrieves what matters to the query.
create table if not exists solana.log (

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
    instructions frozen<list<solana.compiled_instr>>,
    versioned boolean,
    address_table_lookups frozen<list<solana.message_addr_table_lookup>>,
    meta solana.transaction_meta,
    is_vote boolean,
    tx_index bigint,


    -- meta data field for debugging purposes
    created_at timestamp,

    primary key ((shard_id, period, producer_id), offset)
)
WITH CLUSTERING ORDER BY (offset desc);


create materialized view if not exists solana.slot_map_mv
as
select
    slot,
    producer_id,
    shard_id,
    period,
    offset
from solana.log
where
    slot is not null
    and producer_id is not null
    and shard_id is not null
    and period is not null
    and offset is not null
primary key (slot, producer_id, shard_id, period, offset);