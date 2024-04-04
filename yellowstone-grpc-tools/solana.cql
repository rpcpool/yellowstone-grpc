-- message SubscribeUpdateAccount {
--   SubscribeUpdateAccountInfo account = 1;
--   uint64 slot = 2;
--   bool is_startup = 3;
-- }

-- message SubscribeUpdateAccountInfo {
--   bytes pubkey = 1;
--   uint64 lamports = 2;
--   bytes owner = 3;
--   bool executable = 4;
--   uint64 rent_epoch = 5;
--   bytes data = 6;
--   uint64 write_version = 7;
--   optional bytes txn_signature = 8;
-- }


-- ## transaction

-- message SubscribeUpdateTransaction {
--   SubscribeUpdateTransactionInfo transaction = 1;
--   uint64 slot = 2;
-- }

-- message Transaction {
--     repeated bytes signatures = 1;
--     Message message = 2;
-- }

-- message Message {
--     MessageHeader header = 1;
--     repeated bytes account_keys = 2;
--     bytes recent_blockhash = 3;
--     repeated CompiledInstruction instructions = 4;
--     bool versioned = 5;
--     repeated MessageAddressTableLookup address_table_lookups = 6;
-- }

-- message MessageHeader {
--     uint32 num_required_signatures = 1;
--     uint32 num_readonly_signed_accounts = 2;
--     uint32 num_readonly_unsigned_accounts = 3;
-- }

-- message MessageAddressTableLookup {
--     bytes account_key = 1;
--     bytes writable_indexes = 2;
--     bytes readonly_indexes = 3;
-- }

-- message SubscribeUpdateTransactionInfo {
--   bytes signature = 1;
--   bool is_vote = 2;
--   solana.storage.ConfirmedBlock.Transaction transaction = 3;
--   solana.storage.ConfirmedBlock.TransactionStatusMeta meta = 4;
--   uint64 index = 5;
-- }

CREATE KEYSPACE solana WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}  AND durable_writes = true;

create table if not exists solana.account_update_log (
    slot bigint,
    pubkey blob,
    lamports bigint,
    owner blob,
    executable boolean,
    rent_epoch bigint,
    write_version bigint,
    data blob,
    txn_signature blob,
    primary key ((slot, pubkey), write_version)
) WITH CLUSTERING ORDER BY (write_version DESC);

-- create table if not exists solana.transaction_log (
--     signatures list<blob>
--     pubkey blob,
--     lamports bigint,
--     owner blob,
--     executable boolean,
--     rent_epoch bigint,
--     uint64 write_version = 7,
--     data blobk,
--     txn_signature blob
-- );