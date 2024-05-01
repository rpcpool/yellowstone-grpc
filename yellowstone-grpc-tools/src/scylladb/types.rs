use {
    anyhow::anyhow,
    deepsize::DeepSizeOf,
    scylla::{
        cql_to_rust::{FromCqlVal, FromCqlValError},
        frame::response::result::CqlValue,
        serialize::value::SerializeCql,
        FromRow, FromUserType, SerializeCql, SerializeRow,
    },
    std::{collections::HashMap, iter::repeat},
    yellowstone_grpc_proto::{
        geyser::{SubscribeUpdateAccount, SubscribeUpdateTransaction},
        solana::storage::confirmed_block,
    },
};

pub const SHARD_OFFSET_MODULO: i64 = 10000;

pub type ShardId = i16;
pub type ShardPeriod = i64;
pub type ShardOffset = i64;

pub type ProducerId = [u8; 1]; // one byte is enough to assign an id to a machine

#[derive(SerializeRow, Clone, Debug, FromRow)]
pub(crate) struct ShardStatistics {
    pub(crate) shard_id: ShardId,
    pub(crate) period: ShardPeriod,
    pub(crate) producer_id: ProducerId,
    pub(crate) offset: ShardOffset,
    pub(crate) min_slot: i64,
    pub(crate) max_slot: i64,
    pub(crate) total_events: i64,
    pub(crate) slot_event_counter: HashMap<i64, i32>,
}

#[derive(SerializeRow, Clone, Debug, FromRow)]
pub(crate) struct ProducerInfo {
    pub(crate) producer_id: ProducerId,
    pub(crate) min_offset_per_shard: HashMap<ShardId, ShardOffset>,
}

impl ShardStatistics {
    pub(crate) fn from_slot_event_counter(
        shard_id: ShardId,
        period: ShardPeriod,
        producer_id: ProducerId,
        offset: ShardOffset,
        counter_map: &HashMap<i64, i32>,
    ) -> Self {
        let min_slot = counter_map.keys().min().copied().unwrap_or(-1);
        let max_slot = counter_map.keys().max().copied().unwrap_or(-1);
        let total_events: i64 = counter_map.values().map(|cnt| *cnt as i64).sum();
        ShardStatistics {
            shard_id,
            period,
            producer_id,
            offset,
            min_slot,
            max_slot,
            total_events,
            slot_event_counter: counter_map.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Copy, DeepSizeOf)]
pub enum BlockchainEventType {
    AccountUpdate = 0,
    NewTransaction = 1,
}

impl TryFrom<i16> for BlockchainEventType {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BlockchainEventType::AccountUpdate),
            1 => Ok(BlockchainEventType::NewTransaction),
            x => Err(anyhow!("Unknown LogEntryType equivalent for {:?}", x)),
        }
    }
}

impl From<BlockchainEventType> for i16 {
    fn from(val: BlockchainEventType) -> Self {
        match val {
            BlockchainEventType::AccountUpdate => 0,
            BlockchainEventType::NewTransaction => 1,
        }
    }
}

impl SerializeCql for BlockchainEventType {
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

impl FromCqlVal<CqlValue> for BlockchainEventType {
    fn from_cql(cql_val: CqlValue) -> Result<Self, scylla::cql_to_rust::FromCqlValError> {
        match cql_val {
            CqlValue::SmallInt(x) => x.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[derive(SerializeRow, Clone, Debug, FromRow, DeepSizeOf)]
pub struct BlockchainEvent {
    // Common
    pub shard_id: ShardId,
    pub period: ShardPeriod,
    pub producer_id: ProducerId,
    pub offset: ShardOffset,
    pub slot: i64,
    pub entry_type: BlockchainEventType,

    // AccountUpdate
    pub pubkey: Pubkey,
    pub lamports: i64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: i64,
    pub write_version: i64,
    pub data: Vec<u8>,
    pub txn_signature: Option<Vec<u8>>,

    // Transaction
    pub signature: Vec<u8>,
    pub signatures: Vec<Vec<u8>>,
    pub num_required_signatures: i32,
    pub num_readonly_signed_accounts: i32,
    pub num_readonly_unsigned_accounts: i32,
    pub account_keys: Vec<Vec<u8>>,
    pub recent_blockhash: Vec<u8>,
    pub instructions: Vec<CompiledInstr>,
    pub versioned: bool,
    pub address_table_lookups: Vec<MessageAddrTableLookup>,
    pub meta: TransactionMeta,
}

type Pubkey = [u8; 32];

#[derive(SerializeRow, Clone, Debug, DeepSizeOf)]
pub struct AccountUpdate {
    pub slot: i64,
    pub pubkey: Pubkey,
    pub lamports: i64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: i64,
    pub write_version: i64,
    pub data: Vec<u8>,
    pub txn_signature: Option<Vec<u8>>,
}

fn try_collect<U, I: IntoIterator>(it: I) -> Result<Vec<U>, <I::Item as TryInto<U>>::Error>
where
    I::Item: TryInto<U>,
{
    it.into_iter().map(|item| item.try_into()).collect()
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct MessageAddrTableLookup {
    pub account_key: Vec<u8>,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

impl From<confirmed_block::MessageAddressTableLookup> for MessageAddrTableLookup {
    fn from(msg: confirmed_block::MessageAddressTableLookup) -> Self {
        // Extract fields from MessageAddressLookup
        let account_key = msg.account_key;
        let writable_indexes = msg.writable_indexes;
        let readonly_indexes = msg.readonly_indexes;

        // Create a new instance of AddressLookup
        MessageAddrTableLookup {
            account_key,
            writable_indexes,
            readonly_indexes,
        }
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct CompiledInstr {
    pub program_id_index: i64,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

impl From<confirmed_block::CompiledInstruction> for CompiledInstr {
    fn from(compiled_instr: confirmed_block::CompiledInstruction) -> Self {
        // Extract fields from CompiledInstruction
        let program_id_index = compiled_instr.program_id_index.into();
        let accounts = compiled_instr.accounts;
        let data = compiled_instr.data;

        // Create a new instance of CompileInstr
        CompiledInstr {
            program_id_index,
            accounts,
            data,
        }

        // Return the new CompileInstr instance
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct InnerInstr {
    pub program_id_index: i64,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<i64>,
}

impl From<confirmed_block::InnerInstruction> for InnerInstr {
    fn from(value: confirmed_block::InnerInstruction) -> Self {
        InnerInstr {
            program_id_index: value.program_id_index.into(),
            accounts: value.accounts,
            data: value.data,
            stack_height: value.stack_height.map(|x| x.into()),
        }
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct InnerInstrs {
    pub index: i64,
    pub instructions: Vec<InnerInstr>,
}

impl TryFrom<confirmed_block::InnerInstructions> for InnerInstrs {
    type Error = anyhow::Error;

    fn try_from(value: confirmed_block::InnerInstructions) -> Result<Self, Self::Error> {
        let instructions: Vec<InnerInstr> = try_collect(value.instructions)?;

        let index = value.index.into();
        Ok(InnerInstrs {
            index,
            instructions,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct UiTokenAmount {
    pub ui_amount: f64,
    pub decimals: i64,
    pub amount: String,
    pub ui_amount_string: String,
}

impl From<confirmed_block::UiTokenAmount> for UiTokenAmount {
    fn from(value: confirmed_block::UiTokenAmount) -> Self {
        UiTokenAmount {
            ui_amount: value.ui_amount,
            decimals: value.decimals.into(),
            amount: value.amount,
            ui_amount_string: value.ui_amount_string,
        }
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct TxTokenBalance {
    pub account_index: i64,
    pub mint: String,
    pub ui_token_amount: Option<UiTokenAmount>,
    pub owner: String,
}

impl From<confirmed_block::TokenBalance> for TxTokenBalance {
    fn from(value: confirmed_block::TokenBalance) -> Self {
        TxTokenBalance {
            account_index: value.account_index.into(),
            mint: value.mint,
            ui_token_amount: value.ui_token_amount.map(|x| x.into()),
            owner: value.owner,
        }
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct Reward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: i64,
    pub reward_type: i32,
    pub commission: String,
}

impl TryFrom<confirmed_block::Reward> for Reward {
    type Error = anyhow::Error;
    fn try_from(value: confirmed_block::Reward) -> Result<Self, Self::Error> {
        Ok(Reward {
            pubkey: value.pubkey,
            lamports: value.lamports,
            post_balance: value.post_balance.try_into()?,
            reward_type: value.reward_type,
            commission: value.commission,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default)]
#[scylla(flavor = "match_by_name")]
pub struct TransactionMeta {
    pub error: Option<Vec<u8>>,
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Vec<InnerInstrs>,
    pub log_messages: Vec<String>,
    pub pre_token_balances: Vec<TxTokenBalance>,
    pub post_token_balances: Vec<TxTokenBalance>,
    pub rewards: Vec<Reward>,
}

impl TryFrom<confirmed_block::TransactionStatusMeta> for TransactionMeta {
    type Error = anyhow::Error;

    fn try_from(status_meta: confirmed_block::TransactionStatusMeta) -> Result<Self, Self::Error> {
        let error = status_meta.err.map(|err| err.err);
        let fee = status_meta.fee.try_into()?;
        let pre_balances: Vec<i64> = try_collect(status_meta.pre_balances)?;
        let post_balances = try_collect(status_meta.post_balances)?;
        let inner_instructions: Vec<InnerInstrs> = try_collect(status_meta.inner_instructions)?;
        let log_messages = status_meta.log_messages;

        let pre_token_balances: Vec<TxTokenBalance> = status_meta
            .pre_token_balances
            .into_iter()
            .map(|pre_tb| pre_tb.into())
            .collect();

        let post_token_balances: Vec<TxTokenBalance> = status_meta
            .post_token_balances
            .into_iter()
            .map(|pre_tb| pre_tb.into())
            .collect();

        let rewards: Vec<Reward> = try_collect(status_meta.rewards)?;

        // Create a new TransactionMeta instance
        let transaction_meta = TransactionMeta {
            error,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances,
            post_token_balances,
            rewards,
        };

        // Return the new TransactionMeta instance
        Ok(transaction_meta)
    }
}

#[derive(Debug, SerializeRow, Clone, DeepSizeOf)]
pub struct Transaction {
    pub slot: i64,
    pub signature: Vec<u8>,
    pub signatures: Vec<Vec<u8>>,
    pub num_required_signatures: i32,
    pub num_readonly_signed_accounts: i32,
    pub num_readonly_unsigned_accounts: i32,
    pub account_keys: Vec<Vec<u8>>,
    pub recent_blockhash: Vec<u8>,
    pub instructions: Vec<CompiledInstr>,
    pub versioned: bool,
    pub address_table_lookups: Vec<MessageAddrTableLookup>,
    pub meta: TransactionMeta,
}

impl TryFrom<SubscribeUpdateTransaction> for Transaction {
    type Error = anyhow::Error;

    fn try_from(value: SubscribeUpdateTransaction) -> Result<Transaction, Self::Error> {
        let slot: i64 = value.slot as i64;

        let val_tx = value
            .transaction
            .ok_or(anyhow!("missing transaction info object"))?;

        let signature = val_tx.signature;
        let meta = val_tx
            .meta
            .ok_or(anyhow!("missing transaction status meta"))?;
        let tx = val_tx
            .transaction
            .ok_or(anyhow!("missing transaction object from transaction info"))?;
        let message = tx
            .message
            .ok_or(anyhow!("missing message object from transaction"))?;
        let message_header = message.header.ok_or(anyhow!("missing message header"))?;

        let res = Transaction {
            slot,
            signature,
            signatures: tx.signatures,
            num_readonly_signed_accounts: message_header.num_readonly_signed_accounts as i32,
            num_readonly_unsigned_accounts: message_header.num_readonly_unsigned_accounts as i32,
            num_required_signatures: message_header.num_required_signatures as i32,
            account_keys: message.account_keys,
            recent_blockhash: message.recent_blockhash,
            instructions: message
                .instructions
                .into_iter()
                .map(|ci| ci.into())
                .collect(),
            versioned: message.versioned,
            address_table_lookups: message
                .address_table_lookups
                .into_iter()
                .map(|atl| atl.into())
                .collect(),
            meta: meta.try_into()?,
        };

        Ok(res)
    }
}

impl From<AccountUpdate>
    for (
        i64,
        Pubkey,
        i64,
        Pubkey,
        bool,
        i64,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
    )
{
    fn from(acc: AccountUpdate) -> Self {
        (
            acc.slot,
            acc.pubkey,
            acc.lamports,
            acc.owner,
            acc.executable,
            acc.rent_epoch,
            acc.write_version,
            acc.data,
            acc.txn_signature,
        )
    }
}

impl AccountUpdate {
    pub fn zero_account() -> Self {
        let bytes_vec: Vec<u8> = repeat(0).take(32).collect();
        let bytes_arr: [u8; 32] = bytes_vec.try_into().unwrap();
        AccountUpdate {
            slot: 0,
            pubkey: bytes_arr,
            lamports: 0,
            owner: bytes_arr,
            executable: false,
            rent_epoch: 0,
            write_version: 0,
            data: vec![],
            txn_signature: None,
        }
    }

    pub fn as_blockchain_event(
        self,
        shard_id: ShardId,
        producer_id: ProducerId,
        offset: ShardOffset,
    ) -> BlockchainEvent {
        BlockchainEvent {
            shard_id,
            period: offset / SHARD_OFFSET_MODULO,
            producer_id,
            offset,
            slot: self.slot,
            entry_type: BlockchainEventType::AccountUpdate,
            pubkey: self.pubkey,
            lamports: self.lamports,
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            write_version: self.write_version,
            data: self.data,
            txn_signature: self.txn_signature,
            signature: Default::default(),
            signatures: Default::default(),
            num_required_signatures: Default::default(),
            num_readonly_signed_accounts: Default::default(),
            num_readonly_unsigned_accounts: Default::default(),
            account_keys: Default::default(),
            recent_blockhash: Default::default(),
            instructions: Default::default(),
            versioned: Default::default(),
            address_table_lookups: Default::default(),
            meta: Default::default(),
        }
    }
}

impl TryFrom<SubscribeUpdateAccount> for AccountUpdate {
    type Error = anyhow::Error;
    fn try_from(value: SubscribeUpdateAccount) -> Result<Self, Self::Error> {
        let slot = value.slot;
        if value.account.is_none() {
            Err(anyhow!("Missing account update."))
        } else {
            let acc: yellowstone_grpc_proto::prelude::SubscribeUpdateAccountInfo =
                value.account.unwrap();
            let pubkey: Pubkey = acc
                .pubkey
                .try_into()
                .map_err(|err| anyhow!("Invalid pubkey: {:?}", err))?;
            let owner: Pubkey = acc
                .owner
                .try_into()
                .map_err(|err| anyhow!("Invalid owner: {:?}", err))?;

            let ret = AccountUpdate {
                slot: slot as i64,
                pubkey,
                lamports: acc.lamports as i64,
                owner,
                executable: acc.executable,
                rent_epoch: acc.rent_epoch as i64,
                write_version: acc.write_version as i64,
                data: acc.data,
                txn_signature: acc.txn_signature,
            };
            Ok(ret)
        }
    }
}

impl Transaction {
    pub fn as_blockchain_event(
        self,
        shard_id: ShardId,
        producer_id: ProducerId,
        offset: ShardOffset,
    ) -> BlockchainEvent {
        BlockchainEvent {
            shard_id,
            period: offset / SHARD_OFFSET_MODULO,
            producer_id,
            offset,
            slot: self.slot,
            entry_type: BlockchainEventType::NewTransaction,

            pubkey: Default::default(),
            lamports: Default::default(),
            owner: Default::default(),
            executable: Default::default(),
            rent_epoch: Default::default(),
            write_version: Default::default(),
            data: Default::default(),
            txn_signature: Default::default(),

            signature: self.signature,
            signatures: self.signatures,
            num_required_signatures: self.num_required_signatures,
            num_readonly_signed_accounts: self.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: self.num_readonly_unsigned_accounts,
            account_keys: self.account_keys,
            recent_blockhash: self.recent_blockhash,
            instructions: self.instructions,
            versioned: self.versioned,
            address_table_lookups: self.address_table_lookups,
            meta: self.meta,
        }
    }
}

#[derive(SerializeRow, Debug, Clone, DeepSizeOf)]
pub struct ShardedAccountUpdate {
    // Common
    pub shard_id: ShardId,
    pub period: ShardPeriod,
    pub producer_id: ProducerId,
    pub offset: ShardOffset,
    pub slot: i64,
    pub entry_type: BlockchainEventType,

    // AccountUpdate
    pub pubkey: Pubkey,
    pub lamports: i64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: i64,
    pub write_version: i64,
    pub data: Vec<u8>,
    pub txn_signature: Option<Vec<u8>>,
}

#[derive(SerializeRow, Debug, Clone, DeepSizeOf)]
pub struct ShardedTransaction {
    // Common
    pub shard_id: ShardId,
    pub period: ShardPeriod,
    pub producer_id: ProducerId,
    pub offset: ShardOffset,
    pub slot: i64,
    pub entry_type: BlockchainEventType,

    // Transaction
    pub signature: Vec<u8>,
    pub signatures: Vec<Vec<u8>>,
    pub num_required_signatures: i32,
    pub num_readonly_signed_accounts: i32,
    pub num_readonly_unsigned_accounts: i32,
    pub account_keys: Vec<Vec<u8>>,
    pub recent_blockhash: Vec<u8>,
    pub instructions: Vec<CompiledInstr>,
    pub versioned: bool,
    pub address_table_lookups: Vec<MessageAddrTableLookup>,
    pub meta: TransactionMeta,
}

// Implement Into<ShardedAccountUpdate> for BlockchainEvent
impl From<BlockchainEvent> for ShardedAccountUpdate {
    fn from(val: BlockchainEvent) -> Self {
        ShardedAccountUpdate {
            shard_id: val.shard_id,
            period: val.period,
            producer_id: val.producer_id,
            offset: val.offset,
            slot: val.slot,
            entry_type: val.entry_type,
            pubkey: val.pubkey,
            lamports: val.lamports,
            owner: val.owner,
            executable: val.executable,
            rent_epoch: val.rent_epoch,
            write_version: val.write_version,
            data: val.data,
            txn_signature: val.txn_signature,
        }
    }
}

// Implement Into<ShardedTransaction> for BlockchainEvent
impl From<BlockchainEvent> for ShardedTransaction {
    fn from(val: BlockchainEvent) -> Self {
        ShardedTransaction {
            shard_id: val.shard_id,
            period: val.period,
            producer_id: val.producer_id,
            offset: val.offset,
            slot: val.slot,
            entry_type: val.entry_type,
            signature: val.signature,
            signatures: val.signatures,
            num_required_signatures: val.num_required_signatures,
            num_readonly_signed_accounts: val.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: val.num_readonly_unsigned_accounts,
            account_keys: val.account_keys,
            recent_blockhash: val.recent_blockhash,
            instructions: val.instructions,
            versioned: val.versioned,
            address_table_lookups: val.address_table_lookups,
            meta: val.meta,
        }
    }
}
