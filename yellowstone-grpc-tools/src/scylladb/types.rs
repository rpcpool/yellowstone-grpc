use {
    anyhow::{anyhow, Ok},
    core::fmt,
    deepsize::DeepSizeOf,
    scylla::{
        cql_to_rust::{FromCqlVal, FromCqlValError},
        frame::response::result::CqlValue,
        serialize::value::SerializeCql,
        FromRow, FromUserType, SerializeCql, SerializeRow,
    },
    std::iter::repeat,
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeUpdateAccount, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        },
        solana::storage::confirmed_block::{self, CompiledInstruction},
    },
};

pub type ProgramId = [u8; 32];
pub type Pubkey = [u8; 32];
pub type Slot = i64;
pub type ShardId = i16;
pub type ShardPeriod = i64;
pub type ShardOffset = i64;
pub type ProducerId = [u8; 1]; // one byte is enough to assign an id to a machine
pub type ConsumerId = String;
pub const SHARD_OFFSET_MODULO: i64 = 10000;
pub const MIN_PROCUDER: ProducerId = [0x00];
pub const MAX_PRODUCER: ProducerId = [0xFF];
pub const UNDEFINED_SLOT: Slot = -1;

#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct ConsumerInfo {
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    //pub initital_shard_offsets: Vec<ConsumerShardOffset>,
    pub subscribed_blockchain_event_types: Vec<BlockchainEventType>,
}

#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct ConsumerShardOffset {
    pub consumer_id: ConsumerId,
    pub producer_id: ProducerId,
    pub shard_id: ShardId,
    pub event_type: BlockchainEventType,
    pub offset: ShardOffset,
    pub slot: Slot,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Copy, DeepSizeOf)]
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

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Copy)]
pub enum CommitmentLevel {
    Processed = 0,
    Confirmed = 1,
    Finalized = 2,
}

impl fmt::Display for CommitmentLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitmentLevel::Processed => f.write_str("Processed"),
            CommitmentLevel::Confirmed => f.write_str("Confirmed"),
            CommitmentLevel::Finalized => f.write_str("Finalized"),
        }
    }
}

impl From<CommitmentLevel> for i16 {
    fn from(val: CommitmentLevel) -> Self {
        match val {
            CommitmentLevel::Processed => 0,
            CommitmentLevel::Confirmed => 1,
            CommitmentLevel::Finalized => 2,
        }
    }
}

impl TryFrom<i16> for CommitmentLevel {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CommitmentLevel::Processed),
            1 => Ok(CommitmentLevel::Confirmed),
            2 => Ok(CommitmentLevel::Finalized),
            x => Err(anyhow!(
                "Unknown CommitmentLevel equivalent for code {:?}",
                x
            )),
        }
    }
}

impl SerializeCql for CommitmentLevel {
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

impl FromCqlVal<CqlValue> for CommitmentLevel {
    fn from_cql(cql_val: CqlValue) -> Result<Self, scylla::cql_to_rust::FromCqlValError> {
        match cql_val {
            CqlValue::SmallInt(x) => x.try_into().map_err(|_| FromCqlValError::BadVal),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

#[derive(SerializeRow, Clone, Debug, FromRow, DeepSizeOf, PartialEq)]
pub struct BlockchainEvent {
    // Common
    pub shard_id: ShardId,
    pub period: ShardPeriod,
    pub producer_id: ProducerId,
    pub offset: ShardOffset,
    pub slot: i64,
    pub event_type: BlockchainEventType,

    // AccountUpdate
    pub pubkey: Option<Pubkey>,
    pub lamports: Option<i64>,
    pub owner: Option<Pubkey>,
    pub executable: Option<bool>,
    pub rent_epoch: Option<i64>,
    pub write_version: Option<i64>,
    pub data: Option<Vec<u8>>,
    pub txn_signature: Option<Vec<u8>>,

    // Transaction
    pub signature: Option<Vec<u8>>,
    pub signatures: Option<Vec<Vec<u8>>>,
    pub num_required_signatures: Option<i32>,
    pub num_readonly_signed_accounts: Option<i32>,
    pub num_readonly_unsigned_accounts: Option<i32>,
    pub account_keys: Option<Vec<Vec<u8>>>,
    pub recent_blockhash: Option<Vec<u8>>,
    pub instructions: Option<Vec<CompiledInstr>>,
    pub versioned: Option<bool>,
    pub address_table_lookups: Option<Vec<MessageAddrTableLookup>>,
    pub meta: Option<TransactionMeta>,
    pub is_vote: Option<bool>,
    pub tx_index: Option<i64>,
}

#[derive(SerializeRow, Clone, Debug, DeepSizeOf, PartialEq, Eq)]
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

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, Eq, PartialEq)]
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

impl From<MessageAddrTableLookup> for confirmed_block::MessageAddressTableLookup {
    fn from(msg: MessageAddrTableLookup) -> Self {
        // Create a new instance of AddressLookup
        confirmed_block::MessageAddressTableLookup {
            account_key: msg.account_key,
            writable_indexes: msg.writable_indexes,
            readonly_indexes: msg.readonly_indexes,
        }
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, Eq, PartialEq)]
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

impl TryFrom<CompiledInstr> for confirmed_block::CompiledInstruction {
    type Error = anyhow::Error;

    fn try_from(value: CompiledInstr) -> Result<Self, Self::Error> {
        Ok(CompiledInstruction {
            program_id_index: value.program_id_index.try_into()?,
            accounts: value.accounts,
            data: value.data,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, Eq, PartialEq)]
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

impl TryFrom<InnerInstr> for confirmed_block::InnerInstruction {
    type Error = anyhow::Error;

    fn try_from(value: InnerInstr) -> Result<Self, Self::Error> {
        Ok(confirmed_block::InnerInstruction {
            program_id_index: value.program_id_index.try_into()?,
            accounts: value.accounts,
            data: value.data,
            stack_height: value.stack_height.map(|x| x.try_into()).transpose()?,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, Eq, PartialEq)]
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

impl TryFrom<InnerInstrs> for confirmed_block::InnerInstructions {
    type Error = anyhow::Error;

    fn try_from(value: InnerInstrs) -> Result<Self, Self::Error> {
        Ok(confirmed_block::InnerInstructions {
            index: value.index.try_into()?,
            instructions: try_collect(value.instructions)?,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, PartialEq)]
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

impl TryFrom<UiTokenAmount> for confirmed_block::UiTokenAmount {
    type Error = anyhow::Error;

    fn try_from(value: UiTokenAmount) -> Result<Self, Self::Error> {
        Ok(confirmed_block::UiTokenAmount {
            ui_amount: value.ui_amount,
            decimals: value.decimals.try_into()?,
            amount: value.amount,
            ui_amount_string: value.ui_amount_string,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, PartialEq)]
#[scylla(flavor = "match_by_name")]
pub struct TxTokenBalance {
    pub account_index: i64,
    pub mint: String,
    pub ui_token_amount: Option<UiTokenAmount>,
    pub owner: String,
    pub program_id: String,
}

impl From<confirmed_block::TokenBalance> for TxTokenBalance {
    fn from(value: confirmed_block::TokenBalance) -> Self {
        TxTokenBalance {
            account_index: value.account_index.into(),
            mint: value.mint,
            ui_token_amount: value.ui_token_amount.map(Into::into),
            owner: value.owner,
            program_id: value.program_id,
        }
    }
}

impl TryFrom<TxTokenBalance> for confirmed_block::TokenBalance {
    type Error = anyhow::Error;

    fn try_from(value: TxTokenBalance) -> Result<Self, Self::Error> {
        Ok(confirmed_block::TokenBalance {
            account_index: value.account_index.try_into()?,
            mint: value.mint,
            ui_token_amount: value.ui_token_amount.map(TryInto::try_into).transpose()?,
            owner: value.owner,
            program_id: value.program_id,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, Eq, PartialEq)]
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

impl TryFrom<Reward> for confirmed_block::Reward {
    type Error = anyhow::Error;

    fn try_from(value: Reward) -> Result<Self, Self::Error> {
        Ok(confirmed_block::Reward {
            pubkey: value.pubkey,
            lamports: value.lamports,
            post_balance: value.post_balance.try_into()?,
            reward_type: value.reward_type,
            commission: value.commission,
        })
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, PartialEq, Eq)]
#[scylla(flavor = "match_by_name")]
pub struct ReturnData {
    pub program_id: ProgramId,
    pub data: Vec<u8>,
}

impl TryFrom<confirmed_block::ReturnData> for ReturnData {
    type Error = anyhow::Error;
    fn try_from(value: confirmed_block::ReturnData) -> Result<Self, Self::Error> {
        Ok(ReturnData {
            program_id: value
                .program_id
                .try_into()
                .map_err(|e| anyhow::anyhow!("Inavlid readonly address, got: {:?}", e))?,
            data: value.data,
        })
    }
}

impl From<ReturnData> for confirmed_block::ReturnData {
    fn from(value: ReturnData) -> Self {
        confirmed_block::ReturnData {
            program_id: value.program_id.into(),
            data: value.data,
        }
    }
}

#[derive(Debug, SerializeCql, Clone, DeepSizeOf, FromUserType, Default, PartialEq)]
#[scylla(flavor = "match_by_name")]
pub struct TransactionMeta {
    pub error: Option<Vec<u8>>,
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Option<Vec<InnerInstrs>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Vec<TxTokenBalance>,
    pub post_token_balances: Vec<TxTokenBalance>,
    pub rewards: Vec<Reward>,
    pub loaded_writable_addresses: Vec<Pubkey>,
    pub loaded_readonly_addresses: Vec<Pubkey>,
    pub return_data: Option<ReturnData>,
    pub compute_units_consumed: Option<i64>,
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

        let loaded_readonly_addresses: Vec<Pubkey> =
            try_collect(status_meta.loaded_readonly_addresses)
                .map_err(|e| anyhow::anyhow!("Inavlid readonly address, got: {:?}", e))?;
        let loaded_writable_addresses = try_collect(status_meta.loaded_writable_addresses)
            .map_err(|e| anyhow::anyhow!("Inavlid readonly address, got: {:?}", e))?;

        let return_data = status_meta
            .return_data
            .map(|rd| rd.try_into())
            .transpose()?;
        let compute_units_consumed = status_meta
            .compute_units_consumed
            .map(|cu| cu.try_into())
            .transpose()?;

        // Create a new TransactionMeta instance
        let transaction_meta = TransactionMeta {
            error,
            fee,
            pre_balances,
            post_balances,
            inner_instructions: if status_meta.inner_instructions_none {
                Some(inner_instructions)
            } else {
                None
            },
            log_messages: if status_meta.log_messages_none {
                Some(log_messages)
            } else {
                None
            },
            pre_token_balances,
            post_token_balances,
            rewards,
            loaded_readonly_addresses,
            loaded_writable_addresses,
            return_data,
            compute_units_consumed,
        };

        // Return the new TransactionMeta instance
        Ok(transaction_meta)
    }
}

impl TryFrom<TransactionMeta> for confirmed_block::TransactionStatusMeta {
    type Error = anyhow::Error;

    fn try_from(value: TransactionMeta) -> Result<Self, Self::Error> {
        let inner_instructions_none = value.inner_instructions.is_none();
        let log_messages_none = value.log_messages.is_none();
        let return_data_none = value.return_data.is_none();
        Ok(confirmed_block::TransactionStatusMeta {
            err: value
                .error
                .map(|bindata| confirmed_block::TransactionError { err: bindata }),
            fee: value.fee.try_into()?,
            pre_balances: try_collect(value.pre_balances)?,
            post_balances: try_collect(value.post_balances)?,
            inner_instructions: value
                .inner_instructions
                .map(try_collect)
                .transpose()?
                .unwrap_or(Vec::new()),
            inner_instructions_none,
            log_messages: value
                .log_messages
                .map(try_collect)
                .transpose()?
                .unwrap_or(Vec::new()),
            log_messages_none,
            pre_token_balances: try_collect(value.pre_token_balances)?,
            post_token_balances: try_collect(value.post_token_balances)?,
            rewards: try_collect(value.rewards)?,
            loaded_writable_addresses: try_collect(value.loaded_writable_addresses)?,
            loaded_readonly_addresses: try_collect(value.loaded_readonly_addresses)?,
            return_data: value.return_data.map(Into::into),
            return_data_none,
            compute_units_consumed: value
                .compute_units_consumed
                .map(TryInto::try_into)
                .transpose()?,
        })
    }
}

#[derive(Debug, SerializeRow, Clone, DeepSizeOf, PartialEq)]
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
    pub is_vote: bool,
    pub tx_index: i64,
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
            is_vote: val_tx.is_vote,
            tx_index: val_tx.index as i64,
        };

        Ok(res)
    }
}

impl TryFrom<Transaction> for SubscribeUpdateTransaction {
    type Error = anyhow::Error;

    fn try_from(value: Transaction) -> Result<Self, Self::Error> {
        let ret = SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: value.signature,
                is_vote: value.is_vote,
                transaction: Some(confirmed_block::Transaction {
                    signatures: value.signatures,
                    message: Some(confirmed_block::Message {
                        header: Some(confirmed_block::MessageHeader {
                            num_required_signatures: value.num_required_signatures.try_into()?,
                            num_readonly_signed_accounts: value
                                .num_readonly_signed_accounts
                                .try_into()?,
                            num_readonly_unsigned_accounts: value
                                .num_readonly_unsigned_accounts
                                .try_into()?,
                        }),
                        account_keys: value.account_keys,
                        recent_blockhash: value.recent_blockhash,
                        instructions: try_collect(value.instructions)?,
                        versioned: value.versioned,
                        address_table_lookups: try_collect(value.address_table_lookups)?,
                    }),
                }),
                meta: Some(value.meta.try_into()).transpose()?,
                index: value.tx_index.try_into()?,
            }),
            slot: value.slot.try_into()?,
        };
        Ok(ret)
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
            event_type: BlockchainEventType::AccountUpdate,
            pubkey: Some(self.pubkey),
            lamports: Some(self.lamports),
            owner: Some(self.owner),
            executable: Some(self.executable),
            rent_epoch: Some(self.rent_epoch),
            write_version: Some(self.write_version),
            data: Some(self.data),
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
            is_vote: Default::default(),
            tx_index: Default::default(),
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
            event_type: BlockchainEventType::NewTransaction,

            pubkey: Default::default(),
            lamports: Default::default(),
            owner: Default::default(),
            executable: Default::default(),
            rent_epoch: Default::default(),
            write_version: Default::default(),
            data: Default::default(),
            txn_signature: Default::default(),

            signature: Some(self.signature),
            signatures: Some(self.signatures),
            num_required_signatures: Some(self.num_required_signatures),
            num_readonly_signed_accounts: Some(self.num_readonly_signed_accounts),
            num_readonly_unsigned_accounts: Some(self.num_readonly_unsigned_accounts),
            account_keys: Some(self.account_keys),
            recent_blockhash: Some(self.recent_blockhash),
            instructions: Some(self.instructions),
            versioned: Some(self.versioned),
            address_table_lookups: Some(self.address_table_lookups),
            meta: Some(self.meta),
            is_vote: Some(self.is_vote),
            tx_index: Some(self.tx_index),
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
    pub event_type: BlockchainEventType,

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
    pub event_type: BlockchainEventType,

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
    pub is_vote: bool,
    pub tx_index: i64,
}

// Implement Into<ShardedAccountUpdate> for BlockchainEvent
impl From<BlockchainEvent> for ShardedAccountUpdate {
    fn from(val: BlockchainEvent) -> Self {
        ShardedAccountUpdate {
            shard_id: val.shard_id,
            period: val.period,
            producer_id: val.producer_id,
            offset: val.offset,
            event_type: val.event_type,
            slot: val.slot,
            pubkey: val.pubkey.expect("pubkey is none"),
            lamports: val.lamports.expect("lamports is none"),
            owner: val.owner.expect("owner is none"),
            executable: val.executable.expect("executable is none"),
            rent_epoch: val.rent_epoch.expect("rent_epch is none"),
            write_version: val.write_version.expect("write_version is none"),
            data: val.data.expect("data is none"),
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
            event_type: val.event_type,
            slot: val.slot,
            signature: val.signature.expect("signature is none"),
            signatures: val.signatures.expect("signatures is none"),
            num_required_signatures: val
                .num_required_signatures
                .expect("num_required_signature is none"),
            num_readonly_signed_accounts: val
                .num_readonly_signed_accounts
                .expect("num_readonly_signed_accounts is none"),
            num_readonly_unsigned_accounts: val
                .num_readonly_unsigned_accounts
                .expect("num_readonly_unsigned_accounts is none"),
            account_keys: val.account_keys.expect("account_keys is none"),
            recent_blockhash: val.recent_blockhash.expect("recent_blockhash is none"),
            instructions: val.instructions.expect("instructions is none"),
            versioned: val.versioned.expect("versioned is none"),
            address_table_lookups: val
                .address_table_lookups
                .expect("address_table_lookups is none"),
            meta: val.meta.expect("meta is none"),
            is_vote: val.is_vote.expect("is_vote is none"),
            tx_index: val.tx_index.expect("tx_index is none"),
        }
    }
}

impl From<BlockchainEvent> for Transaction {
    fn from(val: BlockchainEvent) -> Self {
        Transaction {
            slot: val.slot,
            signature: val.signature.expect("signature is none"),
            signatures: val.signatures.expect("signatures is none"),
            num_required_signatures: val
                .num_required_signatures
                .expect("num_required_signature is none"),
            num_readonly_signed_accounts: val
                .num_readonly_signed_accounts
                .expect("num_readonly_signed_accounts is none"),
            num_readonly_unsigned_accounts: val
                .num_readonly_unsigned_accounts
                .expect("num_readonly_unsigned_accounts is none"),
            account_keys: val.account_keys.expect("account_keys is none"),
            recent_blockhash: val.recent_blockhash.expect("recent_blockhash is none"),
            instructions: val.instructions.expect("instructions is none"),
            versioned: val.versioned.expect("versioned is none"),
            address_table_lookups: val
                .address_table_lookups
                .expect("address_table_lookups is none"),
            meta: val.meta.expect("meta is none"),
            is_vote: val.is_vote.expect("is_vote is none"),
            tx_index: val.tx_index.expect("tx_index is none"),
        }
    }
}

impl From<BlockchainEvent> for AccountUpdate {
    fn from(val: BlockchainEvent) -> Self {
        AccountUpdate {
            slot: val.slot,
            pubkey: val.pubkey.expect("pubkey is none"),
            lamports: val.lamports.expect("lamports is none"),
            owner: val.owner.expect("owner is none"),
            executable: val.executable.expect("executable is none"),
            rent_epoch: val.rent_epoch.expect("rent_epch is none"),
            write_version: val.write_version.expect("write_version is none"),
            data: val.data.expect("data is none"),
            txn_signature: val.txn_signature,
        }
    }
}

#[derive(FromRow, Debug, Clone)]
pub struct ProducerInfo {
    pub producer_id: ProducerId,
    pub num_shards: ShardId,
    pub commitment_level: CommitmentLevel,
}

impl TryFrom<AccountUpdate> for SubscribeUpdateAccount {
    type Error = anyhow::Error;

    fn try_from(acc_update: AccountUpdate) -> anyhow::Result<Self> {
        let _pubkey_bytes: [u8; 32] = acc_update.pubkey;
        let _owner_bytes: [u8; 32] = acc_update.owner;

        // Create the SubscribeUpdateAccount instance
        let subscribe_update_account = SubscribeUpdateAccount {
            slot: acc_update.slot as u64,
            account: Some(
                yellowstone_grpc_proto::prelude::SubscribeUpdateAccountInfo {
                    pubkey: Vec::from(acc_update.pubkey),
                    lamports: acc_update.lamports as u64,
                    owner: Vec::from(acc_update.owner),
                    executable: acc_update.executable,
                    rent_epoch: acc_update.rent_epoch as u64,
                    write_version: acc_update.write_version as u64,
                    data: acc_update.data,
                    txn_signature: acc_update.txn_signature,
                },
            ),
            is_startup: false,
        };

        Ok(subscribe_update_account)
    }
}

impl TryFrom<BlockchainEvent> for SubscribeUpdateAccount {
    type Error = anyhow::Error;
    fn try_from(value: BlockchainEvent) -> Result<Self, Self::Error> {
        anyhow::ensure!(
            value.event_type == BlockchainEventType::AccountUpdate,
            "BlockchainEvent is not an AccountUpdate"
        );
        let ret: AccountUpdate = value.into();
        ret.try_into()
    }
}

impl TryFrom<BlockchainEvent> for SubscribeUpdateTransaction {
    type Error = anyhow::Error;
    fn try_from(value: BlockchainEvent) -> Result<Self, Self::Error> {
        anyhow::ensure!(
            value.event_type == BlockchainEventType::NewTransaction,
            "BlockchainEvent is not a Transaction"
        );
        let ret: Transaction = value.into();
        ret.try_into()
    }
}
