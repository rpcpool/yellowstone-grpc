use {
    anyhow::anyhow, core::fmt, scylla::{serialize::value::SerializeCql, SerializeCql}, std::{convert::Infallible, iter::repeat}, yellowstone_grpc_proto::{
        geyser::{SubscribeUpdateAccount, SubscribeUpdateTransaction},
        solana::storage::confirmed_block,
    }
};

type Pubkey = [u8; 32];

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

fn try_vec_into<U: fmt::Debug, I: IntoIterator>(
    it: I,
) -> Result<Vec<U>, <I::Item as TryInto<U>>::Error>
where
    I::Item: TryInto<U> + fmt::Debug,
    <I::Item as TryInto<U>>::Error: fmt::Debug,
{
    let mut res = Vec::new();
    for x in it.into_iter() {
        let y = x.try_into();
        if y.is_err() {
            return Err(y.unwrap_err());
        }
        res.push(y.unwrap());
    }
    Ok(res)
}

// fn try_vec_collect<T: fmt::Debug, E: fmt::Debug>(it: impl Iterator<Item=Result<T, E>>) -> Result<Vec<T>, E> {
//     let mut res = Vec::new();
//     for x in it {
//         if x.is_err() {
//             return Err(x.unwrap_err());
//         }
//         res.push(x.unwrap());
//     }
//     Ok(res)
// }

#[derive(Debug, SerializeCql)]
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

#[derive(Debug, SerializeCql)]
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

#[derive(Debug, SerializeCql)]
#[scylla(flavor = "match_by_name")]
pub struct InnerInstr {
    pub program_id_index: i64,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<i64>,
}

impl TryFrom<confirmed_block::InnerInstruction> for InnerInstr {
    type Error = anyhow::Error;

    fn try_from(value: confirmed_block::InnerInstruction) -> Result<Self, Self::Error> {
        let ret = InnerInstr {
            program_id_index: value.program_id_index.into(),
            accounts: value.accounts,
            data: value.data,
            stack_height: value
                .stack_height.map(|x| x.try_into())
                .transpose()
                .map_err( anyhow::Error::new)?
        };
        Ok(ret)
    }
}

#[derive(Debug, SerializeCql)]
#[scylla(flavor = "match_by_name")]
pub struct InnerInstrs {
    pub index: i64,
    pub instructions: Vec<InnerInstr>,
}

impl TryFrom<confirmed_block::InnerInstructions> for InnerInstrs {
    type Error = anyhow::Error;

    fn try_from(value: confirmed_block::InnerInstructions) -> Result<Self, Self::Error> {
        let mut instructions: Vec<InnerInstr> = try_vec_into(value.instructions)?;

        let index = value.index.into();
        Ok(InnerInstrs {
            index,
            instructions,
        })
    }
}

#[derive(Debug, SerializeCql)]
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

#[derive(Debug, SerializeCql)]
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

#[derive(Debug, SerializeCql)]
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

#[derive(Debug, SerializeCql)]
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

        let error = status_meta
            .err
            .map(|err| err.err);
        let fee = status_meta.fee.try_into()?;
        let pre_balances: Vec<i64> = try_vec_into(status_meta.pre_balances)?;
        let post_balances = try_vec_into(status_meta.post_balances)?;
        let inner_instructions: Vec<InnerInstrs> = try_vec_into(status_meta.inner_instructions)?;
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

        let rewards: Vec<Reward> = try_vec_into(status_meta.rewards)?;

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

#[derive(Debug, SerializeCql)]
#[scylla(flavor = "match_by_name")]
pub struct Transaction {
    pub slot: i64,
    pub signature: Vec<u8>,
    pub signatures: Vec<Vec<u8>>,
    pub num_required_signatures: i64,
    pub num_readonly_signed_accounts: i64,
    pub num_readonly_unsigned_accounts: i64,
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
        let slot: i64 = value.slot.try_into()?;

        let val_tx = value.transaction.ok_or(anyhow!("missing transaction info object"))?;

        let signature = val_tx.signature;
        let meta = val_tx.meta.ok_or(anyhow!("missing transaction status meta"))?;
        let tx = val_tx.transaction.ok_or(anyhow!("missing transaction object from transaction info"))?;
        let message = tx.message.ok_or(anyhow!("missing message object from transaction"))?;
        let message_header = message.header.ok_or(anyhow!("missing message header"))?;

        let res = Transaction {
            slot,
            signature,
            signatures: tx.signatures,
            num_readonly_signed_accounts: <i64>::try_from(
                message_header.num_readonly_signed_accounts,
            ).map_err(anyhow::Error::new)?,
            num_readonly_unsigned_accounts: <i64>::try_from(
                message_header.num_readonly_unsigned_accounts,
            ).map_err(anyhow::Error::new)?,
            num_required_signatures: <i64>::try_from(message_header.num_required_signatures)
                .map_err(anyhow::Error::new)?,
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
    #[allow(clippy::type_complexity)]
    pub fn as_row(
        self,
    ) -> (
        i64,
        Pubkey,
        i64,
        Pubkey,
        bool,
        i64,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
    ) {
        self.into()
    }

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
            let pubkey: Pubkey = acc.pubkey.try_into().map_err(|err| anyhow!("Invalid pubkey: {:?}", err))?;
            let owner: Pubkey = acc.owner.try_into().map_err(|err| anyhow!("Invalid owner: {:?}", err))?;
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
