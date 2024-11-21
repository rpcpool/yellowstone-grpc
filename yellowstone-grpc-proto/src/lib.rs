#![allow(clippy::large_enum_variant)]

pub mod geyser {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    #[cfg(feature = "tonic")]
    include!(concat!(env!("OUT_DIR"), "/geyser.rs"));
    #[cfg(not(feature = "tonic"))]
    include!(concat!(env!("OUT_DIR"), "/no-tonic/geyser.rs"));
}

pub mod solana {
    #![allow(clippy::missing_const_for_fn)]

    pub mod storage {
        pub mod confirmed_block {
            #[cfg(feature = "tonic")]
            include!(concat!(
                env!("OUT_DIR"),
                "/solana.storage.confirmed_block.rs"
            ));
            #[cfg(not(feature = "tonic"))]
            include!(concat!(
                env!("OUT_DIR"),
                "/no-tonic/solana.storage.confirmed_block.rs"
            ));
        }
    }
}

pub mod prelude {
    pub use super::{geyser::*, solana::storage::confirmed_block::*};
}

pub use prost;
#[cfg(feature = "tonic")]
pub use tonic;

#[cfg(feature = "plugin")]
pub mod plugin;

#[cfg(feature = "convert")]
pub mod convert_to {
    use {
        super::prelude as proto,
        solana_sdk::{
            clock::UnixTimestamp,
            instruction::CompiledInstruction,
            message::{
                v0::{LoadedMessage, MessageAddressTableLookup},
                LegacyMessage, MessageHeader, SanitizedMessage,
            },
            pubkey::Pubkey,
            signature::Signature,
            transaction::{SanitizedTransaction, TransactionError},
            transaction_context::TransactionReturnData,
        },
        solana_transaction_status::{
            InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta,
            TransactionTokenBalance,
        },
    };

    pub fn create_transaction(tx: &SanitizedTransaction) -> proto::Transaction {
        proto::Transaction {
            signatures: tx
                .signatures()
                .iter()
                .map(|signature| <Signature as AsRef<[u8]>>::as_ref(signature).into())
                .collect(),
            message: Some(create_message(tx.message())),
        }
    }

    pub fn create_message(message: &SanitizedMessage) -> proto::Message {
        match message {
            SanitizedMessage::Legacy(LegacyMessage { message, .. }) => proto::Message {
                header: Some(create_header(&message.header)),
                account_keys: create_pubkeys(&message.account_keys),
                recent_blockhash: message.recent_blockhash.to_bytes().into(),
                instructions: create_instructions(&message.instructions),
                versioned: false,
                address_table_lookups: vec![],
            },
            SanitizedMessage::V0(LoadedMessage { message, .. }) => proto::Message {
                header: Some(create_header(&message.header)),
                account_keys: create_pubkeys(&message.account_keys),
                recent_blockhash: message.recent_blockhash.to_bytes().into(),
                instructions: create_instructions(&message.instructions),
                versioned: true,
                address_table_lookups: create_lookups(&message.address_table_lookups),
            },
        }
    }

    pub const fn create_header(header: &MessageHeader) -> proto::MessageHeader {
        proto::MessageHeader {
            num_required_signatures: header.num_required_signatures as u32,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
        }
    }

    pub fn create_pubkeys(pubkeys: &[Pubkey]) -> Vec<Vec<u8>> {
        pubkeys
            .iter()
            .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
            .collect()
    }

    pub fn create_instructions(ixs: &[CompiledInstruction]) -> Vec<proto::CompiledInstruction> {
        ixs.iter().map(create_instruction).collect()
    }

    pub fn create_instruction(ix: &CompiledInstruction) -> proto::CompiledInstruction {
        proto::CompiledInstruction {
            program_id_index: ix.program_id_index as u32,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
        }
    }

    pub fn create_lookups(
        lookups: &[MessageAddressTableLookup],
    ) -> Vec<proto::MessageAddressTableLookup> {
        lookups.iter().map(create_lookup).collect()
    }

    pub fn create_lookup(lookup: &MessageAddressTableLookup) -> proto::MessageAddressTableLookup {
        proto::MessageAddressTableLookup {
            account_key: <Pubkey as AsRef<[u8]>>::as_ref(&lookup.account_key).into(),
            writable_indexes: lookup.writable_indexes.clone(),
            readonly_indexes: lookup.readonly_indexes.clone(),
        }
    }

    pub fn create_transaction_meta(meta: &TransactionStatusMeta) -> proto::TransactionStatusMeta {
        let TransactionStatusMeta {
            status,
            fee,
            pre_balances,
            post_balances,
            inner_instructions,
            log_messages,
            pre_token_balances,
            post_token_balances,
            rewards,
            loaded_addresses,
            return_data,
            compute_units_consumed,
        } = meta;
        let err = create_transaction_error(status);
        let inner_instructions_none = inner_instructions.is_none();
        let inner_instructions = inner_instructions
            .as_deref()
            .map(create_inner_instructions_vec)
            .unwrap_or_default();
        let log_messages_none = log_messages.is_none();
        let log_messages = log_messages.clone().unwrap_or_default();
        let pre_token_balances = pre_token_balances
            .as_deref()
            .map(create_token_balances)
            .unwrap_or_default();
        let post_token_balances = post_token_balances
            .as_deref()
            .map(create_token_balances)
            .unwrap_or_default();
        let rewards = rewards.as_deref().map(create_rewards).unwrap_or_default();
        let loaded_writable_addresses = create_pubkeys(&loaded_addresses.writable);
        let loaded_readonly_addresses = create_pubkeys(&loaded_addresses.readonly);

        proto::TransactionStatusMeta {
            err,
            fee: *fee,
            pre_balances: pre_balances.clone(),
            post_balances: post_balances.clone(),
            inner_instructions,
            inner_instructions_none,
            log_messages,
            log_messages_none,
            pre_token_balances,
            post_token_balances,
            rewards,
            loaded_writable_addresses,
            loaded_readonly_addresses,
            return_data: return_data.as_ref().map(create_return_data),
            return_data_none: return_data.is_none(),
            compute_units_consumed: *compute_units_consumed,
        }
    }

    pub fn create_transaction_error(
        status: &Result<(), TransactionError>,
    ) -> Option<proto::TransactionError> {
        match status {
            Ok(()) => None,
            Err(err) => Some(proto::TransactionError {
                err: bincode::serialize(&err).expect("transaction error to serialize to bytes"),
            }),
        }
    }

    pub fn create_inner_instructions_vec(
        ixs: &[InnerInstructions],
    ) -> Vec<proto::InnerInstructions> {
        ixs.iter().map(create_inner_instructions).collect()
    }

    pub fn create_inner_instructions(instructions: &InnerInstructions) -> proto::InnerInstructions {
        proto::InnerInstructions {
            index: instructions.index as u32,
            instructions: create_inner_instruction_vec(&instructions.instructions),
        }
    }

    pub fn create_inner_instruction_vec(ixs: &[InnerInstruction]) -> Vec<proto::InnerInstruction> {
        ixs.iter().map(create_inner_instruction).collect()
    }

    pub fn create_inner_instruction(instruction: &InnerInstruction) -> proto::InnerInstruction {
        proto::InnerInstruction {
            program_id_index: instruction.instruction.program_id_index as u32,
            accounts: instruction.instruction.accounts.clone(),
            data: instruction.instruction.data.clone(),
            stack_height: instruction.stack_height,
        }
    }

    pub fn create_token_balances(balances: &[TransactionTokenBalance]) -> Vec<proto::TokenBalance> {
        balances.iter().map(create_token_balance).collect()
    }

    pub fn create_token_balance(balance: &TransactionTokenBalance) -> proto::TokenBalance {
        proto::TokenBalance {
            account_index: balance.account_index as u32,
            mint: balance.mint.clone(),
            ui_token_amount: Some(proto::UiTokenAmount {
                ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
                decimals: balance.ui_token_amount.decimals as u32,
                amount: balance.ui_token_amount.amount.clone(),
                ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(),
            }),
            owner: balance.owner.clone(),
            program_id: balance.program_id.clone(),
        }
    }

    pub fn create_rewards_obj(rewards: &[Reward], num_partitions: Option<u64>) -> proto::Rewards {
        proto::Rewards {
            rewards: create_rewards(rewards),
            num_partitions: num_partitions.map(create_num_partitions),
        }
    }

    pub fn create_rewards(rewards: &[Reward]) -> Vec<proto::Reward> {
        rewards.iter().map(create_reward).collect()
    }

    pub fn create_reward(reward: &Reward) -> proto::Reward {
        proto::Reward {
            pubkey: reward.pubkey.clone(),
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: create_reward_type(reward.reward_type) as i32,
            commission: reward.commission.map(|c| c.to_string()).unwrap_or_default(),
        }
    }

    pub const fn create_reward_type(reward_type: Option<RewardType>) -> proto::RewardType {
        match reward_type {
            None => proto::RewardType::Unspecified,
            Some(RewardType::Fee) => proto::RewardType::Fee,
            Some(RewardType::Rent) => proto::RewardType::Rent,
            Some(RewardType::Staking) => proto::RewardType::Staking,
            Some(RewardType::Voting) => proto::RewardType::Voting,
        }
    }

    pub const fn create_num_partitions(num_partitions: u64) -> proto::NumPartitions {
        proto::NumPartitions { num_partitions }
    }

    pub fn create_return_data(return_data: &TransactionReturnData) -> proto::ReturnData {
        proto::ReturnData {
            program_id: return_data.program_id.to_bytes().into(),
            data: return_data.data.clone(),
        }
    }

    pub const fn create_block_height(block_height: u64) -> proto::BlockHeight {
        proto::BlockHeight { block_height }
    }

    pub const fn create_timestamp(timestamp: UnixTimestamp) -> proto::UnixTimestamp {
        proto::UnixTimestamp { timestamp }
    }
}

#[cfg(feature = "convert")]
pub mod convert_from {
    use {
        super::prelude as proto,
        solana_account_decoder::parse_token::UiTokenAmount,
        solana_sdk::{
            account::Account,
            hash::{Hash, HASH_BYTES},
            instruction::CompiledInstruction,
            message::{
                v0::{LoadedAddresses, Message as MessageV0, MessageAddressTableLookup},
                Message, MessageHeader, VersionedMessage,
            },
            pubkey::Pubkey,
            signature::Signature,
            transaction::{TransactionError, VersionedTransaction},
            transaction_context::TransactionReturnData,
        },
        solana_transaction_status::{
            ConfirmedBlock, InnerInstruction, InnerInstructions, Reward, RewardType,
            RewardsAndNumPartitions, TransactionStatusMeta, TransactionTokenBalance,
            TransactionWithStatusMeta, VersionedTransactionWithStatusMeta,
        },
    };

    type CreateResult<T> = Result<T, &'static str>;

    pub fn create_block(block: proto::SubscribeUpdateBlock) -> CreateResult<ConfirmedBlock> {
        let mut transactions = vec![];
        for tx in block.transactions {
            transactions.push(create_tx_with_meta(tx)?);
        }

        let block_rewards = block.rewards.ok_or("failed to get rewards")?;
        let mut rewards = vec![];
        for reward in block_rewards.rewards {
            rewards.push(create_reward(reward)?);
        }

        Ok(ConfirmedBlock {
            previous_blockhash: block.parent_blockhash,
            blockhash: block.blockhash,
            parent_slot: block.parent_slot,
            transactions,
            rewards,
            num_partitions: block_rewards.num_partitions.map(|msg| msg.num_partitions),
            block_time: Some(
                block
                    .block_time
                    .map(|wrapper| wrapper.timestamp)
                    .ok_or("failed to get block_time")?,
            ),
            block_height: Some(
                block
                    .block_height
                    .map(|wrapper| wrapper.block_height)
                    .ok_or("failed to get block_height")?,
            ),
        })
    }

    pub fn create_tx_with_meta(
        tx: proto::SubscribeUpdateTransactionInfo,
    ) -> CreateResult<TransactionWithStatusMeta> {
        let meta = tx.meta.ok_or("failed to get transaction meta")?;
        let tx = tx
            .transaction
            .ok_or("failed to get transaction transaction")?;

        Ok(TransactionWithStatusMeta::Complete(
            VersionedTransactionWithStatusMeta {
                transaction: create_tx_versioned(tx)?,
                meta: create_tx_meta(meta)?,
            },
        ))
    }

    pub fn create_tx_versioned(tx: proto::Transaction) -> CreateResult<VersionedTransaction> {
        let mut signatures = Vec::with_capacity(tx.signatures.len());
        for signature in tx.signatures {
            signatures.push(match Signature::try_from(signature.as_slice()) {
                Ok(signature) => signature,
                Err(_error) => return Err("failed to parse Signature"),
            });
        }

        Ok(VersionedTransaction {
            signatures,
            message: create_message(tx.message.ok_or("failed to get message")?)?,
        })
    }

    pub fn create_message(message: proto::Message) -> CreateResult<VersionedMessage> {
        let header = message.header.ok_or("failed to get MessageHeader")?;
        let header = MessageHeader {
            num_required_signatures: header
                .num_required_signatures
                .try_into()
                .map_err(|_| "failed to parse num_required_signatures")?,
            num_readonly_signed_accounts: header
                .num_readonly_signed_accounts
                .try_into()
                .map_err(|_| "failed to parse num_readonly_signed_accounts")?,
            num_readonly_unsigned_accounts: header
                .num_readonly_unsigned_accounts
                .try_into()
                .map_err(|_| "failed to parse num_readonly_unsigned_accounts")?,
        };

        if message.recent_blockhash.len() != HASH_BYTES {
            return Err("failed to parse hash");
        }

        Ok(if message.versioned {
            let mut address_table_lookups = Vec::with_capacity(message.address_table_lookups.len());
            for table in message.address_table_lookups {
                address_table_lookups.push(MessageAddressTableLookup {
                    account_key: Pubkey::try_from(table.account_key.as_slice())
                        .map_err(|_| "failed to parse Pubkey")?,
                    writable_indexes: table.writable_indexes,
                    readonly_indexes: table.readonly_indexes,
                });
            }

            VersionedMessage::V0(MessageV0 {
                header,
                account_keys: create_pubkey_vec(message.account_keys)?,
                recent_blockhash: Hash::new(message.recent_blockhash.as_slice()),
                instructions: create_message_instructions(message.instructions)?,
                address_table_lookups,
            })
        } else {
            VersionedMessage::Legacy(Message {
                header,
                account_keys: create_pubkey_vec(message.account_keys)?,
                recent_blockhash: Hash::new(message.recent_blockhash.as_slice()),
                instructions: create_message_instructions(message.instructions)?,
            })
        })
    }

    pub fn create_message_instructions(
        ixs: Vec<proto::CompiledInstruction>,
    ) -> CreateResult<Vec<CompiledInstruction>> {
        ixs.into_iter().map(create_message_instruction).collect()
    }

    pub fn create_message_instruction(
        ix: proto::CompiledInstruction,
    ) -> CreateResult<CompiledInstruction> {
        Ok(CompiledInstruction {
            program_id_index: ix
                .program_id_index
                .try_into()
                .map_err(|_| "failed to decode CompiledInstruction.program_id_index)")?,
            accounts: ix.accounts,
            data: ix.data,
        })
    }

    pub fn create_tx_meta(
        meta: proto::TransactionStatusMeta,
    ) -> CreateResult<TransactionStatusMeta> {
        let meta_status = match create_tx_error(meta.err.as_ref())? {
            Some(err) => Err(err),
            None => Ok(()),
        };
        let meta_rewards = meta
            .rewards
            .into_iter()
            .map(create_reward)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TransactionStatusMeta {
            status: meta_status,
            fee: meta.fee,
            pre_balances: meta.pre_balances,
            post_balances: meta.post_balances,
            inner_instructions: Some(create_meta_inner_instructions(meta.inner_instructions)?),
            log_messages: Some(meta.log_messages),
            pre_token_balances: Some(create_token_balances(meta.pre_token_balances)?),
            post_token_balances: Some(create_token_balances(meta.post_token_balances)?),
            rewards: Some(meta_rewards),
            loaded_addresses: create_loaded_addresses(
                meta.loaded_writable_addresses,
                meta.loaded_readonly_addresses,
            )?,
            return_data: if meta.return_data_none {
                None
            } else {
                let data = meta.return_data.ok_or("failed to get return_data")?;
                Some(TransactionReturnData {
                    program_id: Pubkey::try_from(data.program_id.as_slice())
                        .map_err(|_| "failed to parse program_id")?,
                    data: data.data,
                })
            },
            compute_units_consumed: meta.compute_units_consumed,
        })
    }

    pub fn create_tx_error(
        err: Option<&proto::TransactionError>,
    ) -> CreateResult<Option<TransactionError>> {
        err.map(|err| bincode::deserialize::<TransactionError>(&err.err))
            .transpose()
            .map_err(|_| "failed to decode TransactionError")
    }

    pub fn create_meta_inner_instructions(
        ixs: Vec<proto::InnerInstructions>,
    ) -> CreateResult<Vec<InnerInstructions>> {
        ixs.into_iter().map(create_meta_inner_instruction).collect()
    }

    pub fn create_meta_inner_instruction(
        ix: proto::InnerInstructions,
    ) -> CreateResult<InnerInstructions> {
        let mut instructions = vec![];
        for ix in ix.instructions {
            instructions.push(InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: ix
                        .program_id_index
                        .try_into()
                        .map_err(|_| "failed to decode CompiledInstruction.program_id_index)")?,
                    accounts: ix.accounts,
                    data: ix.data,
                },
                stack_height: ix.stack_height,
            });
        }
        Ok(InnerInstructions {
            index: ix
                .index
                .try_into()
                .map_err(|_| "failed to decode InnerInstructions.index")?,
            instructions,
        })
    }

    pub fn create_rewards_obj(rewards: proto::Rewards) -> CreateResult<RewardsAndNumPartitions> {
        Ok(RewardsAndNumPartitions {
            rewards: rewards
                .rewards
                .into_iter()
                .map(create_reward)
                .collect::<Result<_, _>>()?,
            num_partitions: rewards.num_partitions.map(|wrapper| wrapper.num_partitions),
        })
    }

    pub fn create_reward(reward: proto::Reward) -> CreateResult<Reward> {
        Ok(Reward {
            pubkey: reward.pubkey,
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: match proto::RewardType::try_from(reward.reward_type)
                .map_err(|_| "failed to parse reward_type")?
            {
                proto::RewardType::Unspecified => None,
                proto::RewardType::Fee => Some(RewardType::Fee),
                proto::RewardType::Rent => Some(RewardType::Rent),
                proto::RewardType::Staking => Some(RewardType::Staking),
                proto::RewardType::Voting => Some(RewardType::Voting),
            },
            commission: if reward.commission.is_empty() {
                None
            } else {
                Some(
                    reward
                        .commission
                        .parse()
                        .map_err(|_| "failed to parse reward commission")?,
                )
            },
        })
    }

    pub fn create_token_balances(
        balances: Vec<proto::TokenBalance>,
    ) -> CreateResult<Vec<TransactionTokenBalance>> {
        let mut vec = Vec::with_capacity(balances.len());
        for balance in balances {
            let ui_amount = balance
                .ui_token_amount
                .ok_or("failed to get ui_token_amount")?;
            vec.push(TransactionTokenBalance {
                account_index: balance
                    .account_index
                    .try_into()
                    .map_err(|_| "failed to parse account_index")?,
                mint: balance.mint,
                ui_token_amount: UiTokenAmount {
                    ui_amount: Some(ui_amount.ui_amount),
                    decimals: ui_amount
                        .decimals
                        .try_into()
                        .map_err(|_| "failed to parse decimals")?,
                    amount: ui_amount.amount,
                    ui_amount_string: ui_amount.ui_amount_string,
                },
                owner: balance.owner,
                program_id: balance.program_id,
            });
        }
        Ok(vec)
    }

    pub fn create_loaded_addresses(
        writable: Vec<Vec<u8>>,
        readonly: Vec<Vec<u8>>,
    ) -> CreateResult<LoadedAddresses> {
        Ok(LoadedAddresses {
            writable: create_pubkey_vec(writable)?,
            readonly: create_pubkey_vec(readonly)?,
        })
    }

    pub fn create_pubkey_vec(pubkeys: Vec<Vec<u8>>) -> CreateResult<Vec<Pubkey>> {
        pubkeys
            .iter()
            .map(|pubkey| create_pubkey(pubkey.as_slice()))
            .collect()
    }

    pub fn create_pubkey(pubkey: &[u8]) -> CreateResult<Pubkey> {
        Pubkey::try_from(pubkey).map_err(|_| "failed to parse Pubkey")
    }

    pub fn create_account(
        account: proto::SubscribeUpdateAccountInfo,
    ) -> CreateResult<(Pubkey, Account)> {
        let pubkey = create_pubkey(&account.pubkey)?;
        let account = Account {
            lamports: account.lamports,
            data: account.data,
            owner: create_pubkey(&account.owner)?,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        };
        Ok((pubkey, account))
    }
}
