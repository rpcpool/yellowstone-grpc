pub use triton_grpc_proto::prelude::*;

pub mod convert {
    use {
        solana_sdk::{
            clock::UnixTimestamp,
            instruction::CompiledInstruction,
            message::{
                v0::{LoadedMessage, MessageAddressTableLookup},
                LegacyMessage, MessageHeader, SanitizedMessage,
            },
            pubkey::Pubkey,
            signature::Signature,
            transaction::SanitizedTransaction,
            transaction_context::TransactionReturnData,
        },
        solana_transaction_status::{
            InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta,
            TransactionTokenBalance,
        },
    };

    pub fn create_transaction(tx: &SanitizedTransaction) -> super::Transaction {
        super::Transaction {
            signatures: tx
                .signatures()
                .iter()
                .map(|signature| <Signature as AsRef<[u8]>>::as_ref(signature).into())
                .collect(),
            message: Some(create_message(tx.message())),
        }
    }

    pub fn create_message(message: &SanitizedMessage) -> super::Message {
        match message {
            SanitizedMessage::Legacy(LegacyMessage { message, .. }) => super::Message {
                header: Some(create_header(&message.header)),
                account_keys: message
                    .account_keys
                    .iter()
                    .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
                    .collect(),
                recent_blockhash: message.recent_blockhash.to_bytes().into(),
                instructions: message
                    .instructions
                    .iter()
                    .map(create_instruction)
                    .collect(),
                versioned: false,
                address_table_lookups: vec![],
            },
            SanitizedMessage::V0(LoadedMessage { message, .. }) => super::Message {
                header: Some(create_header(&message.header)),
                account_keys: message
                    .account_keys
                    .iter()
                    .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
                    .collect(),
                recent_blockhash: message.recent_blockhash.to_bytes().into(),
                instructions: message
                    .instructions
                    .iter()
                    .map(create_instruction)
                    .collect(),
                versioned: true,
                address_table_lookups: message
                    .address_table_lookups
                    .iter()
                    .map(create_lookup)
                    .collect(),
            },
        }
    }

    pub fn create_header(header: &MessageHeader) -> super::MessageHeader {
        super::MessageHeader {
            num_required_signatures: header.num_required_signatures as u32,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
        }
    }

    pub fn create_instruction(ix: &CompiledInstruction) -> super::CompiledInstruction {
        super::CompiledInstruction {
            program_id_index: ix.program_id_index as u32,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
        }
    }

    pub fn create_lookup(lookup: &MessageAddressTableLookup) -> super::MessageAddressTableLookup {
        super::MessageAddressTableLookup {
            account_key: <Pubkey as AsRef<[u8]>>::as_ref(&lookup.account_key).into(),
            writable_indexes: lookup.writable_indexes.clone(),
            readonly_indexes: lookup.readonly_indexes.clone(),
        }
    }

    pub fn create_transaction_meta(meta: &TransactionStatusMeta) -> super::TransactionStatusMeta {
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
        let err = match status {
            Ok(()) => None,
            Err(err) => Some(super::TransactionError {
                err: bincode::serialize(&err).expect("transaction error to serialize to bytes"),
            }),
        };
        let inner_instructions_none = inner_instructions.is_none();
        let inner_instructions = inner_instructions
            .as_ref()
            .map(|v| v.iter().map(create_inner_instructions).collect())
            .unwrap_or_default();
        let log_messages_none = log_messages.is_none();
        let log_messages = log_messages.clone().unwrap_or_default();
        let pre_token_balances = pre_token_balances
            .as_ref()
            .map(|v| v.iter().map(create_token_balance).collect())
            .unwrap_or_default();
        let post_token_balances = post_token_balances
            .as_ref()
            .map(|v| v.iter().map(create_token_balance).collect())
            .unwrap_or_default();
        let rewards = rewards
            .as_ref()
            .map(|vec| vec.iter().map(create_reward).collect())
            .unwrap_or_default();
        let loaded_writable_addresses = loaded_addresses
            .writable
            .iter()
            .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
            .collect();
        let loaded_readonly_addresses = loaded_addresses
            .readonly
            .iter()
            .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
            .collect();

        super::TransactionStatusMeta {
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

    pub fn create_inner_instructions(instructions: &InnerInstructions) -> super::InnerInstructions {
        super::InnerInstructions {
            index: instructions.index as u32,
            instructions: instructions
                .instructions
                .iter()
                .map(create_inner_instruction)
                .collect(),
        }
    }

    pub fn create_inner_instruction(instruction: &InnerInstruction) -> super::InnerInstruction {
        super::InnerInstruction {
            program_id_index: instruction.instruction.program_id_index as u32,
            accounts: instruction.instruction.accounts.clone(),
            data: instruction.instruction.data.clone(),
            stack_height: instruction.stack_height,
        }
    }

    pub fn create_token_balance(balance: &TransactionTokenBalance) -> super::TokenBalance {
        super::TokenBalance {
            account_index: balance.account_index as u32,
            mint: balance.mint.clone(),
            ui_token_amount: Some(super::UiTokenAmount {
                ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
                decimals: balance.ui_token_amount.decimals as u32,
                amount: balance.ui_token_amount.amount.clone(),
                ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(),
            }),
            owner: balance.owner.clone(),
            program_id: balance.program_id.clone(),
        }
    }

    pub fn create_reward(reward: &Reward) -> super::Reward {
        super::Reward {
            pubkey: reward.pubkey.clone(),
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: match reward.reward_type {
                None => super::RewardType::Unspecified,
                Some(RewardType::Fee) => super::RewardType::Fee,
                Some(RewardType::Rent) => super::RewardType::Rent,
                Some(RewardType::Staking) => super::RewardType::Staking,
                Some(RewardType::Voting) => super::RewardType::Voting,
            } as i32,
            commission: reward.commission.map(|c| c.to_string()).unwrap_or_default(),
        }
    }

    pub fn create_return_data(return_data: &TransactionReturnData) -> super::ReturnData {
        super::ReturnData {
            program_id: return_data.program_id.to_bytes().into(),
            data: return_data.data.clone(),
        }
    }

    pub fn create_rewards(rewards: &[Reward]) -> super::Rewards {
        super::Rewards {
            rewards: rewards.iter().map(create_reward).collect(),
        }
    }

    pub fn create_block_height(block_height: u64) -> super::BlockHeight {
        super::BlockHeight { block_height }
    }

    pub fn create_timestamp(timestamp: UnixTimestamp) -> super::UnixTimestamp {
        super::UnixTimestamp { timestamp }
    }
}
