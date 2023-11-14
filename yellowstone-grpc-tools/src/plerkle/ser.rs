use {
    chrono::Utc,
    flatbuffers::FlatBufferBuilder,
    plerkle_serialization::{
        AccountInfo, AccountInfoArgs, BlockInfo, BlockInfoArgs, CompiledInnerInstruction,
        CompiledInnerInstructionArgs, CompiledInnerInstructions, CompiledInnerInstructionsArgs,
        CompiledInstruction, CompiledInstructionArgs, Pubkey, TransactionInfo, TransactionInfoArgs,
        TransactionVersion,
    },
    solana_sdk::signature::Signature,
    yellowstone_grpc_proto::prelude::{
        SubscribeUpdateAccount, SubscribeUpdateBlockMeta, SubscribeUpdateTransaction,
    },
};

pub fn serialize_account(
    builder: &mut FlatBufferBuilder<'_>,
    SubscribeUpdateAccount {
        account,
        slot,
        is_startup,
    }: SubscribeUpdateAccount,
) {
    let account = account.expect("failed to get an account");

    // Serialize vector data.
    let pubkey: Pubkey = account.pubkey.as_slice().into();
    let owner: Pubkey = account.owner.as_slice().into();
    let data = builder.create_vector(account.data.as_slice());

    // Serialize everything into Account Info table.
    let seen_at = Utc::now();
    let account_info = AccountInfo::create(
        builder,
        &AccountInfoArgs {
            pubkey: Some(&pubkey),
            lamports: account.lamports,
            owner: Some(&owner),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: Some(data),
            write_version: account.write_version,
            slot,
            is_startup,
            seen_at: seen_at.timestamp_millis(),
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(account_info, None);
}

pub fn serialize_transaction(
    builder: &mut FlatBufferBuilder<'_>,
    SubscribeUpdateTransaction { transaction, slot }: SubscribeUpdateTransaction,
) {
    let transaction_info = transaction.expect("failed to get transaction info");
    let transaction = transaction_info
        .transaction
        .expect("failed to get transaction");
    let message = transaction.message.expect("failed to get message");
    let meta = transaction_info.meta.expect("failed to get meta");

    // Flatten and serialize account keys.
    let account_keys = {
        let mut account_keys_fb_vec = vec![];
        for key in message.account_keys {
            let key: Pubkey = key.as_slice().into();
            account_keys_fb_vec.push(key);
        }

        for key in meta.loaded_writable_addresses {
            let pubkey: Pubkey = key.as_slice().into();
            account_keys_fb_vec.push(pubkey);
        }

        for key in meta.loaded_readonly_addresses {
            let pubkey: Pubkey = key.as_slice().into();
            account_keys_fb_vec.push(pubkey);
        }

        if !account_keys_fb_vec.is_empty() {
            Some(builder.create_vector(&account_keys_fb_vec))
        } else {
            None
        }
    };

    // Serialize log messages.
    let log_messages = if meta.log_messages_none {
        None
    } else {
        let mut log_messages_fb_vec = Vec::with_capacity(meta.log_messages.len());
        for message in meta.log_messages {
            log_messages_fb_vec.push(builder.create_string(&message));
        }
        Some(builder.create_vector(&log_messages_fb_vec))
    };

    // Serialize inner instructions.
    let inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        let mut overall_fb_vec = Vec::with_capacity(meta.inner_instructions.len());
        for inner_instructions in meta.inner_instructions {
            let index = inner_instructions
                .index
                .try_into()
                .expect("failed to convert program_id_index");
            let mut instructions_fb_vec = Vec::with_capacity(inner_instructions.instructions.len());
            for inner_instruction in inner_instructions.instructions {
                let program_id_index = inner_instruction
                    .program_id_index
                    .try_into()
                    .expect("failed to convert program_id_index");
                let accounts = Some(builder.create_vector(&inner_instruction.accounts));
                let data = Some(builder.create_vector(&inner_instruction.data));
                let compiled = CompiledInstruction::create(
                    builder,
                    &CompiledInstructionArgs {
                        program_id_index,
                        accounts,
                        data,
                    },
                );
                instructions_fb_vec.push(CompiledInnerInstruction::create(
                    builder,
                    &CompiledInnerInstructionArgs {
                        compiled_instruction: Some(compiled),
                        stack_height: 0, // Desperatley need this when it comes in 1.15
                    },
                ));
            }

            let instructions = Some(builder.create_vector(&instructions_fb_vec));
            overall_fb_vec.push(CompiledInnerInstructions::create(
                builder,
                &CompiledInnerInstructionsArgs {
                    index,
                    instructions,
                },
            ))
        }

        Some(builder.create_vector(&overall_fb_vec))
    };
    // let message = transaction_info.transaction.message();
    // let version = match message {
    //     SanitizedMessage::Legacy(_) => TransactionVersion::Legacy,
    //     SanitizedMessage::V0(_) => TransactionVersion::V0,
    // };
    let version = if message.versioned {
        TransactionVersion::V0
    } else {
        TransactionVersion::Legacy
    };

    // Serialize outer instructions.
    let outer_instructions = message.instructions;
    let outer_instructions = if !outer_instructions.is_empty() {
        let mut instructions_fb_vec = Vec::with_capacity(outer_instructions.len());
        for compiled_instruction in outer_instructions.iter() {
            let program_id_index = compiled_instruction
                .program_id_index
                .try_into()
                .expect("failed to convert program_id_index");
            let accounts = Some(builder.create_vector(&compiled_instruction.accounts));
            let data = Some(builder.create_vector(&compiled_instruction.data));
            instructions_fb_vec.push(CompiledInstruction::create(
                builder,
                &CompiledInstructionArgs {
                    program_id_index,
                    accounts,
                    data,
                },
            ));
        }
        Some(builder.create_vector(&instructions_fb_vec))
    } else {
        None
    };
    let seen_at = Utc::now();
    let sig = Signature::try_from(transaction_info.signature).expect("failed to get signature");
    let signature_offset = builder.create_string(&sig.to_string());
    let slot_idx = format!("{}_{}", slot, transaction_info.index);
    let slot_index_offset = builder.create_string(&slot_idx);
    // Serialize everything into Transaction Info table.
    let transaction_info_ser = TransactionInfo::create(
        builder,
        &TransactionInfoArgs {
            is_vote: transaction_info.is_vote,
            account_keys,
            log_messages,
            inner_instructions: None,
            outer_instructions,
            slot,
            slot_index: Some(slot_index_offset),
            seen_at: seen_at.timestamp_millis(),
            signature: Some(signature_offset),
            compiled_inner_instructions: inner_instructions,
            version,
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(transaction_info_ser, None);
}

pub fn serialize_block(builder: &mut FlatBufferBuilder<'_>, block_meta: SubscribeUpdateBlockMeta) {
    // Serialize blockash.
    let blockhash = Some(builder.create_string(&block_meta.blockhash));

    // Serialize rewards.
    let rewards = None;

    // Serialize everything into Block Info table.
    let seen_at = Utc::now();
    let block_info = BlockInfo::create(
        builder,
        &BlockInfoArgs {
            slot: block_meta.slot,
            blockhash,
            rewards,
            block_time: block_meta.block_time.map(|ts| ts.timestamp),
            block_height: block_meta.block_height.map(|bh| bh.block_height),
            seen_at: seen_at.timestamp_millis(),
        },
    );

    // Finalize buffer and return to caller.
    builder.finish(block_info, None);
}
