use {
    agave_votor_messages::{
        certificate::CertSignature,
        reward_certificate::{NotarRewardCertificate, SkipRewardCertificate},
    },
    solana_account::Account,
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_bls_signatures::{
        Signature as BLSSignature, SignatureCompressed as BLSSignatureCompressed,
        BLS_SIGNATURE_COMPRESSED_SIZE,
    },
    solana_entry::block_component::{BlockFinalizationCert, VotesAggregate},
    solana_hash::{Hash, HASH_BYTES},
    solana_message::{
        compiled_instruction::CompiledInstruction,
        v0::{LoadedAddresses, Message as MessageV0, MessageAddressTableLookup},
        v1::{Message as MessageV1, TransactionConfig},
        Message, MessageHeader, VersionedMessage,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_context::transaction::TransactionReturnData,
    solana_transaction_error::TransactionError,
    solana_transaction_status::{
        ConfirmedBlock, InnerInstruction, InnerInstructions, Reward, RewardType,
        RewardsAndNumPartitions, TransactionStatusMeta, TransactionTokenBalance,
        TransactionWithStatusMeta, VersionedTransactionWithStatusMeta,
    },
    yellowstone_grpc_proto::prelude as proto,
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

    let Ok(blockhash) = <[u8; HASH_BYTES]>::try_from(message.recent_blockhash.as_slice()) else {
        return Err("failed to parse hash");
    };
    let recent_blockhash = Hash::new_from_array(blockhash);

    // `config` is set only for V1 messages, whose lifetime specifier is carried in
    // `recent_blockhash` and which have no address table lookups. `versioned` is
    // true for both V0 and V1, so it cannot tell them apart on its own.
    if let Some(config) = message.config {
        return Ok(VersionedMessage::V1(MessageV1 {
            header,
            config: TransactionConfig {
                priority_fee: config.priority_fee,
                compute_unit_limit: config.compute_unit_limit,
                loaded_accounts_data_size_limit: config.loaded_accounts_data_size_limit,
                heap_size: config.heap_size,
            },
            lifetime_specifier: recent_blockhash,
            account_keys: create_pubkey_vec(message.account_keys)?,
            instructions: create_message_instructions(message.instructions)?,
        }));
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
            recent_blockhash,
            instructions: create_message_instructions(message.instructions)?,
            address_table_lookups,
        })
    } else {
        VersionedMessage::Legacy(Message {
            header,
            account_keys: create_pubkey_vec(message.account_keys)?,
            recent_blockhash,
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

pub fn create_tx_meta(meta: proto::TransactionStatusMeta) -> CreateResult<TransactionStatusMeta> {
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
        inner_instructions: if meta.inner_instructions_none {
            None
        } else {
            Some(create_meta_inner_instructions(meta.inner_instructions)?)
        },
        log_messages: if meta.log_messages_none {
            None
        } else {
            Some(meta.log_messages)
        },
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
        cost_units: meta.cost_units,
    })
}

pub fn create_tx_error(
    err: Option<&proto::TransactionError>,
) -> CreateResult<Option<TransactionError>> {
    err.map(|err| wincode::deserialize::<TransactionError>(&err.err))
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
        // Unknown values decode as None, like agave's storage-proto, so a reward type added
        // by a newer validator does not fail the whole block.
        reward_type: proto::RewardType::try_from(reward.reward_type)
            .ok()
            .and_then(|reward_type| match reward_type {
                proto::RewardType::Unspecified => None,
                proto::RewardType::Fee => Some(RewardType::Fee),
                proto::RewardType::Rent => Some(RewardType::Rent),
                proto::RewardType::Staking => Some(RewardType::Staking),
                proto::RewardType::Voting => Some(RewardType::Voting),
                proto::RewardType::DeactivatedStake => Some(RewardType::DeactivatedStake),
                proto::RewardType::VatDebit => Some(RewardType::VATDebit),
            }),
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
        commission_bps: if reward.commission_bps.is_empty() {
            None
        } else {
            Some(
                reward
                    .commission_bps
                    .parse()
                    .map_err(|_| "failed to parse reward commission_bps")?,
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

fn take_account_data(account: &mut proto::SubscribeUpdateAccountInfo) -> Vec<u8> {
    let bytes = std::mem::take(&mut account.data);
    bytes.into()
}

pub fn create_account(
    mut account: proto::SubscribeUpdateAccountInfo,
) -> CreateResult<(Pubkey, Account)> {
    let pubkey = create_pubkey(&account.pubkey)?;
    let account_data = take_account_data(&mut account);
    let account = Account {
        lamports: account.lamports,
        data: account_data,
        owner: create_pubkey(&account.owner)?,
        executable: account.executable,
        rent_epoch: account.rent_epoch,
    };
    Ok((pubkey, account))
}

// The block_id sits on the notarize aggregate in a slow finalization, and on the final one in a fast one.
pub fn create_block_final_cert(
    cert: &proto::BlockFooterFinalCert,
) -> CreateResult<BlockFinalizationCert> {
    let final_aggregate = cert
        .final_aggregate
        .as_ref()
        .ok_or("failed to get final_aggregate")?;
    let block_aggregate = cert.notar_aggregate.as_ref().unwrap_or(final_aggregate);
    Ok(BlockFinalizationCert {
        slot: cert.slot,
        block_id: create_hash(&block_aggregate.block_id)?,
        final_aggregate: create_votes_aggregate(final_aggregate)?,
        notar_aggregate: cert
            .notar_aggregate
            .as_ref()
            .map(create_votes_aggregate)
            .transpose()?,
    })
}

fn create_votes_aggregate(
    aggregate: &proto::BlockFooterVotesAggregate,
) -> CreateResult<VotesAggregate> {
    let signature = BLSSignature::try_from(create_bls_signature(aggregate)?)
        .map_err(|_| "failed to decompress BLS signature")?;
    Ok(VotesAggregate::from_cert_signature(CertSignature {
        signature,
        bitmap: aggregate.signer_bitmap.clone(),
    }))
}

pub fn create_skip_reward_cert(
    cert: &proto::BlockFooterSkipRewardCert,
) -> CreateResult<SkipRewardCertificate> {
    let aggregate = cert.aggregate.as_ref().ok_or("failed to get aggregate")?;
    SkipRewardCertificate::try_new(
        cert.slot,
        create_bls_signature(aggregate)?,
        aggregate.signer_bitmap.clone(),
    )
    .map_err(|_| "failed to create skip reward cert")
}

pub fn create_notar_reward_cert(
    cert: &proto::BlockFooterNotarRewardCert,
) -> CreateResult<NotarRewardCertificate> {
    let aggregate = cert.aggregate.as_ref().ok_or("failed to get aggregate")?;
    NotarRewardCertificate::try_new(
        cert.slot,
        create_hash(&aggregate.block_id)?,
        create_bls_signature(aggregate)?,
        aggregate.signer_bitmap.clone(),
    )
    .map_err(|_| "failed to create notar reward cert")
}

fn create_hash(hash: &[u8]) -> CreateResult<Hash> {
    <[u8; HASH_BYTES]>::try_from(hash)
        .map(Hash::new_from_array)
        .map_err(|_| "failed to parse hash")
}

fn create_bls_signature(
    aggregate: &proto::BlockFooterVotesAggregate,
) -> CreateResult<BLSSignatureCompressed> {
    // try_from rejects unknown kinds; the signature_kind() getter would map them to BLS.
    match proto::BlockFooterSignatureKind::try_from(aggregate.signature_kind) {
        Ok(proto::BlockFooterSignatureKind::CompressedBls12381G2) => {}
        Err(_) => return Err("unsupported signature kind"),
    }
    <[u8; BLS_SIGNATURE_COMPRESSED_SIZE]>::try_from(aggregate.signature.as_slice())
        .map(BLSSignatureCompressed)
        .map_err(|_| "failed to parse BLS signature")
}

#[cfg(test)]
mod tests {
    use {
        super::{
            create_block_final_cert, create_notar_reward_cert, create_reward,
            create_skip_reward_cert, create_tx_meta,
        },
        crate::plugin::convert_to,
        agave_geyser_plugin_interface::block_footer as plugin,
        agave_votor_messages::{
            certificate::CertSignature,
            reward_certificate::{NotarRewardCertificate, SkipRewardCertificate},
        },
        solana_bls_signatures::{
            keypair::Keypair, Signature as BLSSignature,
            SignatureCompressed as BLSSignatureCompressed,
        },
        solana_entry::block_component::{BlockFinalizationCert, VotesAggregate},
        solana_hash::Hash,
        solana_transaction_status::RewardType,
        yellowstone_grpc_proto::prelude as proto,
    };

    fn bls_signature(seed: u8) -> BLSSignature {
        let keypair = Keypair::derive(&[seed; 32]).unwrap();
        BLSSignature::from(&keypair.sign(b"block footer"))
    }

    fn votes_aggregate(seed: u8, bitmap: Vec<u8>) -> VotesAggregate {
        VotesAggregate::from_cert_signature(CertSignature {
            signature: bls_signature(seed),
            bitmap,
        })
    }

    // convert_to takes the plugin interface's borrowed mirrors of the agave types.
    fn plugin_aggregate(aggregate: &VotesAggregate) -> plugin::VotesAggregate<'_> {
        plugin::VotesAggregate {
            signature: *aggregate.signature(),
            bitmap: aggregate.bitmap(),
        }
    }

    fn plugin_final_cert(cert: &BlockFinalizationCert) -> plugin::BlockFinalizationCert<'_> {
        plugin::BlockFinalizationCert {
            slot: cert.slot,
            block_id: cert.block_id,
            final_aggregate: plugin_aggregate(&cert.final_aggregate),
            notar_aggregate: cert.notar_aggregate.as_ref().map(plugin_aggregate),
        }
    }

    #[test]
    fn block_final_cert_round_trip() {
        let block_id = Hash::new_from_array([4; 32]);
        for notar_aggregate in [None, Some(votes_aggregate(2, vec![1, 6, 0, 9]))] {
            let slow = notar_aggregate.is_some();
            let cert = BlockFinalizationCert {
                slot: 42,
                block_id,
                final_aggregate: votes_aggregate(1, vec![0, 10, 0, 0b1101, 0b11]),
                notar_aggregate,
            };
            let proto = convert_to::create_block_final_cert(&plugin_final_cert(&cert));
            let final_aggregate = proto.final_aggregate.as_ref().unwrap();
            assert_eq!(final_aggregate.signature.len(), 96);
            assert_eq!(final_aggregate.signer_bitmap, vec![0, 10, 0, 0b1101, 0b11]);
            if slow {
                assert!(final_aggregate.block_id.is_empty());
                assert_eq!(
                    proto.notar_aggregate.as_ref().unwrap().block_id,
                    block_id.to_bytes()
                );
            } else {
                assert_eq!(final_aggregate.block_id, block_id.to_bytes());
            }
            assert_eq!(create_block_final_cert(&proto).unwrap(), cert);
        }
    }

    #[test]
    fn reward_certs_round_trip() {
        let signature = BLSSignatureCompressed::try_from(&bls_signature(3)).unwrap();
        let skip = SkipRewardCertificate::try_new(42, signature, vec![1, 6, 0, 9]).unwrap();
        let proto = convert_to::create_skip_reward_cert(&plugin::SkipRewardCertificate {
            slot: skip.slot,
            signature: skip.signature,
            bitmap: skip.to_bitmap(),
        });
        assert!(proto.aggregate.as_ref().unwrap().block_id.is_empty());
        assert_eq!(create_skip_reward_cert(&proto).unwrap(), skip);

        let notar = NotarRewardCertificate::try_new(
            43,
            Hash::new_from_array([5; 32]),
            signature,
            Vec::new(),
        )
        .unwrap();
        let proto = convert_to::create_notar_reward_cert(&plugin::NotarRewardCertificate {
            slot: notar.slot,
            block_id: notar.block_id,
            signature: notar.signature,
            bitmap: notar.bitmap(),
        });
        assert_eq!(proto.aggregate.as_ref().unwrap().block_id, vec![5; 32]);
        assert_eq!(create_notar_reward_cert(&proto).unwrap(), notar);
    }

    #[test]
    fn block_final_cert_rejects_malformed_fields() {
        let fast =
            convert_to::create_block_final_cert(&plugin_final_cert(&BlockFinalizationCert {
                slot: 42,
                block_id: Hash::new_from_array([4; 32]),
                final_aggregate: votes_aggregate(1, Vec::new()),
                notar_aggregate: None,
            }));

        let mut cert = fast.clone();
        cert.final_aggregate.as_mut().unwrap().block_id.pop();
        assert!(create_block_final_cert(&cert).is_err());

        let mut cert = fast.clone();
        cert.final_aggregate = None;
        assert!(create_block_final_cert(&cert).is_err());

        // A slow finalization takes its block_id from the notarize aggregate.
        let mut cert = fast.clone();
        cert.notar_aggregate = cert.final_aggregate.clone();
        cert.notar_aggregate.as_mut().unwrap().block_id.clear();
        assert!(create_block_final_cert(&cert).is_err());

        let mut cert = fast.clone();
        // A future kind this reader does not know.
        cert.final_aggregate.as_mut().unwrap().signature_kind = 1;
        assert!(create_block_final_cert(&cert).is_err());

        let mut cert = fast;
        cert.final_aggregate.as_mut().unwrap().signature = vec![0xff; 96];
        assert!(create_block_final_cert(&cert).is_err());
    }

    fn base_proto_meta() -> proto::TransactionStatusMeta {
        proto::TransactionStatusMeta {
            return_data_none: true,
            ..Default::default()
        }
    }

    #[test]
    fn tx_meta_respects_none_flags_set() {
        let meta = create_tx_meta(proto::TransactionStatusMeta {
            inner_instructions_none: true,
            log_messages_none: true,
            ..base_proto_meta()
        })
        .expect("failed to create meta");

        assert_eq!(meta.inner_instructions, None);
        assert_eq!(meta.log_messages, None);
    }

    #[test]
    fn tx_meta_respects_none_flags_unset() {
        let log_messages = vec!["Program log: hello".to_owned()];
        let meta = create_tx_meta(proto::TransactionStatusMeta {
            inner_instructions_none: false,
            log_messages_none: false,
            log_messages: log_messages.clone(),
            ..base_proto_meta()
        })
        .expect("failed to create meta");

        assert_eq!(meta.inner_instructions, Some(vec![]));
        assert_eq!(meta.log_messages, Some(log_messages));
    }

    fn base_proto_reward(reward_type: i32) -> proto::Reward {
        proto::Reward {
            pubkey: "11111111111111111111111111111111".to_owned(),
            lamports: 1_000,
            post_balance: 50_000,
            reward_type,
            ..Default::default()
        }
    }

    #[test]
    fn reward_type_known_values_are_decoded() {
        for (proto_type, expected) in [
            (proto::RewardType::Unspecified, None),
            (
                proto::RewardType::DeactivatedStake,
                Some(RewardType::DeactivatedStake),
            ),
            (proto::RewardType::VatDebit, Some(RewardType::VATDebit)),
        ] {
            let reward = create_reward(base_proto_reward(proto_type as i32))
                .expect("failed to create reward");
            assert_eq!(reward.reward_type, expected);
        }
    }

    #[test]
    fn reward_type_unknown_value_decodes_as_none() {
        let reward = create_reward(base_proto_reward(99)).expect("failed to create reward");

        assert_eq!(reward.reward_type, None);
    }

    #[test]
    fn vat_debit_keeps_negative_lamports() {
        let reward = create_reward(proto::Reward {
            lamports: -1_000,
            ..base_proto_reward(proto::RewardType::VatDebit as i32)
        })
        .expect("failed to create reward");

        assert_eq!(reward.lamports, -1_000);
        assert_eq!(reward.reward_type, Some(RewardType::VATDebit));
    }
}
