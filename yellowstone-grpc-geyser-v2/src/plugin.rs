use {
    crate::{
        config::Config,
        grpc::server::spawn_grpc_server,
        metrics::{self, PrometheusService},
        proto::{
            geyser::{
                subscribe_update::UpdateOneof, SubscribeUpdateAccount, SubscribeUpdateBlockMeta,
                SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction,
            },
            solana::storage::confirmed_block,
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoV3, ReplicaAccountInfoVersions,
        ReplicaBlockInfoV4, ReplicaBlockInfoVersions, ReplicaEntryInfoV2, ReplicaEntryInfoVersions,
        ReplicaTransactionInfoV3, ReplicaTransactionInfoVersions, Result as PluginResult,
        SlotStatus,
    },
    bytes::Bytes,
    solana_clock::{Slot, UnixTimestamp},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction_status::RewardsAndNumPartitions,
    std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration},
    tokio::{
        runtime::{Builder, Runtime},
        sync::broadcast,
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

#[derive(Debug, Clone, PartialEq)]
pub struct MessageSlot {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: SlotStatus,
    pub dead_error: Option<String>,
}

impl MessageSlot {
    pub fn from_geyser(slot: Slot, parent: Option<Slot>, status: &SlotStatus) -> Self {
        MessageSlot {
            slot,
            parent,
            status: status.clone(),
            dead_error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MessageEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
}

impl MessageEntry {
    pub fn from_geyser(entry: &ReplicaEntryInfoV2<'_>) -> Self {
        MessageEntry {
            slot: entry.slot,
            index: entry.index,
            num_hashes: entry.num_hashes,
            hash: Hash::new_from_array(entry.hash.try_into().expect("valid hash array")),
            executed_transaction_count: entry.executed_transaction_count,
            starting_transaction_index: entry.starting_transaction_index as u64,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Bytes,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageAccount {
    pub account: Arc<MessageAccountInfo>,
    pub slot: Slot,
    pub is_startup: bool,
}

impl MessageAccount {
    #[allow(dead_code)]
    fn from_replace_info(slot: Slot, is_startup: bool, account: &ReplicaAccountInfoV3<'_>) -> Self {
        MessageAccount {
            account: Arc::new(MessageAccountInfo {
                pubkey: Pubkey::try_from(account.pubkey).expect("valid pubkey"),
                lamports: account.lamports,
                owner: Pubkey::try_from(account.owner).expect("valid owner pubkey"),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: Bytes::copy_from_slice(account.data),
                write_version: account.write_version,
                txn_signature: account.txn.map(|txn| txn.signature().clone()),
            }),
            slot: slot,
            is_startup: is_startup,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBlockMeta {
    pub slot: Slot,
    pub blockhash: Hash,
    pub rewards: RewardsAndNumPartitions,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
}

impl MessageBlockMeta {
    pub fn from_geyser(info: &ReplicaBlockInfoV4) -> Self {
        MessageBlockMeta {
            slot: info.slot,
            blockhash: Hash::from_str(info.blockhash).expect("valid blockhash"),
            rewards: info.rewards.clone(),
            block_time: info.block_time.clone(),
            block_height: info.block_height.clone(),
            executed_transaction_count: info.executed_transaction_count,
            entry_count: info.entry_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageTransaction {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl MessageTransaction {
    pub fn from_geyser(info: &ReplicaTransactionInfoV3<'_>, slot: u64) -> Self {
        let account_keys: HashSet<Pubkey> = info
            .transaction
            .message
            .static_account_keys() // Since V3, dynamic account are only available in `loaded_addresses`
            .iter()
            .chain(
                info.transaction_status_meta
                    .loaded_addresses
                    .writable
                    .iter(),
            )
            .chain(
                info.transaction_status_meta
                    .loaded_addresses
                    .readonly
                    .iter(),
            )
            .copied()
            .collect();

        MessageTransaction {
            transaction: Arc::new(MessageTransactionInfo {
                signature: *info.signature,
                is_vote: info.is_vote,
                transaction: convert_to::create_transaction(info.transaction),
                meta: convert_to::create_transaction_meta(info.transaction_status_meta),
                index: info.index,
                account_keys,
            }),
            slot,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: confirmed_block::Transaction,
    pub meta: confirmed_block::TransactionStatusMeta,
    pub index: usize,
    pub account_keys: HashSet<Pubkey>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Slot(SubscribeUpdateSlot),
    Account(SubscribeUpdateAccount),
    Transaction(SubscribeUpdateTransaction),
    Entry(SubscribeUpdateEntry),
    BlockMeta(SubscribeUpdateBlockMeta),
}

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    plugin_cancellation_token: CancellationToken,
    broadcast_tx: broadcast::Sender<Arc<UpdateOneof>>,
    task_tracker: TaskTracker,
}

impl PluginInner {
    fn send_message(&self, message: UpdateOneof) {
        // If there is not receiver, broadcast always fails, so we ignore the error
        // even if the error is "channel is closed" it is not actually closed.
        let _ = self.broadcast_tx.send(Arc::new(message)).map(drop);
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn with_inner<F>(&self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&PluginInner) -> PluginResult<()>,
    {
        let inner = self.inner.as_ref().expect("initialized");
        f(inner)
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;

        // Setup logger
        solana_logger::setup_with_default(&config.log.level);

        log::info!("Loading plugin: {}", self.name());
        // Create inner
        let mut builder = Builder::new_multi_thread();
        if let Some(worker_threads) = config.tokio.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(tokio_cpus) = config.tokio.affinity.clone() {
            builder.on_thread_start(move || {
                affinity::set_thread_affinity(&tokio_cpus).expect("failed to set affinity")
            });
        }
        let runtime = builder
            .thread_name_fn(crate::get_thread_name)
            .enable_all()
            .build()
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        let prom_config = config.prometheus.clone();

        let (broadcast_tx, _) = broadcast::channel(config.grpc.channel_capacity);
        let plugin_cancellation_token = CancellationToken::new();
        let tokio_broadcast_tx = broadcast_tx.clone();
        let grpc_cancellation_token = plugin_cancellation_token.child_token();
        let prometheus_cancellation_token = plugin_cancellation_token.child_token();
        let task_tracker = TaskTracker::new();
        let prometheus_task_tracker = task_tracker.clone();
        let grpc_task_tracker = task_tracker.clone();
        runtime
            .block_on(async move {
                if let Some(prom_config) = prom_config {
                    PrometheusService::spawn(
                        prom_config,
                        prometheus_task_tracker,
                        prometheus_cancellation_token,
                    )
                    .await
                    .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
                };
                let _jh = spawn_grpc_server(
                    tokio_broadcast_tx,
                    config.grpc.clone(),
                    grpc_task_tracker,
                    grpc_cancellation_token,
                )
                .await
                .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
                Ok::<_, GeyserPluginError>(())
            })
            .inspect_err(|_e| {
                plugin_cancellation_token.cancel();
            })?;

        self.inner = Some(PluginInner {
            runtime,
            plugin_cancellation_token,
            broadcast_tx,
            task_tracker,
        });

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            let running_tasks = inner.task_tracker.len();
            log::info!("Unloading plugin, waiting for {running_tasks} tasks to finish");
            inner.task_tracker.close();
            inner.plugin_cancellation_token.cancel();
            drop(inner.broadcast_tx);
            inner.runtime.shutdown_timeout(Duration::from_secs(30));
            let remaining_tasks = inner.task_tracker.len();
            if remaining_tasks > 0 {
                log::info!(
                    "Geyser plugin {} leaked {remaining_tasks} tasks",
                    self.name()
                );
            }
        }
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let account = match account {
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_3(info) => info,
            };

            if !is_startup {
                let message = UpdateOneof::Account(convert_to::create_account_update(
                    &account, slot, is_startup,
                ));
                inner.send_message(message);
            }

            Ok(())
        })
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let message = UpdateOneof::Slot(convert_to::from_geyser_slot_status(
                slot,
                parent,
                status.clone(),
            ));
            inner.send_message(message);
            metrics::update_slot_status(status, slot);
            Ok(())
        })
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let transaction = match transaction {
                ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaTransactionInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaTransactionInfoVersions::V0_0_3(info) => info,
            };

            let message =
                UpdateOneof::Transaction(convert_to::create_transaction_update(transaction, slot));
            inner.send_message(message);
            Ok(())
        })
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        self.with_inner(|inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let entry = match entry {
                ReplicaEntryInfoVersions::V0_0_1(_entry) => {
                    unreachable!("ReplicaEntryInfoVersions::V0_0_1 is not supported")
                }
                ReplicaEntryInfoVersions::V0_0_2(entry) => entry,
            };

            let message = UpdateOneof::Entry(convert_to::create_entry_update(entry));
            inner.send_message(message);
            Ok(())
        })
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        self.with_inner(|inner| {
            let blockinfo = match blockinfo {
                ReplicaBlockInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_2 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_3(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_3 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_4(info) => info,
            };

            let message = UpdateOneof::BlockMeta(convert_to::create_blockmeta_update(blockinfo));
            inner.send_message(message);
            Ok(())
        })
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

pub mod convert_to {
    use {
        crate::proto::{
            geyser::{
                SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateSlot,
                SubscribeUpdateTransaction,
            },
            prelude::{self as proto},
        },
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV3,
            SlotStatus,
        },
        bytes::Bytes,
        solana_clock::UnixTimestamp,
        solana_message::{
            compiled_instruction::CompiledInstruction, v0::MessageAddressTableLookup,
            MessageHeader, VersionedMessage,
        },
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        solana_transaction::versioned::VersionedTransaction,
        solana_transaction_context::TransactionReturnData,
        solana_transaction_error::TransactionError,
        solana_transaction_status::{
            InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta,
            TransactionTokenBalance,
        },
    };

    pub fn create_transaction(tx: &VersionedTransaction) -> proto::Transaction {
        proto::Transaction {
            signatures: tx
                .signatures
                .iter()
                .map(|signature| <Signature as AsRef<[u8]>>::as_ref(signature).into())
                .collect(),
            message: Some(create_message(&tx.message)),
        }
    }

    pub fn create_message(message: &VersionedMessage) -> proto::Message {
        match message {
            VersionedMessage::Legacy(message) => proto::Message {
                header: Some(create_header(&message.header)),
                account_keys: create_pubkeys(&message.account_keys),
                recent_blockhash: message.recent_blockhash.to_bytes().into(),
                instructions: create_instructions(&message.instructions),
                versioned: false,
                address_table_lookups: vec![],
            },
            VersionedMessage::V0(message) => proto::Message {
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
            cost_units,
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
            cost_units: *cost_units,
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

    pub fn from_geyser_slot_status(
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> SubscribeUpdateSlot {
        SubscribeUpdateSlot {
            slot,
            parent: parent,
            status: encode_geyser_slot_status(&status),
            dead_error: match status {
                SlotStatus::Dead(err) => Some(err),
                _ => None,
            },
        }
    }

    pub fn encode_geyser_slot_status(slot_status: &SlotStatus) -> i32 {
        match slot_status {
            SlotStatus::Processed => 0,
            SlotStatus::Confirmed => 1,
            SlotStatus::Rooted => 2,
            SlotStatus::FirstShredReceived => 3,
            SlotStatus::Completed => 4,
            SlotStatus::CreatedBank => 5,
            SlotStatus::Dead(_) => 6,
        }
    }

    pub fn create_account_update_info(
        account: &ReplicaAccountInfoV3<'_>,
    ) -> SubscribeUpdateAccountInfo {
        SubscribeUpdateAccountInfo {
            pubkey: account.pubkey.to_vec(),
            lamports: account.lamports,
            owner: account.owner.to_vec(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: Bytes::copy_from_slice(account.data),
            write_version: account.write_version,
            txn_signature: account.txn.map(|txn| txn.signature().as_ref().to_vec()),
        }
    }

    pub fn create_account_update(
        account: &ReplicaAccountInfoV3<'_>,
        slot: u64,
        is_startup: bool,
    ) -> SubscribeUpdateAccount {
        SubscribeUpdateAccount {
            account: Some(create_account_update_info(account)),
            slot,
            is_startup,
        }
    }

    pub fn create_transaction_info_update(
        transaction: &ReplicaTransactionInfoV3<'_>,
    ) -> proto::SubscribeUpdateTransactionInfo {
        proto::SubscribeUpdateTransactionInfo {
            signature: transaction.signature.as_ref().to_vec(),
            is_vote: transaction.is_vote,
            transaction: Some(create_transaction(transaction.transaction)),
            meta: Some(create_transaction_meta(transaction.transaction_status_meta)),
            index: transaction.index as u64,
        }
    }

    pub fn create_transaction_update(
        transaction: &ReplicaTransactionInfoV3<'_>,
        slot: u64,
    ) -> SubscribeUpdateTransaction {
        SubscribeUpdateTransaction {
            transaction: Some(create_transaction_info_update(transaction)),
            slot,
        }
    }

    pub fn create_entry_update(entry: &ReplicaEntryInfoV2<'_>) -> proto::SubscribeUpdateEntry {
        proto::SubscribeUpdateEntry {
            slot: entry.slot,
            index: entry.index as u64,
            num_hashes: entry.num_hashes,
            hash: entry.hash.to_vec(),
            executed_transaction_count: entry.executed_transaction_count,
            starting_transaction_index: entry.starting_transaction_index as u64,
        }
    }

    pub fn create_blockmeta_update(
        blockinfo: &ReplicaBlockInfoV4,
    ) -> proto::SubscribeUpdateBlockMeta {
        proto::SubscribeUpdateBlockMeta {
            slot: blockinfo.slot,
            blockhash: blockinfo.blockhash.to_string(),
            rewards: Some(blockinfo.rewards)
                .map(|rw| create_rewards_obj(&rw.rewards, rw.num_partitions)),
            block_time: blockinfo.block_time.map(create_timestamp),
            block_height: blockinfo.block_height.map(create_block_height),
            executed_transaction_count: blockinfo.executed_transaction_count,
            parent_slot: blockinfo.parent_slot,
            parent_blockhash: blockinfo.parent_blockhash.to_string(),
            entries_count: blockinfo.entry_count,
        }
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
