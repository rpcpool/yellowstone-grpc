use {
    crate::{
        config::{
            ConfigGrpcFilters, ConfigGrpcFiltersAccounts, ConfigGrpcFiltersBlocks,
            ConfigGrpcFiltersBlocksMeta, ConfigGrpcFiltersSlots, ConfigGrpcFiltersTransactions,
        },
        grpc::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageRef, MessageSlot,
            MessageTransaction,
        },
        proto::{
            subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
            subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
            CommitmentLevel, SubscribeRequest, SubscribeRequestAccountsDataSlice,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
            SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate,
        },
    },
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    spl_token_2022::{generic_token_account::GenericTokenAccount, state::Account as TokenAccount},
    std::{
        collections::{HashMap, HashSet},
        iter::FromIterator,
        str::FromStr,
    },
};

#[derive(Debug, Clone)]
pub struct Filter {
    accounts: FilterAccounts,
    slots: FilterSlots,
    transactions: FilterTransactions,
    blocks: FilterBlocks,
    blocks_meta: FilterBlocksMeta,
    commitment: CommitmentLevel,
    accounts_data_slice: Vec<FilterAccountsDataSlice>,
}

impl Filter {
    pub fn new(config: &SubscribeRequest, limit: &ConfigGrpcFilters) -> anyhow::Result<Self> {
        Ok(Self {
            accounts: FilterAccounts::new(&config.accounts, &limit.accounts)?,
            slots: FilterSlots::new(&config.slots, &limit.slots)?,
            transactions: FilterTransactions::new(&config.transactions, &limit.transactions)?,
            blocks: FilterBlocks::new(&config.blocks, &limit.blocks)?,
            blocks_meta: FilterBlocksMeta::new(&config.blocks_meta, &limit.blocks_meta)?,
            commitment: Self::decode_commitment(config.commitment)?,
            accounts_data_slice: FilterAccountsDataSlice::create(&config.accounts_data_slice)?,
        })
    }

    fn decode_commitment(commitment: Option<i32>) -> anyhow::Result<CommitmentLevel> {
        let commitment = commitment.unwrap_or(CommitmentLevel::Processed as i32);
        CommitmentLevel::from_i32(commitment)
            .ok_or_else(|| anyhow::anyhow!("failed to create CommitmentLevel from {commitment:?}"))
    }

    fn decode_pubkeys<T: FromIterator<Pubkey>>(
        pubkeys: &[String],
        limit: &HashSet<Pubkey>,
    ) -> anyhow::Result<T> {
        pubkeys
            .iter()
            .map(|value| match Pubkey::from_str(value) {
                Ok(pubkey) => {
                    ConfigGrpcFilters::check_pubkey_reject(&pubkey, limit)?;
                    Ok(pubkey)
                }
                Err(error) => Err(error.into()),
            })
            .collect::<_>()
    }

    pub const fn get_commitment_level(&self) -> CommitmentLevel {
        self.commitment
    }

    pub fn get_filters<'a>(&self, message: &'a Message) -> Vec<(Vec<String>, MessageRef<'a>)> {
        match message {
            Message::Account(message) => self.accounts.get_filters(message),
            Message::Slot(message) => self.slots.get_filters(message),
            Message::Transaction(message) => self.transactions.get_filters(message),
            Message::Block(message) => self.blocks.get_filters(message),
            Message::BlockMeta(message) => self.blocks_meta.get_filters(message),
        }
    }

    pub fn get_update(&self, message: &Message) -> Vec<SubscribeUpdate> {
        self.get_filters(message)
            .into_iter()
            .filter_map(|(filters, message)| {
                if filters.is_empty() {
                    None
                } else {
                    Some(SubscribeUpdate {
                        filters,
                        update_oneof: Some(message.to_proto(&self.accounts_data_slice)),
                    })
                }
            })
            .collect()
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccounts {
    filters: Vec<(String, FilterAccountsData)>,
    account: HashMap<Pubkey, HashSet<String>>,
    account_required: HashSet<String>,
    owner: HashMap<Pubkey, HashSet<String>>,
    owner_required: HashSet<String>,
}

impl FilterAccounts {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterAccounts>,
        limit: &ConfigGrpcFiltersAccounts,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            ConfigGrpcFilters::check_any(
                filter.account.is_empty() && filter.owner.is_empty(),
                limit.any,
            )?;
            ConfigGrpcFilters::check_pubkey_max(filter.account.len(), limit.account_max)?;
            ConfigGrpcFilters::check_pubkey_max(filter.owner.len(), limit.owner_max)?;

            Self::set(
                &mut this.account,
                &mut this.account_required,
                name,
                Filter::decode_pubkeys(&filter.account, &limit.account_reject)?,
            );

            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                name,
                Filter::decode_pubkeys(&filter.owner, &limit.owner_reject)?,
            );

            this.filters
                .push((name.clone(), FilterAccountsData::new(&filter.filters)?));
        }
        Ok(this)
    }

    fn set(
        map: &mut HashMap<Pubkey, HashSet<String>>,
        map_required: &mut HashSet<String>,
        name: &str,
        keys: Vec<Pubkey>,
    ) -> bool {
        let mut required = false;
        for key in keys.into_iter() {
            if map.entry(key).or_default().insert(name.to_string()) {
                required = true;
            }
        }

        if required {
            map_required.insert(name.to_string());
        }
        required
    }

    fn get_filters<'a>(&self, message: &'a MessageAccount) -> Vec<(Vec<String>, MessageRef<'a>)> {
        let mut filter = FilterAccountsMatch::new(self);
        filter.match_account(&message.account.pubkey);
        filter.match_owner(&message.account.owner);
        filter.match_data(&message.account.data);
        vec![(filter.get_filters(), MessageRef::Account(message))]
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccountsData {
    memcmp: Vec<(usize, Vec<u8>)>,
    datasize: Option<usize>,
    token_account_state: bool,
}

impl FilterAccountsData {
    fn new(filters: &[SubscribeRequestFilterAccountsFilter]) -> anyhow::Result<Self> {
        const MAX_FILTERS: usize = 4;
        const MAX_DATA_SIZE: usize = 128;
        const MAX_DATA_BASE58_SIZE: usize = 175;
        const MAX_DATA_BASE64_SIZE: usize = 172;

        anyhow::ensure!(
            filters.len() <= MAX_FILTERS,
            "Too many filters provided; max {MAX_FILTERS}"
        );

        let mut this = Self::default();
        for filter in filters {
            match &filter.filter {
                Some(AccountsFilterDataOneof::Memcmp(memcmp)) => {
                    let data = match &memcmp.data {
                        Some(AccountsFilterMemcmpOneof::Bytes(data)) => data.clone(),
                        Some(AccountsFilterMemcmpOneof::Base58(data)) => {
                            anyhow::ensure!(data.len() <= MAX_DATA_BASE58_SIZE, "data too large");
                            bs58::decode(data)
                                .into_vec()
                                .map_err(|_| anyhow::anyhow!("invalid base58"))?
                        }
                        Some(AccountsFilterMemcmpOneof::Base64(data)) => {
                            anyhow::ensure!(data.len() <= MAX_DATA_BASE64_SIZE, "data too large");
                            base64_engine
                                .decode(data)
                                .map_err(|_| anyhow::anyhow!("invalid base64"))?
                        }
                        None => anyhow::bail!("data for memcmp should be defined"),
                    };
                    anyhow::ensure!(data.len() <= MAX_DATA_SIZE, "data too large");
                    this.memcmp.push((memcmp.offset as usize, data));
                }
                Some(AccountsFilterDataOneof::Datasize(datasize)) => {
                    anyhow::ensure!(
                        this.datasize.replace(*datasize as usize).is_none(),
                        "datasize used more than once",
                    );
                }
                Some(AccountsFilterDataOneof::TokenAccountState(value)) => {
                    anyhow::ensure!(value, "token_account_state only allowed to be true");
                    this.token_account_state = true;
                }
                None => {
                    anyhow::bail!("filter should be defined");
                }
            }
        }
        Ok(this)
    }

    fn is_empty(&self) -> bool {
        self.memcmp.is_empty() && self.datasize.is_none() && !self.token_account_state
    }

    fn is_match(&self, data: &[u8]) -> bool {
        if matches!(self.datasize, Some(datasize) if data.len() != datasize) {
            return false;
        }
        if self.token_account_state && !TokenAccount::valid_account_data(data) {
            return false;
        }
        for (offset, bytes) in self.memcmp.iter() {
            if data.len() < *offset + bytes.len() {
                return false;
            }
            let data = &data[*offset..*offset + bytes.len()];
            if data != bytes {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct FilterAccountsMatch<'a> {
    filter: &'a FilterAccounts,
    account: HashSet<&'a str>,
    owner: HashSet<&'a str>,
    data: HashSet<&'a str>,
}

impl<'a> FilterAccountsMatch<'a> {
    fn new(filter: &'a FilterAccounts) -> Self {
        Self {
            filter,
            account: Default::default(),
            owner: Default::default(),
            data: Default::default(),
        }
    }

    fn extend(set: &mut HashSet<&'a str>, map: &'a HashMap<Pubkey, HashSet<String>>, key: &Pubkey) {
        if let Some(names) = map.get(key) {
            for name in names {
                if !set.contains(name.as_str()) {
                    set.insert(name);
                }
            }
        }
    }

    pub fn match_account(&mut self, pubkey: &Pubkey) {
        Self::extend(&mut self.account, &self.filter.account, pubkey)
    }

    pub fn match_owner(&mut self, pubkey: &Pubkey) {
        Self::extend(&mut self.owner, &self.filter.owner, pubkey)
    }

    pub fn match_data(&mut self, data: &[u8]) {
        for (name, filter) in self.filter.filters.iter() {
            if filter.is_match(data) {
                self.data.insert(name);
            }
        }
    }

    pub fn get_filters(&self) -> Vec<String> {
        self.filter
            .filters
            .iter()
            .filter_map(|(name, filter)| {
                let name = name.as_str();
                let af = &self.filter;

                // If filter name in required but not in matched => return `false`
                if af.account_required.contains(name) && !self.account.contains(name) {
                    return None;
                }
                if af.owner_required.contains(name) && !self.owner.contains(name) {
                    return None;
                }
                if !filter.is_empty() && !self.data.contains(name) {
                    return None;
                }

                Some(name.to_string())
            })
            .collect()
    }
}

#[derive(Debug, Default, Clone)]
struct FilterSlots {
    filters: Vec<String>,
}

impl FilterSlots {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterSlots>,
        limit: &ConfigGrpcFiltersSlots,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        Ok(Self {
            filters: configs
                .iter()
                // .filter_map(|(name, _filter)| Some(name.clone()))
                .map(|(name, _filter)| name.clone())
                .collect(),
        })
    }

    fn get_filters<'a>(&self, message: &'a MessageSlot) -> Vec<(Vec<String>, MessageRef<'a>)> {
        vec![(self.filters.clone(), MessageRef::Slot(message))]
    }
}

#[derive(Debug, Clone)]
pub struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    account_include: HashSet<Pubkey>,
    account_exclude: HashSet<Pubkey>,
    account_required: HashSet<Pubkey>,
}

#[derive(Debug, Default, Clone)]
pub struct FilterTransactions {
    filters: HashMap<String, FilterTransactionsInner>,
}

impl FilterTransactions {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterTransactions>,
        limit: &ConfigGrpcFiltersTransactions,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            ConfigGrpcFilters::check_any(
                filter.vote.is_none()
                    && filter.failed.is_none()
                    && filter.account_include.is_empty()
                    && filter.account_exclude.is_empty()
                    && filter.account_required.is_empty(),
                limit.any,
            )?;
            ConfigGrpcFilters::check_pubkey_max(
                filter.account_include.len(),
                limit.account_include_max,
            )?;
            ConfigGrpcFilters::check_pubkey_max(
                filter.account_exclude.len(),
                limit.account_exclude_max,
            )?;
            ConfigGrpcFilters::check_pubkey_max(
                filter.account_required.len(),
                limit.account_required_max,
            )?;

            this.filters.insert(
                name.clone(),
                FilterTransactionsInner {
                    vote: filter.vote,
                    failed: filter.failed,
                    signature: filter
                        .signature
                        .as_ref()
                        .map(|signature_str| {
                            signature_str
                                .parse()
                                .map_err(|error| anyhow::anyhow!("invalid signature: {error}"))
                        })
                        .transpose()?,
                    account_include: Filter::decode_pubkeys(
                        &filter.account_include,
                        &limit.account_include_reject,
                    )?,
                    account_exclude: Filter::decode_pubkeys(
                        &filter.account_exclude,
                        &HashSet::new(),
                    )?,
                    account_required: Filter::decode_pubkeys(
                        &filter.account_required,
                        &HashSet::new(),
                    )?,
                },
            );
        }
        Ok(this)
    }

    pub fn get_filters<'a>(
        &self,
        message: &'a MessageTransaction,
    ) -> Vec<(Vec<String>, MessageRef<'a>)> {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if let Some(is_vote) = inner.vote {
                    if is_vote != message.transaction.is_vote {
                        return None;
                    }
                }

                if let Some(is_failed) = inner.failed {
                    if is_failed != message.transaction.meta.status.is_err() {
                        return None;
                    }
                }

                if let Some(signature) = &inner.signature {
                    if signature != message.transaction.transaction.signature() {
                        return None;
                    }
                }

                if !inner.account_include.is_empty()
                    && message
                        .transaction
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .all(|pubkey| !inner.account_include.contains(pubkey))
                {
                    return None;
                }

                if !inner.account_exclude.is_empty()
                    && message
                        .transaction
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .any(|pubkey| inner.account_exclude.contains(pubkey))
                {
                    return None;
                }

                // check if transaction contains all required account keys
                if !inner.account_required.is_empty()
                    && !inner.account_required.is_subset(
                        &message
                            .transaction
                            .transaction
                            .message()
                            .account_keys()
                            .iter()
                            .cloned()
                            .collect(),
                    )
                {
                    return None;
                }

                Some(name.clone())
            })
            .collect();
        vec![(filters, MessageRef::Transaction(message))]
    }
}

#[derive(Debug, Clone)]
pub struct FilterBlocksInner {
    account_include: HashSet<Pubkey>,
}

#[derive(Debug, Default, Clone)]
struct FilterBlocks {
    filters: HashMap<String, FilterBlocksInner>,
}

impl FilterBlocks {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocks>,
        limit: &ConfigGrpcFiltersBlocks,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            ConfigGrpcFilters::check_any(filter.account_include.is_empty(), limit.any)?;
            ConfigGrpcFilters::check_pubkey_max(
                filter.account_include.len(),
                limit.account_include_max,
            )?;

            this.filters.insert(
                name.clone(),
                FilterBlocksInner {
                    account_include: Filter::decode_pubkeys(
                        &filter.account_include,
                        &limit.account_include_reject,
                    )?,
                },
            );
        }
        Ok(this)
    }

    fn get_filters<'a>(&self, message: &'a MessageBlock) -> Vec<(Vec<String>, MessageRef<'a>)> {
        self.filters
            .iter()
            .filter_map(|(filter, inner)| {
                #[allow(clippy::unnecessary_filter_map)]
                let transactions = message
                    .transactions
                    .iter()
                    .filter_map(|tx| {
                        if !inner.account_include.is_empty()
                            && tx
                                .transaction
                                .message()
                                .account_keys()
                                .iter()
                                .all(|pubkey| !inner.account_include.contains(pubkey))
                        {
                            return None;
                        }

                        Some(tx)
                    })
                    .collect::<Vec<_>>();

                if transactions.is_empty() {
                    None
                } else {
                    Some((
                        vec![filter.clone()],
                        MessageRef::Block((message, transactions).into()),
                    ))
                }
            })
            .collect()
    }
}

#[derive(Debug, Default, Clone)]
struct FilterBlocksMeta {
    filters: Vec<String>,
}

impl FilterBlocksMeta {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocksMeta>,
        limit: &ConfigGrpcFiltersBlocksMeta,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        Ok(Self {
            filters: configs
                .iter()
                // .filter_map(|(name, _filter)| Some(name.clone()))
                .map(|(name, _filter)| name.clone())
                .collect(),
        })
    }

    fn get_filters<'a>(&self, message: &'a MessageBlockMeta) -> Vec<(Vec<String>, MessageRef<'a>)> {
        vec![(self.filters.clone(), MessageRef::BlockMeta(message))]
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FilterAccountsDataSlice {
    pub start: usize,
    pub end: usize,
    pub length: usize,
}

impl From<&SubscribeRequestAccountsDataSlice> for FilterAccountsDataSlice {
    fn from(data_slice: &SubscribeRequestAccountsDataSlice) -> Self {
        Self {
            start: data_slice.offset as usize,
            end: (data_slice.offset + data_slice.length) as usize,
            length: data_slice.length as usize,
        }
    }
}

impl FilterAccountsDataSlice {
    pub fn create(slices: &[SubscribeRequestAccountsDataSlice]) -> anyhow::Result<Vec<Self>> {
        let slices = slices.iter().map(Into::into).collect::<Vec<Self>>();

        for (i, slice_a) in slices.iter().enumerate() {
            // check order
            for slice_b in slices[i + 1..].iter() {
                anyhow::ensure!(slice_a.start <= slice_b.start, "data slices out of order");
            }

            // check overlap
            for slice_b in slices[0..i].iter() {
                anyhow::ensure!(slice_a.start >= slice_b.end, "data slices overlap");
            }
        }

        Ok(slices)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::ConfigGrpcFilters,
            filters::Filter,
            grpc::{Message, MessageTransaction, MessageTransactionInfo},
        },
        solana_sdk::{
            hash::Hash,
            message::Message as SolMessage,
            message::{v0::LoadedAddresses, MessageHeader},
            pubkey::Pubkey,
            signer::{keypair::Keypair, Signer},
            transaction::{SanitizedTransaction, Transaction},
        },
        solana_transaction_status::TransactionStatusMeta,
        std::collections::HashMap,
        yellowstone_grpc_proto::geyser::{
            SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
        },
    };

    fn create_message_transaction(
        keypair: &Keypair,
        account_keys: Vec<Pubkey>,
    ) -> MessageTransaction {
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys,
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let sanitized_transaction = SanitizedTransaction::from_transaction_for_tests(
            Transaction::new(&[keypair], message, recent_blockhash),
        );
        let meta = TransactionStatusMeta {
            status: Ok(()),
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
        };
        let sig = sanitized_transaction.signature();
        MessageTransaction {
            transaction: MessageTransactionInfo {
                signature: *sig,
                is_vote: true,
                transaction: sanitized_transaction,
                meta,
                index: 1,
            },
            slot: 100,
        }
    }

    #[test]
    fn test_filters_all_empty() {
        // ensure Filter can be created with empty values
        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_filters_account_empty() {
        let mut accounts = HashMap::new();

        accounts.insert(
            "solend".to_owned(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts,
            slots: HashMap::new(),
            transactions: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.accounts.any = false;
        let filter = Filter::new(&config, &limit);
        // filter should fail
        assert!(filter.is_err());
    }

    #[test]
    fn test_filters_transaction_empty() {
        let mut transactions = HashMap::new();

        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.transactions.any = false;
        let filter = Filter::new(&config, &limit);
        // filter should fail
        assert!(filter.is_err());
    }

    #[test]
    fn test_filters_transaction_not_null() {
        let mut transactions = HashMap::new();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(true),
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.transactions.any = false;
        let filter_res = Filter::new(&config, &limit);
        // filter should succeed
        assert!(filter_res.is_ok());
    }

    #[test]
    fn test_transaction_include_a() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = vec![account_key_a].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        for (filters, _message) in filter.get_filters(&message) {
            assert!(!filters.is_empty());
        }
    }

    #[test]
    fn test_transaction_include_b() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = vec![account_key_b].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        for (filters, _message) in filter.get_filters(&message) {
            assert!(!filters.is_empty());
        }
    }

    #[test]
    fn test_transaction_exclude() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_exclude = vec![account_key_b].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude,
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        for (filters, _message) in filter.get_filters(&message) {
            assert!(filters.is_empty());
        }
    }

    #[test]
    fn test_transaction_required_x_include_y_z_case001() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        // require x, include y, z
        let account_include = vec![account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        let account_required = vec![account_key_x].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required,
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction = create_message_transaction(
            &keypair_x,
            vec![account_key_x, account_key_y, account_key_z],
        );
        let message = Message::Transaction(message_transaction);
        for (filters, _message) in filter.get_filters(&message) {
            assert!(!filters.is_empty());
        }
    }

    #[test]
    fn test_transaction_required_y_z_include_x() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        // require x, include y, z
        let account_include = vec![account_key_x].iter().map(|k| k.to_string()).collect();
        let account_required = vec![account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required,
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_x, vec![account_key_x, account_key_z]);
        let message = Message::Transaction(message_transaction);
        for (filters, _message) in filter.get_filters(&message) {
            assert!(filters.is_empty());
        }
    }
}
