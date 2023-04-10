use {
    crate::{
        config::{
            ConfigGrpcFilters, ConfigGrpcFiltersAccounts, ConfigGrpcFiltersBlocks,
            ConfigGrpcFiltersBlocksMeta, ConfigGrpcFiltersSlots, ConfigGrpcFiltersTransactions,
        },
        grpc::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageSlot,
            MessageTransaction,
        },
        proto::{
            subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
            subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
            SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
            SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
        },
    },
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::{
        collections::{HashMap, HashSet},
        iter::FromIterator,
        str::FromStr,
    },
};

#[derive(Debug)]
pub struct Filter {
    accounts: FilterAccounts,
    slots: FilterSlots,
    transactions: FilterTransactions,
    blocks: FilterBlocks,
    blocks_meta: FilterBlocksMeta,
}

impl Filter {
    pub fn new(config: &SubscribeRequest, limit: &ConfigGrpcFilters) -> anyhow::Result<Self> {
        Ok(Self {
            accounts: FilterAccounts::new(&config.accounts, &limit.accounts)?,
            slots: FilterSlots::new(&config.slots, &limit.slots)?,
            transactions: FilterTransactions::new(&config.transactions, &limit.transactions)?,
            blocks: FilterBlocks::new(&config.blocks, &limit.blocks)?,
            blocks_meta: FilterBlocksMeta::new(&config.blocks_meta, &limit.blocks_meta)?,
        })
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

    pub fn get_filters(&self, message: &Message) -> Vec<String> {
        match message {
            Message::Account(message) => self.accounts.get_filters(message),
            Message::Slot(message) => self.slots.get_filters(message),
            Message::Transaction(message) => self.transactions.get_filters(message),
            Message::Block(message) => self.blocks.get_filters(message),
            Message::BlockMeta(message) => self.blocks_meta.get_filters(message),
        }
    }
}

#[derive(Debug, Default)]
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

    fn get_filters(&self, message: &MessageAccount) -> Vec<String> {
        let mut filter = FilterAccountsMatch::new(self);
        filter.match_account(&message.account.pubkey);
        filter.match_owner(&message.account.owner);
        filter.match_data(&message.account.data);
        filter.get_filters()
    }
}

#[derive(Debug, Default)]
struct FilterAccountsData {
    memcmp: Vec<(usize, Vec<u8>)>,
    datasize: Option<usize>,
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
                None => {
                    anyhow::bail!("filter should be defined");
                }
            }
        }
        Ok(this)
    }

    fn is_empty(&self) -> bool {
        self.memcmp.is_empty() && self.datasize.is_none()
    }

    fn is_match(&self, data: &[u8]) -> bool {
        if matches!(self.datasize, Some(datasize) if data.len() != datasize) {
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

#[derive(Debug, Default)]
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

    fn get_filters(&self, _message: &MessageSlot) -> Vec<String> {
        self.filters.clone()
    }
}

#[derive(Debug)]
pub struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    account_include: HashSet<Pubkey>,
    account_exclude: HashSet<Pubkey>,
}

#[derive(Debug, Default)]
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
                    && filter.account_exclude.is_empty(),
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
                },
            );
        }
        Ok(this)
    }

    pub fn get_filters(
        &self,
        MessageTransaction { transaction, .. }: &MessageTransaction,
    ) -> Vec<String> {
        self.filters
            .iter()
            .filter_map(|(name, inner)| {
                if let Some(is_vote) = inner.vote {
                    if is_vote != transaction.is_vote {
                        return None;
                    }
                }

                if let Some(is_failed) = inner.failed {
                    if is_failed != transaction.meta.status.is_err() {
                        return None;
                    }
                }

                if let Some(signature) = &inner.signature {
                    if signature != transaction.transaction.signature() {
                        return None;
                    }
                }

                if !inner.account_include.is_empty()
                    && transaction
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .all(|pubkey| !inner.account_include.contains(pubkey))
                {
                    return None;
                }

                if !inner.account_exclude.is_empty()
                    && transaction
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .any(|pubkey| inner.account_exclude.contains(pubkey))
                {
                    return None;
                }

                Some(name.clone())
            })
            .collect()
    }
}

#[derive(Debug, Default)]
struct FilterBlocks {
    filters: Vec<String>,
}

impl FilterBlocks {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocks>,
        limit: &ConfigGrpcFiltersBlocks,
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

    fn get_filters(&self, _message: &MessageBlock) -> Vec<String> {
        self.filters.clone()
    }
}

#[derive(Debug, Default)]
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

    fn get_filters(&self, _message: &MessageBlockMeta) -> Vec<String> {
        self.filters.clone()
    }
}
