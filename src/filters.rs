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
            SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::{
        collections::{HashMap, HashSet},
        hash::Hash,
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
    pub fn new(
        config: &SubscribeRequest,
        limit: Option<&ConfigGrpcFilters>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            accounts: FilterAccounts::new(&config.accounts, limit.map(|v| &v.accounts))?,
            slots: FilterSlots::new(&config.slots, limit.map(|v| &v.slots))?,
            transactions: FilterTransactions::new(
                &config.transactions,
                limit.map(|v| &v.transactions),
            )?,
            blocks: FilterBlocks::new(&config.blocks, limit.map(|v| &v.blocks))?,
            blocks_meta: FilterBlocksMeta::new(&config.blocks_meta, limit.map(|v| &v.blocks_meta))?,
        })
    }

    fn decode_pubkeys<T: FromIterator<Pubkey>>(
        pubkeys: &[String],
        limit: Option<&HashSet<Pubkey>>,
    ) -> anyhow::Result<T> {
        pubkeys
            .iter()
            .map(|value| match Pubkey::from_str(value) {
                Ok(pubkey) => {
                    if let Some(limit) = limit {
                        ConfigGrpcFilters::check_pubkey_reject(&pubkey, limit)?;
                    }
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
    filters: Vec<String>,
    account: HashMap<Pubkey, HashSet<String>>,
    account_required: HashSet<String>,
    owner: HashMap<Pubkey, HashSet<String>>,
    owner_required: HashSet<String>,
}

impl FilterAccounts {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterAccounts>,
        limit: Option<&ConfigGrpcFiltersAccounts>,
    ) -> anyhow::Result<Self> {
        if let Some(limit) = limit {
            ConfigGrpcFilters::check_max(configs.len(), limit.max)?;
        }

        let mut this = Self::default();
        for (name, filter) in configs {
            if let Some(limit) = limit {
                ConfigGrpcFilters::check_any(
                    filter.account.is_empty() && filter.owner.is_empty(),
                    limit.any,
                )?;
                ConfigGrpcFilters::check_pubkey_max(filter.account.len(), limit.account_max)?;
                ConfigGrpcFilters::check_pubkey_max(filter.owner.len(), limit.owner_max)?;
            }

            Self::set(
                &mut this.account,
                &mut this.account_required,
                name,
                Filter::decode_pubkeys(&filter.account, limit.map(|v| &v.account_reject))?,
            );

            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                name,
                Filter::decode_pubkeys(&filter.owner, limit.map(|v| &v.owner_reject))?,
            );

            this.filters.push(name.clone());
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
        filter.get_filters()
    }
}

#[derive(Debug)]
pub struct FilterAccountsMatch<'a> {
    filter: &'a FilterAccounts,
    account: HashSet<String>,
    owner: HashSet<String>,
}

impl<'a> FilterAccountsMatch<'a> {
    fn new(filter: &'a FilterAccounts) -> Self {
        Self {
            filter,
            account: Default::default(),
            owner: Default::default(),
        }
    }

    fn extend<Q: Hash + Eq>(
        set: &mut HashSet<String>,
        map: &HashMap<Q, HashSet<String>>,
        key: &Q,
    ) -> bool {
        if let Some(names) = map.get(key) {
            for name in names {
                if !set.contains(name) {
                    set.insert(name.clone());
                }
            }
            true
        } else {
            false
        }
    }

    pub fn match_account(&mut self, pubkey: &Pubkey) -> bool {
        Self::extend(&mut self.account, &self.filter.account, pubkey)
    }

    pub fn match_owner(&mut self, pubkey: &Pubkey) -> bool {
        Self::extend(&mut self.owner, &self.filter.owner, pubkey)
    }

    pub fn get_filters(&self) -> Vec<String> {
        self.filter
            .filters
            .iter()
            .filter_map(|name| {
                let name = name.as_str();
                let af = &self.filter;

                // If filter name in required but not in matched => return `false`
                if af.account_required.contains(name) && !self.account.contains(name) {
                    return None;
                }
                if af.owner_required.contains(name) && !self.owner.contains(name) {
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
        limit: Option<&ConfigGrpcFiltersSlots>,
    ) -> anyhow::Result<Self> {
        if let Some(limit) = limit {
            ConfigGrpcFilters::check_max(configs.len(), limit.max)?;
        }

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
        limit: Option<&ConfigGrpcFiltersTransactions>,
    ) -> anyhow::Result<Self> {
        if let Some(limit) = limit {
            ConfigGrpcFilters::check_max(configs.len(), limit.max)?;
        }

        let mut this = Self::default();
        for (name, filter) in configs {
            if let Some(limit) = limit {
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
            }

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
                        limit.map(|v| &v.account_include_reject),
                    )?,
                    account_exclude: Filter::decode_pubkeys(&filter.account_exclude, None)?,
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
        limit: Option<&ConfigGrpcFiltersBlocks>,
    ) -> anyhow::Result<Self> {
        if let Some(limit) = limit {
            ConfigGrpcFilters::check_max(configs.len(), limit.max)?;
        }

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
        limit: Option<&ConfigGrpcFiltersBlocksMeta>,
    ) -> anyhow::Result<Self> {
        if let Some(limit) = limit {
            ConfigGrpcFilters::check_max(configs.len(), limit.max)?;
        }

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
