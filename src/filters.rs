use {
    crate::{
        grpc::{Message, MessageAccount, MessageBlock, MessageSlot, MessageTransaction},
        proto::{
            SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
        },
    },
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        convert::TryFrom,
        hash::Hash,
        str::FromStr,
    },
};

#[derive(Debug)]
pub struct Filter {
    accounts: FilterAccounts,
    slots: FilterSlots,
    transactions: FilterTransactions,
    blocks: FilterBlocks,
}

impl TryFrom<&SubscribeRequest> for Filter {
    type Error = anyhow::Error;

    fn try_from(config: &SubscribeRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            accounts: FilterAccounts::try_from(&config.accounts)?,
            slots: FilterSlots::try_from(&config.slots)?,
            transactions: FilterTransactions::try_from(&config.transactions)?,
            blocks: FilterBlocks::try_from(&config.blocks)?,
        })
    }
}

impl Filter {
    pub fn get_filters(&self, message: &Message) -> Vec<String> {
        match message {
            Message::Account(message) => self.accounts.get_filters(message),
            Message::Slot(message) => self.slots.get_filters(message),
            Message::Transaction(message) => self.transactions.get_filters(message),
            Message::Block(message) => self.blocks.get_filters(message),
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

impl TryFrom<&HashMap<String, SubscribeRequestFilterAccounts>> for FilterAccounts {
    type Error = anyhow::Error;

    fn try_from(
        configs: &HashMap<String, SubscribeRequestFilterAccounts>,
    ) -> Result<Self, Self::Error> {
        let mut this = Self::default();
        for (name, filter) in configs {
            Self::set(
                &mut this.account,
                &mut this.account_required,
                name,
                filter
                    .account
                    .iter()
                    .map(|v| Pubkey::from_str(v))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter(),
            );

            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                name,
                filter
                    .owner
                    .iter()
                    .map(|v| Pubkey::from_str(v))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter(),
            );

            this.filters.push(name.clone());
        }
        Ok(this)
    }
}

impl FilterAccounts {
    fn set<Q, I>(
        map: &mut HashMap<Q, HashSet<String>>,
        map_required: &mut HashSet<String>,
        name: &str,
        keys: I,
    ) -> bool
    where
        Q: Hash + Eq + Clone,
        I: Iterator<Item = Q>,
    {
        let mut required = false;
        for key in keys {
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

impl TryFrom<&HashMap<String, SubscribeRequestFilterSlots>> for FilterSlots {
    type Error = anyhow::Error;

    fn try_from(
        configs: &HashMap<String, SubscribeRequestFilterSlots>,
    ) -> Result<Self, Self::Error> {
        Ok(FilterSlots {
            filters: configs
                .iter()
                // .filter_map(|(name, _filter)| Some(name.clone()))
                .map(|(name, _filter)| name.clone())
                .collect(),
        })
    }
}

impl FilterSlots {
    fn get_filters(&self, _message: &MessageSlot) -> Vec<String> {
        self.filters.clone()
    }
}

#[derive(Debug)]
pub struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    accounts_include: HashSet<Pubkey>,
    accounts_exclude: HashSet<Pubkey>,
}

#[derive(Debug, Default)]
pub struct FilterTransactions {
    filters: HashMap<String, FilterTransactionsInner>,
}

impl TryFrom<&HashMap<String, SubscribeRequestFilterTransactions>> for FilterTransactions {
    type Error = anyhow::Error;

    fn try_from(
        configs: &HashMap<String, SubscribeRequestFilterTransactions>,
    ) -> Result<Self, Self::Error> {
        let mut this = Self::default();
        for (name, filter) in configs {
            this.filters.insert(
                name.clone(),
                FilterTransactionsInner {
                    vote: filter.vote,
                    failed: filter.failed,
                    accounts_include: filter
                        .accounts_include
                        .iter()
                        .map(|v| Pubkey::from_str(v))
                        .collect::<Result<_, _>>()?,
                    accounts_exclude: filter
                        .accounts_exclude
                        .iter()
                        .map(|v| Pubkey::from_str(v))
                        .collect::<Result<_, _>>()?,
                },
            );
        }
        Ok(this)
    }
}

impl FilterTransactions {
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

                if !inner.accounts_include.is_empty()
                    && transaction
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .all(|pubkey| !inner.accounts_include.contains(pubkey))
                {
                    return None;
                }

                if !inner.accounts_exclude.is_empty()
                    && transaction
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .any(|pubkey| inner.accounts_exclude.contains(pubkey))
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

impl TryFrom<&HashMap<String, SubscribeRequestFilterBlocks>> for FilterBlocks {
    type Error = anyhow::Error;

    fn try_from(
        configs: &HashMap<String, SubscribeRequestFilterBlocks>,
    ) -> Result<Self, Self::Error> {
        Ok(FilterBlocks {
            filters: configs
                .iter()
                // .filter_map(|(name, _filter)| Some(name.clone()))
                .map(|(name, _filter)| name.clone())
                .collect(),
        })
    }
}

impl FilterBlocks {
    fn get_filters(&self, _message: &MessageBlock) -> Vec<String> {
        self.filters.clone()
    }
}
