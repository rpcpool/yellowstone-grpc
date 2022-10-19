use {
    crate::grpc::proto::{SubscribeRequest, SubscribeRequestAccounts, SubscribeRequestSlots},
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        convert::{From, TryFrom},
        hash::Hash,
        str::FromStr,
    },
};

#[derive(Debug, Default)]
struct FilterSlots {
    enabled: bool,
}

impl From<&SubscribeRequestSlots> for FilterSlots {
    fn from(config: &SubscribeRequestSlots) -> Self {
        FilterSlots {
            enabled: config.enabled,
        }
    }
}

#[derive(Debug)]
struct FilterAccountsExistence {
    account: bool,
    owner: bool,
}

impl FilterAccountsExistence {
    fn is_empty(&self) -> bool {
        !(self.account || self.owner)
    }
}

#[derive(Debug, Default)]
struct FilterAccounts {
    filters: HashMap<String, FilterAccountsExistence>,
    account: HashMap<Pubkey, HashSet<String>>,
    account_required: HashSet<String>,
    owner: HashMap<Pubkey, HashSet<String>>,
    owner_required: HashSet<String>,
}

impl TryFrom<&Vec<SubscribeRequestAccounts>> for FilterAccounts {
    type Error = anyhow::Error;

    fn try_from(configs: &Vec<SubscribeRequestAccounts>) -> Result<Self, Self::Error> {
        let mut this = Self::default();
        for config in configs {
            let existence = FilterAccountsExistence {
                account: Self::set(
                    &mut this.account,
                    &mut this.account_required,
                    &config.filter,
                    config
                        .account
                        .iter()
                        .map(|v| Pubkey::from_str(v))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter(),
                ),
                owner: Self::set(
                    &mut this.owner,
                    &mut this.owner_required,
                    &config.filter,
                    config
                        .owner
                        .iter()
                        .map(|v| Pubkey::from_str(v))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter(),
                ),
            };

            anyhow::ensure!(
                this.filters
                    .insert(config.filter.clone(), existence)
                    .is_none(),
                "filter {} duplicated",
                config.filter
            );
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
}

#[derive(Debug)]
pub struct Filter {
    slots: FilterSlots,
    accounts: FilterAccounts,
}

impl TryFrom<&SubscribeRequest> for Filter {
    type Error = anyhow::Error;

    fn try_from(config: &SubscribeRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            slots: config
                .slots
                .as_ref()
                .map(FilterSlots::from)
                .unwrap_or_default(),
            accounts: FilterAccounts::try_from(&config.accounts)?,
        })
    }
}

impl Filter {
    pub fn is_slots_enabled(&self) -> bool {
        self.slots.enabled
    }

    pub fn create_accounts_match(&self) -> FilterAccountsMatch {
        FilterAccountsMatch::new(&self.accounts)
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
            .filter_map(|(name, existence)| {
                if existence.is_empty() {
                    return Some(name.clone());
                }

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
