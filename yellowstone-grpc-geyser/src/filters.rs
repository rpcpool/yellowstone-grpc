use {
    crate::config::{
        ConfigGrpcFilters, ConfigGrpcFiltersAccounts, ConfigGrpcFiltersBlocks,
        ConfigGrpcFiltersBlocksMeta, ConfigGrpcFiltersEntry, ConfigGrpcFiltersSlots,
        ConfigGrpcFiltersTransactions,
    },
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    smallvec::SmallVec,
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    spl_token_2022::{generic_token_account::GenericTokenAccount, state::Account as TokenAccount},
    std::{
        collections::{HashMap, HashSet},
        ops::Range,
        str::FromStr,
        sync::Arc,
    },
    yellowstone_grpc_proto::{
        plugin::{
            filter::{FilterAccountsDataSlice, FilterName, FilterNames},
            message::{
                CommitmentLevel, Message, MessageAccount, MessageBlock, MessageBlockMeta,
                MessageEntry, MessageSlot, MessageTransaction,
            },
            message_ref::{
                Message as FilteredMessage, MessageFilters as FilteredMessageFilters,
                MessageRef as FilteredMessageRef, MessageRefBlock as FilteredMessageRefBlock,
            },
        },
        prelude::{
            subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
            subscribe_request_filter_accounts_filter_lamports::Cmp as AccountsFilterLamports,
            subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
            CommitmentLevel as CommitmentLevelProto, SubscribeRequest,
            SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
            SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
    },
};

pub type FilteredMessages = SmallVec<[FilteredMessage; 2]>;

macro_rules! filtered_messages_once_owned {
    ($filters:ident, $message:expr) => {{
        let mut messages = FilteredMessages::new();
        if !$filters.is_empty() {
            messages.push(FilteredMessage::new($filters, $message));
        }
        messages
    }};
}

macro_rules! filtered_messages_once_ref {
    ($filters:ident, $message:expr) => {{
        let mut messages = FilteredMessages::new();
        if !$filters.is_empty() {
            let mut message_filters = FilteredMessageFilters::new();
            message_filters.clone_from_slice($filters);
            messages.push(FilteredMessage::new(message_filters, $message));
        }
        messages
    }};
}

#[derive(Debug, Clone)]
pub struct Filter {
    accounts: FilterAccounts,
    slots: FilterSlots,
    transactions: FilterTransactions,
    transactions_status: FilterTransactions,
    entries: FilterEntries,
    blocks: FilterBlocks,
    blocks_meta: FilterBlocksMeta,
    commitment: CommitmentLevel,
    accounts_data_slice: FilterAccountsDataSlice,
    ping: Option<i32>,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            accounts: FilterAccounts::default(),
            slots: FilterSlots::default(),
            transactions: FilterTransactions {
                filter_type: FilterTransactionsType::Transaction,
                filters: HashMap::new(),
            },
            transactions_status: FilterTransactions {
                filter_type: FilterTransactionsType::TransactionStatus,
                filters: HashMap::new(),
            },
            entries: FilterEntries::default(),
            blocks: FilterBlocks::default(),
            blocks_meta: FilterBlocksMeta::default(),
            commitment: CommitmentLevel::Processed,
            accounts_data_slice: FilterAccountsDataSlice::default(),
            ping: None,
        }
    }
}

impl Filter {
    pub fn new(
        config: &SubscribeRequest,
        limit: &ConfigGrpcFilters,
        names: &mut FilterNames,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            accounts: FilterAccounts::new(&config.accounts, &limit.accounts, names)?,
            slots: FilterSlots::new(&config.slots, &limit.slots, names)?,
            transactions: FilterTransactions::new(
                &config.transactions,
                &limit.transactions,
                FilterTransactionsType::Transaction,
                names,
            )?,
            transactions_status: FilterTransactions::new(
                &config.transactions_status,
                &limit.transactions_status,
                FilterTransactionsType::TransactionStatus,
                names,
            )?,
            entries: FilterEntries::new(&config.entry, &limit.entry, names)?,
            blocks: FilterBlocks::new(&config.blocks, &limit.blocks, names)?,
            blocks_meta: FilterBlocksMeta::new(&config.blocks_meta, &limit.blocks_meta, names)?,
            commitment: Self::decode_commitment(config.commitment)?,
            accounts_data_slice: parse_accounts_data_slice_create(
                &config.accounts_data_slice,
                limit.accounts.data_slice_max,
            )?,
            ping: config.ping.as_ref().map(|msg| msg.id),
        })
    }

    fn decode_commitment(commitment: Option<i32>) -> anyhow::Result<CommitmentLevel> {
        let commitment = commitment.unwrap_or(CommitmentLevel::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| {
                anyhow::anyhow!("failed to create CommitmentLevel from {commitment:?}")
            })
    }

    fn decode_pubkeys<'a>(
        pubkeys: &'a [String],
        limit: &'a HashSet<Pubkey>,
    ) -> impl Iterator<Item = anyhow::Result<Pubkey>> + 'a {
        pubkeys.iter().map(|value| match Pubkey::from_str(value) {
            Ok(pubkey) => {
                ConfigGrpcFilters::check_pubkey_reject(&pubkey, limit)?;
                Ok::<Pubkey, anyhow::Error>(pubkey)
            }
            Err(error) => Err(error.into()),
        })
    }

    fn decode_pubkeys_into_vec(
        pubkeys: &[String],
        limit: &HashSet<Pubkey>,
    ) -> anyhow::Result<Vec<Pubkey>> {
        let mut vec =
            Self::decode_pubkeys(pubkeys, limit).collect::<anyhow::Result<Vec<Pubkey>>>()?;
        vec.sort();
        Ok(vec)
    }

    pub fn get_metrics(&self) -> [(&'static str, usize); 8] {
        [
            ("accounts", self.accounts.filters.len()),
            ("slots", self.slots.filters.len()),
            ("transactions", self.transactions.filters.len()),
            (
                "transactions_status",
                self.transactions_status.filters.len(),
            ),
            ("entry", self.entries.filters.len()),
            ("blocks", self.blocks.filters.len()),
            ("blocks_meta", self.blocks_meta.filters.len()),
            (
                "all",
                self.accounts.filters.len()
                    + self.slots.filters.len()
                    + self.transactions.filters.len()
                    + self.transactions_status.filters.len()
                    + self.entries.filters.len()
                    + self.blocks.filters.len()
                    + self.blocks_meta.filters.len(),
            ),
        ]
    }

    pub const fn get_commitment_level(&self) -> CommitmentLevel {
        self.commitment
    }

    pub fn get_filters(
        &self,
        message: &Message,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredMessages {
        match message {
            Message::Account(message) => self
                .accounts
                .get_filters(message, &self.accounts_data_slice),
            Message::Slot(message) => self.slots.get_filters(message, commitment),
            Message::Transaction(message) => {
                let mut messages = self.transactions.get_filters(message);
                messages.append(&mut self.transactions_status.get_filters(message));
                messages
            }
            Message::Entry(message) => self.entries.get_filters(message),
            Message::Block(message) => self.blocks.get_filters(message, &self.accounts_data_slice),
            Message::BlockMeta(message) => self.blocks_meta.get_filters(message),
        }
    }

    pub fn get_pong_msg(&self) -> Option<FilteredMessage> {
        self.ping.map(|id| FilteredMessage {
            filters: FilteredMessageFilters::new(),
            message: FilteredMessageRef::pong(id),
        })
    }

    pub fn create_ping_message() -> FilteredMessage {
        FilteredMessage {
            filters: FilteredMessageFilters::new(),
            message: FilteredMessageRef::Ping,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccounts {
    nonempty_txn_signature: Vec<(FilterName, Option<bool>)>,
    nonempty_txn_signature_required: HashSet<FilterName>,
    account: HashMap<Pubkey, HashSet<FilterName>>,
    account_required: HashSet<FilterName>,
    owner: HashMap<Pubkey, HashSet<FilterName>>,
    owner_required: HashSet<FilterName>,
    filters: Vec<(FilterName, FilterAccountsState)>,
}

impl FilterAccounts {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterAccounts>,
        limit: &ConfigGrpcFiltersAccounts,
        names: &mut FilterNames,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            this.nonempty_txn_signature
                .push((names.get(name)?, filter.nonempty_txn_signature));
            if filter.nonempty_txn_signature.is_some() {
                this.nonempty_txn_signature_required
                    .insert(names.get(name)?);
            }

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
                names,
                Filter::decode_pubkeys(&filter.account, &limit.account_reject),
            )?;

            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                name,
                names,
                Filter::decode_pubkeys(&filter.owner, &limit.owner_reject),
            )?;

            this.filters
                .push((names.get(name)?, FilterAccountsState::new(&filter.filters)?));
        }
        Ok(this)
    }

    fn set(
        map: &mut HashMap<Pubkey, HashSet<FilterName>>,
        map_required: &mut HashSet<FilterName>,
        name: &str,
        names: &mut FilterNames,
        keys: impl Iterator<Item = anyhow::Result<Pubkey>>,
    ) -> anyhow::Result<bool> {
        let mut required = false;
        for maybe_key in keys {
            if map.entry(maybe_key?).or_default().insert(names.get(name)?) {
                required = true;
            }
        }

        if required {
            map_required.insert(names.get(name)?);
        }
        Ok(required)
    }

    fn get_filters(
        &self,
        message: &MessageAccount,
        accounts_data_slice: &FilterAccountsDataSlice,
    ) -> FilteredMessages {
        let mut filter = FilterAccountsMatch::new(self);
        filter.match_txn_signature(&message.account.txn_signature);
        filter.match_account(&message.account.pubkey);
        filter.match_owner(&message.account.owner);
        filter.match_data_lamports(&message.account.data, message.account.lamports);
        let filters = filter.get_filters();
        filtered_messages_once_owned!(
            filters,
            FilteredMessageRef::account(message, accounts_data_slice.clone())
        )
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccountsState {
    memcmp: Vec<(usize, Vec<u8>)>,
    datasize: Option<usize>,
    token_account_state: bool,
    lamports: Vec<FilterAccountsLamports>,
}

impl FilterAccountsState {
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
                Some(AccountsFilterDataOneof::Lamports(
                    SubscribeRequestFilterAccountsFilterLamports { cmp },
                )) => {
                    let Some(cmp) = cmp else {
                        anyhow::bail!("cmp for lamports should be defined");
                    };
                    this.lamports.push(cmp.into());
                }
                None => {
                    anyhow::bail!("filter should be defined");
                }
            }
        }
        Ok(this)
    }

    fn is_empty(&self) -> bool {
        self.memcmp.is_empty()
            && self.datasize.is_none()
            && !self.token_account_state
            && self.lamports.is_empty()
    }

    fn is_match(&self, data: &[u8], lamports: u64) -> bool {
        if matches!(self.datasize, Some(datasize) if data.len() != datasize) {
            return false;
        }
        if self.token_account_state && !TokenAccount::valid_account_data(data) {
            return false;
        }
        if self.lamports.iter().any(|f| !f.is_match(lamports)) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterAccountsLamports {
    Eq(u64),
    Ne(u64),
    Lt(u64),
    Gt(u64),
}

impl From<&AccountsFilterLamports> for FilterAccountsLamports {
    fn from(cmp: &AccountsFilterLamports) -> Self {
        match cmp {
            AccountsFilterLamports::Eq(value) => Self::Eq(*value),
            AccountsFilterLamports::Ne(value) => Self::Ne(*value),
            AccountsFilterLamports::Lt(value) => Self::Lt(*value),
            AccountsFilterLamports::Gt(value) => Self::Gt(*value),
        }
    }
}

impl FilterAccountsLamports {
    const fn is_match(self, lamports: u64) -> bool {
        match self {
            Self::Eq(value) => value == lamports,
            Self::Ne(value) => value != lamports,
            Self::Lt(value) => value < lamports,
            Self::Gt(value) => value > lamports,
        }
    }
}

#[derive(Debug)]
pub struct FilterAccountsMatch<'a> {
    filter: &'a FilterAccounts,
    nonempty_txn_signature: HashSet<&'a str>,
    account: HashSet<&'a str>,
    owner: HashSet<&'a str>,
    data: HashSet<&'a str>,
}

impl<'a> FilterAccountsMatch<'a> {
    fn new(filter: &'a FilterAccounts) -> Self {
        Self {
            filter,
            nonempty_txn_signature: Default::default(),
            account: Default::default(),
            owner: Default::default(),
            data: Default::default(),
        }
    }

    fn extend(
        set: &mut HashSet<&'a str>,
        map: &'a HashMap<Pubkey, HashSet<FilterName>>,
        key: &Pubkey,
    ) {
        if let Some(names) = map.get(key) {
            for name in names {
                set.insert(name.as_ref());
            }
        }
    }

    pub fn match_txn_signature(&mut self, txn_signature: &Option<Signature>) {
        for (name, filter) in self.filter.nonempty_txn_signature.iter() {
            if let Some(nonempty_txn_signature) = filter {
                if *nonempty_txn_signature == txn_signature.is_some() {
                    self.nonempty_txn_signature.insert(name.as_ref());
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

    pub fn match_data_lamports(&mut self, data: &[u8], lamports: u64) {
        for (name, filter) in self.filter.filters.iter() {
            if filter.is_match(data, lamports) {
                self.data.insert(name.as_ref());
            }
        }
    }

    pub fn get_filters(&self) -> FilteredMessageFilters {
        self.filter
            .filters
            .iter()
            .filter_map(|(filter_name, filter)| {
                let name = filter_name.as_ref();
                let af = &self.filter;

                // If filter name in required but not in matched => return `false`
                if af.nonempty_txn_signature_required.contains(name)
                    && !self.nonempty_txn_signature.contains(name)
                {
                    return None;
                }
                if af.account_required.contains(name) && !self.account.contains(name) {
                    return None;
                }
                if af.owner_required.contains(name) && !self.owner.contains(name) {
                    return None;
                }
                if !filter.is_empty() && !self.data.contains(name) {
                    return None;
                }

                Some(filter_name.clone())
            })
            .collect()
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct FilterSlotsInner {
    filter_by_commitment: bool,
}

impl FilterSlotsInner {
    fn new(filter: SubscribeRequestFilterSlots) -> Self {
        Self {
            filter_by_commitment: filter.filter_by_commitment.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct FilterSlots {
    filters: HashMap<FilterName, FilterSlotsInner>,
}

impl FilterSlots {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterSlots>,
        limit: &ConfigGrpcFiltersSlots,
        names: &mut FilterNames,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, filter)| {
                    names
                        .get(name)
                        .map(|name| (name, FilterSlotsInner::new(*filter)))
                })
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_filters(
        &self,
        message: &MessageSlot,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredMessages {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if !inner.filter_by_commitment || commitment == Some(message.status) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<FilteredMessageFilters>();
        filtered_messages_once_owned!(filters, FilteredMessageRef::slot(*message))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterTransactionsType {
    Transaction,
    TransactionStatus,
}

#[derive(Debug, Clone)]
pub struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    account_include: Vec<Pubkey>,
    account_exclude: Vec<Pubkey>,
    account_required: Vec<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct FilterTransactions {
    filter_type: FilterTransactionsType,
    filters: HashMap<FilterName, FilterTransactionsInner>,
}

impl FilterTransactions {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterTransactions>,
        limit: &ConfigGrpcFiltersTransactions,
        filter_type: FilterTransactionsType,
        names: &mut FilterNames,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        let mut filters = HashMap::new();
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

            filters.insert(
                names.get(name)?,
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
                    account_include: Filter::decode_pubkeys_into_vec(
                        &filter.account_include,
                        &limit.account_include_reject,
                    )?,
                    account_exclude: Filter::decode_pubkeys_into_vec(
                        &filter.account_exclude,
                        &HashSet::new(),
                    )?,
                    account_required: Filter::decode_pubkeys_into_vec(
                        &filter.account_required,
                        &HashSet::new(),
                    )?,
                },
            );
        }
        Ok(Self {
            filter_type,
            filters,
        })
    }

    pub fn get_filters(&self, message: &MessageTransaction) -> FilteredMessages {
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
                        .all(|pubkey| inner.account_include.binary_search(pubkey).is_err())
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
                        .any(|pubkey| inner.account_exclude.binary_search(pubkey).is_ok())
                {
                    return None;
                }

                if !inner.account_required.is_empty() {
                    let mut other: Vec<&Pubkey> = message
                        .transaction
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .collect();

                    let is_subset = if inner.account_required.len() <= other.len() {
                        other.sort();
                        inner
                            .account_required
                            .iter()
                            .all(|pubkey| other.binary_search(&pubkey).is_ok())
                    } else {
                        false
                    };

                    if !is_subset {
                        return None;
                    }
                }

                Some(name.clone())
            })
            .collect::<FilteredMessageFilters>();

        filtered_messages_once_owned!(
            filters,
            match self.filter_type {
                FilterTransactionsType::Transaction => FilteredMessageRef::transaction(message),
                FilterTransactionsType::TransactionStatus => {
                    FilteredMessageRef::transaction_status(message)
                }
            }
        )
    }
}

#[derive(Debug, Default, Clone)]
struct FilterEntries {
    filters: Vec<FilterName>,
}

impl FilterEntries {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterEntry>,
        limit: &ConfigGrpcFiltersEntry,
        names: &mut FilterNames,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, _filter)| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_filters(&self, message: &Arc<MessageEntry>) -> FilteredMessages {
        let filters = self.filters.as_slice();
        filtered_messages_once_ref!(filters, FilteredMessageRef::entry(Arc::clone(message)))
    }
}

#[derive(Debug, Clone)]
pub struct FilterBlocksInner {
    account_include: Vec<Pubkey>,
    include_transactions: Option<bool>,
    include_accounts: Option<bool>,
    include_entries: Option<bool>,
}

#[derive(Debug, Default, Clone)]
struct FilterBlocks {
    filters: HashMap<FilterName, FilterBlocksInner>,
}

impl FilterBlocks {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocks>,
        limit: &ConfigGrpcFiltersBlocks,
        names: &mut FilterNames,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            ConfigGrpcFilters::check_any(
                filter.account_include.is_empty(),
                limit.account_include_any,
            )?;
            ConfigGrpcFilters::check_pubkey_max(
                filter.account_include.len(),
                limit.account_include_max,
            )?;
            anyhow::ensure!(
                filter.include_transactions == Some(false) || limit.include_transactions,
                "`include_transactions` is not allowed"
            );
            anyhow::ensure!(
                matches!(filter.include_accounts, None | Some(false)) || limit.include_accounts,
                "`include_accounts` is not allowed"
            );
            anyhow::ensure!(
                matches!(filter.include_entries, None | Some(false)) || limit.include_accounts,
                "`include_entries` is not allowed"
            );

            this.filters.insert(
                names.get(name)?,
                FilterBlocksInner {
                    account_include: Filter::decode_pubkeys_into_vec(
                        &filter.account_include,
                        &limit.account_include_reject,
                    )?,
                    include_transactions: filter.include_transactions,
                    include_accounts: filter.include_accounts,
                    include_entries: filter.include_entries,
                },
            );
        }
        Ok(this)
    }

    fn get_filters(
        &self,
        message: &Arc<MessageBlock>,
        accounts_data_slice: &FilterAccountsDataSlice,
    ) -> FilteredMessages {
        let mut messages = FilteredMessages::new();
        for (filter, inner) in self.filters.iter() {
            #[allow(clippy::unnecessary_filter_map)]
            let transactions = if matches!(inner.include_transactions, None | Some(true)) {
                message
                    .transactions
                    .iter()
                    .filter_map(|tx| {
                        if !inner.account_include.is_empty()
                            && tx
                                .transaction
                                .message()
                                .account_keys()
                                .iter()
                                .all(|pubkey| inner.account_include.binary_search(pubkey).is_err())
                        {
                            return None;
                        }

                        Some(Arc::clone(tx))
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };

            #[allow(clippy::unnecessary_filter_map)]
            let accounts = if inner.include_accounts == Some(true) {
                message
                    .accounts
                    .iter()
                    .filter_map(|account| {
                        if !inner.account_include.is_empty()
                            && inner
                                .account_include
                                .binary_search(&account.pubkey)
                                .is_err()
                        {
                            return None;
                        }

                        Some(Arc::clone(account))
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };

            let entries = if inner.include_entries == Some(true) {
                message.entries.to_vec()
            } else {
                vec![]
            };

            let mut message_filters = FilteredMessageFilters::new();
            message_filters.push(filter.clone());
            messages.push(FilteredMessage::new(
                message_filters,
                FilteredMessageRef::block(FilteredMessageRefBlock {
                    meta: Arc::clone(&message.meta),
                    transactions,
                    updated_account_count: message.updated_account_count,
                    accounts_data_slice: accounts_data_slice.clone(),
                    accounts,
                    entries,
                }),
            ));
        }
        messages
    }
}

#[derive(Debug, Default, Clone)]
struct FilterBlocksMeta {
    filters: Vec<FilterName>,
}

impl FilterBlocksMeta {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocksMeta>,
        limit: &ConfigGrpcFiltersBlocksMeta,
        names: &mut FilterNames,
    ) -> anyhow::Result<Self> {
        ConfigGrpcFilters::check_max(configs.len(), limit.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, _filter)| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_filters(&self, message: &Arc<MessageBlockMeta>) -> FilteredMessages {
        let filters = self.filters.as_slice();
        filtered_messages_once_ref!(filters, FilteredMessageRef::block_meta(Arc::clone(message)))
    }
}

pub fn parse_accounts_data_slice_create(
    slices: &[SubscribeRequestAccountsDataSlice],
    limit: usize,
) -> anyhow::Result<FilterAccountsDataSlice> {
    anyhow::ensure!(
        slices.len() <= limit,
        "Max amount of data_slices reached, only {} allowed",
        limit
    );

    let slices = slices
        .iter()
        .map(|s| Range {
            start: s.offset as usize,
            end: (s.offset + s.length) as usize,
        })
        .collect::<FilterAccountsDataSlice>();

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

#[cfg(test)]
mod tests {
    use {
        super::{FilterName, FilterNames},
        crate::{config::ConfigGrpcFilters, filters::Filter},
        solana_sdk::{
            hash::Hash,
            message::{v0::LoadedAddresses, Message as SolMessage, MessageHeader},
            pubkey::Pubkey,
            signer::{keypair::Keypair, Signer},
            transaction::{SanitizedTransaction, Transaction},
        },
        solana_transaction_status::TransactionStatusMeta,
        std::{collections::HashMap, sync::Arc, time::Duration},
        yellowstone_grpc_proto::{
            geyser::{
                SubscribeRequest, SubscribeRequestFilterAccounts,
                SubscribeRequestFilterTransactions,
            },
            plugin::{
                message::{Message, MessageTransaction, MessageTransactionInfo},
                message_ref::{
                    MessageFilters as FilteredMessageFilters, MessageRef as FilteredMessageRef,
                },
            },
        },
    };

    fn create_filter_names() -> FilterNames {
        FilterNames::new(64, 1024, Duration::from_secs(1))
    }

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
            transaction: Arc::new(MessageTransactionInfo {
                signature: *sig,
                is_vote: true,
                transaction: sanitized_transaction,
                meta,
                index: 1,
            }),
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
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names());
        assert!(filter.is_ok());
    }

    #[test]
    fn test_filters_account_empty() {
        let mut accounts = HashMap::new();

        accounts.insert(
            "solend".to_owned(),
            SubscribeRequestFilterAccounts {
                nonempty_txn_signature: None,
                account: vec![],
                owner: vec![],
                filters: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts,
            slots: HashMap::new(),
            transactions: HashMap::new(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.accounts.any = false;
        let filter = Filter::new(&config, &limit, &mut create_filter_names());
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
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.transactions.any = false;
        let filter = Filter::new(&config, &limit, &mut create_filter_names());
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
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let mut limit = ConfigGrpcFilters::default();
        limit.transactions.any = false;
        let filter_res = Filter::new(&config, &limit, &mut create_filter_names());
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
        let account_include = [account_key_a].iter().map(|k| k.to_string()).collect();
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

        let mut config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: transactions.clone(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_filters(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredMessageFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredMessageRef::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_filters(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredMessageFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredMessageRef::TransactionStatus(_)
        ));
    }

    #[test]
    fn test_transaction_include_b() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = [account_key_b].iter().map(|k| k.to_string()).collect();
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

        let mut config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: transactions.clone(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_filters(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredMessageFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredMessageRef::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_filters(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredMessageFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredMessageRef::TransactionStatus(_)
        ));
    }

    #[test]
    fn test_transaction_exclude() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_exclude = [account_key_b].iter().map(|k| k.to_string()).collect();
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
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        for message in filter.get_filters(&message, None) {
            assert!(message.filters.is_empty());
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
        let account_include = [account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        let account_required = [account_key_x].iter().map(|k| k.to_string()).collect();
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

        let mut config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: transactions.clone(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction = create_message_transaction(
            &keypair_x,
            vec![account_key_x, account_key_y, account_key_z],
        );
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_filters(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredMessageFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredMessageRef::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_filters(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredMessageFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredMessageRef::TransactionStatus(_)
        ));
    }

    #[test]
    fn test_transaction_required_y_z_include_x() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        // require x, include y, z
        let account_include = [account_key_x].iter().map(|k| k.to_string()).collect();
        let account_required = [account_key_y, account_key_z]
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
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
        };
        let limit = ConfigGrpcFilters::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_x, vec![account_key_x, account_key_z]);
        let message = Message::Transaction(message_transaction);
        for message in filter.get_filters(&message, None) {
            assert!(message.filters.is_empty());
        }
    }
}
