use {
    crate::{
        geyser::{
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
        plugin::{
            filter::{
                limits::{
                    FilterLimits, FilterLimitsAccounts, FilterLimitsBlocks, FilterLimitsBlocksMeta,
                    FilterLimitsCheckError, FilterLimitsEntries, FilterLimitsSlots,
                    FilterLimitsTransactions,
                },
                message::{
                    FilteredUpdate, FilteredUpdateBlock, FilteredUpdateFilters,
                    FilteredUpdateOneof, FilteredUpdates,
                },
                name::{FilterName, FilterNameError, FilterNames},
            },
            message::{
                CommitmentLevel, Message, MessageAccount, MessageBlock, MessageBlockMeta,
                MessageEntry, MessageSlot, MessageTransaction,
            },
        },
    },
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    bytes::buf::BufMut,
    prost::encoding::{encode_key, encode_varint, WireType},
    solana_sdk::{
        pubkey::{ParsePubkeyError, Pubkey},
        signature::{ParseSignatureError, Signature},
    },
    spl_token_2022::{generic_token_account::GenericTokenAccount, state::Account as TokenAccount},
    std::{
        collections::{HashMap, HashSet},
        ops::Range,
        str::FromStr,
        sync::Arc,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error(transparent)]
    Name(#[from] FilterNameError),
    #[error(transparent)]
    LimitsCheck(#[from] FilterLimitsCheckError),

    #[error("failed to create CommitmentLevel from {commitment}")]
    InvalidCommitment { commitment: i32 },
    #[error(transparent)]
    InvalidPubkey(#[from] ParsePubkeyError),
    #[error(transparent)]
    InvalidSignature(#[from] ParseSignatureError),

    #[error("Too many filters provided; max {max}")]
    CreateAccountStateMaxFilters { max: usize },
    #[error("{0}")]
    CreateAccountState(&'static str),
    #[error("`include_{0}` is not allowed")]
    CreateBlocksNotAllowed(&'static str),
    #[error("failed to create filter: data slices out of order")]
    CreateDataSliceOutOfOrder,
    #[error("failed to create filter: data slices overlapped")]
    CreateDataSliceOverlap,
}

pub type FilterResult<T> = Result<T, FilterError>;

macro_rules! filtered_updates_once_owned {
    ($filters:ident, $message:expr) => {{
        let mut messages = FilteredUpdates::new();
        if !$filters.is_empty() {
            messages.push(FilteredUpdate::new($filters, $message));
        }
        messages
    }};
}

macro_rules! filtered_updates_once_ref {
    ($filters:ident, $message:expr) => {{
        let mut messages = FilteredUpdates::new();
        if !$filters.is_empty() {
            let mut message_filters = FilteredUpdateFilters::new();
            for filter in $filters {
                message_filters.push(filter.clone());
            }
            messages.push(FilteredUpdate::new(message_filters, $message));
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
        limits: &FilterLimits,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        Ok(Self {
            accounts: FilterAccounts::new(&config.accounts, &limits.accounts, names)?,
            slots: FilterSlots::new(&config.slots, &limits.slots, names)?,
            transactions: FilterTransactions::new(
                &config.transactions,
                &limits.transactions,
                FilterTransactionsType::Transaction,
                names,
            )?,
            transactions_status: FilterTransactions::new(
                &config.transactions_status,
                &limits.transactions_status,
                FilterTransactionsType::TransactionStatus,
                names,
            )?,
            entries: FilterEntries::new(&config.entry, &limits.entries, names)?,
            blocks: FilterBlocks::new(&config.blocks, &limits.blocks, names)?,
            blocks_meta: FilterBlocksMeta::new(&config.blocks_meta, &limits.blocks_meta, names)?,
            commitment: Self::decode_commitment(config.commitment)?,
            accounts_data_slice: FilterAccountsDataSlice::new(
                &config.accounts_data_slice,
                limits.accounts.data_slice_max,
            )?,
            ping: config.ping.as_ref().map(|msg| msg.id),
        })
    }

    fn decode_commitment(commitment: Option<i32>) -> FilterResult<CommitmentLevel> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| FilterError::InvalidCommitment { commitment })
    }

    fn decode_pubkeys<'a>(
        pubkeys: &'a [String],
        limit: &'a HashSet<Pubkey>,
    ) -> impl Iterator<Item = FilterResult<Pubkey>> + 'a {
        pubkeys.iter().map(|value| {
            let pubkey = Pubkey::from_str(value)?;
            FilterLimits::check_pubkey_reject(&pubkey, limit)?;
            Ok(pubkey)
        })
    }

    fn decode_pubkeys_into_set(
        pubkeys: &[String],
        limit: &HashSet<Pubkey>,
    ) -> FilterResult<HashSet<Pubkey>> {
        Self::decode_pubkeys(pubkeys, limit).collect::<FilterResult<_>>()
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
            ("entries", self.entries.filters.len()),
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

    pub fn get_updates(
        &self,
        message: &Message,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredUpdates {
        match message {
            Message::Account(message) => self
                .accounts
                .get_updates(message, &self.accounts_data_slice),
            Message::Slot(message) => self.slots.get_updates(message, commitment),
            Message::Transaction(message) => {
                let mut updates = self.transactions.get_updates(message);
                updates.append(&mut self.transactions_status.get_updates(message));
                updates
            }
            Message::Entry(message) => self.entries.get_updates(message),
            Message::Block(message) => self.blocks.get_updates(message, &self.accounts_data_slice),
            Message::BlockMeta(message) => self.blocks_meta.get_updates(message),
        }
    }

    pub fn get_pong_msg(&self) -> Option<FilteredUpdate> {
        self.ping
            .map(|id| FilteredUpdate::new_empty(FilteredUpdateOneof::pong(id)))
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
        limits: &FilterLimitsAccounts,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            this.nonempty_txn_signature
                .push((names.get(name)?, filter.nonempty_txn_signature));
            if filter.nonempty_txn_signature.is_some() {
                this.nonempty_txn_signature_required
                    .insert(names.get(name)?);
            }

            FilterLimits::check_any(
                filter.account.is_empty() && filter.owner.is_empty(),
                limits.any,
            )?;
            FilterLimits::check_pubkey_max(filter.account.len(), limits.account_max)?;
            FilterLimits::check_pubkey_max(filter.owner.len(), limits.owner_max)?;

            Self::set(
                &mut this.account,
                &mut this.account_required,
                name,
                names,
                Filter::decode_pubkeys(&filter.account, &limits.account_reject),
            )?;

            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                name,
                names,
                Filter::decode_pubkeys(&filter.owner, &limits.owner_reject),
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
        keys: impl Iterator<Item = FilterResult<Pubkey>>,
    ) -> FilterResult<bool> {
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

    fn get_updates(
        &self,
        message: &MessageAccount,
        accounts_data_slice: &FilterAccountsDataSlice,
    ) -> FilteredUpdates {
        let mut filter = FilterAccountsMatch::new(self);
        filter.match_txn_signature(&message.account.txn_signature);
        filter.match_account(&message.account.pubkey);
        filter.match_owner(&message.account.owner);
        filter.match_data_lamports(&message.account.data, message.account.lamports);
        let filters = filter.get_filters();
        filtered_updates_once_owned!(
            filters,
            FilteredUpdateOneof::account(message, accounts_data_slice.clone())
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
    fn new(filters: &[SubscribeRequestFilterAccountsFilter]) -> FilterResult<Self> {
        const MAX_FILTERS: usize = 4;
        const MAX_DATA_SIZE: usize = 128;
        const MAX_DATA_BASE58_SIZE: usize = 175;
        const MAX_DATA_BASE64_SIZE: usize = 172;

        if filters.len() > MAX_FILTERS {
            return Err(FilterError::CreateAccountStateMaxFilters { max: MAX_FILTERS });
        }

        let mut this = Self::default();
        for filter in filters {
            match &filter.filter {
                Some(AccountsFilterDataOneof::Memcmp(memcmp)) => {
                    let data = match &memcmp.data {
                        Some(AccountsFilterMemcmpOneof::Bytes(data)) => data.clone(),
                        Some(AccountsFilterMemcmpOneof::Base58(data)) => {
                            if data.len() > MAX_DATA_BASE58_SIZE {
                                return Err(FilterError::CreateAccountState("data too large"));
                            }
                            bs58::decode(data)
                                .into_vec()
                                .map_err(|_| FilterError::CreateAccountState("invalid base58"))?
                        }
                        Some(AccountsFilterMemcmpOneof::Base64(data)) => {
                            if data.len() > MAX_DATA_BASE64_SIZE {
                                return Err(FilterError::CreateAccountState("data too large"));
                            }
                            base64_engine
                                .decode(data)
                                .map_err(|_| FilterError::CreateAccountState("invalid base64"))?
                        }
                        None => {
                            return Err(FilterError::CreateAccountState(
                                "data for memcmp should be defined",
                            ))
                        }
                    };
                    if data.len() > MAX_DATA_SIZE {
                        return Err(FilterError::CreateAccountState("data too large"));
                    }
                    this.memcmp.push((memcmp.offset as usize, data));
                }
                Some(AccountsFilterDataOneof::Datasize(datasize)) => {
                    if this.datasize.replace(*datasize as usize).is_some() {
                        return Err(FilterError::CreateAccountState(
                            "datasize used more than once",
                        ));
                    }
                }
                Some(AccountsFilterDataOneof::TokenAccountState(value)) => {
                    if !value {
                        return Err(FilterError::CreateAccountState(
                            "token_account_state only allowed to be true",
                        ));
                    }
                    this.token_account_state = true;
                }
                Some(AccountsFilterDataOneof::Lamports(
                    SubscribeRequestFilterAccountsFilterLamports { cmp },
                )) => {
                    let Some(cmp) = cmp else {
                        return Err(FilterError::CreateAccountState(
                            "cmp for lamports should be defined",
                        ));
                    };
                    this.lamports.push(cmp.into());
                }
                None => {
                    return Err(FilterError::CreateAccountState("filter should be defined"));
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
struct FilterAccountsMatch<'a> {
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

    fn match_txn_signature(&mut self, txn_signature: &Option<Signature>) {
        for (name, filter) in self.filter.nonempty_txn_signature.iter() {
            if let Some(nonempty_txn_signature) = filter {
                if *nonempty_txn_signature == txn_signature.is_some() {
                    self.nonempty_txn_signature.insert(name.as_ref());
                }
            }
        }
    }

    fn match_account(&mut self, pubkey: &Pubkey) {
        Self::extend(&mut self.account, &self.filter.account, pubkey)
    }

    fn match_owner(&mut self, pubkey: &Pubkey) {
        Self::extend(&mut self.owner, &self.filter.owner, pubkey)
    }

    fn match_data_lamports(&mut self, data: &[u8], lamports: u64) {
        for (name, filter) in self.filter.filters.iter() {
            if filter.is_match(data, lamports) {
                self.data.insert(name.as_ref());
            }
        }
    }

    fn get_filters(&self) -> FilteredUpdateFilters {
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
        limits: &FilterLimitsSlots,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

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

    fn get_updates(
        &self,
        message: &MessageSlot,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredUpdates {
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
            .collect::<FilteredUpdateFilters>();
        filtered_updates_once_owned!(filters, FilteredUpdateOneof::slot(*message))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterTransactionsType {
    Transaction,
    TransactionStatus,
}

#[derive(Debug, Clone)]
struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    account_include: HashSet<Pubkey>,
    account_exclude: HashSet<Pubkey>,
    account_required: HashSet<Pubkey>,
}

#[derive(Debug, Clone)]
struct FilterTransactions {
    filter_type: FilterTransactionsType,
    filters: HashMap<FilterName, FilterTransactionsInner>,
}

impl FilterTransactions {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterTransactions>,
        limits: &FilterLimitsTransactions,
        filter_type: FilterTransactionsType,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut filters = HashMap::new();
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.vote.is_none()
                    && filter.failed.is_none()
                    && filter.account_include.is_empty()
                    && filter.account_exclude.is_empty()
                    && filter.account_required.is_empty(),
                limits.any,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_include.len(),
                limits.account_include_max,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_exclude.len(),
                limits.account_exclude_max,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_required.len(),
                limits.account_required_max,
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
                            signature_str.parse().map_err(FilterError::InvalidSignature)
                        })
                        .transpose()?,
                    account_include: Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?,
                    account_exclude: Filter::decode_pubkeys_into_set(
                        &filter.account_exclude,
                        &HashSet::new(),
                    )?,
                    account_required: Filter::decode_pubkeys_into_set(
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

    pub fn get_updates(&self, message: &MessageTransaction) -> FilteredUpdates {
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
                    if is_failed != message.transaction.meta.err.is_some() {
                        return None;
                    }
                }

                if let Some(signature) = &inner.signature {
                    let tx_sig = message.transaction.transaction.signatures.first();
                    if Some(signature.as_ref()) != tx_sig.map(|sig| sig.as_ref()) {
                        return None;
                    }
                }

                if !inner.account_include.is_empty()
                    && inner
                        .account_include
                        .intersection(&message.transaction.account_keys)
                        .next()
                        .is_none()
                {
                    return None;
                }

                if !inner.account_exclude.is_empty()
                    && inner
                        .account_exclude
                        .intersection(&message.transaction.account_keys)
                        .next()
                        .is_some()
                {
                    return None;
                }

                if !inner.account_required.is_empty()
                    && !inner
                        .account_required
                        .is_subset(&message.transaction.account_keys)
                {
                    return None;
                }

                Some(name.clone())
            })
            .collect::<FilteredUpdateFilters>();

        filtered_updates_once_owned!(
            filters,
            match self.filter_type {
                FilterTransactionsType::Transaction => FilteredUpdateOneof::transaction(message),
                FilterTransactionsType::TransactionStatus => {
                    FilteredUpdateOneof::transaction_status(message)
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
        limits: &FilterLimitsEntries,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, _filter)| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_updates(&self, message: &Arc<MessageEntry>) -> FilteredUpdates {
        let filters = self.filters.as_slice();
        filtered_updates_once_ref!(filters, FilteredUpdateOneof::entry(Arc::clone(message)))
    }
}

#[derive(Debug, Clone)]
struct FilterBlocksInner {
    account_include: HashSet<Pubkey>,
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
        limits: &FilterLimitsBlocks,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.account_include.is_empty(),
                limits.account_include_any,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_include.len(),
                limits.account_include_max,
            )?;
            if !(filter.include_transactions == Some(false) || limits.include_transactions) {
                return Err(FilterError::CreateBlocksNotAllowed("transactions"));
            }
            if !(matches!(filter.include_accounts, None | Some(false)) || limits.include_accounts) {
                return Err(FilterError::CreateBlocksNotAllowed("accounts"));
            }
            if !(matches!(filter.include_entries, None | Some(false)) || limits.include_accounts) {
                return Err(FilterError::CreateBlocksNotAllowed("entries"));
            }

            this.filters.insert(
                names.get(name)?,
                FilterBlocksInner {
                    account_include: Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?,
                    include_transactions: filter.include_transactions,
                    include_accounts: filter.include_accounts,
                    include_entries: filter.include_entries,
                },
            );
        }
        Ok(this)
    }

    fn get_updates(
        &self,
        message: &Arc<MessageBlock>,
        accounts_data_slice: &FilterAccountsDataSlice,
    ) -> FilteredUpdates {
        let mut updates = FilteredUpdates::new();
        for (filter, inner) in self.filters.iter() {
            #[allow(clippy::unnecessary_filter_map)]
            let transactions = if matches!(inner.include_transactions, None | Some(true)) {
                message
                    .transactions
                    .iter()
                    .filter_map(|tx| {
                        if !inner.account_include.is_empty()
                            && inner
                                .account_include
                                .intersection(&tx.account_keys)
                                .next()
                                .is_none()
                        {
                            None
                        } else {
                            Some(Arc::clone(tx))
                        }
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
                            && !inner.account_include.contains(&account.pubkey)
                        {
                            None
                        } else {
                            Some(Arc::clone(account))
                        }
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

            let mut filters = FilteredUpdateFilters::new();
            filters.push(filter.clone());
            updates.push(FilteredUpdate::new(
                filters,
                FilteredUpdateOneof::block(Box::new(FilteredUpdateBlock {
                    meta: Arc::clone(&message.meta),
                    transactions,
                    updated_account_count: message.updated_account_count,
                    accounts_data_slice: accounts_data_slice.clone(),
                    accounts,
                    entries,
                })),
            ));
        }
        updates
    }
}

#[derive(Debug, Default, Clone)]
struct FilterBlocksMeta {
    filters: Vec<FilterName>,
}

impl FilterBlocksMeta {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocksMeta>,
        limits: &FilterLimitsBlocksMeta,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, _filter)| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_updates(&self, message: &Arc<MessageBlockMeta>) -> FilteredUpdates {
        let filters = self.filters.as_slice();
        filtered_updates_once_ref!(
            filters,
            FilteredUpdateOneof::block_meta(Arc::clone(message))
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FilterAccountsDataSlice(Arc<Vec<Range<usize>>>);

impl AsRef<[Range<usize>]> for FilterAccountsDataSlice {
    #[inline]
    fn as_ref(&self) -> &[Range<usize>] {
        &self.0
    }
}

impl FilterAccountsDataSlice {
    pub fn new(slices: &[SubscribeRequestAccountsDataSlice], limits: usize) -> FilterResult<Self> {
        FilterLimits::check_max(slices.len(), limits)?;

        let slices = slices
            .iter()
            .map(|s| Range {
                start: s.offset as usize,
                end: (s.offset + s.length) as usize,
            })
            .collect::<Vec<_>>();

        for (i, slice_a) in slices.iter().enumerate() {
            // check order
            for slice_b in slices[i + 1..].iter() {
                if slice_a.start > slice_b.start {
                    return Err(FilterError::CreateDataSliceOutOfOrder);
                }
            }

            // check overlap
            for slice_b in slices[0..i].iter() {
                if slice_a.start < slice_b.end {
                    return Err(FilterError::CreateDataSliceOverlap);
                }
            }
        }

        Ok(Self::new_unchecked(Arc::new(slices)))
    }

    pub fn new_unchecked(slices: Arc<Vec<Range<usize>>>) -> Self {
        Self(slices)
    }

    pub fn get_slice(&self, source: &[u8]) -> Vec<u8> {
        if self.0.is_empty() {
            source.to_vec()
        } else {
            let mut data = Vec::with_capacity(self.0.iter().map(|ds| ds.end - ds.start).sum());
            for data_slice in self.0.iter() {
                if source.len() >= data_slice.end {
                    data.extend_from_slice(&source[data_slice.start..data_slice.end]);
                }
            }
            data
        }
    }

    pub fn get_slice_len(&self, source: &[u8]) -> usize {
        if self.0.is_empty() {
            source.len()
        } else {
            let mut len = 0;
            for slice in self.0.iter() {
                if source.len() >= slice.end {
                    len += source[slice.start..slice.end].len();
                }
            }
            len
        }
    }

    pub fn slice_encode_raw(&self, tag: u32, source: &[u8], buf: &mut impl BufMut) {
        let len = self.get_slice_len(source) as u64;
        if len > 0 {
            encode_key(tag, WireType::LengthDelimited, buf);
            encode_varint(len, buf);

            if self.0.is_empty() {
                buf.put_slice(source);
            } else {
                for data_slice in self.0.iter() {
                    if source.len() >= data_slice.end {
                        buf.put_slice(&source[data_slice.start..data_slice.end]);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Filter,
        crate::{
            convert_to,
            geyser::{
                SubscribeRequest, SubscribeRequestFilterAccounts,
                SubscribeRequestFilterTransactions,
            },
            plugin::{
                filter::{
                    limits::FilterLimits,
                    message::{FilteredUpdateFilters, FilteredUpdateOneof},
                    name::{FilterName, FilterNames},
                },
                message::{Message, MessageTransaction, MessageTransactionInfo},
            },
        },
        solana_sdk::{
            hash::Hash,
            message::{v0::LoadedAddresses, Message as SolMessage, MessageHeader},
            pubkey::Pubkey,
            signer::{keypair::Keypair, Signer},
            transaction::{SanitizedTransaction, Transaction},
        },
        solana_transaction_status::TransactionStatusMeta,
        std::{collections::HashMap, sync::Arc, time::Duration},
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
        let meta = convert_to::create_transaction_meta(&TransactionStatusMeta {
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
        });
        let sig = sanitized_transaction.signature();
        let account_keys = sanitized_transaction
            .message()
            .account_keys()
            .iter()
            .copied()
            .collect();
        MessageTransaction {
            transaction: Arc::new(MessageTransactionInfo {
                signature: *sig,
                is_vote: true,
                transaction: convert_to::create_transaction(&sanitized_transaction),
                meta,
                index: 1,
                account_keys,
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
        let limit = FilterLimits::default();
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
        let mut limit = FilterLimits::default();
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
        let mut limit = FilterLimits::default();
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
        let mut limit = FilterLimits::default();
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
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredUpdateOneof::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredUpdateOneof::TransactionStatus(_)
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
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredUpdateOneof::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredUpdateOneof::TransactionStatus(_)
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
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        for message in filter.get_updates(&message, None) {
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
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction = create_message_transaction(
            &keypair_x,
            vec![account_key_x, account_key_y, account_key_z],
        );
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredUpdateOneof::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredUpdateOneof::TransactionStatus(_)
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
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_x, vec![account_key_x, account_key_z]);
        let message = Message::Transaction(message_transaction);
        for message in filter.get_updates(&message, None) {
            assert!(message.filters.is_empty());
        }
    }
}
