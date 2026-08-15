use {
    crate::{
        plugin::{
            filter::{
                limits::{
                    FilterLimits, FilterLimitsAccounts, FilterLimitsBlocks, FilterLimitsBlocksMeta,
                    FilterLimitsCheckError, FilterLimitsDeshredTransactions, FilterLimitsEntries,
                    FilterLimitsSlots, FilterLimitsTransactions,
                },
                message::{
                    FilteredUpdate, FilteredUpdateBlock, FilteredUpdateDeshred,
                    FilteredUpdateDeshredOneof, FilteredUpdateFilters, FilteredUpdateOneof,
                    FilteredUpdates, FilteredUpdatesDeshred,
                },
                name::{FilterName, FilterNameError, FilterNames},
            },
            message::{
                CommitmentLevel, Message, MessageAccount, MessageAccountInfo, MessageBlock,
                MessageBlockMeta, MessageDeshredTransaction, MessageEntry, MessageSlot,
                MessageTransaction, SlotStatus,
            },
        },
        pubkey_collections::{PubkeyHashSet, PubkeyRefHashSet},
    },
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    bytes::buf::BufMut,
    foldhash::{HashMap as FoldHashMap, HashMapExt, HashSet as FoldHashSet, HashSetExt},
    prost::encoding::{encode_key, encode_varint, WireType},
    solana_pubkey::{ParsePubkeyError, Pubkey},
    solana_signature::{ParseSignatureError, Signature},
    spl_token_2022_interface::{
        generic_token_account::GenericTokenAccount, state::Account as TokenAccount,
    },
    std::{collections::HashMap, ops::Range, str::FromStr, sync::Arc},
    yellowstone_grpc_proto::{
        cuckoo::CuckooFilter,
        geyser::{
            subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
            subscribe_request_filter_accounts_filter_lamports::Cmp as AccountsFilterLamports,
            subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
            CommitmentLevel as CommitmentLevelProto, SubscribeDeshredRequest, SubscribeRequest,
            SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
            SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterEntry,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
            TokenAccountExpansionControlFlag,
        },
        solana::storage::confirmed_block,
    },
};

#[derive(Debug, Clone)]
struct HybridSet<T> {
    vec: Vec<T>,
    set: FoldHashSet<T>,
}

impl<T: Eq + std::hash::Hash> HybridSet<T>
where
    T: Clone,
{
    fn new_with_set(set: FoldHashSet<T>) -> Self {
        let vec = set.iter().cloned().collect();
        Self { vec, set }
    }

    const fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    const fn len(&self) -> usize {
        self.vec.len()
    }

    fn overlaps<'a, I>(
        &self,
        other_len: usize,
        contains_other: impl Fn(&T) -> bool,
        other: impl Fn() -> I,
    ) -> bool
    where
        I: Iterator<Item = &'a T>,
        T: 'a,
    {
        /* iterate vec if we are smaller than the other collection */
        if self.vec.len() <= other_len {
            self.vec.iter().any(contains_other)
        } else {
            other().any(|k| self.set.contains(k))
        }
    }
}

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
    #[error("invalid token_accounts mode value {0}; expected ALL (0) or BALANCE_CHANGED (1)")]
    InvalidTokenAccountsMode(i32),
}

pub type FilterResult<T> = Result<T, FilterError>;

macro_rules! filtered_updates_once_owned {
    ($filters:ident, $message:expr, $created_at:expr) => {{
        let mut messages = FilteredUpdates::new();
        if !$filters.is_empty() {
            messages.push(FilteredUpdate::new($filters, $message, $created_at));
        }
        messages
    }};
}

macro_rules! filtered_updates_once_ref {
    ($filters:ident, $message:expr, $created_at:expr) => {{
        let mut messages = FilteredUpdates::new();
        if !$filters.is_empty() {
            let mut message_filters = FilteredUpdateFilters::new();
            for filter in $filters {
                message_filters.push(filter.clone());
            }
            messages.push(FilteredUpdate::new(message_filters, $message, $created_at));
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
                filters: Vec::new(),
            },
            transactions_status: FilterTransactions {
                filter_type: FilterTransactionsType::TransactionStatus,
                filters: Vec::new(),
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

/// Statistic snapshot for one account filter definition.
///
/// Counts represent how many account/owner keys were configured in that
/// filter, not how many runtime matches occurred.
#[derive(Debug, Clone)]
pub struct AccountFilterStats {
    /// Number of configured account pubkeys in this filter.
    pub accounts_len: usize,
    /// Number of configured owner pubkeys in this filter.
    pub owners_len: usize,
}

/// Statistic snapshot for one transaction (or transaction status) filter.
#[derive(Debug, Clone)]
pub struct TxnFilterStats {
    /// Number of pubkeys in `account_include`.
    pub accounts_include_len: usize,
    /// Number of pubkeys in `account_exclude`.
    pub accounts_exclude_len: usize,
    /// Number of pubkeys in `account_required`.
    pub accounts_required_len: usize,
    /// Whether token-account expansion is enabled for this filter.
    pub token_accounts_enabled: bool,
}

/// Statistic snapshot for one block filter.
#[derive(Debug, Clone)]
pub struct BlocksFilterStats {
    /// Number of inclusion conditions enabled on this filter.
    pub include_len: usize,
    /// Number of explicit exclusion toggles on this filter.
    pub exclude_len: usize,
}

/// Aggregate statistics view across all filter groups.
///
/// This is intended for diagnostics/telemetry and should not drive business logic decisions.
#[derive(Clone, Debug)]
pub struct FilterStats {
    /// Per-account-filter complexity entries.
    pub accounts: Vec<AccountFilterStats>,
    /// Per-transaction-filter complexity entries.
    pub transactions: Vec<TxnFilterStats>,
    /// Per-transaction-status-filter complexity entries.
    pub transaction_status: Vec<TxnFilterStats>,
    /// Per-block-filter complexity entries.
    pub blocks: Vec<BlocksFilterStats>,
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

    /// Build a static statistics snapshot of the currently parsed filter.
    ///
    /// The returned values are derived from configured filter criteria only.
    /// They do not depend on live traffic and are safe to call for logging,
    /// metrics, and debugging.
    pub fn get_filter_stats(&self) -> FilterStats {
        // We iterate `aggregates` as the canonical list of account filters.
        // Reason: an account filter can exist without account/owner pubkeys
        // (for example only state checks or nonempty_txn_signature), and those
        // filters would be missing if we iterated only account/owner maps.
        let accounts = self
            .accounts
            .aggregates
            .iter()
            .map(|aggregate| AccountFilterStats {
                // `accounts.account` is indexed by pubkey -> set(filter_name).
                // For complexity we need the inverse shape (filter_name -> count(pubkeys)),
                // so we accumulate how many pubkeys point to each filter.
                accounts_len: aggregate
                    .accounts
                    .as_ref()
                    .map(|set| set.len())
                    .unwrap_or(0),
                owners_len: aggregate.owners.as_ref().map(|set| set.len()).unwrap_or(0),
            })
            .collect();

        // Transaction filter complexity is direct: one observation per parsed
        // filter with sizes of include/exclude/required lists plus token mode.
        let transactions = self
            .transactions
            .filters
            .iter()
            .map(|(_name, inner)| TxnFilterStats {
                accounts_include_len: inner.account_include.len(),
                accounts_exclude_len: inner.account_exclude.len(),
                accounts_required_len: inner.account_required.len(),
                token_accounts_enabled: inner.token_accounts.is_some(),
            })
            .collect();

        // Same logic for transaction-status filters.
        let transaction_status = self
            .transactions_status
            .filters
            .iter()
            .map(|(_name, inner)| TxnFilterStats {
                accounts_include_len: inner.account_include.len(),
                accounts_exclude_len: inner.account_exclude.len(),
                accounts_required_len: inner.account_required.len(),
                token_accounts_enabled: inner.token_accounts.is_some(),
            })
            .collect();

        // Block filters have both selector lists and include/exclude toggles.
        // We summarize these as simple counts so logs/metrics can compare
        // relative complexity without serializing full filter internals.
        let blocks = self
            .blocks
            .filters
            .values()
            .map(|inner| BlocksFilterStats {
                include_len: inner.account_include.len()
                    + usize::from(inner.account_cuckoo.is_some())
                    + usize::from(matches!(inner.include_transactions, None | Some(true)))
                    + usize::from(inner.include_accounts == Some(true))
                    + usize::from(inner.include_entries == Some(true)),
                exclude_len: usize::from(inner.include_transactions == Some(false))
                    + usize::from(inner.include_accounts == Some(false))
                    + usize::from(inner.include_entries == Some(false)),
            })
            .collect();

        // Return a full snapshot grouped by filter category.
        FilterStats {
            accounts,
            transactions,
            transaction_status,
            blocks,
        }
    }

    fn decode_commitment(commitment: Option<i32>) -> FilterResult<CommitmentLevel> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        let commitment = CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| FilterError::InvalidCommitment { commitment })?;
        if !matches!(
            commitment,
            CommitmentLevel::Processed | CommitmentLevel::Confirmed | CommitmentLevel::Finalized
        ) {
            Err(FilterError::InvalidCommitment {
                commitment: commitment as i32,
            })
        } else {
            Ok(commitment)
        }
    }

    fn decode_pubkeys<'a>(
        pubkeys: &'a [String],
        limit: &'a FoldHashSet<Pubkey>,
    ) -> impl Iterator<Item = FilterResult<Pubkey>> + 'a {
        pubkeys.iter().map(|value| {
            let pubkey = Pubkey::from_str(value)?;
            FilterLimits::check_pubkey_reject(&pubkey, limit)?;
            Ok(pubkey)
        })
    }

    fn decode_pubkeys_into_set(
        pubkeys: &[String],
        limit: &FoldHashSet<Pubkey>,
    ) -> FilterResult<FoldHashSet<Pubkey>> {
        Self::decode_pubkeys(pubkeys, limit).collect::<FilterResult<_>>()
    }

    pub fn get_metrics(&self) -> [(&'static str, usize); 8] {
        [
            ("accounts", self.accounts.aggregates.len()),
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
                self.accounts.aggregates.len()
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
            Message::DeshredTransaction(_) => FilteredUpdates::new(),
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

// Collective representation of all the required conditions of a single account filter. This is used to determine if a filter should be included in the filtered updates.
// A type of filtering condition is set to 'true' if the filter requires that condition to be satisfied. If a filter does not require a certain condition, it is set to 'false'.
// The current filtering mechanism does not have an 'OR' condition. Everything listed inside of a single filter sent by the client has to be satisfied.
#[derive(Debug, Clone)]
struct FilterAccountAggregate {
    pub filter_name: FilterName,
    pub txn_signature: Option<bool>,
    pub accounts: Option<FoldHashSet<Pubkey>>,
    pub accounts_cuckoo: Option<Arc<CuckooFilter<[u8; 32]>>>,
    pub owners: Option<FoldHashSet<Pubkey>>,
    pub account_state: Option<FilterAccountsState>,
}

impl FilterAccountAggregate {
    pub const fn new(filter_name: FilterName) -> Self {
        Self {
            filter_name,
            txn_signature: None,
            accounts: None,
            accounts_cuckoo: None,
            owners: None,
            account_state: None,
        }
    }

    const fn match_txn_signature(&self, txn_signature: &Option<Signature>) -> bool {
        if let Some(required_txn_signature) = self.txn_signature {
            if required_txn_signature != txn_signature.is_some() {
                return false;
            }
        }

        true
    }

    fn match_account(&self, pubkey: &Pubkey, matched_account: Option<bool>) -> bool {
        if self.accounts.is_none() && self.accounts_cuckoo.is_none() {
            return true;
        }

        if !self.accounts.as_ref().is_some_and(|accounts| {
            if let Some(matched_account) = matched_account {
                return matched_account;
            }

            accounts.contains(pubkey)
        }) && !self
            .accounts_cuckoo
            .as_ref()
            .is_some_and(|accounts_cuckoo| accounts_cuckoo.contains(&pubkey.to_bytes()))
        {
            return false;
        }

        true
    }

    fn match_owner(&self, pubkey: &Pubkey, matched_owner: Option<bool>) -> bool {
        if let Some(owners) = &self.owners {
            if let Some(matched_owner) = matched_owner {
                return matched_owner;
            }

            if !owners.contains(pubkey) {
                return false;
            }
        }
        true
    }

    fn match_data_lamports(&self, data: &[u8], lamports: u64) -> bool {
        if let Some(account_state) = &self.account_state {
            if !account_state.is_match(data, lamports) {
                return false;
            }
        }

        true
    }

    // Returns the filter name if all requirements are satisfied, otherwise returns None.
    // This is used to determine if a filter should be included in the filtered updates.
    // If a filter is not included, it means that the filter's requirements are not satisfied by the current message.
    // If no filters are satisfied by the current message, then the update will not be sent to the client.
    pub fn match_filter(
        &self,
        account: &MessageAccountInfo,
        matched_account: Option<bool>,
        matched_owner: Option<bool>,
    ) -> Option<FilterName> {
        if !self.match_txn_signature(&account.txn_signature) {
            return None;
        }

        if !self.match_account(&account.pubkey, matched_account) {
            return None;
        }

        if !self.match_owner(&account.owner, matched_owner) {
            return None;
        }

        if !self.match_data_lamports(&account.data, account.lamports) {
            return None;
        }

        Some(self.filter_name.clone())
    }
}

#[derive(Debug, Clone)]
struct FilterBitSet(Box<[u64]>);

impl FilterBitSet {
    const BITS_PER_WORD: usize = u64::BITS as usize;

    fn new(bits: usize) -> Self {
        Self(vec![0; bits.div_ceil(Self::BITS_PER_WORD)].into_boxed_slice())
    }

    fn insert(&mut self, index: usize) {
        self.0[index / Self::BITS_PER_WORD] |= 1 << (index % Self::BITS_PER_WORD);
    }

    fn is_empty(&self) -> bool {
        self.0.iter().all(|word| *word == 0)
    }

    fn word(&self, index: usize) -> u64 {
        self.0[index]
    }

    #[cfg(test)]
    fn contains(&self, index: usize) -> bool {
        self.word(index / Self::BITS_PER_WORD) & (1 << (index % Self::BITS_PER_WORD)) != 0
    }
}

#[derive(Debug, Clone)]
struct FilterAccountsIndex {
    accounts: FoldHashMap<Pubkey, FilterBitSet>,
    owners: FoldHashMap<Pubkey, FilterBitSet>,
    account_fallback: FilterBitSet,
    owner_fallback: FilterBitSet,
}

impl FilterAccountsIndex {
    const MAX_BITSET_BYTES: usize = 16 * 1024 * 1024;

    fn new(aggregates: &[FilterAccountAggregate]) -> Option<Self> {
        if aggregates.len() <= 1 {
            return None;
        }

        let word_bytes = aggregates
            .len()
            .div_ceil(FilterBitSet::BITS_PER_WORD)
            .saturating_mul(std::mem::size_of::<u64>());
        let memberships = aggregates.iter().fold(0usize, |count, aggregate| {
            count
                .saturating_add(
                    aggregate
                        .accounts
                        .as_ref()
                        .map_or(0, |accounts| accounts.len()),
                )
                .saturating_add(aggregate.owners.as_ref().map_or(0, |owners| owners.len()))
        });
        if word_bytes.saturating_mul(memberships.saturating_add(2)) > Self::MAX_BITSET_BYTES {
            return None;
        }

        let mut this = Self {
            accounts: FoldHashMap::default(),
            owners: FoldHashMap::default(),
            account_fallback: FilterBitSet::new(aggregates.len()),
            owner_fallback: FilterBitSet::new(aggregates.len()),
        };

        for (index, aggregate) in aggregates.iter().enumerate() {
            if let Some(accounts) = &aggregate.accounts {
                for account in accounts {
                    this.accounts
                        .entry(*account)
                        .or_insert_with(|| FilterBitSet::new(aggregates.len()))
                        .insert(index);
                }
            }

            if aggregate.accounts.is_none() || aggregate.accounts_cuckoo.is_some() {
                this.account_fallback.insert(index);
            }

            if let Some(owners) = &aggregate.owners {
                for owner in owners {
                    this.owners
                        .entry(*owner)
                        .or_insert_with(|| FilterBitSet::new(aggregates.len()))
                        .insert(index);
                }
            } else {
                this.owner_fallback.insert(index);
            }
        }

        if this.accounts.is_empty() && this.owners.is_empty() {
            None
        } else {
            Some(this)
        }
    }

    fn get_filters(
        &self,
        aggregates: &[FilterAccountAggregate],
        account: &MessageAccountInfo,
    ) -> FilteredUpdateFilters {
        let matched_accounts = self.accounts.get(&account.pubkey);
        if matched_accounts.is_none() && self.account_fallback.is_empty() {
            return FilteredUpdateFilters::new();
        }

        let matched_owners = self.owners.get(&account.owner);
        if matched_owners.is_none() && self.owner_fallback.is_empty() {
            return FilteredUpdateFilters::new();
        }

        let mut filters = FilteredUpdateFilters::new();
        for word_index in 0..self.account_fallback.0.len() {
            let matched_accounts_word = matched_accounts
                .map(|bitset| bitset.word(word_index))
                .unwrap_or_default();
            let matched_owners_word = matched_owners
                .map(|bitset| bitset.word(word_index))
                .unwrap_or_default();
            let mut candidates = (matched_accounts_word | self.account_fallback.word(word_index))
                & (matched_owners_word | self.owner_fallback.word(word_index));

            while candidates != 0 {
                let bit_index = candidates.trailing_zeros() as usize;
                let aggregate_index = word_index * FilterBitSet::BITS_PER_WORD + bit_index;
                let bit = 1 << bit_index;

                if let Some(filter) = aggregates[aggregate_index].match_filter(
                    account,
                    Some(matched_accounts_word & bit != 0),
                    Some(matched_owners_word & bit != 0),
                ) {
                    filters.push(filter);
                }

                candidates &= candidates - 1;
            }
        }

        filters
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccounts {
    aggregates: Vec<FilterAccountAggregate>,
    index: Option<Arc<FilterAccountsIndex>>,
}

impl FilterAccounts {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterAccounts>,
        limits: &FilterLimitsAccounts,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut filter_accounts_result = Self::default();

        for (name, filter) in configs {
            let has_filter_criteria = !filter.account.is_empty()
                || !filter.owner.is_empty()
                || filter.cuckoo_accounts_filter.is_some();

            FilterLimits::check_any(!has_filter_criteria, limits.any)?;
            FilterLimits::check_pubkey_max(filter.account.len(), limits.account_max)?;
            FilterLimits::check_pubkey_max(filter.owner.len(), limits.owner_max)?;

            let filter_name = names.get(name)?;
            let mut filter_aggregate = FilterAccountAggregate::new(filter_name);

            if filter.nonempty_txn_signature.is_some() {
                filter_aggregate.txn_signature = filter.nonempty_txn_signature;
            }

            if !filter.account.is_empty() {
                let accounts =
                    Filter::decode_pubkeys_into_set(&filter.account, &limits.account_reject)?;
                if !accounts.is_empty() {
                    filter_aggregate.accounts = Some(accounts);
                }
            }

            if !filter.owner.is_empty() {
                let owners = Filter::decode_pubkeys_into_set(&filter.owner, &limits.owner_reject)?;
                if !owners.is_empty() {
                    filter_aggregate.owners = Some(owners);
                }
            }

            let filter_accounts_state = FilterAccountsState::new(&filter.filters)?;

            let require_state_check = !filter_accounts_state.is_empty();
            if require_state_check {
                filter_aggregate.account_state = Some(filter_accounts_state);
            }

            if let Some(proto_cuckoo) = &filter.cuckoo_accounts_filter {
                FilterLimits::check_max(proto_cuckoo.data.len(), limits.cuckoo_max_size)?;
                filter_aggregate.accounts_cuckoo = Some(Arc::new(CuckooFilter::from(proto_cuckoo)));
            }

            filter_accounts_result.aggregates.push(filter_aggregate);
        }

        filter_accounts_result.index =
            FilterAccountsIndex::new(&filter_accounts_result.aggregates).map(Arc::new);

        Ok(filter_accounts_result)
    }

    fn get_updates(
        &self,
        message: &Arc<MessageAccount>,
        accounts_data_slice: &FilterAccountsDataSlice,
    ) -> FilteredUpdates {
        if self.aggregates.is_empty() {
            return FilteredUpdates::new();
        }

        let filters = if let Some(index) = &self.index {
            index.get_filters(&self.aggregates, &message.account)
        } else {
            self.aggregates
                .iter()
                .filter_map(|aggregate| aggregate.match_filter(&message.account, None, None))
                .collect()
        };

        filtered_updates_once_owned!(
            filters,
            FilteredUpdateOneof::account(Arc::clone(message), accounts_data_slice.clone()),
            message.created_at
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

    const fn is_empty(&self) -> bool {
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
            Self::Lt(value) => value > lamports,
            Self::Gt(value) => value < lamports,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct FilterSlotsInner {
    filter_by_commitment: bool,
    interslot_updates: bool,
}

impl FilterSlotsInner {
    fn new(filter: SubscribeRequestFilterSlots) -> Self {
        Self {
            filter_by_commitment: filter.filter_by_commitment.unwrap_or_default(),
            interslot_updates: filter.interslot_updates.unwrap_or_default(),
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
                if (!inner.filter_by_commitment
                    || commitment
                        .map(|commitment| commitment == message.status)
                        .unwrap_or(false))
                    && (inner.interslot_updates
                        || matches!(
                            message.status,
                            SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Finalized
                        ))
                {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<FilteredUpdateFilters>();
        filtered_updates_once_owned!(
            filters,
            FilteredUpdateOneof::slot(message.clone()),
            message.created_at
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterTransactionsType {
    Transaction,
    TransactionStatus,
}

/// ATA / token-account expansion mode for a transaction filter. When set,
/// `account_include` / `account_exclude` / `account_required` also match
/// against owners of pre/post token balances on each transaction. The
/// proto field is `optional`, so absence (`None`) means no expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenAccountsMode {
    /// Match an owner if it owns a pre OR post token balance on the tx.
    All,
    /// Match an owner if any of its token balances changed in amount
    /// (per `account_index`) or the account was closed.
    BalanceChanged,
}

impl TokenAccountsMode {
    fn from_proto(value: i32) -> FilterResult<Self> {
        match TokenAccountExpansionControlFlag::try_from(value) {
            Ok(TokenAccountExpansionControlFlag::All) => Ok(Self::All),
            Ok(TokenAccountExpansionControlFlag::BalanceChanged) => Ok(Self::BalanceChanged),
            Err(_) => Err(FilterError::InvalidTokenAccountsMode(value)),
        }
    }
}

#[derive(Debug, Clone)]
struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    account_include: HybridSet<Pubkey>,
    account_exclude: HybridSet<Pubkey>,
    account_required: Vec<Pubkey>,
    account_cuckoo: Option<Arc<CuckooFilter<[u8; 32]>>>,
    /// `None` means no ATA expansion (the proto field is absent).
    token_accounts: Option<TokenAccountsMode>,
}

#[derive(Debug, Clone)]
struct FilterTransactions {
    filter_type: FilterTransactionsType,
    filters: Vec<(FilterName, FilterTransactionsInner)>,
}

impl FilterTransactions {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterTransactions>,
        limits: &FilterLimitsTransactions,
        filter_type: FilterTransactionsType,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut filters = HashMap::with_capacity(configs.len());
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.vote.is_none()
                    && filter.failed.is_none()
                    && filter.account_include.is_empty()
                    && filter.cuckoo_account_include.is_none()
                    && filter.account_exclude.is_empty()
                    && filter.account_required.is_empty()
                    && filter.token_accounts.is_none(),
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

            let account_cuckoo = if let Some(proto_cuckoo) = &filter.cuckoo_account_include {
                FilterLimits::check_max(proto_cuckoo.data.len(), limits.cuckoo_max_size)?;
                Some(Arc::new(CuckooFilter::from(proto_cuckoo)))
            } else {
                None
            };

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
                    account_include: HybridSet::new_with_set(Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?),
                    account_exclude: HybridSet::new_with_set(Filter::decode_pubkeys_into_set(
                        &filter.account_exclude,
                        &FoldHashSet::new(),
                    )?),
                    account_required: Filter::decode_pubkeys_into_set(
                        &filter.account_required,
                        &FoldHashSet::new(),
                    )?
                    .into_iter()
                    .collect(),
                    account_cuckoo,
                    token_accounts: filter
                        .token_accounts
                        .map(TokenAccountsMode::from_proto)
                        .transpose()?,
                },
            );
        }
        Ok(Self {
            filter_type,
            filters: filters.into_iter().collect(),
        })
    }

    pub fn get_updates(&self, message: &Arc<MessageTransaction>) -> FilteredUpdates {
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

                // Effective account set used by include/exclude/required.
                // When token_accounts mode is set, owners of pre/post token
                // balances are included alongside the real account keys.
                // The owner set is built lazily on the tx via OnceLock so
                // the pre/post scan runs at most once per (tx, mode) across
                // all filters evaluating against this tx. A `None`
                // configured mode skips the scan entirely.
                let token_owners: Option<&PubkeyHashSet> =
                    inner.token_accounts.map(|mode| match mode {
                        TokenAccountsMode::All => message
                            .transaction
                            .token_owners_all
                            .get_or_init(|| owners_in_any_balance(&message.transaction.meta)),
                        TokenAccountsMode::BalanceChanged => message
                            .transaction
                            .token_owners_changed
                            .get_or_init(|| owners_with_changed_balance(&message.transaction.meta)),
                    });
                let in_effective_set = |pubkey: &Pubkey| -> bool {
                    message.transaction.account_keys.contains(pubkey)
                        || token_owners.is_some_and(|set| set.contains(pubkey))
                };

                // Iterate the transaction's keys, not the filter's lists.
                // A tx carries tens of keys; include/exclude lists reach
                // tens of thousands.
                // NOTE: We can have duplicate entries between account_keys and token_owners here, worth revisiting at some point.
                let effective_keys = || {
                    message
                        .transaction
                        .account_keys
                        .iter()
                        .chain(token_owners.into_iter().flatten())
                };

                if !inner.account_required.iter().all(in_effective_set) {
                    return None;
                }

                let effective_len = message.transaction.account_keys.len()
                    + token_owners.map_or(0, |set| set.len());

                if !(inner.account_include.is_empty() && inner.account_cuckoo.is_none()) {
                    let include_hit = !inner.account_include.is_empty()
                        && inner.account_include.overlaps(
                            effective_len,
                            in_effective_set,
                            effective_keys,
                        );

                    let cuckoo_hit = || {
                        inner.account_cuckoo.as_ref().is_some_and(|cuckoo| {
                            effective_keys().any(|pk| cuckoo.contains(&pk.to_bytes()))
                        })
                    };

                    if !include_hit && !cuckoo_hit() {
                        return None;
                    }
                }

                if !inner.account_exclude.is_empty()
                    && inner.account_exclude.overlaps(
                        effective_len,
                        in_effective_set,
                        effective_keys,
                    )
                {
                    return None;
                }

                Some(name.clone())
            })
            .collect::<FilteredUpdateFilters>();

        filtered_updates_once_owned!(
            filters,
            match self.filter_type {
                FilterTransactionsType::Transaction =>
                    FilteredUpdateOneof::transaction(Arc::clone(message)),
                FilterTransactionsType::TransactionStatus => {
                    FilteredUpdateOneof::transaction_status(Arc::clone(message))
                }
            },
            message.created_at
        )
    }
}

/// Owners of every parseable pre OR post token balance on the tx.
fn owners_in_any_balance(meta: &confirmed_block::TransactionStatusMeta) -> PubkeyHashSet {
    let mut owners = PubkeyHashSet::with_capacity_and_hasher(
        meta.pre_token_balances.len() + meta.post_token_balances.len(),
        Default::default(),
    );
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Some(owner) = parse_token_balance_owner(balance) {
            owners.insert(owner);
        }
    }
    owners
}

/// Owners whose token balance changed (compared by `account_index`) between
/// pre and post, OR whose account was closed (present in pre, missing in post).
fn owners_with_changed_balance(meta: &confirmed_block::TransactionStatusMeta) -> PubkeyHashSet {
    let pre_by_index = index_pre_token_balances(&meta.pre_token_balances);

    let mut changed = PubkeyHashSet::default();
    let mut seen_in_post = FoldHashSet::with_capacity(meta.post_token_balances.len());

    for bal in &meta.post_token_balances {
        let Some(pubkey) = parse_token_balance_owner(bal) else {
            continue;
        };
        seen_in_post.insert(bal.account_index);
        let post_amount = token_balance_amount(bal);
        let amount_differs = match pre_by_index.get(&bal.account_index) {
            Some((_, pre_amount)) => *pre_amount != post_amount,
            None => true, // new account
        };
        if amount_differs {
            changed.insert(pubkey);
        }
    }

    // Closed accounts: present in pre, absent in post.
    for (index, (pubkey, _)) in &pre_by_index {
        if !seen_in_post.contains(index) {
            changed.insert(*pubkey);
        }
    }
    changed
}

/// Index parseable pre-balances by `account_index`, carrying the owner
/// pubkey and a borrowed amount string for comparison against post.
fn index_pre_token_balances(
    pre: &[confirmed_block::TokenBalance],
) -> FoldHashMap<u32, (Pubkey, &str)> {
    let mut by_index = FoldHashMap::with_capacity(pre.len());
    for bal in pre {
        if let Some(pubkey) = parse_token_balance_owner(bal) {
            by_index.insert(bal.account_index, (pubkey, token_balance_amount(bal)));
        }
    }
    by_index
}

/// Parse the `owner` string on a token balance into a `Pubkey`, or `None`
/// if it doesn't decode. Suitable for `Iterator::filter_map`.
#[inline]
fn parse_token_balance_owner(balance: &confirmed_block::TokenBalance) -> Option<Pubkey> {
    balance.owner.parse::<Pubkey>().ok()
}

/// Borrowed raw on-chain `amount` string for a token balance, or `""` when
/// missing. Empty is a sentinel that won't equal any real amount, so a
/// missing pre or post amount reliably reads as "changed".
#[inline]
fn token_balance_amount(balance: &confirmed_block::TokenBalance) -> &str {
    balance
        .ui_token_amount
        .as_ref()
        .map(|a| a.amount.as_str())
        .unwrap_or("")
}

#[derive(Debug, Clone)]
struct FilterDeshredTransactionsInner {
    vote: Option<bool>,
    account_include: FoldHashSet<Pubkey>,
    account_exclude: FoldHashSet<Pubkey>,
    account_required: FoldHashSet<Pubkey>,
}

#[derive(Debug, Default, Clone)]
struct FilterDeshredTransactions {
    filters: HashMap<FilterName, FilterDeshredTransactionsInner>,
}

impl FilterDeshredTransactions {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterDeshredTransactions>,
        limits: &FilterLimitsDeshredTransactions,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut filters = HashMap::new();
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.vote.is_none()
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
                FilterDeshredTransactionsInner {
                    vote: filter.vote,
                    account_include: Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?,
                    account_exclude: Filter::decode_pubkeys_into_set(
                        &filter.account_exclude,
                        &FoldHashSet::new(),
                    )?,
                    account_required: Filter::decode_pubkeys_into_set(
                        &filter.account_required,
                        &FoldHashSet::new(),
                    )?,
                },
            );
        }
        Ok(Self { filters })
    }

    pub fn get_updates(&self, message: &Arc<MessageDeshredTransaction>) -> FilteredUpdatesDeshred {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if let Some(is_vote) = inner.vote {
                    if is_vote != message.transaction.is_vote {
                        return None;
                    }
                }

                let tx = &message.transaction;

                if !inner.account_include.is_empty()
                    && !tx
                        .all_account_keys()
                        .any(|key| inner.account_include.contains(key))
                {
                    return None;
                }

                if !inner.account_exclude.is_empty()
                    && tx
                        .all_account_keys()
                        .any(|key| inner.account_exclude.contains(key))
                {
                    return None;
                }

                if !inner.account_required.is_empty() {
                    let all_keys: PubkeyRefHashSet<'_> = tx.all_account_keys().collect();
                    if !inner
                        .account_required
                        .iter()
                        .all(|key| all_keys.contains(key))
                    {
                        return None;
                    }
                }

                Some(name.clone())
            })
            .collect::<FilteredUpdateFilters>();

        let mut messages = FilteredUpdatesDeshred::new();
        if !filters.is_empty() {
            messages.push(FilteredUpdateDeshred::new(
                filters,
                FilteredUpdateDeshredOneof::deshred_transaction(message),
                message.created_at,
            ));
        }
        messages
    }
}

/// Filter for the SubscribeDeshred RPC endpoint.
/// Handles deshred transaction subscriptions separately from the main Subscribe RPC.
#[derive(Debug, Clone, Default)]
pub struct DeshredFilter {
    deshred_transactions: FilterDeshredTransactions,
    slots: FilterSlots,
    ping: Option<i32>,
}

impl DeshredFilter {
    pub fn new(
        config: &SubscribeDeshredRequest,
        limits: &FilterLimits,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        Ok(Self {
            deshred_transactions: FilterDeshredTransactions::new(
                &config.deshred_transactions,
                &limits.deshred_transactions,
                names,
            )?,
            slots: FilterSlots::new(&config.slots, &limits.slots, names)?,
            ping: config.ping.as_ref().map(|msg| msg.id),
        })
    }

    pub fn get_updates(
        &self,
        message: &Message,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredUpdatesDeshred {
        match message {
            Message::DeshredTransaction(message) => self.deshred_transactions.get_updates(message),
            Message::Slot(message) => self.get_slot_updates(message, commitment),
            _ => FilteredUpdatesDeshred::new(),
        }
    }

    fn get_slot_updates(
        &self,
        message: &MessageSlot,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredUpdatesDeshred {
        let filters = self
            .slots
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if (!inner.filter_by_commitment
                    || commitment
                        .map(|commitment| commitment == message.status)
                        .unwrap_or(false))
                    && (inner.interslot_updates
                        || matches!(
                            message.status,
                            SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Finalized
                        ))
                {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<FilteredUpdateFilters>();

        let mut messages = FilteredUpdatesDeshred::new();
        if !filters.is_empty() {
            messages.push(FilteredUpdateDeshred::new(
                filters,
                FilteredUpdateDeshredOneof::slot(message.clone()),
                message.created_at,
            ));
        }
        messages
    }

    pub fn get_pong_msg(&self) -> Option<FilteredUpdateDeshred> {
        self.ping.map(FilteredUpdateDeshred::pong)
    }

    pub fn get_metrics(&self) -> [(&'static str, usize); 3] {
        [
            (
                "deshred_transactions",
                self.deshred_transactions.filters.len(),
            ),
            ("slots", self.slots.filters.len()),
            (
                "all",
                self.deshred_transactions.filters.len() + self.slots.filters.len(),
            ),
        ]
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
                .keys()
                .map(|name| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_updates(&self, message: &Arc<MessageEntry>) -> FilteredUpdates {
        let filters = self.filters.as_slice();
        filtered_updates_once_ref!(
            filters,
            FilteredUpdateOneof::entry(Arc::clone(message)),
            message.created_at
        )
    }
}

#[derive(Debug, Clone)]
struct FilterBlocksInner {
    account_include: FoldHashSet<Pubkey>,
    account_cuckoo: Option<Arc<CuckooFilter<[u8; 32]>>>,
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

            let account_cuckoo = if let Some(proto_cuckoo) = &filter.cuckoo_account_include {
                FilterLimits::check_max(proto_cuckoo.data.len(), limits.cuckoo_max_size)?;
                Some(Arc::new(CuckooFilter::from(proto_cuckoo)))
            } else {
                None
            };

            this.filters.insert(
                names.get(name)?,
                FilterBlocksInner {
                    account_include: Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?,
                    account_cuckoo,
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
                        if inner.matches_any_in_set(&tx.transaction.account_keys) {
                            Some(Arc::clone(tx))
                        } else {
                            None
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
                        if inner.matches_account(&account.account.pubkey) {
                            Some(Arc::clone(account))
                        } else {
                            None
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
                message.created_at,
            ));
        }
        updates
    }
}

impl FilterBlocksInner {
    fn matches_account(&self, pubkey: &Pubkey) -> bool {
        if self.account_include.is_empty() && self.account_cuckoo.is_none() {
            return true;
        }

        if self.account_include.contains(pubkey) {
            return true;
        }

        if let Some(cuckoo) = &self.account_cuckoo {
            if cuckoo.contains(&pubkey.to_bytes()) {
                return true;
            }
        }

        false
    }

    fn matches_any_in_set(&self, account_keys: &PubkeyHashSet) -> bool {
        if self.account_include.is_empty() && self.account_cuckoo.is_none() {
            return true;
        }

        let explicit_match = if self.account_include.len() <= account_keys.len() {
            self.account_include
                .iter()
                .any(|pubkey| account_keys.contains(pubkey))
        } else {
            account_keys
                .iter()
                .any(|pubkey| self.account_include.contains(pubkey))
        };
        if explicit_match {
            return true;
        }

        if let Some(cuckoo) = &self.account_cuckoo {
            if account_keys
                .iter()
                .any(|pk| cuckoo.contains(&pk.to_bytes()))
            {
                return true;
            }
        }

        false
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
                .keys()
                .map(|name| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_updates(&self, message: &Arc<MessageBlockMeta>) -> FilteredUpdates {
        let filters = self.filters.as_slice();
        filtered_updates_once_ref!(
            filters,
            FilteredUpdateOneof::block_meta(Arc::clone(message)),
            message.created_at
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FilterAccountsDataSlice(Arc<[Range<usize>]>);

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

        Ok(Self::new_unchecked(Arc::from(slices.into_boxed_slice())))
    }

    pub const fn new_unchecked(slices: Arc<[Range<usize>]>) -> Self {
        Self(slices)
    }

    pub fn get_slice(&self, source: &[u8]) -> Vec<u8> {
        if self.0.is_empty() {
            source.to_vec()
        } else {
            // Make sure the vec capacity fit exaclty the data we want to copy
            // Why: fitting capacity to length avoid reallocation if we ever need to promote the vector to `Bytes`.
            let mut data = Vec::with_capacity(
                self.0
                    .iter()
                    .filter(|range| source.len() > range.end)
                    .map(|ds| ds.end - ds.start)
                    .sum(),
            );
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
        super::{DeshredFilter, Filter, FilterBitSet, FilterBlocksInner},
        crate::{
            plugin::{
                convert_to,
                filter::{
                    limits::FilterLimits,
                    message::{
                        FilteredUpdateDeshredOneof, FilteredUpdateFilters, FilteredUpdateOneof,
                    },
                    name::{FilterName, FilterNames},
                },
                message::{
                    Message, MessageAccount, MessageAccountInfo, MessageDeshredTransaction,
                    MessageDeshredTransactionInfo, MessageTransaction, MessageTransactionInfo,
                },
            },
            pubkey_collections::PubkeyHashSet,
        },
        foldhash::HashSet as FoldHashSet,
        prost_types::Timestamp,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::{v0::LoadedAddresses, Message as SolMessage, MessageHeader},
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_transaction::{versioned::VersionedTransaction, Transaction},
        solana_transaction_status::TransactionStatusMeta,
        std::{
            collections::HashMap,
            sync::{Arc, OnceLock},
            time::{Duration, SystemTime},
        },
        yellowstone_grpc_proto::geyser::{
            SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeRequestPing,
        },
    };

    pub(super) fn create_filter_names() -> FilterNames {
        FilterNames::new(64, 1024, Duration::from_secs(1))
    }

    #[test]
    fn filter_bitset_word_boundaries() {
        for len in [1, 63, 64, 65, 127, 128, 129, 1023, 1024, 1025] {
            let mut bitset = FilterBitSet::new(len);
            let indexes = [0, len / 2, len - 1];
            for index in indexes {
                bitset.insert(index);
            }

            for index in 0..len {
                assert_eq!(bitset.contains(index), indexes.contains(&index));
            }
        }
    }

    #[test]
    fn block_explicit_account_matching_works_across_hashers() {
        let shared = Pubkey::new_unique();
        let other_filter_keys = [Pubkey::new_unique(), Pubkey::new_unique()];
        let other_message_keys = [Pubkey::new_unique(), Pubkey::new_unique()];

        // Probe the transaction-local set when the client filter is smaller.
        let small_filter = FilterBlocksInner {
            account_include: [shared].into_iter().collect::<FoldHashSet<_>>(),
            account_cuckoo: None,
            include_transactions: None,
            include_accounts: None,
            include_entries: None,
        };
        let message_keys = [shared, other_message_keys[0], other_message_keys[1]]
            .into_iter()
            .collect::<PubkeyHashSet>();
        assert!(small_filter.matches_any_in_set(&message_keys));

        // Probe the collision-resistant client filter when it is larger.
        let large_filter = FilterBlocksInner {
            account_include: [shared, other_filter_keys[0], other_filter_keys[1]]
                .into_iter()
                .collect::<FoldHashSet<_>>(),
            account_cuckoo: None,
            include_transactions: None,
            include_accounts: None,
            include_entries: None,
        };
        let matching_message_keys = [shared].into_iter().collect::<PubkeyHashSet>();
        assert!(large_filter.matches_any_in_set(&matching_message_keys));

        let nonmatching_message_keys = [Pubkey::new_unique()]
            .into_iter()
            .collect::<PubkeyHashSet>();
        assert!(!large_filter.matches_any_in_set(&nonmatching_message_keys));
    }

    fn create_message_transaction(
        keypair: &Keypair,
        account_keys: Vec<Pubkey>,
    ) -> Arc<MessageTransaction> {
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys,
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let versioned_transaction =
            VersionedTransaction::from(Transaction::new(&[keypair], message, recent_blockhash));
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
            cost_units: None,
        });
        let sig = versioned_transaction
            .signatures
            .first()
            .expect("No signature found");
        let account_keys = versioned_transaction
            .message
            .static_account_keys()
            .iter()
            .copied()
            .collect();
        Arc::new(MessageTransaction {
            transaction: MessageTransactionInfo {
                signature: *sig,
                is_vote: true,
                transaction: convert_to::create_transaction(&versioned_transaction),
                meta,
                index: 1,
                account_keys,
                pre_encoded: OnceLock::new(),
                token_owners_all: OnceLock::new(),
                token_owners_changed: OnceLock::new(),
            },
            slot: 100,
            created_at: Timestamp::from(SystemTime::now()),
        })
    }

    pub(super) fn create_message_account(pubkey: Pubkey, owner: Pubkey) -> Arc<MessageAccount> {
        use {
            bytes::Bytes,
            prost_types::Timestamp,
            std::{sync::Arc, time::SystemTime},
        };

        Arc::new(MessageAccount {
            account: MessageAccountInfo {
                pubkey,
                lamports: 1000,
                owner,
                executable: false,
                rent_epoch: 0,
                data: Bytes::new(),
                write_version: 1,
                txn_signature: None,
                pre_encoded: std::sync::OnceLock::new(),
            },
            slot: 100,
            is_startup: false,
            created_at: Timestamp::from(SystemTime::now()),
        })
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
            from_slot: None,
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
                cuckoo_accounts_filter: None,
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
            from_slot: None,
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
                cuckoo_account_include: None,
                token_accounts: None,
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
            from_slot: None,
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
                cuckoo_account_include: None,
                token_accounts: None,
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
            from_slot: None,
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
                cuckoo_account_include: None,
                token_accounts: None,
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
            from_slot: None,
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
                cuckoo_account_include: None,
                token_accounts: None,
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
            from_slot: None,
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
                cuckoo_account_include: None,
                token_accounts: None,
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
            from_slot: None,
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
                cuckoo_account_include: None,
                token_accounts: None,
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
            from_slot: None,
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

    fn create_message_deshred_transaction(
        keypair: &Keypair,
        static_account_keys: Vec<Pubkey>,
        loaded_writable: Vec<Pubkey>,
        loaded_readonly: Vec<Pubkey>,
        is_vote: bool,
    ) -> Arc<MessageDeshredTransaction> {
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: static_account_keys.clone(),
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let versioned_transaction =
            VersionedTransaction::from(Transaction::new(&[keypair], message, recent_blockhash));

        Arc::new(MessageDeshredTransaction {
            transaction: MessageDeshredTransactionInfo {
                signature: *versioned_transaction
                    .signatures
                    .first()
                    .expect("No signature found"),
                is_vote,
                transaction: convert_to::create_transaction(&versioned_transaction),
                static_account_keys: static_account_keys.into_iter().collect(),
                loaded_writable_addresses: loaded_writable,
                loaded_readonly_addresses: loaded_readonly,
                completed_data_set_starting_shred_index: 0,
                completed_data_set_ending_shred_index_exclusive: 0,
            },
            slot: 100,
            created_at: Timestamp::from(SystemTime::now()),
        })
    }

    #[test]
    fn test_deshred_filter_empty_rejects() {
        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
            slots: HashMap::new(),
        };
        let mut limit = FilterLimits::default();
        limit.deshred_transactions.any = false;
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names());
        assert!(filter.is_err());
    }

    #[test]
    fn test_deshred_filter_vote() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();

        // Filter: vote=true
        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "votes".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(true),
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
            slots: HashMap::new(),
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Vote transaction should match
        let msg_vote =
            create_message_deshred_transaction(&keypair, vec![key_a], vec![], vec![], true);
        let message = Message::DeshredTransaction(msg_vote);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
        assert!(matches!(
            updates[0].message,
            FilteredUpdateDeshredOneof::DeshredTransaction(_)
        ));

        // Non-vote transaction should not match
        let msg_nonvote =
            create_message_deshred_transaction(&keypair, vec![key_a], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg_nonvote);
        let updates = filter.get_updates(&message, None);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_deshred_filter_account_include_static() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();
        let key_c = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_b.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
            slots: HashMap::new(),
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Transaction with key_b in static keys should match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_b], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);

        // Transaction without key_b should not match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_c], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message, None);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_deshred_filter_account_include_loaded_addresses() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_alt_w = Pubkey::new_unique();
        let key_alt_r = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_alt_w.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
            slots: HashMap::new(),
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // key_alt_w in loaded writable should match
        let msg = create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![key_alt_w],
            vec![],
            false,
        );
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);

        // key_alt_r in loaded readonly should match when filtering for it
        let mut deshred_transactions2 = HashMap::new();
        deshred_transactions2.insert(
            "f2".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_alt_r.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
        let config2 = SubscribeDeshredRequest {
            deshred_transactions: deshred_transactions2,
            ping: None,
            slots: HashMap::new(),
        };
        let filter2 = DeshredFilter::new(&config2, &limit, &mut create_filter_names()).unwrap();

        let msg = create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![],
            vec![key_alt_r],
            false,
        );
        let message = Message::DeshredTransaction(msg);
        let updates = filter2.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
    }

    #[test]
    fn test_deshred_filter_account_exclude() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![key_b.to_string()],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
            slots: HashMap::new(),
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Transaction with excluded key should not match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_b], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message, None);
        assert!(updates.is_empty());

        // Transaction without excluded key should match
        let msg = create_message_deshred_transaction(&keypair, vec![key_a], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
    }

    #[test]
    fn test_deshred_filter_account_required() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();
        let key_c = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![key_b.to_string(), key_c.to_string()],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
            slots: HashMap::new(),
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Transaction with both required keys should match
        let msg = create_message_deshred_transaction(
            &keypair,
            vec![key_a, key_b],
            vec![key_c],
            vec![],
            false,
        );
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);

        // Transaction missing one required key should not match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_b], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message, None);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_deshred_filter_pong() {
        let config = SubscribeDeshredRequest {
            deshred_transactions: HashMap::new(),
            ping: Some(SubscribeRequestPing { id: 42 }),
            slots: HashMap::new(),
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let pong = filter.get_pong_msg();
        assert!(pong.is_some());
        let pong = pong.unwrap();
        assert!(matches!(
            pong.message,
            FilteredUpdateDeshredOneof::Pong(ref p) if p.id == 42
        ));

        // Without ping, no pong
        let config_no_ping = SubscribeDeshredRequest {
            deshred_transactions: HashMap::new(),
            ping: None,
            slots: HashMap::new(),
        };
        let filter2 =
            DeshredFilter::new(&config_no_ping, &limit, &mut create_filter_names()).unwrap();
        assert!(filter2.get_pong_msg().is_none());
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
                cuckoo_account_include: None,
                token_accounts: None,
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
            from_slot: None,
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

    #[test]
    fn no_account_filters_yields_no_updates() {
        let mut slots = HashMap::new();
        slots.insert("s".to_owned(), SubscribeRequestFilterSlots::default());

        let filter = Filter::new(
            &SubscribeRequest {
                slots,
                ..Default::default()
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();

        let message = create_message_account(Pubkey::new_unique(), Pubkey::new_unique());
        assert!(filter
            .get_updates(&Message::Account(message), None)
            .is_empty());
    }

    #[test]
    fn account_bitset_index_matches_original_semantics() {
        let account = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let other_account = Pubkey::new_unique();
        let other_owner = Pubkey::new_unique();

        let accounts = HashMap::from([
            (
                "account".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: vec![account.to_string()],
                    ..Default::default()
                },
            ),
            (
                "account-and-owner".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: vec![account.to_string()],
                    owner: vec![owner.to_string()],
                    ..Default::default()
                },
            ),
            (
                "owner".to_owned(),
                SubscribeRequestFilterAccounts {
                    owner: vec![owner.to_string()],
                    ..Default::default()
                },
            ),
            (
                "other".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: vec![other_account.to_string()],
                    owner: vec![other_owner.to_string()],
                    ..Default::default()
                },
            ),
        ]);
        let request = SubscribeRequest {
            accounts: accounts.clone(),
            ..Default::default()
        };
        let filter = Filter::new(
            &request,
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();

        let index = filter.accounts.index.as_ref().unwrap();
        let account_matches = index.accounts.get(&account).unwrap();
        let owner_matches = index.owners.get(&owner).unwrap();
        assert_eq!(
            account_matches
                .0
                .iter()
                .map(|word| word.count_ones())
                .sum::<u32>(),
            2
        );
        assert_eq!(
            owner_matches
                .0
                .iter()
                .map(|word| word.count_ones())
                .sum::<u32>(),
            2
        );

        for (aggregate_index, aggregate) in filter.accounts.aggregates.iter().enumerate() {
            assert_eq!(
                index.account_fallback.contains(aggregate_index),
                aggregate.accounts.is_none() || aggregate.accounts_cuckoo.is_some()
            );
            assert_eq!(
                index.owner_fallback.contains(aggregate_index),
                aggregate.owners.is_none()
            );
        }

        let matched_names = |account, owner| {
            let message = create_message_account(account, owner);
            let updates = filter.get_updates(&Message::Account(message), None);
            let mut names = updates
                .first()
                .map(|update| {
                    update
                        .filters
                        .iter()
                        .map(|name| name.as_ref().to_owned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            names.sort_unstable();
            names
        };

        assert_eq!(
            matched_names(account, owner),
            [
                "account".to_owned(),
                "account-and-owner".to_owned(),
                "owner".to_owned(),
            ]
        );
        assert_eq!(
            matched_names(account, Pubkey::new_unique()),
            ["account".to_owned()]
        );
        assert_eq!(
            matched_names(other_account, other_owner),
            ["other".to_owned()]
        );
        assert!(matched_names(Pubkey::new_unique(), Pubkey::new_unique()).is_empty());

        let single_filter = Filter::new(
            &SubscribeRequest {
                accounts: HashMap::from([(
                    "single".to_owned(),
                    SubscribeRequestFilterAccounts {
                        account: vec![account.to_string()],
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();
        assert!(single_filter.accounts.index.is_none());
    }
}

#[cfg(test)]
mod cuckoo_tests {
    use {
        super::{
            tests::{create_filter_names, create_message_account},
            *,
        },
        yellowstone_grpc_proto::{cuckoo::CuckooFilter, geyser::CuckooFilter as ProtoCuckooFilter},
    };

    fn create_cuckoo_with_pubkeys(pubkeys: &[Pubkey]) -> ProtoCuckooFilter {
        let mut filter = CuckooFilter::<[u8; 32]>::with_capacity(pubkeys.len().max(1)).unwrap();
        for pk in pubkeys {
            filter.insert(&pk.to_bytes()).unwrap();
        }
        ProtoCuckooFilter::from(&filter)
    }

    #[test]
    fn test_cuckoo_filter_matches_pubkey() {
        let pubkey = Pubkey::new_unique();
        let cuckoo = create_cuckoo_with_pubkeys(&[pubkey]);

        let mut accounts = HashMap::new();
        accounts.insert(
            "test".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: Some(cuckoo),
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
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        let filter = Filter::new(
            &config,
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();
        let message = create_message_account(pubkey, Pubkey::new_unique());
        let updates = filter.get_updates(&Message::Account(message), None);

        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("test")])
        );
    }

    #[test]
    fn test_cuckoo_filter_no_match() {
        let in_filter = Pubkey::new_unique();
        let not_in_filter = Pubkey::new_unique();
        let cuckoo = create_cuckoo_with_pubkeys(&[in_filter]);

        let mut accounts = HashMap::new();
        accounts.insert(
            "test".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: Some(cuckoo),
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
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        let filter = Filter::new(
            &config,
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();
        let message = create_message_account(not_in_filter, Pubkey::new_unique());
        let updates = filter.get_updates(&Message::Account(message), None);

        assert!(updates.is_empty());
    }

    #[test]
    fn test_cuckoo_or_explicit_account() {
        let pk_in_cuckoo = Pubkey::new_unique();
        let pk_in_list = Pubkey::new_unique();
        let pk_in_other_filter = Pubkey::new_unique();
        let cuckoo = create_cuckoo_with_pubkeys(&[pk_in_cuckoo]);

        let mut accounts = HashMap::new();
        accounts.insert(
            "test".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![pk_in_list.to_string()],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: Some(cuckoo),
            },
        );
        accounts.insert(
            "other".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![pk_in_other_filter.to_string()],
                ..Default::default()
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
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        let filter = Filter::new(
            &config,
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();

        // Match via cuckoo
        let message1 = create_message_account(pk_in_cuckoo, Pubkey::new_unique());
        let updates1 = filter.get_updates(&Message::Account(message1), None);
        assert_eq!(updates1.len(), 1);

        // Match via explicit list
        let message2 = create_message_account(pk_in_list, Pubkey::new_unique());
        let updates2 = filter.get_updates(&Message::Account(message2), None);
        assert_eq!(updates2.len(), 1);

        // Match neither
        let message3 = create_message_account(Pubkey::new_unique(), Pubkey::new_unique());
        let updates3 = filter.get_updates(&Message::Account(message3), None);
        assert!(updates3.is_empty());
    }

    #[test]
    fn test_cuckoo_with_owner_filter() {
        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let wrong_owner = Pubkey::new_unique();
        let cuckoo = create_cuckoo_with_pubkeys(&[pubkey]);

        let mut accounts = HashMap::new();
        accounts.insert(
            "test".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![owner.to_string()],
                filters: vec![],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: Some(cuckoo),
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
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        let filter = Filter::new(
            &config,
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();

        // pubkey in cuckoo AND owner matches
        let message1 = create_message_account(pubkey, owner);
        let updates1 = filter.get_updates(&Message::Account(message1), None);
        assert_eq!(updates1.len(), 1);

        // pubkey in cuckoo BUT owner doesn't match
        let message2 = create_message_account(pubkey, wrong_owner);
        let updates2 = filter.get_updates(&Message::Account(message2), None);
        assert!(updates2.is_empty());
    }

    mod token_accounts {
        //! Tests for the optional `token_accounts` field on
        //! `SubscribeRequestFilterTransactions` that expands the effective
        //! account set of a tx with owners of its pre/post token balances.

        use {
            super::{create_filter_names, Filter, FilterError, TokenAccountsMode},
            crate::plugin::{
                convert_to,
                filter::message::{FilteredUpdateFilters, FilteredUpdateOneof},
                message::{Message, MessageTransaction, MessageTransactionInfo},
            },
            prost_types::Timestamp,
            solana_hash::Hash,
            solana_keypair::Keypair,
            solana_message::{v0::LoadedAddresses, Message as SolMessage, MessageHeader},
            solana_pubkey::Pubkey,
            solana_signer::Signer,
            solana_transaction::{versioned::VersionedTransaction, Transaction},
            solana_transaction_status::TransactionStatusMeta,
            std::{
                collections::HashMap,
                sync::{Arc, OnceLock},
                time::SystemTime,
            },
            yellowstone_grpc_proto::{
                geyser::{SubscribeRequest, SubscribeRequestFilterTransactions},
                solana::storage::confirmed_block,
            },
        };

        // --- mode parsing -----------------------------------------------

        #[test]
        fn mode_from_proto_accepts_known_values() {
            assert_eq!(
                TokenAccountsMode::from_proto(super::TokenAccountExpansionControlFlag::All as i32)
                    .unwrap(),
                TokenAccountsMode::All
            );
            assert_eq!(
                TokenAccountsMode::from_proto(
                    super::TokenAccountExpansionControlFlag::BalanceChanged as i32
                )
                .unwrap(),
                TokenAccountsMode::BalanceChanged
            );
        }

        #[test]
        fn mode_from_proto_rejects_unknown_value() {
            let err = TokenAccountsMode::from_proto(42).unwrap_err();
            assert!(matches!(err, FilterError::InvalidTokenAccountsMode(_)));

            let err = TokenAccountsMode::from_proto(-1).unwrap_err();
            assert!(matches!(err, FilterError::InvalidTokenAccountsMode(_)));
        }

        // --- helpers ----------------------------------------------------

        fn token_balance(
            account_index: u32,
            owner: Pubkey,
            amount: &str,
        ) -> confirmed_block::TokenBalance {
            confirmed_block::TokenBalance {
                account_index,
                mint: Pubkey::new_unique().to_string(),
                ui_token_amount: Some(confirmed_block::UiTokenAmount {
                    ui_amount: 0.0,
                    decimals: 0,
                    amount: amount.to_owned(),
                    ui_amount_string: amount.to_owned(),
                }),
                owner: owner.to_string(),
                program_id: spl_token_2022_interface::id().to_string(),
            }
        }

        /// Build a `MessageTransaction` carrying explicit pre/post token
        /// balances. `account_keys` is the static account-key set used by
        /// the include/exclude/required match (unchanged from upstream).
        ///
        /// A fresh signer is generated and prepended to `account_keys` so
        /// `Transaction::sign` does not reject the build with
        /// `KeypairPubkeyMismatch`. Tests should treat the signer's
        /// pubkey as opaque and only rely on the keys they pass in.
        fn make_tx_with_balances(
            account_keys: Vec<Pubkey>,
            pre: Vec<confirmed_block::TokenBalance>,
            post: Vec<confirmed_block::TokenBalance>,
        ) -> Arc<MessageTransaction> {
            let signer = Keypair::new();
            let signer_pk = signer.pubkey();
            let mut full_keys = Vec::with_capacity(1 + account_keys.len());
            full_keys.push(signer_pk);
            full_keys.extend(account_keys);
            let sol_message = SolMessage {
                header: MessageHeader {
                    num_required_signatures: 1,
                    ..MessageHeader::default()
                },
                account_keys: full_keys,
                ..SolMessage::default()
            };
            let versioned = VersionedTransaction::from(Transaction::new(
                &[&signer],
                sol_message,
                Hash::default(),
            ));
            // Build the proto meta via convert_to so we get sensible defaults,
            // then override the pre/post token balance vectors directly.
            let mut meta = convert_to::create_transaction_meta(&TransactionStatusMeta {
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
                cost_units: None,
            });
            meta.pre_token_balances = pre;
            meta.post_token_balances = post;
            let sig = *versioned
                .signatures
                .first()
                .expect("transaction should be signed");
            let key_set = versioned
                .message
                .static_account_keys()
                .iter()
                .copied()
                .collect();
            Arc::new(MessageTransaction {
                transaction: MessageTransactionInfo {
                    signature: sig,
                    is_vote: false,
                    transaction: convert_to::create_transaction(&versioned),
                    meta,
                    index: 0,
                    account_keys: key_set,
                    pre_encoded: OnceLock::new(),
                    token_owners_all: OnceLock::new(),
                    token_owners_changed: OnceLock::new(),
                },
                slot: 100,
                created_at: Timestamp::from(SystemTime::now()),
            })
        }

        fn filter_with_transactions(
            tx_filters: HashMap<String, SubscribeRequestFilterTransactions>,
        ) -> Filter {
            let config = SubscribeRequest {
                accounts: HashMap::new(),
                slots: HashMap::new(),
                transactions: tx_filters,
                transactions_status: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                entry: HashMap::new(),
                commitment: None,
                accounts_data_slice: Vec::new(),
                ping: None,
                from_slot: None,
            };
            Filter::new(
                &config,
                &super::FilterLimits::default(),
                &mut create_filter_names(),
            )
            .expect("filter should build")
        }

        fn make_filter_def(
            account_include: Vec<Pubkey>,
            account_exclude: Vec<Pubkey>,
            account_required: Vec<Pubkey>,
            mode: Option<super::TokenAccountExpansionControlFlag>,
        ) -> SubscribeRequestFilterTransactions {
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include: account_include.into_iter().map(|k| k.to_string()).collect(),
                account_exclude: account_exclude.into_iter().map(|k| k.to_string()).collect(),
                account_required: account_required
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect(),
                cuckoo_account_include: None,
                token_accounts: mode.map(|m| m as i32),
            }
        }

        fn matches(filter: &Filter, message: &Message) -> bool {
            !filter.get_updates(message, None).is_empty()
        }

        // --- mode == None preserves legacy behavior ---------------------

        #[test]
        fn mode_none_ignores_token_balance_owner() {
            let owner = Pubkey::new_unique();
            let other_key = Pubkey::new_unique();
            // owner is NOT in account_keys; it only shows up in token balances.
            let tx = make_tx_with_balances(
                vec![other_key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "200")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(vec![owner], vec![], vec![], None),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(
                !matches(&filter, &message),
                "mode=None must not promote token-balance owners into the include set"
            );
        }

        #[test]
        fn mode_none_account_keys_match_unchanged() {
            let pubkey = Pubkey::new_unique();
            let tx = make_tx_with_balances(vec![pubkey], vec![], vec![]);
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(vec![pubkey], vec![], vec![], None),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        // --- mode == "all" ----------------------------------------------

        #[test]
        fn mode_all_matches_pre_only_owner() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![], // account closed
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn mode_all_matches_post_only_owner() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![],
                vec![token_balance(0, owner, "100")], // freshly created
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn mode_all_matches_owner_in_pre_and_post() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "100")], // unchanged amount
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn mode_all_rejects_owner_not_in_any_balance() {
            let owner = Pubkey::new_unique();
            let unrelated_owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, unrelated_owner, "100")],
                vec![token_balance(0, unrelated_owner, "100")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(!matches(&filter, &message));
        }

        // --- mode == "balanceChanged" -----------------------------------

        #[test]
        fn mode_balance_changed_matches_amount_change() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "150")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn mode_balance_changed_skips_unchanged_amount() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "100")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(!matches(&filter, &message));
        }

        #[test]
        fn mode_balance_changed_matches_account_close() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![], // closed
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn mode_balance_changed_matches_new_account() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![],
                vec![token_balance(0, owner, "100")], // brand new
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn mode_balance_changed_multi_ata_same_owner_keyed_by_index() {
            // Same owner holds two ATAs; one changes, one does not.
            // Owner must be in `changed` because at least one balance moved.
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![
                    token_balance(0, owner, "100"),
                    token_balance(1, owner, "500"),
                ],
                vec![
                    token_balance(0, owner, "100"), // unchanged
                    token_balance(1, owner, "600"), // changed
                ],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        // --- exclude / required uniformity ------------------------------

        #[test]
        fn mode_all_exclude_drops_owner_only_tx() {
            // Filter requires include=key, excludes owner via token-balance.
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "200")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![key],
                    vec![owner],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(
                !matches(&filter, &message),
                "exclude with mode=All must drop a tx whose token-balance owner matches"
            );
        }

        #[test]
        fn mode_all_required_satisfied_by_owner_set() {
            // `account_required` lists `owner` which is NOT an account key,
            // but is a token-balance owner. With mode=All this must match.
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(vec![key], vec![], vec![token_balance(0, owner, "100")]);
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![],
                    vec![],
                    vec![owner],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn mode_balance_changed_required_unsatisfied_when_balance_static() {
            // Owner present in pre+post but amount unchanged — under
            // BalanceChanged the owner is NOT in the effective set, so a
            // required-on-owner filter must reject the tx.
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "100")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![],
                    vec![],
                    vec![owner],
                    Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(!matches(&filter, &message));
        }

        // --- robustness -------------------------------------------------

        #[test]
        fn mode_all_skips_unparseable_owner_strings() {
            let valid_owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            // Insert a balance whose owner string is garbage. It must be
            // silently skipped; valid balances still match.
            let mut bad_pre = token_balance(0, valid_owner, "100");
            bad_pre.owner = "not-a-real-pubkey".to_owned();
            let tx = make_tx_with_balances(
                vec![key],
                vec![bad_pre],
                vec![token_balance(1, valid_owner, "200")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![valid_owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            assert!(matches(&filter, &message));
        }

        #[test]
        fn empty_balances_under_mode_does_not_panic() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(vec![key], vec![], vec![]);
            let message = Message::Transaction(tx);

            for mode in [
                Some(super::TokenAccountExpansionControlFlag::All),
                Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                None,
            ] {
                let mut tx_filters = HashMap::new();
                tx_filters.insert(
                    "f".to_owned(),
                    make_filter_def(vec![owner], vec![], vec![], mode),
                );
                let filter = filter_with_transactions(tx_filters);
                // owner never appears anywhere — must not match.
                assert!(!matches(&filter, &message));
            }
        }

        // --- multi-filter / sister path ---------------------------------

        #[test]
        fn multiple_filters_independent_modes() {
            // Same tx, two filters: one with mode=All matches via owner,
            // the other with mode=None does not.
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "200")],
            );
            let message = Message::Transaction(tx);

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "with_ata".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            tx_filters.insert(
                "without_ata".to_owned(),
                make_filter_def(vec![owner], vec![], vec![], None),
            );
            let filter = filter_with_transactions(tx_filters);

            let updates = filter.get_updates(&message, None);
            assert_eq!(updates.len(), 1, "expect exactly one matching filter");
            assert_eq!(
                updates[0].filters,
                FilteredUpdateFilters::from_vec(vec![super::FilterName::new("with_ata")])
            );
            assert!(matches!(
                updates[0].message,
                FilteredUpdateOneof::Transaction(_)
            ));
        }

        #[test]
        fn transactions_status_path_honors_token_accounts_mode() {
            // The sister filter map (transactions_status) shares the same
            // FilterTransactions impl, so mode must work there too.
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(vec![key], vec![], vec![token_balance(0, owner, "100")]);
            let message = Message::Transaction(tx);

            let mut tx_status_filters = HashMap::new();
            tx_status_filters.insert(
                "status".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );

            let config = SubscribeRequest {
                accounts: HashMap::new(),
                slots: HashMap::new(),
                transactions: HashMap::new(),
                transactions_status: tx_status_filters,
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                entry: HashMap::new(),
                commitment: None,
                accounts_data_slice: Vec::new(),
                ping: None,
                from_slot: None,
            };
            let filter = Filter::new(
                &config,
                &super::FilterLimits::default(),
                &mut create_filter_names(),
            )
            .unwrap();

            let updates = filter.get_updates(&message, None);
            assert_eq!(updates.len(), 1);
            assert!(matches!(
                updates[0].message,
                FilteredUpdateOneof::TransactionStatus(_)
            ));
        }

        // --- limits.any interplay --------------------------------------

        #[test]
        fn token_accounts_mode_counts_as_non_any_filter() {
            // limits.any = false rejects empty filters. A filter with ONLY
            // token_accounts set (no include/exclude/required, no vote/sig)
            // must still build — it's a real constraint, not "match all".
            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );

            let config = SubscribeRequest {
                accounts: HashMap::new(),
                slots: HashMap::new(),
                transactions: tx_filters,
                transactions_status: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                entry: HashMap::new(),
                commitment: None,
                accounts_data_slice: Vec::new(),
                ping: None,
                from_slot: None,
            };
            let mut limits = super::FilterLimits::default();
            limits.transactions.any = false;

            let result = Filter::new(&config, &limits, &mut create_filter_names());
            assert!(
                result.is_ok(),
                "filter with token_accounts set should not be considered \"any\""
            );
        }

        #[test]
        fn invalid_mode_value_rejected_at_filter_build() {
            // 99 is not a valid TokenAccountExpansionControlFlag variant.
            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                SubscribeRequestFilterTransactions {
                    vote: None,
                    failed: None,
                    signature: None,
                    account_include: vec![Pubkey::new_unique().to_string()],
                    account_exclude: vec![],
                    account_required: vec![],
                    cuckoo_account_include: None,
                    token_accounts: Some(99),
                },
            );

            let config = SubscribeRequest {
                accounts: HashMap::new(),
                slots: HashMap::new(),
                transactions: tx_filters,
                transactions_status: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                entry: HashMap::new(),
                commitment: None,
                accounts_data_slice: Vec::new(),
                ping: None,
                from_slot: None,
            };
            let err = Filter::new(
                &config,
                &super::FilterLimits::default(),
                &mut create_filter_names(),
            )
            .expect_err("invalid mode must fail filter construction");
            assert!(matches!(err, FilterError::InvalidTokenAccountsMode(_)));
        }

        // --- OnceLock cache semantics -----------------------------------

        #[test]
        fn cache_is_lazy_and_per_mode() {
            // Build a tx, run a single mode=All filter against it, and
            // assert: (1) the All cache is populated, (2) the
            // BalanceChanged cache stays empty (we never asked for it).
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "200")],
            );

            assert!(tx.transaction.token_owners_all.get().is_none());
            assert!(tx.transaction.token_owners_changed.get().is_none());

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);
            assert!(matches(&filter, &Message::Transaction(Arc::clone(&tx))));

            assert!(
                tx.transaction.token_owners_all.get().is_some(),
                "mode=All must populate token_owners_all"
            );
            assert!(
                tx.transaction.token_owners_changed.get().is_none(),
                "mode=All must NOT touch token_owners_changed"
            );
        }

        #[test]
        fn cache_is_reused_across_filter_evaluations() {
            // Two filters, both mode=All, against the same tx. The owner
            // set must be the SAME backing allocation (pointer-equal) on
            // both filter checks — proves we read from the OnceLock cache,
            // not rebuild per filter.
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "200")],
            );
            let message = Message::Transaction(Arc::clone(&tx));

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f1".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            tx_filters.insert(
                "f2".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            let filter = filter_with_transactions(tx_filters);

            let updates = filter.get_updates(&message, None);
            assert_eq!(
                updates.len(),
                1,
                "both filters match, but they share one FilteredUpdate"
            );

            // First run populated the cache. Capture the pointer.
            let first_addr = tx.transaction.token_owners_all.get().expect("populated") as *const _;

            // Run the filter again on the same tx (same Arc). The cache
            // must still be there and point at the SAME allocation.
            let _ = filter.get_updates(&message, None);
            let second_addr = tx.transaction.token_owners_all.get().expect("populated") as *const _;
            assert_eq!(
                first_addr, second_addr,
                "cache backing storage must be reused, not reallocated"
            );
        }

        #[test]
        fn cache_separates_all_and_balance_changed_modes() {
            // A filter set with both modes against the same tx populates
            // both OnceLocks. The two cached sets are independent.
            let owner = Pubkey::new_unique();
            let unrelated = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            // pre has both owners; post drops `unrelated`, so `unrelated`
            // is in `all` (it was present in pre) AND in `changed` (its
            // account closed). `owner` is in `all` and in `changed`
            // (amount moved). Different from a strict same-set guarantee,
            // but the assertion below only relies on both caches having
            // populated.
            let tx = make_tx_with_balances(
                vec![key],
                vec![
                    token_balance(0, owner, "100"),
                    token_balance(1, unrelated, "50"),
                ],
                vec![token_balance(0, owner, "200")],
            );
            let message = Message::Transaction(Arc::clone(&tx));

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "all_f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::All),
                ),
            );
            tx_filters.insert(
                "changed_f".to_owned(),
                make_filter_def(
                    vec![owner],
                    vec![],
                    vec![],
                    Some(super::TokenAccountExpansionControlFlag::BalanceChanged),
                ),
            );
            let filter = filter_with_transactions(tx_filters);
            let _ = filter.get_updates(&message, None);

            assert!(
                tx.transaction.token_owners_all.get().is_some(),
                "mode=All filter must populate token_owners_all"
            );
            assert!(
                tx.transaction.token_owners_changed.get().is_some(),
                "mode=BalanceChanged filter must populate token_owners_changed"
            );
            // Sanity: they're at different addresses (different OnceLocks).
            let all_addr = tx.transaction.token_owners_all.get().unwrap() as *const _;
            let changed_addr = tx.transaction.token_owners_changed.get().unwrap() as *const _;
            assert_ne!(all_addr as usize, changed_addr as usize);
        }

        #[test]
        fn cache_untouched_when_mode_is_none() {
            let owner = Pubkey::new_unique();
            let key = Pubkey::new_unique();
            let tx = make_tx_with_balances(
                vec![key],
                vec![token_balance(0, owner, "100")],
                vec![token_balance(0, owner, "200")],
            );
            let message = Message::Transaction(Arc::clone(&tx));

            let mut tx_filters = HashMap::new();
            tx_filters.insert(
                "f".to_owned(),
                make_filter_def(vec![key], vec![], vec![], None),
            );
            let filter = filter_with_transactions(tx_filters);
            let _ = filter.get_updates(&message, None);

            assert!(
                tx.transaction.token_owners_all.get().is_none(),
                "mode=None must NOT populate any owner-set cache"
            );
            assert!(tx.transaction.token_owners_changed.get().is_none());
        }
    }
}
