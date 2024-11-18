use {
    serde::{de, Deserialize, Deserializer},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashSet,
};

#[derive(Debug, thiserror::Error)]
pub enum FilterLimitsCheckError {
    #[error("Max amount of filters/data_slices reached, only {max} allowed")]
    Max { max: usize },
    #[error("Subscribe on full stream with `any` is not allowed, at least one filter required")]
    Any,
    #[error("Max amount of Pubkeys reached, only {max} allowed")]
    MaxPubkey { max: usize },
    #[error("Pubkey {pubkey} in filters is not allowed")]
    PubkeyReject { pubkey: Pubkey },
}

pub type FilterLimitsCheckResult = Result<(), FilterLimitsCheckError>;

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilterLimits {
    pub accounts: FilterLimitsAccounts,
    pub slots: FilterLimitsSlots,
    pub transactions: FilterLimitsTransactions,
    pub transactions_status: FilterLimitsTransactions,
    pub blocks: FilterLimitsBlocks,
    pub blocks_meta: FilterLimitsBlocksMeta,
    pub entries: FilterLimitsEntries,
}

impl FilterLimits {
    pub const fn check_max(len: usize, max: usize) -> FilterLimitsCheckResult {
        if len <= max {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::Max { max })
        }
    }

    pub const fn check_any(is_empty: bool, any: bool) -> FilterLimitsCheckResult {
        if !is_empty || any {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::Any)
        }
    }

    pub const fn check_pubkey_max(len: usize, max: usize) -> FilterLimitsCheckResult {
        if len <= max {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::MaxPubkey { max })
        }
    }

    pub fn check_pubkey_reject(pubkey: &Pubkey, set: &HashSet<Pubkey>) -> FilterLimitsCheckResult {
        if !set.contains(pubkey) {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::PubkeyReject { pubkey: *pubkey })
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilterLimitsAccounts {
    pub max: usize,
    pub any: bool,
    pub account_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_reject: HashSet<Pubkey>,
    pub owner_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub owner_reject: HashSet<Pubkey>,
    pub data_slice_max: usize,
}

impl Default for FilterLimitsAccounts {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            any: true,
            account_max: usize::MAX,
            account_reject: HashSet::new(),
            owner_max: usize::MAX,
            owner_reject: HashSet::new(),
            data_slice_max: usize::MAX,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilterLimitsSlots {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
}

impl Default for FilterLimitsSlots {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilterLimitsTransactions {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
    pub any: bool,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_include_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_include_reject: HashSet<Pubkey>,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_exclude_max: usize,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_required_max: usize,
}

impl Default for FilterLimitsTransactions {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            any: true,
            account_include_max: usize::MAX,
            account_include_reject: HashSet::new(),
            account_exclude_max: usize::MAX,
            account_required_max: usize::MAX,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilterLimitsBlocks {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub account_include_max: usize,
    pub account_include_any: bool,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_include_reject: HashSet<Pubkey>,
    pub include_transactions: bool,
    pub include_accounts: bool,
    pub include_entries: bool,
}

impl Default for FilterLimitsBlocks {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            account_include_max: usize::MAX,
            account_include_any: true,
            account_include_reject: HashSet::new(),
            include_transactions: true,
            include_accounts: true,
            include_entries: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilterLimitsBlocksMeta {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
}

impl Default for FilterLimitsBlocksMeta {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilterLimitsEntries {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max: usize,
}

impl Default for FilterLimitsEntries {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

fn deserialize_usize_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value<'a> {
        Int(usize),
        Str(&'a str),
    }

    match Value::deserialize(deserializer)? {
        Value::Int(value) => Ok(value),
        Value::Str(value) => value
            .replace('_', "")
            .parse::<usize>()
            .map_err(de::Error::custom),
    }
}

fn deserialize_pubkey_set<'de, D>(deserializer: D) -> Result<HashSet<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<&str>::deserialize(deserializer)?
        .into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|error| de::Error::custom(format!("Invalid pubkey: {value} ({error:?})")))
        })
        .collect::<Result<_, _>>()
}
