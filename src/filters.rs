use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountsFilter {
    any: bool,
    accounts: HashSet<Vec<u8>>,
    owners: HashSet<Vec<u8>>,
}

impl Default for AccountsFilter {
    fn default() -> Self {
        Self {
            any: true,
            accounts: HashSet::default(),
            owners: HashSet::default(),
        }
    }
}

impl AccountsFilter {
    pub fn new<T1, T2>(any: bool, accounts: &[T1], owners: &[T2]) -> anyhow::Result<Self>
    where
        for<'a> T1: AsRef<[u8]> + std::cmp::PartialEq<&'a str> + std::fmt::Debug,
        T2: AsRef<[u8]> + std::fmt::Debug,
    {
        anyhow::ensure!(
            !any || accounts.is_empty() && owners.is_empty(),
            "`any` is not allow non-empty `accouts` and `owners`"
        );
        anyhow::ensure!(accounts.len() < 10_000, "Maximum 10k accounts are allowed");
        anyhow::ensure!(owners.len() < 10_000, "Maximum 10k owners are allowed");

        Ok(AccountsFilter {
            any,
            accounts: accounts
                .iter()
                .map(|key| bs58::decode(key).into_vec())
                .collect::<Result<_, _>>()?,
            owners: owners
                .iter()
                .map(|key| bs58::decode(key).into_vec())
                .collect::<Result<_, _>>()?,
        })
    }

    pub fn is_account_selected(&self, account: &[u8], owner: &[u8]) -> bool {
        self.any || self.accounts.contains(account) || self.owners.contains(owner)
    }
}
