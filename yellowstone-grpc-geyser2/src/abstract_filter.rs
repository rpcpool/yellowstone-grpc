///
/// Abstract filters for account and transaction.
///
/// This module provides a set of traits and structs that can be used to filter accounts and transactions in various "shapes", thus the word Abstract.
///
/// The main trait is `AccountFilter` and `TxFilter` which can be used to filter accounts and transactions respectively.
///
/// The module also provides a set of concrete filters that can be used to filter accounts and transactions based on various criteria.
///
/// The filters can be combined using `AndAccountFilter` and `TxAndFilter` to create complex filters.
///
/// In order to use these filters, the account and transaction structs (shapes) you are using must impleemnts the `AbstractAccount` and `AbstractTx` traits.
///
/// Common "shapes" for account/tx representations:
///
/// 1. Solana-sdk : most common structs used
/// 2. gRPC : used in the gRPC service
/// 3. Mock : used in tests
/// 4. Databases : Most database does not support `u64`, the closest type is `i64`.
///    This forces to recreate Account and Transaction structs with `i64` lamports.
///    This is the case for Postgres.
/// 5. AppendVec : used in Solana snapshot, AppendVec have a different representation of account data.
///
use {
    base64::{prelude::BASE64_STANDARD, Engine},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    spl_token_2022::generic_token_account::GenericTokenAccount,
    std::{collections::HashSet, sync::Arc},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReducerDecision {
    Stop,
    Continue,
}

impl ReducerDecision {
    pub fn to_result(self) -> Result<(), ReducerDecision> {
        match self {
            ReducerDecision::Stop => Err(ReducerDecision::Stop),
            ReducerDecision::Continue => Ok(()),
        }
    }
}

pub trait PubkeyReducer {
    fn accept(&mut self, pubkey: Pubkey) -> ReducerDecision;
}

pub trait AbstractBlockMeta {}

pub trait BlockMetaFilter {
    fn filter(&self, block_meta: &dyn AbstractBlockMeta) -> bool;

    #[allow(dead_code)]
    fn boxed(self) -> Box<dyn BlockMetaFilter + Send + Sync + 'static>
    where
        Self: Sized + Send + Sync + 'static,
    {
        Box::new(self)
    }

    #[allow(dead_code)]
    fn complexity_score(&self) -> usize {
        0
    }
}

#[derive(Clone)]
pub struct BlockMetaFilterRef(pub(crate) Arc<dyn BlockMetaFilter + Send + Sync + 'static>);

impl BlockMetaFilter for BlockMetaFilterRef {
    fn filter(&self, block_meta: &dyn AbstractBlockMeta) -> bool {
        self.0.filter(block_meta)
    }
}

pub trait AbstractEntry {}

#[derive(Clone)]
pub struct EntryFilterRef(pub(crate) Arc<dyn EntryFilter + Send + Sync + 'static>);

pub trait EntryFilter {
    fn filter(&self, entry: &dyn AbstractEntry) -> bool;

    #[allow(dead_code)]
    fn boxed(self) -> Box<dyn EntryFilter + Send + Sync + 'static>
    where
        Self: Sized + Send + Sync + 'static,
    {
        Box::new(self)
    }

    #[allow(dead_code)]
    fn complexity_score(&self) -> usize {
        0
    }
}

impl EntryFilter for EntryFilterRef {
    fn filter(&self, entry: &dyn AbstractEntry) -> bool {
        self.0.filter(entry)
    }
}

pub trait AbstractSlotStatus {}

#[derive(Clone)]
pub struct SlotStatusFilterRef(pub(crate) Arc<dyn SlotStatusFilter + Send + Sync + 'static>);

pub trait SlotStatusFilter {
    fn filter(&self, slot: &dyn AbstractSlotStatus) -> bool;
    fn boxed(self) -> Box<dyn SlotStatusFilter + Send + Sync + 'static>
    where
        Self: Sized + Send + Sync + 'static,
    {
        Box::new(self)
    }

    #[allow(dead_code)]
    fn complexity_score(&self) -> usize {
        0
    }
}

impl SlotStatusFilter for SlotStatusFilterRef {
    fn filter(&self, slot: &dyn AbstractSlotStatus) -> bool {
        self.0.filter(slot)
    }
}

impl SlotStatusFilter for TrueFilter {
    fn filter(&self, _slot: &dyn AbstractSlotStatus) -> bool {
        true
    }
}

impl EntryFilter for TrueFilter {
    fn filter(&self, _entry: &dyn AbstractEntry) -> bool {
        true
    }
}

impl BlockMetaFilter for TrueFilter {
    fn filter(&self, _block_meta: &dyn AbstractBlockMeta) -> bool {
        true
    }
}

///
/// Abstract "account"
///
pub trait AbstractAccount {
    fn pubkey(&self) -> Pubkey;
    fn owner(&self) -> Pubkey;
    fn lamports(&self) -> u64;
    fn data(&self) -> &[u8];
    fn tx_signature(&self) -> Option<&[u8]> {
        None
    }
}

pub trait AccountFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool;
    fn boxed(self) -> Box<dyn AccountFilter + Send + Sync + 'static>
    where
        Self: Sized + Send + Sync + 'static,
    {
        Box::new(self)
    }

    fn complexity_score(&self) -> usize;
}

#[derive(Clone)]
pub struct AccountFilterRef(pub(crate) Arc<dyn AccountFilter + Send + Sync + 'static>);

impl AccountFilter for AccountFilterRef {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        self.0.filter(account)
    }
    fn complexity_score(&self) -> usize {
        self.0.complexity_score()
    }
}

#[derive(Clone)]
pub struct TxFilterRef(pub(crate) Arc<dyn TxFilter + Send + Sync + 'static>);

impl TxFilter for TxFilterRef {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        self.0.filter(tx)
    }
    fn complexity_score(&self) -> usize {
        self.0.complexity_score()
    }
}

///
/// Filter that always return true
///
pub struct TrueFilter;

impl AccountFilter for TrueFilter {
    fn filter(&self, _account: &dyn AbstractAccount) -> bool {
        true
    }
    fn complexity_score(&self) -> usize {
        1
    }
}

impl TxFilter for TrueFilter {
    fn filter(&self, _tx: &dyn AbstractTx) -> bool {
        true
    }

    fn complexity_score(&self) -> usize {
        1
    }
}

///
/// Combines multiple account filters with a logical AND
///
pub struct AndAccountFilter {
    pub filters: Vec<Box<dyn AccountFilter + Send + Sync + 'static>>,
}

impl AccountFilter for AndAccountFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        self.filters.iter().all(|filter| filter.filter(account))
    }
    fn complexity_score(&self) -> usize {
        self.filters.iter().map(|f| f.complexity_score()).sum()
    }
}

///
/// Keeps account whose pubkey is equal to any of the specified pubkeys
///
pub struct AccountPubkeyFilter {
    pub pubkeys: HashSet<Pubkey>,
}

///
/// Keeps account whose owner is equal to any of the specified pubkeys
///
pub struct AccountOwnerFilter {
    pub owners: HashSet<Pubkey>,
}

///
/// Account data comparison operators
///
pub enum AccountMemcmpOp {
    /// Keeps account whose data slice starting at offset is equal to the specified bytestring
    Bytes(Vec<u8>),
    /// Keeps account whose data slice starting at offset is equal to the specified base58 string
    Base58(String),
    /// Keeps account whose data slice starting at offset is equal to the specified base64 string
    Base64(String),
}

///
/// Keeps account whose data slice starting at offset returns true for the specified comparison
///
pub struct AccountMemcmpFilter {
    pub offset: usize,
    pub op: AccountMemcmpOp,
}

///
/// Keeps account whose lamports satisfy the specified comparison
///
pub enum AccountLamportFilter {
    Eq(u64),
    Gt(u64),
    Lt(u64),
    Neq(u64),
}

///
/// Keeps account whose data length is equal to the specified length
///
pub struct AccountDataLenFilter {
    pub expected_len: u64,
}

///
/// Keeps account that is a token account
///
pub struct IsTokenAccountFilter;

///
/// Keeps account that has a transaction signature
///
pub struct NonEmptyTxSignature;

impl AccountFilter for NonEmptyTxSignature {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        account.tx_signature().is_some()
    }
    fn complexity_score(&self) -> usize {
        1
    }
}

impl AccountFilter for AccountOwnerFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        self.owners.contains(&account.owner())
    }
    fn complexity_score(&self) -> usize {
        self.owners.len()
    }
}

impl AccountFilter for AccountLamportFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        match self {
            AccountLamportFilter::Eq(lamports) => account.lamports() == *lamports,
            AccountLamportFilter::Gt(lamports) => account.lamports() > *lamports,
            AccountLamportFilter::Lt(lamports) => account.lamports() < *lamports,
            AccountLamportFilter::Neq(lamports) => account.lamports() != *lamports,
        }
    }

    fn complexity_score(&self) -> usize {
        1
    }
}

impl AccountFilter for AccountPubkeyFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        self.pubkeys.contains(&account.pubkey())
    }

    fn complexity_score(&self) -> usize {
        self.pubkeys.len()
    }
}

impl AccountFilter for IsTokenAccountFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        spl_token_2022::state::Account::valid_account_data(account.data())
    }

    fn complexity_score(&self) -> usize {
        1
    }
}

impl AccountFilter for AccountDataLenFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        account.data().len() as u64 == self.expected_len
    }

    fn complexity_score(&self) -> usize {
        1
    }
}

impl AccountFilter for AccountMemcmpFilter {
    fn filter(&self, account: &dyn AbstractAccount) -> bool {
        let data = &account.data()[self.offset..];
        match &self.op {
            AccountMemcmpOp::Bytes(bytes) => data.starts_with(bytes),
            AccountMemcmpOp::Base58(base58) => bs58::decode(base58)
                .into_vec()
                .map(|decoded| data.starts_with(decoded.as_slice()))
                .unwrap_or(false),
            AccountMemcmpOp::Base64(base64) => {
                let result = BASE64_STANDARD.decode(base64);
                result
                    .map(|decoded| data.starts_with(decoded.as_slice()))
                    .unwrap_or(false)
            }
        }
    }

    fn complexity_score(&self) -> usize {
        1
    }
}

///
/// Abstract "transaction"
///
pub trait AbstractTx {
    fn signature(&self) -> Signature;
    fn is_vote(&self) -> bool;
    fn is_failed(&self) -> bool;
    fn account_keys(&self, reducer: &mut dyn PubkeyReducer);
}

pub trait TxFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool;
    fn boxed(self) -> Box<dyn TxFilter + Send + Sync + 'static>
    where
        Self: Sized + Send + Sync + 'static,
    {
        Box::new(self)
    }
    fn complexity_score(&self) -> usize;
}

///
/// Keeps tx that are votes or not votes
///
pub struct TxVoteFilter {
    pub is_vote: bool,
}

///
/// Keeps tx that are failed or not failed
///
pub struct TxFailedFilter {
    pub failed: bool,
}

///
/// Keeps tx with the specified signature
///
pub struct TxSignatureFilter {
    pub signature: Signature,
}

///
/// Keeps tx whose account keys include any of the specified pubkeys
///
pub struct TxIncludeAccountFilter {
    pub pubkeys: HashSet<Pubkey>,
}

///
/// Keeps tx whose account keys does not include any of the specified pubkeys
///
pub struct TxExcludeAccountFilter {
    pub pubkeys: HashSet<Pubkey>,
}

///
/// Keeps tx whose account keys include all the specified pubkeys (subset test)
///
pub struct TxRequiredAccountFilter {
    pub pubkeys: HashSet<Pubkey>,
}

///
/// Combines multiple tx filters with a logical AND
///
pub struct TxAndFilter {
    pub filters: Vec<Box<dyn TxFilter + Send + Sync + 'static>>,
}

impl TxFilter for TxAndFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        self.filters.iter().all(|filter| filter.filter(tx))
    }
    fn complexity_score(&self) -> usize {
        self.filters.iter().map(|f| f.complexity_score()).sum()
    }
}

impl TxFilter for TxVoteFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        tx.is_vote() == self.is_vote
    }
    fn complexity_score(&self) -> usize {
        1
    }
}

impl TxFilter for TxFailedFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        tx.is_failed() == self.failed
    }
    fn complexity_score(&self) -> usize {
        1
    }
}

impl TxFilter for TxSignatureFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        self.signature == tx.signature()
    }
    fn complexity_score(&self) -> usize {
        1
    }
}

impl TxFilter for TxIncludeAccountFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        if self.pubkeys.is_empty() {
            return true; // If no pubkeys are specified, all txs match
        }

        struct Reducer<'pk> {
            pubkeys: &'pk HashSet<Pubkey>,
            result: bool,
        }

        impl PubkeyReducer for Reducer<'_> {
            fn accept(&mut self, account: Pubkey) -> ReducerDecision {
                if self.pubkeys.contains(&account) {
                    self.result = true;
                    ReducerDecision::Stop
                } else {
                    ReducerDecision::Continue
                }
            }
        }

        let mut reducer = Reducer {
            pubkeys: &self.pubkeys,
            result: false,
        };

        tx.account_keys(&mut reducer);
        reducer.result
    }

    fn complexity_score(&self) -> usize {
        self.pubkeys.len()
    }
}

impl TxFilter for TxExcludeAccountFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        if self.pubkeys.is_empty() {
            return true; // If no pubkeys are specified, all txs match
        }

        struct Reducer<'pk> {
            pubkeys: &'pk HashSet<Pubkey>,
            result: bool,
        }
        impl PubkeyReducer for Reducer<'_> {
            fn accept(&mut self, account: Pubkey) -> ReducerDecision {
                if self.pubkeys.contains(&account) {
                    self.result = false;
                    ReducerDecision::Stop
                } else {
                    ReducerDecision::Continue
                }
            }
        }

        let mut reducer = Reducer {
            pubkeys: &self.pubkeys,
            result: true,
        };
        tx.account_keys(&mut reducer);
        reducer.result
    }

    fn complexity_score(&self) -> usize {
        self.pubkeys.len()
    }
}

impl TxFilter for TxRequiredAccountFilter {
    fn filter(&self, tx: &dyn AbstractTx) -> bool {
        if self.pubkeys.is_empty() {
            return true; // If no pubkeys are specified, all txs match
        }
        struct Reducer<'pk> {
            pubkeys: &'pk HashSet<Pubkey>,
            matches: usize,
        }

        impl PubkeyReducer for Reducer<'_> {
            fn accept(&mut self, account: Pubkey) -> ReducerDecision {
                if self.pubkeys.contains(&account) {
                    self.matches += 1;
                }
                if self.matches == self.pubkeys.len() {
                    return ReducerDecision::Stop;
                }
                ReducerDecision::Continue
            }
        }

        let mut reducer = Reducer {
            pubkeys: &self.pubkeys,
            matches: 0,
        };

        tx.account_keys(&mut reducer);
        reducer.matches == reducer.pubkeys.len()
    }

    fn complexity_score(&self) -> usize {
        self.pubkeys.len()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{AbstractAccount, AbstractTx, AccountPubkeyFilter, AndAccountFilter, TxFilter},
        crate::abstract_filter::{
            AccountFilter, AccountOwnerFilter, PubkeyReducer, ReducerDecision, TrueFilter,
        },
        base64::{prelude::BASE64_STANDARD, Engine},
        solana_program_option::COption,
        solana_program_pack::Pack,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        spl_token_2022::state::{Account, AccountState},
        std::collections::HashSet,
    };

    struct MockAccount {
        pub pubkey: Pubkey,
        pub owner: Pubkey,
        pub lamports: u64,
        pub data: Vec<u8>,
        pub tx_sig: Option<Signature>,
    }

    pub const TEST_TOKEN_ACCOUNT: Account = Account {
        mint: Pubkey::new_from_array([1; 32]),
        owner: Pubkey::new_from_array([2; 32]),
        amount: 3,
        delegate: COption::Some(Pubkey::new_from_array([4; 32])),
        state: AccountState::Frozen,
        is_native: COption::Some(5),
        delegated_amount: 6,
        close_authority: COption::Some(Pubkey::new_from_array([7; 32])),
    };

    impl Default for MockAccount {
        fn default() -> Self {
            Self {
                pubkey: Pubkey::new_unique(),
                owner: Pubkey::new_unique(),
                lamports: 0,
                data: vec![],
                tx_sig: None,
            }
        }
    }

    impl AbstractAccount for MockAccount {
        fn pubkey(&self) -> Pubkey {
            self.pubkey
        }

        fn owner(&self) -> Pubkey {
            self.owner
        }

        fn lamports(&self) -> u64 {
            self.lamports
        }

        fn data(&self) -> &[u8] {
            &self.data
        }

        fn tx_signature(&self) -> Option<&[u8]> {
            self.tx_sig.as_ref().map(|sig| sig.as_ref())
        }
    }

    impl MockAccount {
        fn random() -> Self {
            Self {
                pubkey: Pubkey::new_unique(),
                owner: Pubkey::new_unique(),
                lamports: rand::random(),
                data: vec![],
                tx_sig: Some(Signature::new_unique()),
            }
        }
    }

    struct MockTx {
        pub signature: Signature,
        pub is_vote: bool,
        pub is_failed: bool,
        pub account_keys: Vec<Pubkey>,
    }

    impl AbstractTx for MockTx {
        fn signature(&self) -> Signature {
            self.signature
        }

        fn is_vote(&self) -> bool {
            self.is_vote
        }

        fn is_failed(&self) -> bool {
            self.is_failed
        }

        fn account_keys(&self, reducer: &mut dyn PubkeyReducer) {
            for account in &self.account_keys {
                if reducer.accept(*account) == ReducerDecision::Stop {
                    break;
                }
            }
        }
    }

    impl Default for MockTx {
        fn default() -> Self {
            Self {
                signature: Signature::new_unique(),
                is_vote: false,
                is_failed: false,
                account_keys: vec![],
            }
        }
    }

    struct FalseFilter;

    impl AccountFilter for FalseFilter {
        fn filter(&self, _account: &dyn AbstractAccount) -> bool {
            false
        }
        fn complexity_score(&self) -> usize {
            1
        }
    }

    impl TxFilter for FalseFilter {
        fn filter(&self, _tx: &dyn AbstractTx) -> bool {
            false
        }
        fn complexity_score(&self) -> usize {
            1
        }
    }

    #[test]
    pub fn empty_and_account_filter_should_always_return_true() {
        let filter = AndAccountFilter { filters: vec![] };
        let account = MockAccount::random();
        // let account_ref = &account as &dyn AbstractAccount;
        assert!(filter.filter(&account));
        assert_eq!(filter.complexity_score(), 0);
    }

    #[test]
    pub fn and_account_filter_should_return_false_if_any_subfilter_is_false() {
        let filter = AndAccountFilter {
            filters: vec![
                AccountFilter::boxed(TrueFilter),
                AccountFilter::boxed(FalseFilter),
            ],
        };
        let account = MockAccount::random();
        // let account_ref = &account as &dyn AbstractAccount;
        assert!(!filter.filter(&account));
        assert_eq!(filter.complexity_score(), 2);
    }

    #[test]
    pub fn and_account_filter_should_return_true_if_all_subfilter_is_true() {
        let filter = AndAccountFilter {
            filters: vec![
                AccountFilter::boxed(TrueFilter),
                AccountFilter::boxed(TrueFilter),
            ],
        };
        let account = MockAccount::random();
        // let account_ref = &account as &dyn AbstractAccount;
        assert!(filter.filter(&account));
        assert_eq!(filter.complexity_score(), 2);
    }

    #[test]
    pub fn account_pubkey_filter_should_return_true_on_pubkey_match() {
        let account = MockAccount::random();
        let filter = AccountPubkeyFilter {
            pubkeys: HashSet::from([account.pubkey]),
        };
        assert!(filter.filter(&account));
    }

    #[test]
    pub fn account_pubkey_filter_should_return_false_on_invalid_pubkey() {
        let account = MockAccount::random();
        let filter = AccountPubkeyFilter {
            pubkeys: HashSet::from([Pubkey::new_unique()]),
        };
        assert!(!filter.filter(&account));
    }

    #[test]
    pub fn account_pubkey_filter_should_return_true_on_any_match() {
        let account = MockAccount::random();
        let filter = AccountPubkeyFilter {
            pubkeys: HashSet::from([Pubkey::new_unique(), account.pubkey]),
        };
        assert!(filter.filter(&account));
    }

    #[test]
    pub fn account_owner_filter_should_return_true_on_pubkey_match() {
        let account = MockAccount::random();
        let filter = AccountOwnerFilter {
            owners: HashSet::from([account.owner]),
        };
        assert!(filter.filter(&account));
    }

    #[test]
    pub fn account_owner_filter_should_return_false_on_invalid_pubkey() {
        let account = MockAccount::random();
        let filter = AccountOwnerFilter {
            owners: HashSet::from([Pubkey::new_unique()]),
        };
        assert!(!filter.filter(&account));
    }

    #[test]
    pub fn account_owner_filter_should_return_true_on_any_match() {
        let account = MockAccount::random();
        let filter = AccountOwnerFilter {
            owners: HashSet::from([Pubkey::new_unique(), account.owner]),
        };
        assert!(filter.filter(&account));
    }

    #[test]
    pub fn test_account_lamport_filter() {
        let account = MockAccount {
            lamports: 100,
            ..Default::default()
        };
        let filter1 = super::AccountLamportFilter::Eq(account.lamports);
        let filter2 = super::AccountLamportFilter::Neq(10);
        let filter3 = super::AccountLamportFilter::Lt(101);
        let filter4 = super::AccountLamportFilter::Gt(99);
        assert!(filter1.filter(&account));
        assert!(filter2.filter(&account));
        assert!(filter3.filter(&account));
        assert!(filter4.filter(&account));

        let filter5 = super::AccountLamportFilter::Eq(10);
        let filter6 = super::AccountLamportFilter::Neq(account.lamports);
        let filter7 = super::AccountLamportFilter::Lt(99);
        let filter8 = super::AccountLamportFilter::Gt(101);
        assert!(!filter5.filter(&account));
        assert!(!filter6.filter(&account));
        assert!(!filter7.filter(&account));
        assert!(!filter8.filter(&account));
    }

    #[test]
    pub fn test_account_data_len_filter() {
        let account = MockAccount {
            data: vec![1, 2, 3],
            ..Default::default()
        };
        let filter1 = super::AccountDataLenFilter {
            expected_len: account.data.len() as u64,
        };
        let filter2 = super::AccountDataLenFilter { expected_len: 10 };
        assert!(filter1.filter(&account));
        assert!(!filter2.filter(&account));
    }

    #[test]
    pub fn test_account_data_bytes_cmp_filters() {
        let account = MockAccount {
            data: vec![1, 2, 3, 4, 5],
            ..Default::default()
        };
        let filter1 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Bytes(vec![1, 2, 3, 4, 5]),
        };
        let filter2 = super::AccountMemcmpFilter {
            offset: 1,
            op: super::AccountMemcmpOp::Bytes(vec![2, 3, 4, 5]),
        };
        let filter3 = super::AccountMemcmpFilter {
            offset: 4,
            op: super::AccountMemcmpOp::Bytes(vec![5]),
        };

        let filter4 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Bytes(vec![1]),
        };

        assert!(filter1.filter(&account));
        assert!(filter2.filter(&account));
        assert!(filter3.filter(&account));
        assert!(filter4.filter(&account));

        let filter5 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Bytes(vec![1, 2, 3, 4, 5, 6]),
        };

        let filter6 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Bytes(vec![2, 3, 4]),
        };

        assert!(!filter5.filter(&account));
        assert!(!filter6.filter(&account));
    }

    #[test]
    pub fn test_account_data_b58_cmp() {
        let pubkey = Pubkey::new_unique();
        let bs58_string = pubkey.to_string();
        let account = MockAccount {
            data: bs58::decode(bs58_string.clone())
                .into_vec()
                .expect("decode failed"),
            ..Default::default()
        };
        let filter1 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Base58(bs58_string.clone()),
        };

        assert!(filter1.filter(&account));

        // Now test with shifted offset
        let account = MockAccount {
            data: [
                vec![0],
                bs58::decode(bs58_string.clone())
                    .into_vec()
                    .expect("decode failed"),
            ]
            .concat()
            .to_vec(),
            ..Default::default()
        };

        let filter2 = super::AccountMemcmpFilter {
            offset: 1,
            op: super::AccountMemcmpOp::Base58(bs58_string.clone()),
        };
        assert!(filter2.filter(&account));

        // Final test : make sure invalid base58 string fails
        let filter3 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Base58(Pubkey::new_unique().to_string()),
        };

        assert!(!filter3.filter(&account));
    }

    #[test]
    pub fn test_account_data_b64_cmp() {
        let pubkey = Pubkey::new_unique();
        let b64_string = BASE64_STANDARD.encode(pubkey.to_bytes());
        let account = MockAccount {
            data: BASE64_STANDARD
                .decode(b64_string.clone())
                .expect("decode failed"),
            ..Default::default()
        };
        let filter1 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Base64(b64_string.clone()),
        };
        assert!(filter1.filter(&account));

        // Now test with shifted offset
        let account = MockAccount {
            data: [
                vec![0],
                BASE64_STANDARD
                    .decode(b64_string.clone())
                    .expect("decode failed"),
            ]
            .concat()
            .to_vec(),
            ..Default::default()
        };

        let filter2 = super::AccountMemcmpFilter {
            offset: 1,
            op: super::AccountMemcmpOp::Base64(b64_string.clone()),
        };
        assert!(filter2.filter(&account));

        // Final test : make sure invalid base64 string fails
        let filter3 = super::AccountMemcmpFilter {
            offset: 0,
            op: super::AccountMemcmpOp::Base64(
                BASE64_STANDARD.encode(Pubkey::new_unique().to_bytes()),
            ),
        };
        assert!(!filter3.filter(&account));
    }

    #[test]
    pub fn test_is_token_account() {
        let mut buf = [0; Account::LEN];
        TEST_TOKEN_ACCOUNT.pack_into_slice(&mut buf);
        let account = MockAccount {
            data: buf.to_vec(),
            ..Default::default()
        };
        let filter = super::IsTokenAccountFilter;
        assert!(filter.filter(&account));

        let account = MockAccount {
            data: vec![1, 2, 3],
            ..Default::default()
        };
        assert!(!filter.filter(&account));
    }

    #[test]
    pub fn test_account_tx_sig_filter() {
        let account1 = MockAccount {
            tx_sig: Some(Signature::new_unique()),
            ..Default::default()
        };
        let filter = super::NonEmptyTxSignature;
        assert!(filter.filter(&account1));

        let account2 = MockAccount {
            tx_sig: None,
            ..Default::default()
        };
        assert!(!filter.filter(&account2));
    }

    #[test]
    pub fn test_tx_vote_filter() {
        let tx1 = MockTx {
            is_vote: true,
            ..Default::default()
        };
        let filter = super::TxVoteFilter { is_vote: true };
        assert!(filter.filter(&tx1));

        let tx2 = MockTx {
            is_vote: false,
            ..Default::default()
        };
        assert!(!filter.filter(&tx2));
    }

    #[test]
    pub fn test_tx_failed_filter() {
        let tx1 = MockTx {
            is_failed: true,
            ..Default::default()
        };
        let filter = super::TxFailedFilter { failed: true };
        assert!(filter.filter(&tx1));

        let tx2 = MockTx {
            is_failed: false,
            ..Default::default()
        };
        assert!(!filter.filter(&tx2));
    }

    #[test]
    pub fn test_tx_signature_filter() {
        let signature = Signature::new_unique();
        let tx1 = MockTx {
            signature,
            ..Default::default()
        };
        let filter = super::TxSignatureFilter { signature };
        assert!(filter.filter(&tx1));

        let tx2 = MockTx {
            signature: Signature::new_unique(),
            ..Default::default()
        };
        assert!(!filter.filter(&tx2));
    }

    #[test]
    pub fn test_tx_include_account_filter() {
        let pubkey = Pubkey::new_unique();
        let tx1 = MockTx {
            account_keys: vec![pubkey],
            ..Default::default()
        };
        let filter = super::TxIncludeAccountFilter {
            pubkeys: HashSet::from([pubkey]),
        };
        assert!(filter.filter(&tx1));

        let tx2 = MockTx {
            account_keys: vec![Pubkey::new_unique()],
            ..Default::default()
        };
        assert!(!filter.filter(&tx2));
    }

    #[test]
    pub fn test_tx_exclude_account_filter() {
        let pubkey = Pubkey::new_unique();
        let tx1 = MockTx {
            account_keys: vec![pubkey],
            ..Default::default()
        };
        let filter = super::TxExcludeAccountFilter {
            pubkeys: HashSet::from([pubkey]),
        };
        assert!(!filter.filter(&tx1));

        let tx2 = MockTx {
            account_keys: vec![Pubkey::new_unique()],
            ..Default::default()
        };
        assert!(filter.filter(&tx2));
    }

    #[test]
    pub fn test_tx_required_account_filter() {
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let tx1 = MockTx {
            account_keys: vec![pubkey1, pubkey2],
            ..Default::default()
        };
        let filter = super::TxRequiredAccountFilter {
            pubkeys: HashSet::from([pubkey1]),
        };
        assert!(filter.filter(&tx1));

        let tx2 = MockTx {
            account_keys: vec![Pubkey::new_unique()],
            ..Default::default()
        };
        assert!(!filter.filter(&tx2));

        let filter2 = super::TxRequiredAccountFilter {
            pubkeys: HashSet::from([pubkey1, pubkey2]),
        };

        let tx3 = MockTx {
            account_keys: vec![pubkey1],
            ..Default::default()
        };
        assert!(!filter2.filter(&tx3));
    }
}
