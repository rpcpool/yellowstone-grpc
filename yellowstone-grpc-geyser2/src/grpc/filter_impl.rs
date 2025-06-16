use {
    crate::{
        abstract_filter::{
            AbstractAccount, AbstractBlockMeta, AbstractEntry, AbstractSlotStatus, AbstractTx,
            AccountDataLenFilter, AccountFilter, AccountFilterRef, AccountLamportFilter,
            AccountMemcmpFilter, AccountMemcmpOp, AccountOwnerFilter, AccountPubkeyFilter,
            AndAccountFilter, BlockMetaFilter, BlockMetaFilterRef, EntryFilter, EntryFilterRef,
            IsTokenAccountFilter, NonEmptyTxSignature, SlotStatusFilter, SlotStatusFilterRef,
            TrueFilter, TxAndFilter, TxExcludeAccountFilter, TxFailedFilter, TxFilter, TxFilterRef,
            TxIncludeAccountFilter, TxRequiredAccountFilter, TxSignatureFilter, TxVoteFilter,
        },
        plugin::{MessageAccount, MessageBlockMeta, MessageEntry, MessageSlot, MessageTransaction},
        proto::geyser::{
            subscribe_request_filter_accounts_filter::Filter,
            subscribe_request_filter_accounts_filter_lamports::Cmp,
            subscribe_request_filter_accounts_filter_memcmp::Data, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdateAccount, SubscribeUpdateBlockMeta,
            SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction,
        },
    },
    solana_pubkey::{ParsePubkeyError, Pubkey},
    solana_signature::{ParseSignatureError, Signature},
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::Arc,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum CompileAccountFilterError {
    #[error(transparent)]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("Missing account data in memcp filter")]
    MissingAccountData,
}

pub fn compile_account_filter(
    grpc_account_filter: &SubscribeRequestFilterAccounts,
) -> Result<AccountFilterRef, CompileAccountFilterError> {
    let mut filters = vec![];

    if !grpc_account_filter.account.is_empty() {
        let accounts = grpc_account_filter
            .account
            .iter()
            .map(|pubkey| Pubkey::from_str(pubkey))
            .collect::<Result<HashSet<_>, _>>()?;
        let filter = AccountPubkeyFilter { pubkeys: accounts };
        filters.push(filter.boxed());
    }

    if !grpc_account_filter.owner.is_empty() {
        let owners = grpc_account_filter
            .owner
            .iter()
            .map(|owner| Pubkey::from_str(owner))
            .collect::<Result<HashSet<_>, _>>()?;
        let filter = AccountOwnerFilter { owners };
        filters.push(filter.boxed());
    }

    if grpc_account_filter.nonempty_txn_signature() {
        filters.push(NonEmptyTxSignature.boxed());
    }

    for filter in grpc_account_filter.filters.iter() {
        if let Some(filter) = filter.filter.as_ref() {
            let compiled_filter = match filter {
                Filter::Memcmp(oneof) => {
                    let offset = oneof.offset;
                    let data = oneof
                        .data
                        .as_ref()
                        .ok_or(CompileAccountFilterError::MissingAccountData)?;
                    match data {
                        Data::Bytes(data) => {
                            let op = AccountMemcmpOp::Bytes(data.clone());
                            AccountMemcmpFilter {
                                offset: offset as usize,
                                op,
                            }
                        }
                        Data::Base58(data) => {
                            let op = AccountMemcmpOp::Base58(data.clone());
                            AccountMemcmpFilter {
                                offset: offset as usize,
                                op,
                            }
                        }
                        Data::Base64(data) => {
                            let op = AccountMemcmpOp::Base64(data.clone());
                            AccountMemcmpFilter {
                                offset: offset as usize,
                                op,
                            }
                        }
                    }
                    .boxed()
                }
                Filter::Datasize(expected) => AccountDataLenFilter {
                    expected_len: *expected,
                }
                .boxed(),
                Filter::TokenAccountState(_) => IsTokenAccountFilter.boxed(),
                Filter::Lamports(oneof) => {
                    if let Some(cmp) = oneof.cmp {
                        match cmp {
                            Cmp::Eq(x) => AccountLamportFilter::Eq(x),
                            Cmp::Gt(x) => AccountLamportFilter::Gt(x),
                            Cmp::Lt(x) => AccountLamportFilter::Lt(x),
                            Cmp::Ne(x) => AccountLamportFilter::Neq(x),
                        }
                        .boxed()
                    } else {
                        AccountFilter::boxed(TrueFilter)
                    }
                }
            };
            filters.push(compiled_filter);
        }
    }
    let b = AndAccountFilter { filters }.boxed();
    Ok(AccountFilterRef(Arc::from(b)))
}

#[derive(Debug, thiserror::Error)]
pub enum CompileTxFilterError {
    #[error(transparent)]
    InvalidSignature(#[from] ParseSignatureError),
    #[error(transparent)]
    InvalidPubkey(#[from] ParsePubkeyError),
}

pub fn compile_tx_filter(
    grpc_tx_filter: &SubscribeRequestFilterTransactions,
) -> Result<TxFilterRef, CompileTxFilterError> {
    let mut filters = vec![];

    if let Some(vote) = grpc_tx_filter.vote {
        filters.push(TxVoteFilter { is_vote: vote }.boxed());
    }

    if let Some(failed) = grpc_tx_filter.failed {
        filters.push(TxFailedFilter { failed }.boxed());
    }

    if let Some(signature) = grpc_tx_filter.signature.as_ref() {
        let signature = Signature::from_str(signature)?;
        let filter = TxSignatureFilter { signature };
        filters.push(filter.boxed());
    }

    if !grpc_tx_filter.account_include.is_empty() {
        let pubkeys = grpc_tx_filter
            .account_include
            .iter()
            .map(|pubkey| Pubkey::from_str(pubkey))
            .collect::<Result<_, _>>()?;
        let filter = TxIncludeAccountFilter { pubkeys };
        filters.push(filter.boxed());
    }

    if !grpc_tx_filter.account_exclude.is_empty() {
        let pubkeys = grpc_tx_filter
            .account_exclude
            .iter()
            .map(|pubkey| Pubkey::from_str(pubkey))
            .collect::<Result<_, _>>()?;
        let filter = TxExcludeAccountFilter { pubkeys };
        filters.push(filter.boxed());
    }

    if !grpc_tx_filter.account_required.is_empty() {
        let pubkeys = grpc_tx_filter
            .account_required
            .iter()
            .map(|pubkey| Pubkey::from_str(pubkey))
            .collect::<Result<_, _>>()?;
        let filter = TxRequiredAccountFilter { pubkeys };
        filters.push(filter.boxed());
    }

    let b = TxAndFilter { filters }.boxed();

    Ok(TxFilterRef(Arc::from(b)))
}

pub fn compile_slot_filter(_filter: &SubscribeRequestFilterSlots) -> SlotStatusFilterRef {
    let r = SlotStatusFilter::boxed(TrueFilter);
    SlotStatusFilterRef(Arc::from(r))
}

#[derive(Clone, Default)]
pub struct CompiledFilters {
    pub account_filters: HashMap<String, AccountFilterRef>,
    pub tx_filters: HashMap<String, TxFilterRef>,
    pub slot_status: HashMap<String, SlotStatusFilterRef>,
    pub entry: HashMap<String, EntryFilterRef>,
    pub blockmeta: HashMap<String, BlockMetaFilterRef>,
}

#[derive(Debug, thiserror::Error)]
pub enum TryFromSubscribeRequestError {
    #[error(transparent)]
    CompileAccountFilter(CompileAccountFilterError),
    #[error(transparent)]
    CompileTxFilter(CompileTxFilterError),
}

impl TryFrom<&SubscribeRequest> for CompiledFilters {
    type Error = TryFromSubscribeRequestError;

    fn try_from(value: &SubscribeRequest) -> Result<Self, Self::Error> {
        let account_filters: HashMap<String, AccountFilterRef> = value
            .accounts
            .iter()
            .map(|(id, filter)| {
                let compiled_filter = compile_account_filter(filter)
                    .map_err(TryFromSubscribeRequestError::CompileAccountFilter)?;
                Ok((id.clone(), compiled_filter))
            })
            .collect::<Result<_, _>>()?;

        let tx_filters: HashMap<String, TxFilterRef> = value
            .transactions
            .iter()
            .map(|(id, filter)| {
                let compiled_filter = compile_tx_filter(filter)
                    .map_err(TryFromSubscribeRequestError::CompileTxFilter)?;
                Ok((id.clone(), compiled_filter))
            })
            .collect::<Result<_, _>>()?;

        let slot_status: HashMap<String, SlotStatusFilterRef> = value
            .slots
            .iter()
            .map(|(id, filter)| {
                let compiled_filter = compile_slot_filter(filter);
                (id.clone(), compiled_filter)
            })
            .collect();

        let entry: HashMap<String, EntryFilterRef> = value
            .entry
            .iter()
            .map(|(id, _)| (id.clone(), EntryFilterRef(Arc::new(TrueFilter))))
            .collect();

        let blockmeta: HashMap<String, BlockMetaFilterRef> = value
            .blocks_meta
            .iter()
            .map(|(id, _)| (id.clone(), BlockMetaFilterRef(Arc::new(TrueFilter))))
            .collect();

        let compiled = CompiledFilters {
            account_filters,
            tx_filters,
            slot_status,
            entry,
            blockmeta,
        };
        Ok(compiled)
    }
}

impl AbstractAccount for MessageAccount {
    fn pubkey(&self) -> Pubkey {
        self.account.pubkey
    }

    fn lamports(&self) -> u64 {
        self.account.lamports
    }

    fn data(&self) -> &[u8] {
        self.account.data.as_ref()
    }

    fn owner(&self) -> Pubkey {
        self.account.owner
    }
}

impl AbstractTx for MessageTransaction {
    fn signature(&self) -> Signature {
        self.transaction.signature
    }

    fn is_vote(&self) -> bool {
        self.transaction.is_vote
    }

    fn is_failed(&self) -> bool {
        self.transaction.meta.err.is_some()
    }

    fn account_keys(&self) -> Vec<Pubkey> {
        self.transaction.account_keys.iter().cloned().collect()
    }
}

impl AbstractSlotStatus for MessageSlot {}

impl AbstractEntry for MessageEntry {}

impl AbstractBlockMeta for MessageBlockMeta {}

impl CompiledFilters {
    pub fn get_account_matching(
        &self,
        account: &dyn AbstractAccount,
        matches: &mut HashSet<String>,
    ) {
        self.account_filters
            .iter()
            .filter(|(_id, filter)| filter.filter(account))
            .for_each(|(id, _)| {
                matches.insert(id.clone());
            });
    }

    pub fn get_tx_matching(&self, tx: &dyn AbstractTx, matches: &mut HashSet<String>) {
        self.tx_filters
            .iter()
            .filter(|(_id, filter)| filter.filter(tx))
            .for_each(|(id, _)| {
                matches.insert(id.clone());
            });
    }

    pub fn get_slot_matching(&self, slot: &dyn AbstractSlotStatus, matches: &mut HashSet<String>) {
        self.slot_status
            .iter()
            .filter(|(_id, filter)| filter.filter(slot))
            .for_each(|(id, _)| {
                matches.insert(id.clone());
            });
    }

    pub fn get_entry_matching(&self, entry: &dyn AbstractEntry, matches: &mut HashSet<String>) {
        self.entry
            .iter()
            .filter(|(_id, filter)| filter.filter(entry))
            .for_each(|(id, _)| {
                matches.insert(id.clone());
            });
    }

    pub fn get_blockmeta_matching(
        &self,
        blockmeta: &dyn AbstractBlockMeta,
        matches: &mut HashSet<String>,
    ) {
        self.blockmeta
            .iter()
            .filter(|(_id, filter)| filter.filter(blockmeta))
            .for_each(|(id, _)| {
                matches.insert(id.clone());
            });
    }
}

impl AbstractTx for SubscribeUpdateTransaction {
    fn signature(&self) -> Signature {
        let tx_info = self
            .transaction
            .as_ref()
            .expect("Transaction info is missing");
        Signature::try_from(tx_info.signature.as_ref())
            .expect("Failed to parse transaction signature")
    }

    fn is_vote(&self) -> bool {
        let tx_info = self
            .transaction
            .as_ref()
            .expect("Transaction info is missing");
        tx_info.is_vote
    }

    fn is_failed(&self) -> bool {
        let tx_info = self
            .transaction
            .as_ref()
            .expect("Transaction info is missing");
        tx_info
            .meta
            .as_ref()
            .map_or(false, |meta| meta.err.is_some())
    }

    fn account_keys(&self) -> Vec<Pubkey> {
        let tx_info = self
            .transaction
            .as_ref()
            .expect("Transaction info is missing");
        let tx = tx_info
            .transaction
            .as_ref()
            .expect("Transaction data is missing");
        let message = tx.message.as_ref().expect("Message is missing");
        message
            .account_keys
            .iter()
            .map(|key| Pubkey::try_from(key.as_slice()).expect("Failed to parse account key"))
            .collect()
    }

    fn all_account_keys_match(&self, f: &dyn Fn(&Pubkey) -> bool) -> bool {
        let tx_info = self
            .transaction
            .as_ref()
            .expect("Transaction info is missing");
        let tx = tx_info
            .transaction
            .as_ref()
            .expect("Transaction data is missing");
        let message = tx.message.as_ref().expect("Message is missing");
        message
            .account_keys
            .iter()
            .all(|key| f(&Pubkey::try_from(key.as_slice()).expect("Failed to parse account key")))
    }

    fn any_account_key_match(&self, f: &dyn Fn(&Pubkey) -> bool) -> bool {
        let tx_info = self
            .transaction
            .as_ref()
            .expect("Transaction info is missing");
        let tx = tx_info
            .transaction
            .as_ref()
            .expect("Transaction data is missing");
        let message = tx.message.as_ref().expect("Message is missing");
        message
            .account_keys
            .iter()
            .any(|key| f(&Pubkey::try_from(key.as_slice()).expect("Failed to parse account key")))
    }
}

impl AbstractAccount for SubscribeUpdateAccount {
    fn pubkey(&self) -> Pubkey {
        let account = self.account.as_ref().expect("Account info is missing");
        Pubkey::try_from(account.pubkey.as_slice()).expect("Failed to parse account pubkey")
    }

    fn lamports(&self) -> u64 {
        let account = self.account.as_ref().expect("Account info is missing");
        account.lamports
    }

    fn data(&self) -> &[u8] {
        let account = self.account.as_ref().expect("Account info is missing");
        &account.data
    }

    fn owner(&self) -> Pubkey {
        let account = self.account.as_ref().expect("Account info is missing");
        Pubkey::try_from(account.owner.as_slice()).expect("Failed to parse account owner")
    }
}

impl AbstractSlotStatus for SubscribeUpdateSlot {
    // Implement methods as needed
}

impl AbstractEntry for SubscribeUpdateEntry {
    // Implement methods as needed
}

impl AbstractBlockMeta for SubscribeUpdateBlockMeta {
    // Implement methods as needed
}
