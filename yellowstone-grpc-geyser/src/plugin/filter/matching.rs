//! Allocation-free matching: each update a member gets, as the body variant it reads and a mask
//! over its filter generation's sorted names.

use {
    super::Filter,
    crate::plugin::{
        filter::{
            name::{FilterKindNames, FilterName, FilterNameKind, FilterNameMask},
            variant::UpdateBodyVariant,
        },
        message::{CommitmentLevel, Message},
    },
};

/// One update of a member: the shared body it reads and the names it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchedUpdate {
    pub update_body_variant: UpdateBodyVariant,
    /// A mask over the sorted names of the variant's kind, or the recompute fallback.
    pub filter_name_mask: FilterNameMask,
}

/// The owner's scratch for [`Filter::match_update`]. It keeps its capacity, so a warm scratch
/// takes no allocation.
#[derive(Debug, Default)]
pub struct MatchScratch {
    matched_updates: Vec<MatchedUpdate>,
}

impl MatchScratch {
    /// The pairs of the last [`Filter::match_update`], in the member's update order.
    pub fn matched_updates(&self) -> &[MatchedUpdate] {
        &self.matched_updates
    }

    /// Adds the pair when its mask selects a name; a kind with no matched name makes no update.
    fn push_if_named(
        &mut self,
        update_body_variant: UpdateBodyVariant,
        filter_name_mask: FilterNameMask,
    ) {
        if !filter_name_mask.is_empty() {
            self.matched_updates.push(MatchedUpdate {
                update_body_variant,
                filter_name_mask,
            });
        }
    }
}

impl Filter {
    /// Writes one pair per update that [`Filter::get_updates`] makes of `captured_message`, in its
    /// order and with its names. A kind past 31 names gets [`FilterNameMask::RECOMPUTE_NAMES`].
    pub fn match_update(
        &self,
        captured_message: &Message,
        commitment_level: Option<CommitmentLevel>,
        match_scratch: &mut MatchScratch,
    ) {
        match_scratch.matched_updates.clear();
        let generation_names = &self.filter_generation_names;
        match captured_message {
            Message::Account(account_message) => {
                let mut matched_mask = FilterNameMask::NO_NAMES;
                self.accounts
                    .strategy
                    .for_each_matched_aggregate(account_message, |aggregate_index| {
                        matched_mask = matched_mask.with_name_index(aggregate_index)
                    });
                let accounts_names = generation_names.kind_names(FilterNameKind::Accounts);
                match_scratch.push_if_named(
                    self.account_body_variant,
                    kind_name_mask(accounts_names, matched_mask),
                );
            }
            Message::Slot(slot_message) => {
                let slots_mask = mask_of_matching_filters(
                    generation_names.kind_names(FilterNameKind::Slots),
                    &self.slots.filters,
                    |slots_filter| slots_filter.matches_slot(slot_message, commitment_level),
                );
                match_scratch.push_if_named(UpdateBodyVariant::MessageBody, slots_mask);
            }
            Message::Transaction(transaction_message) => {
                for (filter_name_kind, update_body_variant, transactions_filters) in [
                    (
                        FilterNameKind::Transactions,
                        UpdateBodyVariant::TransactionUpdate,
                        &self.transactions,
                    ),
                    (
                        FilterNameKind::TransactionsStatus,
                        UpdateBodyVariant::TransactionStatusUpdate,
                        &self.transactions_status,
                    ),
                ] {
                    let transactions_mask = mask_of_matching_filters(
                        generation_names.kind_names(filter_name_kind),
                        &transactions_filters.filters,
                        |transactions_filter| {
                            transactions_filter.matches_transaction(transaction_message)
                        },
                    );
                    match_scratch.push_if_named(update_body_variant, transactions_mask);
                }
            }
            Message::DeshredTransaction(_) | Message::DeshredUpdateParent(_) => {}
            Message::EntryUpdateParent(_) => {
                let entries_names = generation_names.kind_names(FilterNameKind::Entries);
                match_scratch.push_if_named(
                    UpdateBodyVariant::MessageBody,
                    mask_of_listed_names(entries_names, &self.entries.update_parent_filters),
                );
            }
            Message::Entry(_) => {
                let entries_names = generation_names.kind_names(FilterNameKind::Entries);
                match_scratch.push_if_named(
                    UpdateBodyVariant::MessageBody,
                    entries_names.all_names_mask(),
                );
            }
            Message::BlockMeta(_) => {
                let blocks_meta_names = generation_names.kind_names(FilterNameKind::BlocksMeta);
                match_scratch.push_if_named(
                    UpdateBodyVariant::MessageBody,
                    blocks_meta_names.all_names_mask(),
                );
            }
            Message::BlockFooter(_) => {
                let footer_names = generation_names.kind_names(FilterNameKind::BlockFooter);
                let footer_variants = [
                    UpdateBodyVariant::BlockFooterWithoutCertificates,
                    UpdateBodyVariant::BlockFooterWithCertificates,
                ];
                for (update_body_variant, footer_filter_names) in
                    footer_variants.into_iter().zip(&self.block_footer.filters)
                {
                    match_scratch.push_if_named(
                        update_body_variant,
                        mask_of_listed_names(footer_names, footer_filter_names),
                    );
                }
            }
            Message::Block(_) => {
                let blocks_names = generation_names.kind_names(FilterNameKind::Blocks);
                for (blocks_filter_index, block_shape) in self.block_shapes.iter().enumerate() {
                    match_scratch.push_if_named(
                        UpdateBodyVariant::BlockShape(block_shape.block_shape_id()),
                        blocks_names.name_mask(blocks_filter_index),
                    );
                }
            }
        }
    }
}

/// `matched_mask` as the kind carries it: past 31 names, any match takes the recompute fallback.
fn kind_name_mask(kind_names: &FilterKindNames, matched_mask: FilterNameMask) -> FilterNameMask {
    if matched_mask.is_empty() || kind_names.uses_name_masks() {
        matched_mask
    } else {
        FilterNameMask::RECOMPUTE_NAMES
    }
}

/// The mask of the filters that `filter_matches` accepts. A kind keeps its filters in name
/// order, so a filter's position is its name index.
fn mask_of_matching_filters<FilterInner>(
    kind_names: &FilterKindNames,
    named_filters: &[(FilterName, FilterInner)],
    mut filter_matches: impl FnMut(&FilterInner) -> bool,
) -> FilterNameMask {
    debug_assert_eq!(named_filters.len(), kind_names.name_count());
    let mut matched_mask = FilterNameMask::NO_NAMES;
    for (name_index, (_, filter_inner)) in named_filters.iter().enumerate() {
        if filter_matches(filter_inner) {
            if !kind_names.uses_name_masks() {
                return FilterNameMask::RECOMPUTE_NAMES;
            }
            matched_mask = matched_mask.with_name_index(name_index);
        }
    }
    matched_mask
}

/// The mask of `listed_names`, which are some of the kind's names. A name the table lacks gives
/// the recompute fallback.
fn mask_of_listed_names(
    kind_names: &FilterKindNames,
    listed_names: &[FilterName],
) -> FilterNameMask {
    let mut listed_mask = FilterNameMask::NO_NAMES;
    for listed_name in listed_names {
        let Some(name_index) = kind_names.name_index(listed_name) else {
            return FilterNameMask::RECOMPUTE_NAMES;
        };
        listed_mask = listed_mask.with_name_index(name_index);
    }
    kind_name_mask(kind_names, listed_mask)
}
