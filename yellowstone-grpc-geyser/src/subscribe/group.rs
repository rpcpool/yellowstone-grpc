//! Grouped sends: the counted entries one routed message hands out to the members it reaches.
//! The first member that needs entries adds the group's budget with one count add; each member
//! is queued right after its match, and the surplus releases when the message is done.

use {
    crate::{
        body::block_shape::{BlockShapeEvent, BLOCK_SHAPE_SLOTS},
        plugin::{
            filter::{
                message::{FilteredUpdates, WeightScales},
                Filter, MatchScratch, MatchedUpdate,
            },
            message::{CommitmentLevel, Message, MessageBlock},
        },
        subscribe::entry::{
            blocks_filter_index, AddedIndexedEntries, BlockEntryFlags, IndexedMessageEntry,
            IndexedUpdateRef, SubscribeQueueEntry,
        },
    },
    std::{mem, slice, sync::Arc},
    yellowstone_grpc_concurrency::counted_message::{AddMessageEntries, AddedMessageEntries},
};

/// The most updates one member gets of one routed message: a transaction gives a Transaction and
/// a TransactionStatus update, a footer one per certificates variant, every other kind one.
pub const fn indexed_updates_per_member_bound(routed_message: &Message) -> usize {
    match routed_message {
        Message::Transaction(_) | Message::BlockFooter(_) => 2,
        _ => 1,
    }
}

/// The entries of one routed message that its group's members take, in hit order.
pub struct GroupEntries<'message> {
    routed_message: &'message Message,
    added_entries: Option<AddedIndexedEntries>,
}

impl<'message> GroupEntries<'message> {
    /// A group with no entry added yet.
    pub const fn new(routed_message: &'message Message) -> Self {
        Self {
            routed_message,
            added_entries: None,
        }
    }

    /// The added entries not handed out yet.
    pub fn entries_left(&self) -> usize {
        self.added_entries
            .as_ref()
            .map_or(0, AddedIndexedEntries::entries_left)
    }

    /// Adds `entry_budget` entries with one count add, in place of the entries left. A kind that
    /// routing never delivers adds none.
    pub fn add_entries(&mut self, entry_budget: usize) {
        self.added_entries = AddedIndexedEntries::add_to(self.routed_message, entry_budget);
    }

    /// The next added entry, or `None` once every added entry is handed out.
    pub fn next_entry(&mut self) -> Option<IndexedMessageEntry> {
        self.added_entries.as_mut()?.next_entry()
    }
}

/// `true` when a member's matched updates of one message need the reference matcher: some mask
/// recomputes its names, past 31 names of a kind or for a name the kind's table lacks.
pub fn takes_reference_path(member_updates: &[MatchedUpdate]) -> bool {
    member_updates
        .iter()
        .any(|matched_update| matched_update.filter_name_mask.requires_recompute())
}

/// One member's queue entries of one message, in its update order: a group of one. Its counted
/// entries take one count add; a member on the reference path, or a Block member past its pins,
/// gets the reference matcher's updates instead.
pub enum MemberQueueEntries<'member> {
    Indexed {
        member_filter: &'member Arc<Filter>,
        member_updates: slice::Iter<'member, MatchedUpdate>,
        group_entries: GroupEntries<'member>,
    },
    Block(BlockMemberEntries<'member>),
    Filtered(<FilteredUpdates as IntoIterator>::IntoIter),
}

impl<'member> MemberQueueEntries<'member> {
    /// Matches `routed_message` with `member_filter` into `match_scratch`, then adds one entry on
    /// the message per matched update. A Block entry is weighed with `weight_scales`.
    pub fn of_message(
        member_filter: &'member Arc<Filter>,
        routed_message: &'member Message,
        commitment_level: Option<CommitmentLevel>,
        match_scratch: &'member mut MatchScratch,
        weight_scales: &'member WeightScales,
    ) -> Self {
        member_filter.match_update(routed_message, commitment_level, match_scratch);
        let match_scratch: &'member MatchScratch = match_scratch;
        let member_updates = match_scratch.matched_updates();
        let mut group_entries = GroupEntries::new(routed_message);
        let takes_reference_path = takes_reference_path(member_updates);
        if let Message::Block(message_block) = routed_message {
            if !takes_reference_path && !member_updates.is_empty() {
                if member_filter.block_pins().try_pin_block() {
                    return Self::Block(BlockMemberEntries {
                        member_filter,
                        message_block,
                        member_updates: member_updates.iter(),
                        added_entries: message_block.add_message_entries(member_updates.len()),
                        weight_scales,
                        holds_unclaimed_pin: true,
                    });
                }
                let block_shapes = &message_block.block_shapes;
                block_shapes.count_shape_event(BlockShapeEvent::PinnedBlockFallback);
            }
        } else if !takes_reference_path {
            group_entries.add_entries(member_updates.len());
        }
        if takes_reference_path || group_entries.entries_left() < member_updates.len() {
            let filtered_updates = member_filter.get_updates(routed_message, commitment_level);
            return Self::Filtered(filtered_updates.into_iter());
        }
        Self::Indexed {
            member_filter,
            member_updates: member_updates.iter(),
            group_entries,
        }
    }
}

/// A Block member's entries of one block, one per matched blocks filter, each looked up in the
/// block's shape table as it is yielded. The first entry takes the member's pin on the block.
pub struct BlockMemberEntries<'member> {
    member_filter: &'member Arc<Filter>,
    message_block: &'member Arc<MessageBlock>,
    member_updates: slice::Iter<'member, MatchedUpdate>,
    added_entries: AddedMessageEntries<MessageBlock>,
    weight_scales: &'member WeightScales,
    /// The pin no entry took yet, released if these entries drop before the first is yielded.
    holds_unclaimed_pin: bool,
}

impl Iterator for BlockMemberEntries<'_> {
    type Item = SubscribeQueueEntry;

    fn next(&mut self) -> Option<SubscribeQueueEntry> {
        let matched_update = self.member_updates.next()?;
        let counted_block = self
            .added_entries
            .next()
            .expect("the member added an entry for each matched update");
        let filter_index = blocks_filter_index(matched_update.filter_name_mask);
        let member_block_shape =
            (self.member_filter).member_block_shape(filter_index, self.message_block, None);
        let block_shapes = &self.message_block.block_shapes;
        let shape_lookup = block_shapes.find_or_key_shape(
            &self.member_filter.block_shapes()[filter_index],
            || {
                if member_block_shape.selects_by_account_keys() {
                    block_shapes.count_shape_event(BlockShapeEvent::SelectionWalk);
                }
                // A full table keeps no selection, so the member measures without keeping one.
                if block_shapes.keyed_shape_count() < BLOCK_SHAPE_SLOTS {
                    member_block_shape.measure_selection()
                } else {
                    member_block_shape.measure_totals()
                }
            },
        );
        let block_entry_flags = BlockEntryFlags::new(
            shape_lookup.shape_slot_index,
            mem::take(&mut self.holds_unclaimed_pin),
        );
        let message_entry = IndexedMessageEntry::Block {
            message_block: counted_block,
            update_weight: self
                .weight_scales
                .block_update_weight(&shape_lookup.selection_totals),
            block_entry_flags,
        };
        let indexed_update = IndexedUpdateRef::new(
            message_entry,
            Arc::clone(self.member_filter),
            *matched_update,
        );
        Some(SubscribeQueueEntry::Indexed(indexed_update))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.member_updates.size_hint()
    }
}

impl Drop for BlockMemberEntries<'_> {
    fn drop(&mut self) {
        if self.holds_unclaimed_pin {
            self.member_filter.block_pins().unpin_block();
        }
    }
}

impl Iterator for MemberQueueEntries<'_> {
    type Item = SubscribeQueueEntry;

    fn next(&mut self) -> Option<SubscribeQueueEntry> {
        match self {
            Self::Indexed {
                member_filter,
                member_updates,
                group_entries,
            } => {
                let matched_update = member_updates.next()?;
                let message_entry = group_entries
                    .next_entry()
                    .expect("the group added an entry for each matched update");
                let indexed_update = IndexedUpdateRef::new(
                    message_entry,
                    Arc::clone(member_filter),
                    *matched_update,
                );
                Some(SubscribeQueueEntry::Indexed(indexed_update))
            }
            Self::Block(block_entries) => block_entries.next(),
            Self::Filtered(filtered_updates) => {
                filtered_updates.next().map(SubscribeQueueEntry::Filtered)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let entries_left = match self {
            Self::Indexed { member_updates, .. } => member_updates.len(),
            Self::Block(block_entries) => block_entries.member_updates.len(),
            Self::Filtered(filtered_updates) => filtered_updates.len(),
        };
        (entries_left, Some(entries_left))
    }
}

impl ExactSizeIterator for MemberQueueEntries<'_> {}
