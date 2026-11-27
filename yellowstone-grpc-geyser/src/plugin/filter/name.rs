use {
    bytes::buf::BufMut,
    foldhash::{fast::SeedableRandomState, SharedSeed},
    prost::encoding::{encode_key, encode_varint, WireType},
    std::{
        borrow::Borrow,
        collections::HashSet,
        hash::{BuildHasher, Hasher},
        ops::Deref,
        ptr,
        sync::Arc,
        time::{Duration, Instant},
    },
};

#[derive(Debug, thiserror::Error)]
pub enum FilterNameError {
    #[error("oversized filter name (max allowed size {limit}), found {size}")]
    Oversized { limit: usize, size: usize },
}

pub type FilterNameResult<T> = Result<T, FilterNameError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterName(Arc<String>);

impl AsRef<str> for FilterName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for FilterName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for FilterName {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0[..]
    }
}

impl FilterName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Arc::new(name.into()))
    }

    pub fn is_uniq(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }
}

#[derive(Debug)]
pub struct FilterNames {
    name_size_limit: usize,
    names: HashSet<FilterName>,
    names_size_limit: usize,
    cleanup_ts: Instant,
    cleanup_interval: Duration,
}

impl FilterNames {
    pub fn new(
        name_size_limit: usize,
        names_size_limit: usize,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            name_size_limit,
            names: HashSet::with_capacity(names_size_limit),
            names_size_limit,
            cleanup_ts: Instant::now(),
            cleanup_interval,
        }
    }

    pub fn try_clean(&mut self) {
        if self.names.len() > self.names_size_limit
            && self.cleanup_ts.elapsed() > self.cleanup_interval
        {
            self.names.retain(|name| !name.is_uniq());
            self.cleanup_ts = Instant::now();
        }
    }

    pub fn get(&mut self, name: &str) -> FilterNameResult<FilterName> {
        match self.names.get(name) {
            Some(name) => Ok(name.clone()),
            None => {
                if name.len() > self.name_size_limit {
                    Err(FilterNameError::Oversized {
                        limit: self.name_size_limit,
                        size: name.len(),
                    })
                } else {
                    let name = FilterName::new(name);
                    self.names.insert(name.clone());
                    Ok(name)
                }
            }
        }
    }
}

/// Per-hasher seeds that keep each kind of content hash apart from the others.
const FILTER_NAME_HASH_SALT: u64 = 0x5d1c_7a2e_93b4_0f61;
const FILTER_NAME_LIST_HASH_SALT: u64 = 0xa4e8_316b_c057_d29f;

/// Hashes `content_bytes` under a seed that is random per process and the same for every
/// filter in it, so equal content from two filters hashes equal.
pub fn per_process_seeded_hash(hash_salt: u64, content_bytes: &[u8]) -> u64 {
    per_process_hash_state(hash_salt).hash_one(content_bytes)
}

/// The hasher state behind [`per_process_seeded_hash`], for content hashed in parts.
pub fn per_process_hash_state(hash_salt: u64) -> SeedableRandomState {
    SeedableRandomState::with_seed(hash_salt, SharedSeed::global_random())
}

/// A filter kind whose matched names an update carries in its `filters` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterNameKind {
    Accounts,
    Slots,
    Transactions,
    TransactionsStatus,
    /// Entry filters; EntryUpdateParent updates select the ones that opted in.
    Entries,
    Blocks,
    BlocksMeta,
    /// Footer filters with and without certificates, in one table.
    BlockFooter,
}

impl FilterNameKind {
    pub const ALL_KINDS: [Self; 8] = [
        Self::Accounts,
        Self::Slots,
        Self::Transactions,
        Self::TransactionsStatus,
        Self::Entries,
        Self::Blocks,
        Self::BlocksMeta,
        Self::BlockFooter,
    ];

    const fn table_index(self) -> usize {
        self as usize
    }
}

/// Bit i selects the i-th sorted name of one kind; all ones means "recompute".
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterNameMask(u32);

impl FilterNameMask {
    /// The most names a kind may have and still get masks; bit 31 stays free for the fallback.
    pub const MAX_MASKED_NAMES: usize = 31;
    pub const NO_NAMES: Self = Self(0);
    /// The writer recomputes this member's names with the reference matcher.
    pub const RECOMPUTE_NAMES: Self = Self(u32::MAX);

    pub const fn from_mask_bits(mask_bits: u32) -> Self {
        Self(mask_bits)
    }

    pub const fn mask_bits(self) -> u32 {
        self.0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn requires_recompute(self) -> bool {
        self.0 == Self::RECOMPUTE_NAMES.0
    }

    /// This mask with `name_index` selected too; an index past the mask gives the fallback.
    pub const fn with_name_index(self, name_index: usize) -> Self {
        if name_index < Self::MAX_MASKED_NAMES {
            Self(self.0 | (1 << name_index))
        } else {
            Self::RECOMPUTE_NAMES
        }
    }

    /// How many names the mask selects; 0 for the fallback, which selects none by itself.
    pub const fn selected_name_count(self) -> usize {
        if self.requires_recompute() {
            0
        } else {
            self.0.count_ones() as usize
        }
    }

    /// The selected name indexes in ascending order; none for the fallback.
    pub const fn selected_name_indexes(self) -> SelectedNameIndexes {
        SelectedNameIndexes {
            remaining_mask_bits: if self.requires_recompute() { 0 } else { self.0 },
        }
    }
}

/// The name indexes one [`FilterNameMask`] selects, in ascending order.
#[derive(Debug, Clone)]
pub struct SelectedNameIndexes {
    remaining_mask_bits: u32,
}

impl Iterator for SelectedNameIndexes {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.remaining_mask_bits == 0 {
            return None;
        }
        let name_index = self.remaining_mask_bits.trailing_zeros() as usize;
        self.remaining_mask_bits &= self.remaining_mask_bits - 1;
        Some(name_index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_count = self.remaining_mask_bits.count_ones() as usize;
        (remaining_count, Some(remaining_count))
    }
}

impl ExactSizeIterator for SelectedNameIndexes {}

/// One kind's names in one filter generation, sorted by bytes, with their encoded field-1
/// entries, lengths and per-process seeded hashes.
#[derive(Debug, Default)]
pub struct FilterKindNames {
    sorted_names: Box<[FilterName]>,
    encoded_name_entries: Box<[u8]>,
    encoded_entry_ends: Box<[usize]>,
    name_hashes: Box<[u64]>,
}

impl FilterKindNames {
    /// Builds the table from names already sorted by bytes, each listed once.
    pub fn from_sorted_names(sorted_names: Vec<FilterName>) -> Self {
        debug_assert!(sorted_names
            .windows(2)
            .all(|adjacent_names| adjacent_names[0].as_bytes() < adjacent_names[1].as_bytes()));
        let mut encoded_name_entries = Vec::new();
        let mut encoded_entry_ends = Vec::with_capacity(sorted_names.len());
        let mut name_hashes = Vec::with_capacity(sorted_names.len());
        for filter_name in &sorted_names {
            encode_key(1, WireType::LengthDelimited, &mut encoded_name_entries);
            encode_varint(filter_name.len() as u64, &mut encoded_name_entries);
            encoded_name_entries.extend_from_slice(filter_name.as_bytes());
            encoded_entry_ends.push(encoded_name_entries.len());
            name_hashes.push(per_process_seeded_hash(
                FILTER_NAME_HASH_SALT,
                filter_name.as_bytes(),
            ));
        }
        Self {
            sorted_names: sorted_names.into_boxed_slice(),
            encoded_name_entries: encoded_name_entries.into_boxed_slice(),
            encoded_entry_ends: encoded_entry_ends.into_boxed_slice(),
            name_hashes: name_hashes.into_boxed_slice(),
        }
    }

    pub fn name_count(&self) -> usize {
        self.sorted_names.len()
    }

    /// The index of `filter_name` in the sorted names, if this kind has it.
    pub fn name_index(&self, filter_name: &str) -> Option<usize> {
        self.sorted_names
            .binary_search_by(|sorted_name| sorted_name.as_bytes().cmp(filter_name.as_bytes()))
            .ok()
    }

    /// The name's field-1 entry: the tag, the length and the name's bytes.
    pub fn encoded_name_entry(&self, name_index: usize) -> &[u8] {
        let entry_start = name_index
            .checked_sub(1)
            .map_or(0, |previous_index| self.encoded_entry_ends[previous_index]);
        &self.encoded_name_entries[entry_start..self.encoded_entry_ends[name_index]]
    }

    /// Whether this kind's selections fit a mask; past [`FilterNameMask::MAX_MASKED_NAMES`]
    /// names every mask of the kind is [`FilterNameMask::RECOMPUTE_NAMES`].
    pub fn uses_name_masks(&self) -> bool {
        self.sorted_names.len() <= FilterNameMask::MAX_MASKED_NAMES
    }

    /// The mask that selects the name at `name_index` alone.
    pub fn name_mask(&self, name_index: usize) -> FilterNameMask {
        if self.uses_name_masks() && name_index < self.sorted_names.len() {
            FilterNameMask::NO_NAMES.with_name_index(name_index)
        } else {
            FilterNameMask::RECOMPUTE_NAMES
        }
    }

    pub fn all_names_mask(&self) -> FilterNameMask {
        if self.uses_name_masks() {
            FilterNameMask::from_mask_bits(((1_u64 << self.sorted_names.len()) - 1) as u32)
        } else {
            FilterNameMask::RECOMPUTE_NAMES
        }
    }

    /// The names `name_mask` selects, or `None` for the fallback, a bit past the last name, or
    /// a kind with too many names for masks.
    pub fn name_selection(&self, name_mask: FilterNameMask) -> Option<FilterNameSelection<'_>> {
        let selects_known_names = self.uses_name_masks()
            && !name_mask.requires_recompute()
            && u64::from(name_mask.mask_bits()) >> self.sorted_names.len() == 0;
        selects_known_names.then_some(FilterNameSelection {
            kind_names: self,
            name_mask,
        })
    }
}

/// The names one mask selects from one kind's sorted names, in sorted order.
#[derive(Debug, Clone, Copy)]
pub struct FilterNameSelection<'kind_names> {
    kind_names: &'kind_names FilterKindNames,
    name_mask: FilterNameMask,
}

impl<'kind_names> FilterNameSelection<'kind_names> {
    pub const fn name_mask(&self) -> FilterNameMask {
        self.name_mask
    }

    pub const fn selected_name_count(&self) -> usize {
        self.name_mask.selected_name_count()
    }

    pub fn selected_names(&self) -> impl Iterator<Item = &'kind_names FilterName> {
        let kind_names = self.kind_names;
        self.name_mask
            .selected_name_indexes()
            .map(move |name_index| &kind_names.sorted_names[name_index])
    }

    /// The list's hash, folded from the per-name hashes in sorted order: equal lists from any
    /// two filters of this process hash equal.
    pub fn name_list_hash(&self) -> u64 {
        let mut list_hasher = per_process_hash_state(FILTER_NAME_LIST_HASH_SALT).build_hasher();
        for name_index in self.name_mask.selected_name_indexes() {
            list_hasher.write_u64(self.kind_names.name_hashes[name_index]);
        }
        list_hasher.finish()
    }

    /// The bytes [`Self::write_encoded_entries`] writes.
    pub fn encoded_entries_len(&self) -> usize {
        self.name_mask
            .selected_name_indexes()
            .map(|name_index| self.kind_names.encoded_name_entry(name_index).len())
            .sum()
    }

    /// Writes the selected names' field-1 entries in sorted order.
    pub fn write_encoded_entries(&self, encoded_output: &mut impl BufMut) {
        if self.name_mask == self.kind_names.all_names_mask() {
            encoded_output.put_slice(&self.kind_names.encoded_name_entries);
            return;
        }
        for name_index in self.name_mask.selected_name_indexes() {
            encoded_output.put_slice(self.kind_names.encoded_name_entry(name_index));
        }
    }

    /// Whether both selections are the same names in the same order, compared by bytes.
    pub fn same_names_as(&self, other_selection: &FilterNameSelection<'_>) -> bool {
        if self.selected_name_count() != other_selection.selected_name_count() {
            return false;
        }
        if ptr::eq(self.kind_names, other_selection.kind_names) {
            return self.name_mask == other_selection.name_mask;
        }
        self.name_mask
            .selected_name_indexes()
            .zip(other_selection.name_mask.selected_name_indexes())
            .all(|(own_index, other_index)| {
                self.kind_names.name_hashes[own_index]
                    == other_selection.kind_names.name_hashes[other_index]
                    && self.kind_names.encoded_name_entry(own_index)
                        == other_selection.kind_names.encoded_name_entry(other_index)
            })
    }
}

/// One filter generation's sorted names, one table per kind. A member's matched names for
/// one update are a [`FilterNameMask`] over its kind's table.
#[derive(Debug, Default)]
pub struct FilterGenerationNames {
    kind_name_tables: [FilterKindNames; FilterNameKind::ALL_KINDS.len()],
}

impl FilterGenerationNames {
    /// Builds every kind's table from its names, which `sorted_names_of_kind` gives sorted.
    pub fn from_sorted_kind_names(
        mut sorted_names_of_kind: impl FnMut(FilterNameKind) -> Vec<FilterName>,
    ) -> Self {
        Self {
            kind_name_tables: FilterNameKind::ALL_KINDS.map(|filter_name_kind| {
                FilterKindNames::from_sorted_names(sorted_names_of_kind(filter_name_kind))
            }),
        }
    }

    pub const fn kind_names(&self, filter_name_kind: FilterNameKind) -> &FilterKindNames {
        &self.kind_name_tables[filter_name_kind.table_index()]
    }

    pub fn name_selection(
        &self,
        filter_name_kind: FilterNameKind,
        name_mask: FilterNameMask,
    ) -> Option<FilterNameSelection<'_>> {
        self.kind_names(filter_name_kind).name_selection(name_mask)
    }
}
