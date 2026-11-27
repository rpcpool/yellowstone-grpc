//! [`FilteredUpdateKey`]: what makes two members' updates of one message the same bytes on the
//! wire: the body variant, the response encoding and the sorted filter-name bytes.

use {
    crate::{
        compression::ResponseEncoding,
        plugin::filter::{
            name::{FilterGenerationNames, FilterNameKind, FilterNameMask, FilterNameSelection},
            variant::UpdateBodyVariant,
        },
    },
    std::{
        fmt,
        hash::{Hash, Hasher},
        sync::Arc,
    },
};

/// One member's update of one message, as compression groups and owner groups see it. Equality
/// compares the variant, the encoding and the name bytes; the name hash only speeds up lookups.
#[derive(Clone, Copy)]
pub struct FilteredUpdateKey<'member> {
    pub update_body_variant: UpdateBodyVariant,
    pub response_encoding: ResponseEncoding,
    /// The selected names' list hash under the per-process seed.
    pub filter_name_hash: u64,
    generation_names: &'member Arc<FilterGenerationNames>,
    filter_name_kind: FilterNameKind,
    filter_names: FilterNameSelection<'member>,
}

impl<'member> FilteredUpdateKey<'member> {
    /// The key of a member whose `name_mask` selects names of `filter_name_kind`; `None` for a
    /// mask whose names the reference matcher recomputes, which has no key.
    pub fn new(
        update_body_variant: UpdateBodyVariant,
        response_encoding: ResponseEncoding,
        generation_names: &'member Arc<FilterGenerationNames>,
        filter_name_kind: FilterNameKind,
        name_mask: FilterNameMask,
    ) -> Option<Self> {
        let filter_names = generation_names.name_selection(filter_name_kind, name_mask)?;
        Some(Self {
            update_body_variant,
            response_encoding,
            filter_name_hash: filter_names.name_list_hash(),
            generation_names,
            filter_name_kind,
            filter_names,
        })
    }

    /// The filter generation whose names the key selects.
    pub const fn generation_names(&self) -> &'member Arc<FilterGenerationNames> {
        self.generation_names
    }

    pub const fn filter_name_kind(&self) -> FilterNameKind {
        self.filter_name_kind
    }

    pub const fn filter_names(&self) -> FilterNameSelection<'member> {
        self.filter_names
    }

    pub const fn name_mask(&self) -> FilterNameMask {
        self.filter_names.name_mask()
    }
}

impl PartialEq for FilteredUpdateKey<'_> {
    fn eq(&self, other_key: &Self) -> bool {
        self.update_body_variant == other_key.update_body_variant
            && self.response_encoding == other_key.response_encoding
            && self.filter_name_hash == other_key.filter_name_hash
            && self.filter_names.same_names_as(&other_key.filter_names)
    }
}

impl Eq for FilteredUpdateKey<'_> {}

/// Hashes what equality compares first; equal names always have equal name hashes.
impl Hash for FilteredUpdateKey<'_> {
    fn hash<KeyHasher: Hasher>(&self, key_hasher: &mut KeyHasher) {
        self.update_body_variant.hash(key_hasher);
        self.response_encoding.hash(key_hasher);
        key_hasher.write_u64(self.filter_name_hash);
    }
}

impl fmt::Debug for FilteredUpdateKey<'_> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("FilteredUpdateKey")
            .field("update_body_variant", &self.update_body_variant)
            .field("response_encoding", &self.response_encoding)
            .field("filter_name_hash", &self.filter_name_hash)
            .field(
                "filter_names",
                &self
                    .filter_names
                    .selected_names()
                    .map(|filter_name| filter_name.as_ref())
                    .collect::<Vec<&str>>(),
            )
            .finish()
    }
}
