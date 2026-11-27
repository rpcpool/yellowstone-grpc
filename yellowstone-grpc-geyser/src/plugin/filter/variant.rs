//! Which shared body a member's update reads, and the content ids of data slices and block
//! shapes, built when a filter compiles.

use {
    crate::plugin::filter::name::{per_process_hash_state, per_process_seeded_hash},
    solana_pubkey::Pubkey,
    std::{
        hash::{BuildHasher, Hasher},
        ops::Range,
        sync::Arc,
    },
    yellowstone_grpc_proto::{cuckoo::CuckooFilter, geyser::CuckooFilter as CuckooFilterProto},
};

const DATA_SLICE_HASH_SALT: u64 = 0x3b9f_e2c4_6d18_a705;
const BLOCK_SHAPE_HASH_SALT: u64 = 0xc6e1_0a57_f2d9_4b83;
/// The first byte of every shape's canonical bytes.
const CANONICAL_SHAPE_FORMAT: u8 = 1;
const SHAPE_INCLUDES_TRANSACTIONS: u8 = 1;
const SHAPE_INCLUDES_ACCOUNTS: u8 = 1 << 1;
const SHAPE_INCLUDES_ENTRIES: u8 = 1 << 2;

/// Which shared body of a message a member's update reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpdateBodyVariant {
    /// Account without a data slice, Slot, Entry, EntryUpdateParent and BlockMeta.
    MessageBody,
    TransactionUpdate,
    TransactionStatusUpdate,
    BlockFooterWithoutCertificates,
    BlockFooterWithCertificates,
    SlicedAccount(DataSliceId),
    BlockShape(BlockShapeId),
}

impl UpdateBodyVariant {
    /// The variant of a filter's account updates, from its validated data slice.
    pub fn for_account_data_slice(data_slice_ranges: &[Range<usize>]) -> Self {
        if data_slice_ranges.is_empty() {
            Self::MessageBody
        } else {
            Self::SlicedAccount(DataSliceId::from_data_slice_ranges(data_slice_ranges))
        }
    }
}

/// A content id of a validated data slice: equal ranges get equal ids in one process. A table
/// keyed by it confirms each hit by comparing the ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataSliceId(u64);

impl DataSliceId {
    pub fn from_data_slice_ranges(data_slice_ranges: &[Range<usize>]) -> Self {
        let mut data_slice_hasher = per_process_hash_state(DATA_SLICE_HASH_SALT).build_hasher();
        data_slice_hasher.write_usize(data_slice_ranges.len());
        for data_slice_range in data_slice_ranges {
            data_slice_hasher.write_usize(data_slice_range.start);
            data_slice_hasher.write_usize(data_slice_range.end);
        }
        Self(data_slice_hasher.finish())
    }
}

/// A content id of a normalized blocks filter: equal shapes get equal ids in one process. A
/// table keyed by it confirms each hit with [`NormalizedBlockShape::same_shape_as`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockShapeId(u64);

impl BlockShapeId {
    pub const fn content_hash(self) -> u64 {
        self.0
    }
}

/// What one blocks filter's Block bodies depend on, as canonical bytes: two filters with equal
/// bytes give equal Block bodies for every block.
#[derive(Debug, Clone)]
pub struct NormalizedBlockShape {
    block_shape_id: BlockShapeId,
    canonical_shape_bytes: Arc<[u8]>,
}

impl NormalizedBlockShape {
    /// Normalizes one blocks filter. Parts that cannot change a Block body are left out: the
    /// account selection without transactions or accounts, and the data slice without accounts.
    pub fn from_blocks_filter_parts(
        include_transactions: bool,
        include_accounts: bool,
        include_entries: bool,
        account_include: impl IntoIterator<Item = Pubkey>,
        account_cuckoo: Option<&CuckooFilter<[u8; 32]>>,
        data_slice_ranges: &[Range<usize>],
    ) -> Self {
        let mut include_flags = 0;
        for (shape_includes_part, part_flag) in [
            (include_transactions, SHAPE_INCLUDES_TRANSACTIONS),
            (include_accounts, SHAPE_INCLUDES_ACCOUNTS),
            (include_entries, SHAPE_INCLUDES_ENTRIES),
        ] {
            if shape_includes_part {
                include_flags |= part_flag;
            }
        }
        let mut canonical_shape_bytes = vec![CANONICAL_SHAPE_FORMAT, include_flags];
        if include_transactions || include_accounts {
            let mut sorted_account_include: Vec<Pubkey> = account_include.into_iter().collect();
            sorted_account_include.sort_unstable();
            sorted_account_include.dedup();
            Self::append_fixed_width_number(
                &mut canonical_shape_bytes,
                sorted_account_include.len(),
            );
            for included_pubkey in &sorted_account_include {
                canonical_shape_bytes.extend_from_slice(included_pubkey.as_ref());
            }
            match account_cuckoo {
                None => canonical_shape_bytes.push(0),
                Some(cuckoo_filter) => {
                    let cuckoo_proto = CuckooFilterProto::from(cuckoo_filter);
                    canonical_shape_bytes.push(1);
                    canonical_shape_bytes.extend_from_slice(&cuckoo_proto.hash_seed.to_le_bytes());
                    Self::append_fixed_width_number(
                        &mut canonical_shape_bytes,
                        cuckoo_proto.data.len(),
                    );
                    canonical_shape_bytes.extend_from_slice(&cuckoo_proto.data);
                }
            }
        }
        if include_accounts {
            Self::append_fixed_width_number(&mut canonical_shape_bytes, data_slice_ranges.len());
            for data_slice_range in data_slice_ranges {
                Self::append_fixed_width_number(&mut canonical_shape_bytes, data_slice_range.start);
                Self::append_fixed_width_number(&mut canonical_shape_bytes, data_slice_range.end);
            }
        }
        Self {
            block_shape_id: BlockShapeId(per_process_seeded_hash(
                BLOCK_SHAPE_HASH_SALT,
                &canonical_shape_bytes,
            )),
            canonical_shape_bytes: Arc::from(canonical_shape_bytes),
        }
    }

    fn append_fixed_width_number(canonical_shape_bytes: &mut Vec<u8>, number_value: usize) {
        canonical_shape_bytes.extend_from_slice(&(number_value as u64).to_le_bytes());
    }

    pub const fn block_shape_id(&self) -> BlockShapeId {
        self.block_shape_id
    }

    /// Whether both shapes give equal Block bodies: equal ids, confirmed by one set of bytes or
    /// by equal bytes.
    pub fn same_shape_as(&self, other_shape: &Self) -> bool {
        self.block_shape_id == other_shape.block_shape_id
            && (Arc::ptr_eq(
                &self.canonical_shape_bytes,
                &other_shape.canonical_shape_bytes,
            ) || self.canonical_shape_bytes == other_shape.canonical_shape_bytes)
    }
}
