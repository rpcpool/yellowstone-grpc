use std::fmt;

use yellowstone_grpc_proto::geyser::{
  subscribe_request_filter_accounts_filter, subscribe_request_filter_accounts_filter_lamports,
  subscribe_request_filter_accounts_filter_memcmp, CuckooFilter, CuckooHashAlgorithm,
};
use yellowstone_grpc_proto::prelude::SubscribeRequest;

use crate::AUTORECONNECT_FILTER_KEY;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeRequestValidationError {
  ReservedFilterName,
  MissingFilter {
    path: String,
  },
  MissingMemcmpData {
    path: String,
  },
  MissingLamportsComparator {
    path: String,
  },
  UnsupportedCuckooHashAlgorithm {
    path: String,
    value: i32,
  },
  InvalidCuckooEntriesPerBucket {
    path: String,
    value: u32,
  },
  InvalidCuckooFingerprintBits {
    path: String,
    value: u32,
  },
  InvalidCuckooBucketCount {
    path: String,
    value: u32,
  },
  InvalidCuckooDataLength {
    path: String,
    expected: usize,
    actual: usize,
  },
}

impl fmt::Display for SubscribeRequestValidationError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::ReservedFilterName => {
        write!(
          f,
          "invalid subscribe request: filter name {AUTORECONNECT_FILTER_KEY} is reserved"
        )
      }
      Self::MissingFilter { path } => {
        write!(
          f,
          "invalid subscribe request at {path}: filter should be defined"
        )
      }
      Self::MissingMemcmpData { path } => {
        write!(
          f,
          "invalid subscribe request at {path}: memcmp filter data should be defined"
        )
      }
      Self::MissingLamportsComparator { path } => {
        write!(
          f,
          "invalid subscribe request at {path}: lamports comparator should be defined"
        )
      }
      Self::UnsupportedCuckooHashAlgorithm { path, value } => {
        write!(
          f,
          "invalid subscribe request at {path}: unsupported cuckoo hash algorithm {value}"
        )
      }
      Self::InvalidCuckooEntriesPerBucket { path, value } => {
        write!(
          f,
          "invalid subscribe request at {path}: cuckoo entriesPerBucket should be 4, got {value}"
        )
      }
      Self::InvalidCuckooFingerprintBits { path, value } => {
        write!(
          f,
          "invalid subscribe request at {path}: cuckoo fingerprintBits should be 16, got {value}"
        )
      }
      Self::InvalidCuckooBucketCount { path, value } => {
        write!(
          f,
          "invalid subscribe request at {path}: cuckoo bucketCount should be a non-zero power of two, got {value}"
        )
      }
      Self::InvalidCuckooDataLength {
        path,
        expected,
        actual,
      } => {
        write!(
          f,
          "invalid subscribe request at {path}: cuckoo data length should be {expected} bytes, got {actual}"
        )
      }
    }
  }
}

impl std::error::Error for SubscribeRequestValidationError {}

pub fn validate_subscribe_request(
  request: &SubscribeRequest,
) -> Result<(), SubscribeRequestValidationError> {
  if contains_reserved_autoreconnect_filter(request) {
    return Err(SubscribeRequestValidationError::ReservedFilterName);
  }

  for (account_key, account_filter) in &request.accounts {
    if let Some(cuckoo_filter) = &account_filter.cuckoo_accounts_filter {
      validate_cuckoo_filter(
        cuckoo_filter,
        &format!("accounts[\"{account_key}\"].cuckooAccountsFilter"),
      )?;
    }

    for (index, filter) in account_filter.filters.iter().enumerate() {
      let filter_path = format!("accounts[\"{account_key}\"].filters[{index}]");

      let filter =
        filter
          .filter
          .as_ref()
          .ok_or_else(|| SubscribeRequestValidationError::MissingFilter {
            path: filter_path.clone(),
          })?;

      match filter {
        subscribe_request_filter_accounts_filter::Filter::Memcmp(memcmp_filter) => {
          validate_memcmp_filter(memcmp_filter, &filter_path)?;
        }
        subscribe_request_filter_accounts_filter::Filter::Lamports(lamports_filter) => {
          validate_lamports_filter(lamports_filter, &filter_path)?;
        }
        subscribe_request_filter_accounts_filter::Filter::Datasize(_)
        | subscribe_request_filter_accounts_filter::Filter::TokenAccountState(_) => {}
      }
    }
  }

  for (block_key, block_filter) in &request.blocks {
    if let Some(cuckoo_filter) = &block_filter.cuckoo_account_include {
      validate_cuckoo_filter(
        cuckoo_filter,
        &format!("blocks[\"{block_key}\"].cuckooAccountInclude"),
      )?;
    }
  }

  Ok(())
}

fn contains_reserved_filter_name<T>(filters: &std::collections::HashMap<String, T>) -> bool {
  filters.contains_key(AUTORECONNECT_FILTER_KEY)
}

fn contains_reserved_autoreconnect_filter(request: &SubscribeRequest) -> bool {
  contains_reserved_filter_name(&request.accounts)
    || contains_reserved_filter_name(&request.slots)
    || contains_reserved_filter_name(&request.transactions)
    || contains_reserved_filter_name(&request.transactions_status)
    || contains_reserved_filter_name(&request.blocks)
    || contains_reserved_filter_name(&request.blocks_meta)
    || contains_reserved_filter_name(&request.entry)
}

fn validate_memcmp_filter(
  memcmp_filter: &yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccountsFilterMemcmp,
  filter_path: &str,
) -> Result<(), SubscribeRequestValidationError> {
  if memcmp_filter.data.is_none() {
    return Err(SubscribeRequestValidationError::MissingMemcmpData {
      path: format!("{filter_path}.memcmp.data"),
    });
  }

  match memcmp_filter
    .data
    .as_ref()
    .expect("checked is_some above in validate_memcmp_filter")
  {
    subscribe_request_filter_accounts_filter_memcmp::Data::Bytes(_)
    | subscribe_request_filter_accounts_filter_memcmp::Data::Base58(_)
    | subscribe_request_filter_accounts_filter_memcmp::Data::Base64(_) => Ok(()),
  }
}

fn validate_lamports_filter(
  lamports_filter: &yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccountsFilterLamports,
  filter_path: &str,
) -> Result<(), SubscribeRequestValidationError> {
  if lamports_filter.cmp.is_none() {
    return Err(SubscribeRequestValidationError::MissingLamportsComparator {
      path: format!("{filter_path}.lamports.cmp"),
    });
  }

  match lamports_filter
    .cmp
    .as_ref()
    .expect("checked is_some above in validate_lamports_filter")
  {
    subscribe_request_filter_accounts_filter_lamports::Cmp::Eq(_)
    | subscribe_request_filter_accounts_filter_lamports::Cmp::Ne(_)
    | subscribe_request_filter_accounts_filter_lamports::Cmp::Lt(_)
    | subscribe_request_filter_accounts_filter_lamports::Cmp::Gt(_) => Ok(()),
  }
}

fn validate_cuckoo_filter(
  cuckoo_filter: &CuckooFilter,
  filter_path: &str,
) -> Result<(), SubscribeRequestValidationError> {
  if cuckoo_filter.hash_algorithm != CuckooHashAlgorithm::SipHash as i32 {
    return Err(
      SubscribeRequestValidationError::UnsupportedCuckooHashAlgorithm {
        path: filter_path.to_string(),
        value: cuckoo_filter.hash_algorithm,
      },
    );
  }

  if cuckoo_filter.entries_per_bucket != 4 {
    return Err(
      SubscribeRequestValidationError::InvalidCuckooEntriesPerBucket {
        path: filter_path.to_string(),
        value: cuckoo_filter.entries_per_bucket,
      },
    );
  }

  if cuckoo_filter.fingerprint_bits != 16 {
    return Err(
      SubscribeRequestValidationError::InvalidCuckooFingerprintBits {
        path: filter_path.to_string(),
        value: cuckoo_filter.fingerprint_bits,
      },
    );
  }

  if cuckoo_filter.bucket_count == 0 || !cuckoo_filter.bucket_count.is_power_of_two() {
    return Err(SubscribeRequestValidationError::InvalidCuckooBucketCount {
      path: filter_path.to_string(),
      value: cuckoo_filter.bucket_count,
    });
  }

  let expected_data_len = (cuckoo_filter.bucket_count as usize)
    .checked_mul(cuckoo_filter.entries_per_bucket as usize)
    .and_then(|slot_count| slot_count.checked_mul((cuckoo_filter.fingerprint_bits / 8) as usize))
    .ok_or_else(
      || SubscribeRequestValidationError::InvalidCuckooDataLength {
        path: filter_path.to_string(),
        expected: usize::MAX,
        actual: cuckoo_filter.data.len(),
      },
    )?;

  if cuckoo_filter.data.len() != expected_data_len {
    return Err(SubscribeRequestValidationError::InvalidCuckooDataLength {
      path: filter_path.to_string(),
      expected: expected_data_len,
      actual: cuckoo_filter.data.len(),
    });
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter, subscribe_request_filter_accounts_filter_lamports,
    subscribe_request_filter_accounts_filter_memcmp,
  };
  use yellowstone_grpc_proto::prelude::{
    CuckooFilter, CuckooHashAlgorithm, SubscribeRequest, SubscribeRequestAccountsDataSlice,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterLamports, SubscribeRequestFilterAccountsFilterMemcmp,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
  };

  use super::{validate_subscribe_request, SubscribeRequestValidationError};

  fn valid_cuckoo_filter() -> CuckooFilter {
    CuckooFilter {
      data: vec![0; 8],
      bucket_count: 1,
      entries_per_bucket: 4,
      fingerprint_bits: 16,
      hash_seed: 0x796c_6c77_7374_6e21,
      hash_algorithm: CuckooHashAlgorithm::SipHash as i32,
    }
  }

  fn fully_populated_request() -> SubscribeRequest {
    let mut accounts = HashMap::new();
    accounts.insert(
      "full".to_string(),
      SubscribeRequestFilterAccounts {
        account: vec!["account_a".to_string(), "account_b".to_string()],
        owner: vec!["owner_a".to_string()],
        filters: vec![
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
              SubscribeRequestFilterAccountsFilterMemcmp {
                offset: 7,
                data: Some(
                  subscribe_request_filter_accounts_filter_memcmp::Data::Bytes(vec![1, 2, 3]),
                ),
              },
            )),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
              SubscribeRequestFilterAccountsFilterMemcmp {
                offset: 8,
                data: Some(
                  subscribe_request_filter_accounts_filter_memcmp::Data::Base58(
                    "base58_value".to_string(),
                  ),
                ),
              },
            )),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
              SubscribeRequestFilterAccountsFilterMemcmp {
                offset: 9,
                data: Some(
                  subscribe_request_filter_accounts_filter_memcmp::Data::Base64(
                    "YmFzZTY0X3ZhbHVl".to_string(),
                  ),
                ),
              },
            )),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Datasize(
              165,
            )),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::TokenAccountState(true)),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Lamports(
              SubscribeRequestFilterAccountsFilterLamports {
                cmp: Some(subscribe_request_filter_accounts_filter_lamports::Cmp::Eq(
                  1,
                )),
              },
            )),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Lamports(
              SubscribeRequestFilterAccountsFilterLamports {
                cmp: Some(subscribe_request_filter_accounts_filter_lamports::Cmp::Ne(
                  2,
                )),
              },
            )),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Lamports(
              SubscribeRequestFilterAccountsFilterLamports {
                cmp: Some(subscribe_request_filter_accounts_filter_lamports::Cmp::Lt(
                  3,
                )),
              },
            )),
          },
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Lamports(
              SubscribeRequestFilterAccountsFilterLamports {
                cmp: Some(subscribe_request_filter_accounts_filter_lamports::Cmp::Gt(
                  4,
                )),
              },
            )),
          },
        ],
        nonempty_txn_signature: Some(true),
        cuckoo_accounts_filter: None,
      },
    );

    let mut slots = HashMap::new();
    slots.insert(
      "slot_client".to_string(),
      SubscribeRequestFilterSlots {
        filter_by_commitment: Some(true),
        interslot_updates: Some(false),
      },
    );

    let mut transactions = HashMap::new();
    transactions.insert(
      "transactions_client".to_string(),
      SubscribeRequestFilterTransactions {
        vote: Some(true),
        failed: Some(false),
        signature: Some("tx_sig_1".to_string()),
        account_include: vec!["acc_i".to_string()],
        account_exclude: vec!["acc_x".to_string()],
        account_required: vec!["acc_r".to_string()],
      },
    );

    let mut transactions_status = HashMap::new();
    transactions_status.insert(
      "tx_status_client".to_string(),
      SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(true),
        signature: Some("tx_sig_2".to_string()),
        account_include: vec!["status_i".to_string()],
        account_exclude: vec!["status_x".to_string()],
        account_required: vec!["status_r".to_string()],
      },
    );

    let mut blocks = HashMap::new();
    blocks.insert(
      "blocks_client".to_string(),
      SubscribeRequestFilterBlocks {
        account_include: vec!["block_acc".to_string()],
        include_transactions: Some(true),
        include_accounts: Some(false),
        include_entries: Some(true),
        cuckoo_account_include: None,
      },
    );

    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
      "blocks_meta_client".to_string(),
      SubscribeRequestFilterBlocksMeta {},
    );

    let mut entry = HashMap::new();
    entry.insert("entry_client".to_string(), SubscribeRequestFilterEntry {});

    SubscribeRequest {
      accounts,
      slots,
      transactions,
      transactions_status,
      blocks,
      blocks_meta,
      entry,
      commitment: Some(1),
      accounts_data_slice: vec![
        SubscribeRequestAccountsDataSlice {
          offset: 0,
          length: 32,
        },
        SubscribeRequestAccountsDataSlice {
          offset: 32,
          length: 64,
        },
      ],
      ping: Some(SubscribeRequestPing { id: 42 }),
      from_slot: Some(777),
    }
  }

  #[test]
  fn accepts_fully_populated_request_including_all_nested_fields() {
    let request = fully_populated_request();
    validate_subscribe_request(&request).expect("fully populated request should be valid");
  }

  #[test]
  fn rejects_missing_filter_variant_for_account_filter() {
    let mut request = fully_populated_request();
    request.accounts.get_mut("full").unwrap().filters[0].filter = None;

    let error = validate_subscribe_request(&request).expect_err("missing filter must fail");
    assert_eq!(
      error,
      SubscribeRequestValidationError::MissingFilter {
        path: "accounts[\"full\"].filters[0]".to_string(),
      }
    );
  }

  #[test]
  fn rejects_memcmp_filter_without_data_variant() {
    let mut request = fully_populated_request();
    request.accounts.get_mut("full").unwrap().filters[0].filter =
      Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
        SubscribeRequestFilterAccountsFilterMemcmp {
          offset: 99,
          data: None,
        },
      ));

    let error = validate_subscribe_request(&request).expect_err("missing memcmp data must fail");
    assert_eq!(
      error,
      SubscribeRequestValidationError::MissingMemcmpData {
        path: "accounts[\"full\"].filters[0].memcmp.data".to_string(),
      }
    );
  }

  #[test]
  fn rejects_lamports_filter_without_cmp_variant() {
    let mut request = fully_populated_request();
    request.accounts.get_mut("full").unwrap().filters[0].filter =
      Some(subscribe_request_filter_accounts_filter::Filter::Lamports(
        SubscribeRequestFilterAccountsFilterLamports { cmp: None },
      ));

    let error =
      validate_subscribe_request(&request).expect_err("missing lamports comparator must fail");
    assert_eq!(
      error,
      SubscribeRequestValidationError::MissingLamportsComparator {
        path: "accounts[\"full\"].filters[0].lamports.cmp".to_string(),
      }
    );
  }

  #[test]
  fn accepts_request_when_accounts_map_is_empty() {
    let request = SubscribeRequest {
      accounts: HashMap::new(),
      slots: HashMap::new(),
      transactions: HashMap::new(),
      transactions_status: HashMap::new(),
      blocks: HashMap::new(),
      blocks_meta: HashMap::new(),
      entry: HashMap::new(),
      commitment: Some(1),
      accounts_data_slice: vec![SubscribeRequestAccountsDataSlice {
        offset: 0,
        length: 32,
      }],
      ping: Some(SubscribeRequestPing { id: 9 }),
      from_slot: Some(123),
    };

    validate_subscribe_request(&request).expect("empty accounts map should be valid");
  }

  #[test]
  fn accepts_account_filter_with_empty_filters_array() {
    let mut request = fully_populated_request();
    request.accounts.get_mut("full").unwrap().filters = Vec::new();

    validate_subscribe_request(&request).expect("empty filters array should be valid");
  }

  #[test]
  fn rejects_reserved_autoreconnect_filter_name() {
    let mut request = fully_populated_request();
    let full_filter = request.accounts.remove("full").unwrap();
    request
      .accounts
      .insert(crate::AUTORECONNECT_FILTER_KEY.to_string(), full_filter);

    let error = validate_subscribe_request(&request).expect_err("reserved filter name must fail");

    assert_eq!(error, SubscribeRequestValidationError::ReservedFilterName);
  }

  #[test]
  fn reports_precise_path_for_invalid_filter_index_in_second_account_entry() {
    let mut request = fully_populated_request();
    request.accounts.insert(
      "secondary".to_string(),
      SubscribeRequestFilterAccounts {
        account: vec![],
        owner: vec![],
        filters: vec![
          SubscribeRequestFilterAccountsFilter {
            filter: Some(subscribe_request_filter_accounts_filter::Filter::Datasize(
              10,
            )),
          },
          SubscribeRequestFilterAccountsFilter { filter: None },
        ],
        nonempty_txn_signature: None,
        cuckoo_accounts_filter: None,
      },
    );

    let error =
      validate_subscribe_request(&request).expect_err("second account invalid filter must fail");

    assert_eq!(
      error,
      SubscribeRequestValidationError::MissingFilter {
        path: "accounts[\"secondary\"].filters[1]".to_string(),
      }
    );
  }

  #[test]
  fn accepts_valid_account_and_block_cuckoo_filters() {
    let mut request = fully_populated_request();
    request
      .accounts
      .get_mut("full")
      .unwrap()
      .cuckoo_accounts_filter = Some(valid_cuckoo_filter());
    request
      .blocks
      .get_mut("blocks_client")
      .unwrap()
      .cuckoo_account_include = Some(valid_cuckoo_filter());

    validate_subscribe_request(&request).expect("valid cuckoo filters should be accepted");
  }

  #[test]
  fn rejects_unsupported_cuckoo_hash_algorithm() {
    let mut request = fully_populated_request();
    let mut cuckoo_filter = valid_cuckoo_filter();
    cuckoo_filter.hash_algorithm = 99;
    request
      .accounts
      .get_mut("full")
      .unwrap()
      .cuckoo_accounts_filter = Some(cuckoo_filter);

    let error = validate_subscribe_request(&request).expect_err("invalid algorithm must fail");

    assert_eq!(
      error,
      SubscribeRequestValidationError::UnsupportedCuckooHashAlgorithm {
        path: "accounts[\"full\"].cuckooAccountsFilter".to_string(),
        value: 99,
      }
    );
  }

  #[test]
  fn rejects_cuckoo_data_length_mismatch() {
    let mut request = fully_populated_request();
    let mut cuckoo_filter = valid_cuckoo_filter();
    cuckoo_filter.data.pop();
    request
      .blocks
      .get_mut("blocks_client")
      .unwrap()
      .cuckoo_account_include = Some(cuckoo_filter);

    let error = validate_subscribe_request(&request).expect_err("invalid data length must fail");

    assert_eq!(
      error,
      SubscribeRequestValidationError::InvalidCuckooDataLength {
        path: "blocks[\"blocks_client\"].cuckooAccountInclude".to_string(),
        expected: 8,
        actual: 7,
      }
    );
  }
}
