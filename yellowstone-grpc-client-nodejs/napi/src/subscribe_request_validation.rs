use std::fmt;

use yellowstone_grpc_proto::geyser::{
  subscribe_request_filter_accounts_filter, subscribe_request_filter_accounts_filter_lamports,
  subscribe_request_filter_accounts_filter_memcmp,
};
use yellowstone_grpc_proto::prelude::SubscribeRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeRequestValidationError {
  MissingFilter { path: String },
  MissingMemcmpData { path: String },
  MissingLamportsComparator { path: String },
}

impl fmt::Display for SubscribeRequestValidationError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
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
    }
  }
}

impl std::error::Error for SubscribeRequestValidationError {}

pub fn validate_subscribe_request(
  request: &SubscribeRequest,
) -> Result<(), SubscribeRequestValidationError> {
  for (account_key, account_filter) in &request.accounts {
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

  Ok(())
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

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter, subscribe_request_filter_accounts_filter_lamports,
    subscribe_request_filter_accounts_filter_memcmp,
  };
  use yellowstone_grpc_proto::prelude::{
    SubscribeRequest, SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
    SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocks,
    SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
  };

  use super::{validate_subscribe_request, SubscribeRequestValidationError};

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
}
