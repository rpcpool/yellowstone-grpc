use crate::js_types::*;
use napi::bindgen_prelude::Env;
use std::collections::HashMap;
use yellowstone_grpc_proto::geyser::{
  subscribe_request_filter_accounts_filter,
  subscribe_request_filter_accounts_filter_lamports,
  subscribe_request_filter_accounts_filter_memcmp, subscribe_update,
};
use yellowstone_grpc_proto::prelude::*;
use yellowstone_grpc_proto::solana::storage::confirmed_block::*;

#[test]
fn generated_js_types_expose_expected_conversion_method_signatures() {
  macro_rules! assert_conversion_signatures_for_env_type {
    ($js_type:ident, $proto_type:ty) => {
      let _from_protobuf_to_js_type: fn(
        &'static Env,
        $proto_type,
      ) -> napi::Result<$js_type<'static>> = $js_type::from_protobuf_to_js_type;
      let _from_js_to_protobuf_type: fn($js_type<'static>) -> napi::Result<$proto_type> =
        $js_type::from_js_to_protobuf_type;
    };
  }

  macro_rules! assert_conversion_signatures_for_non_env_type {
    ($js_type:ident, $proto_type:ty) => {
      let _from_protobuf_to_js_type: fn(&Env, $proto_type) -> napi::Result<$js_type> =
        $js_type::from_protobuf_to_js_type;
      let _from_js_to_protobuf_type: fn($js_type) -> napi::Result<$proto_type> =
        $js_type::from_js_to_protobuf_type;
    };
  }

  assert_conversion_signatures_for_env_type!(
    JsSubscribeRequestFilterAccountsFilterFilter,
    subscribe_request_filter_accounts_filter::Filter
  );
  assert_conversion_signatures_for_env_type!(
    JsSubscribeRequestFilterAccountsFilterMemcmpData,
    subscribe_request_filter_accounts_filter_memcmp::Data
  );
  assert_conversion_signatures_for_env_type!(
    JsSubscribeUpdateUpdateOneof,
    subscribe_update::UpdateOneof
  );
  assert_conversion_signatures_for_env_type!(JsConfirmedBlock, ConfirmedBlock);
  assert_conversion_signatures_for_env_type!(JsConfirmedTransaction, ConfirmedTransaction);
  assert_conversion_signatures_for_env_type!(JsTransaction, Transaction);
  assert_conversion_signatures_for_env_type!(JsMessage, Message);
  assert_conversion_signatures_for_env_type!(JsMessageAddressTableLookup, MessageAddressTableLookup);
  assert_conversion_signatures_for_env_type!(JsTransactionStatusMeta, TransactionStatusMeta);
  assert_conversion_signatures_for_env_type!(JsTransactionError, TransactionError);
  assert_conversion_signatures_for_env_type!(JsInnerInstructions, InnerInstructions);
  assert_conversion_signatures_for_env_type!(JsInnerInstruction, InnerInstruction);
  assert_conversion_signatures_for_env_type!(JsCompiledInstruction, CompiledInstruction);
  assert_conversion_signatures_for_env_type!(JsReturnData, ReturnData);
  assert_conversion_signatures_for_env_type!(JsSubscribeRequest, SubscribeRequest);
  assert_conversion_signatures_for_env_type!(
    JsSubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccounts
  );
  assert_conversion_signatures_for_env_type!(
    JsSubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilter
  );
  assert_conversion_signatures_for_env_type!(
    JsSubscribeRequestFilterAccountsFilterMemcmp,
    SubscribeRequestFilterAccountsFilterMemcmp
  );
  assert_conversion_signatures_for_env_type!(JsSubscribeUpdate, SubscribeUpdate);
  assert_conversion_signatures_for_env_type!(JsSubscribeUpdateAccount, SubscribeUpdateAccount);
  assert_conversion_signatures_for_env_type!(
    JsSubscribeUpdateAccountInfo,
    SubscribeUpdateAccountInfo
  );
  assert_conversion_signatures_for_env_type!(JsSubscribeUpdateTransaction, SubscribeUpdateTransaction);
  assert_conversion_signatures_for_env_type!(
    JsSubscribeUpdateTransactionInfo,
    SubscribeUpdateTransactionInfo
  );
  assert_conversion_signatures_for_env_type!(
    JsSubscribeUpdateTransactionStatus,
    SubscribeUpdateTransactionStatus
  );
  assert_conversion_signatures_for_env_type!(JsSubscribeUpdateBlock, SubscribeUpdateBlock);
  assert_conversion_signatures_for_env_type!(JsSubscribeUpdateEntry, SubscribeUpdateEntry);

  assert_conversion_signatures_for_non_env_type!(
    JsSubscribeRequestFilterAccountsFilterLamportsCmp,
    subscribe_request_filter_accounts_filter_lamports::Cmp
  );
  assert_conversion_signatures_for_non_env_type!(JsMessageHeader, MessageHeader);
  assert_conversion_signatures_for_non_env_type!(JsTokenBalance, TokenBalance);
  assert_conversion_signatures_for_non_env_type!(JsUiTokenAmount, UiTokenAmount);
  assert_conversion_signatures_for_non_env_type!(JsReward, Reward);
  assert_conversion_signatures_for_non_env_type!(JsRewards, Rewards);
  assert_conversion_signatures_for_non_env_type!(JsUnixTimestamp, UnixTimestamp);
  assert_conversion_signatures_for_non_env_type!(JsBlockHeight, BlockHeight);
  assert_conversion_signatures_for_non_env_type!(JsNumPartitions, NumPartitions);
  assert_conversion_signatures_for_non_env_type!(
    JsSubscribeRequestFilterAccountsFilterLamports,
    SubscribeRequestFilterAccountsFilterLamports
  );
  assert_conversion_signatures_for_non_env_type!(JsSubscribeRequestFilterSlots, SubscribeRequestFilterSlots);
  assert_conversion_signatures_for_non_env_type!(
    JsSubscribeRequestFilterTransactions,
    SubscribeRequestFilterTransactions
  );
  assert_conversion_signatures_for_non_env_type!(JsSubscribeRequestFilterBlocks, SubscribeRequestFilterBlocks);
  assert_conversion_signatures_for_non_env_type!(
    JsSubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterBlocksMeta
  );
  assert_conversion_signatures_for_non_env_type!(JsSubscribeRequestFilterEntry, SubscribeRequestFilterEntry);
  assert_conversion_signatures_for_non_env_type!(
    JsSubscribeRequestAccountsDataSlice,
    SubscribeRequestAccountsDataSlice
  );
  assert_conversion_signatures_for_non_env_type!(JsSubscribeRequestPing, SubscribeRequestPing);
  assert_conversion_signatures_for_non_env_type!(JsSubscribeUpdateSlot, SubscribeUpdateSlot);
  assert_conversion_signatures_for_non_env_type!(JsSubscribeUpdateBlockMeta, SubscribeUpdateBlockMeta);
  assert_conversion_signatures_for_non_env_type!(JsSubscribeUpdatePing, SubscribeUpdatePing);
  assert_conversion_signatures_for_non_env_type!(JsSubscribeUpdatePong, SubscribeUpdatePong);
  assert_conversion_signatures_for_non_env_type!(
    JsSubscribeReplayInfoRequest,
    SubscribeReplayInfoRequest
  );
  assert_conversion_signatures_for_non_env_type!(
    JsSubscribeReplayInfoResponse,
    SubscribeReplayInfoResponse
  );
  assert_conversion_signatures_for_non_env_type!(JsPingRequest, PingRequest);
  assert_conversion_signatures_for_non_env_type!(JsPongResponse, PongResponse);
  assert_conversion_signatures_for_non_env_type!(
    JsGetLatestBlockhashRequest,
    GetLatestBlockhashRequest
  );
  assert_conversion_signatures_for_non_env_type!(
    JsGetLatestBlockhashResponse,
    GetLatestBlockhashResponse
  );
  assert_conversion_signatures_for_non_env_type!(JsGetBlockHeightRequest, GetBlockHeightRequest);
  assert_conversion_signatures_for_non_env_type!(JsGetBlockHeightResponse, GetBlockHeightResponse);
  assert_conversion_signatures_for_non_env_type!(JsGetSlotRequest, GetSlotRequest);
  assert_conversion_signatures_for_non_env_type!(JsGetSlotResponse, GetSlotResponse);
  assert_conversion_signatures_for_non_env_type!(JsGetVersionRequest, GetVersionRequest);
  assert_conversion_signatures_for_non_env_type!(JsGetVersionResponse, GetVersionResponse);
  assert_conversion_signatures_for_non_env_type!(JsIsBlockhashValidRequest, IsBlockhashValidRequest);
  assert_conversion_signatures_for_non_env_type!(JsIsBlockhashValidResponse, IsBlockhashValidResponse);
}

#[test]
fn js_subscribe_request_accounts_data_slice_parses_valid_u64_strings_to_protobuf() {
  let js_accounts_data_slice_value = JsSubscribeRequestAccountsDataSlice {
    offset: "10".to_string(),
    length: "20".to_string(),
  };
  let protobuf_accounts_data_slice_value = js_accounts_data_slice_value
    .from_js_to_protobuf_type()
    .unwrap();
  assert_eq!(protobuf_accounts_data_slice_value.offset, 10);
  assert_eq!(protobuf_accounts_data_slice_value.length, 20);
}

#[test]
fn js_subscribe_request_accounts_data_slice_rejects_invalid_u64_strings() {
  let js_accounts_data_slice_value = JsSubscribeRequestAccountsDataSlice {
    offset: "not_a_number".to_string(),
    length: "20".to_string(),
  };
  let conversion_error = js_accounts_data_slice_value.from_js_to_protobuf_type().unwrap_err();
  let conversion_error_message = conversion_error.to_string();
  assert!(conversion_error_message.contains("Invalid u64 value"));
}

#[test]
fn js_get_latest_blockhash_response_parses_u64_strings_for_numeric_fields() {
  let js_get_latest_blockhash_response_value = JsGetLatestBlockhashResponse {
    slot: "101".to_string(),
    blockhash: "blockhash".to_string(),
    last_valid_block_height: "202".to_string(),
  };
  let protobuf_get_latest_blockhash_response_value = js_get_latest_blockhash_response_value
    .from_js_to_protobuf_type()
    .unwrap();
  assert_eq!(protobuf_get_latest_blockhash_response_value.slot, 101);
  assert_eq!(protobuf_get_latest_blockhash_response_value.last_valid_block_height, 202);
}

#[test]
fn js_unix_timestamp_rejects_invalid_i64_string() {
  let js_unix_timestamp_value = JsUnixTimestamp {
    timestamp: "not_i64".to_string(),
  };
  let conversion_error = js_unix_timestamp_value.from_js_to_protobuf_type().unwrap_err();
  let conversion_error_message = conversion_error.to_string();
  assert!(conversion_error_message.contains("Invalid i64 value"));
}

#[test]
fn js_reward_parses_i64_string_fields() {
  let js_reward_value = JsReward {
    pubkey: "pubkey".to_string(),
    lamports: "44".to_string(),
    post_balance: "55".to_string(),
    reward_type: 2,
    commission: "0".to_string(),
  };
  let protobuf_reward_value = js_reward_value.from_js_to_protobuf_type().unwrap();
  assert_eq!(protobuf_reward_value.lamports, 44);
  assert_eq!(protobuf_reward_value.post_balance, 55);
  assert_eq!(protobuf_reward_value.reward_type, 2);
}

#[test]
fn js_subscribe_request_filter_accounts_filter_lamports_cmp_accepts_single_variant() {
  let js_lamports_cmp_value = JsSubscribeRequestFilterAccountsFilterLamportsCmp {
    eq: Some("15".to_string()),
    ne: None,
    lt: None,
    gt: None,
  };
  let protobuf_lamports_cmp_value = js_lamports_cmp_value.from_js_to_protobuf_type().unwrap();
  assert!(matches!(
    protobuf_lamports_cmp_value,
    subscribe_request_filter_accounts_filter_lamports::Cmp::Eq(15)
  ));
}

#[test]
fn js_subscribe_request_filter_accounts_filter_lamports_cmp_rejects_multiple_variants() {
  let js_lamports_cmp_value = JsSubscribeRequestFilterAccountsFilterLamportsCmp {
    eq: Some("10".to_string()),
    ne: Some("20".to_string()),
    lt: None,
    gt: None,
  };
  let conversion_error = js_lamports_cmp_value.from_js_to_protobuf_type().unwrap_err();
  let conversion_error_message = conversion_error.to_string();
  assert!(conversion_error_message.contains("Multiple variants set"));
}

#[test]
fn js_subscribe_request_filter_accounts_filter_lamports_cmp_rejects_missing_variant() {
  let js_lamports_cmp_value = JsSubscribeRequestFilterAccountsFilterLamportsCmp {
    eq: None,
    ne: None,
    lt: None,
    gt: None,
  };
  let conversion_error = js_lamports_cmp_value.from_js_to_protobuf_type().unwrap_err();
  let conversion_error_message = conversion_error.to_string();
  assert!(conversion_error_message.contains("No variant set"));
}

#[test]
fn js_subscribe_request_filter_accounts_filter_filter_accepts_datasize_variant() {
  let js_filter_value: JsSubscribeRequestFilterAccountsFilterFilter<'static> =
    JsSubscribeRequestFilterAccountsFilterFilter {
      memcmp: None,
      datasize: Some("64".to_string()),
      token_account_state: None,
      lamports: None,
    };
  let protobuf_filter_value = js_filter_value.from_js_to_protobuf_type().unwrap();
  assert!(matches!(
    protobuf_filter_value,
    subscribe_request_filter_accounts_filter::Filter::Datasize(64)
  ));
}

#[test]
fn js_subscribe_request_filter_accounts_filter_filter_rejects_multiple_variants() {
  let js_filter_value: JsSubscribeRequestFilterAccountsFilterFilter<'static> =
    JsSubscribeRequestFilterAccountsFilterFilter {
      memcmp: None,
      datasize: Some("64".to_string()),
      token_account_state: Some(true),
      lamports: None,
    };
  let conversion_error = js_filter_value.from_js_to_protobuf_type().unwrap_err();
  let conversion_error_message = conversion_error.to_string();
  assert!(conversion_error_message.contains("Multiple variants set"));
}

#[test]
fn js_subscribe_request_filter_accounts_filter_memcmp_data_accepts_base58_variant() {
  let js_memcmp_data_value: JsSubscribeRequestFilterAccountsFilterMemcmpData<'static> =
    JsSubscribeRequestFilterAccountsFilterMemcmpData {
      bytes: None,
      base58: Some("base58_value".to_string()),
      base64: None,
    };
  let protobuf_memcmp_data_value = js_memcmp_data_value.from_js_to_protobuf_type().unwrap();
  assert!(matches!(
    protobuf_memcmp_data_value,
    subscribe_request_filter_accounts_filter_memcmp::Data::Base58(value) if value == "base58_value"
  ));
}

#[test]
fn js_subscribe_update_update_oneof_accepts_ping_variant() {
  let js_update_oneof_value: JsSubscribeUpdateUpdateOneof<'static> = JsSubscribeUpdateUpdateOneof {
    account: None,
    slot: None,
    transaction: None,
    transaction_status: None,
    block: None,
    ping: Some(JsSubscribeUpdatePing {}),
    pong: None,
    block_meta: None,
    entry: None,
  };
  let protobuf_update_oneof_value = js_update_oneof_value.from_js_to_protobuf_type().unwrap();
  assert!(matches!(
    protobuf_update_oneof_value,
    subscribe_update::UpdateOneof::Ping(_)
  ));
}

#[test]
fn js_subscribe_update_update_oneof_rejects_multiple_variants() {
  let js_update_oneof_value: JsSubscribeUpdateUpdateOneof<'static> = JsSubscribeUpdateUpdateOneof {
    account: None,
    slot: None,
    transaction: None,
    transaction_status: None,
    block: None,
    ping: Some(JsSubscribeUpdatePing {}),
    pong: Some(JsSubscribeUpdatePong { id: 1 }),
    block_meta: None,
    entry: None,
  };
  let conversion_error = js_update_oneof_value.from_js_to_protobuf_type().unwrap_err();
  let conversion_error_message = conversion_error.to_string();
  assert!(conversion_error_message.contains("Multiple variants set"));
}

#[test]
fn js_subscribe_request_hash_map_conversion_preserves_account_filter_keys() {
  let mut js_accounts_map: HashMap<String, JsSubscribeRequestFilterAccounts<'static>> =
    HashMap::new();
  js_accounts_map.insert(
    "primary".to_string(),
    JsSubscribeRequestFilterAccounts {
      account: vec!["account_value".to_string()],
      owner: vec![],
      filters: vec![],
      nonempty_txn_signature: None,
    },
  );

  let js_subscribe_request_value: JsSubscribeRequest<'static> = JsSubscribeRequest {
    accounts: js_accounts_map,
    slots: HashMap::new(),
    transactions: HashMap::new(),
    transactions_status: HashMap::new(),
    blocks: HashMap::new(),
    blocks_meta: HashMap::new(),
    entry: HashMap::new(),
    commitment: Some(1),
    accounts_data_slice: vec![JsSubscribeRequestAccountsDataSlice {
      offset: "0".to_string(),
      length: "10".to_string(),
    }],
    ping: Some(JsSubscribeRequestPing { id: 7 }),
    from_slot: Some("42".to_string()),
  };

  let protobuf_subscribe_request_value = js_subscribe_request_value.from_js_to_protobuf_type().unwrap();
  assert_eq!(protobuf_subscribe_request_value.accounts.len(), 1);
  assert!(protobuf_subscribe_request_value.accounts.contains_key("primary"));
  assert_eq!(
    protobuf_subscribe_request_value
      .accounts
      .get("primary")
      .unwrap()
      .account
      .len(),
    1
  );
  assert_eq!(protobuf_subscribe_request_value.commitment, Some(1));
  assert_eq!(protobuf_subscribe_request_value.accounts_data_slice.len(), 1);
  assert_eq!(protobuf_subscribe_request_value.ping.unwrap().id, 7);
  assert_eq!(protobuf_subscribe_request_value.from_slot, Some(42));
}

#[test]
fn js_subscribe_update_slot_parses_numeric_fields() {
  let js_subscribe_update_slot_value = JsSubscribeUpdateSlot {
    slot: "12".to_string(),
    parent: Some("9".to_string()),
    status: 2,
    dead_error: Some("dead".to_string()),
  };
  let protobuf_subscribe_update_slot_value = js_subscribe_update_slot_value
    .from_js_to_protobuf_type()
    .unwrap();
  assert_eq!(protobuf_subscribe_update_slot_value.slot, 12);
  assert_eq!(protobuf_subscribe_update_slot_value.parent, Some(9));
  assert_eq!(protobuf_subscribe_update_slot_value.status, 2);
  assert_eq!(
    protobuf_subscribe_update_slot_value.dead_error,
    Some("dead".to_string())
  );
}

#[test]
fn js_confirmed_block_from_js_to_protobuf_type_handles_empty_vectors() {
  let js_confirmed_block_value: JsConfirmedBlock<'static> = JsConfirmedBlock {
    previous_blockhash: "prev".to_string(),
    blockhash: "hash".to_string(),
    parent_slot: "9".to_string(),
    transactions: Vec::new(),
    rewards: Vec::new(),
    block_time: None,
    block_height: None,
    num_partitions: None,
  };
  let protobuf_confirmed_block_value = js_confirmed_block_value.from_js_to_protobuf_type().unwrap();
  assert_eq!(protobuf_confirmed_block_value.parent_slot, 9);
  assert!(protobuf_confirmed_block_value.transactions.is_empty());
  assert!(protobuf_confirmed_block_value.rewards.is_empty());
}

#[test]
fn js_transaction_status_meta_parses_u64_vector_fields() {
  let js_transaction_status_meta_value: JsTransactionStatusMeta<'static> = JsTransactionStatusMeta {
    err: None,
    fee: "10".to_string(),
    pre_balances: vec!["1".to_string(), "2".to_string()],
    post_balances: vec!["3".to_string()],
    inner_instructions: Vec::new(),
    inner_instructions_none: false,
    log_messages: vec!["log".to_string()],
    log_messages_none: false,
    pre_token_balances: Vec::new(),
    post_token_balances: Vec::new(),
    rewards: Vec::new(),
    loaded_writable_addresses: Vec::new(),
    loaded_readonly_addresses: Vec::new(),
    return_data: None,
    return_data_none: true,
    compute_units_consumed: Some("5".to_string()),
    cost_units: None,
  };
  let protobuf_transaction_status_meta_value = js_transaction_status_meta_value
    .from_js_to_protobuf_type()
    .unwrap();
  assert_eq!(protobuf_transaction_status_meta_value.fee, 10);
  assert_eq!(protobuf_transaction_status_meta_value.pre_balances, vec![1, 2]);
  assert_eq!(protobuf_transaction_status_meta_value.post_balances, vec![3]);
  assert_eq!(protobuf_transaction_status_meta_value.compute_units_consumed, Some(5));
}
