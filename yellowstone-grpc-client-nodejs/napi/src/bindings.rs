use napi_derive::napi;
use serde::Deserialize;
use std::collections::HashMap;

//--------------------------------------------------------------------------------
// Bindings.
//--------------------------------------------------------------------------------

#[napi]
#[derive(Deserialize, Debug, Clone)]
pub enum JsCompressionAlgorithm {
  Gzip,
  Zstd,
}

/// ChannelOptions from JS.
///
/// Struct to hold the channel options configuration
/// passed from JS.
///
/// Note:
/// The type u32 is being used because of napi's built-in
/// support for u32 to number (JS) conversion.
///
#[napi(object)]
#[derive(Deserialize, Debug, Clone)]
pub struct JsChannelOptions {
  //--------------------
  // Configs.
  //--------------------
  pub grpc_connect_timeout: Option<u32>,
  pub grpc_buffer_size: Option<u32>,
  pub grpc_http2_keep_alive_interval: Option<u32>,
  pub grpc_initial_connection_window_size: Option<u32>,
  pub grpc_initial_stream_window_size: Option<u32>,
  pub grpc_keep_alive_timeout: Option<u32>,
  pub grpc_tcp_keepalive: Option<u32>,
  pub grpc_timeout: Option<u32>,
  pub grpc_max_decoding_message_size: Option<u32>,
  pub grpc_max_encoding_message_size: Option<u32>,
  pub grpc_default_compression_algorithm: Option<JsCompressionAlgorithm>,
  //--------------------
  // Flags.
  //--------------------
  pub grpc_set_x_request_snapshot: Option<bool>,
  pub grpc_http2_adaptive_window: Option<bool>,
  pub grpc_keep_alive_while_idle: Option<bool>,
  pub grpc_tcp_nodelay: Option<bool>,
}

impl Default for JsChannelOptions {
  /// Sets the following configuration to
  /// sensible defaults.
  ///
  /// ```rust
  /// JsChannelOptions {
  ///   grpc_connect_timeout: Duration::from_secs(10),
  ///   grpc_timeout: Some(Duration::from_secs(30)),
  ///   grpc_initial_connection_window_size: Some(8 * 1024 * 1024), // 8MB
  ///   grpc_initial_stream_window_size: Some(4 * 1024 * 1024),     // 4MB
  ///   grpc_max_decoding_message_size: Some(1 * 1024 * 1024 * 1024), // 1GB
  ///   grpc_max_encoding_message_size: Some(32 * 1024 * 1024),     // 32MB
  ///   grpc_http2_adaptive_window: Some(true),
  ///   grpc_tcp_nodelay: Some(true),
  ///   grpc_keep_alive_while_idle: None,
  ///   grpc_buffer_size: None,
  ///   grpc_http2_keep_alive_interval: None,
  ///   grpc_keep_alive_timeout: None,
  ///   grpc_tcp_keepalive: None,
  ///   grpc_default_compression_algorithm: None,
  ///   grpc_set_x_request_snapshot: None,
  /// }
  /// ```
  fn default() -> Self {
    Self {
      grpc_connect_timeout: Some(10_000),
      grpc_timeout: Some(30_000),
      grpc_initial_connection_window_size: Some(8 * 1024 * 1024), // 8MB
      grpc_initial_stream_window_size: Some(4 * 1024 * 1024),     // 4MB
      grpc_max_decoding_message_size: Some(1024 * 1024 * 1024),   // 1GB
      grpc_max_encoding_message_size: Some(32 * 1024 * 1024),     // 32MB
      grpc_http2_adaptive_window: Some(true),
      grpc_tcp_nodelay: Some(true),
      grpc_keep_alive_while_idle: None,
      grpc_buffer_size: None,
      grpc_http2_keep_alive_interval: None,
      grpc_keep_alive_timeout: None,
      grpc_tcp_keepalive: None,
      grpc_default_compression_algorithm: None,
      grpc_set_x_request_snapshot: None,
    }
  }
}

// Complete serde-based structures matching yellowstone-grpc proto exactly
#[derive(Deserialize, Debug)]
pub struct JsSubscribeRequest {
  pub accounts: Option<HashMap<String, JsAccountFilter>>,
  pub slots: Option<HashMap<String, JsSlotFilter>>,
  pub transactions: Option<HashMap<String, JsTransactionFilter>>,
  #[serde(alias = "transactionsStatus")]
  pub transactions_status: Option<HashMap<String, JsTransactionFilter>>,
  pub blocks: Option<HashMap<String, JsBlockFilter>>,
  #[serde(alias = "blocksMeta")]
  pub blocks_meta: Option<HashMap<String, JsBlockMetaFilter>>,
  pub entry: Option<HashMap<String, JsEntryFilter>>,
  pub commitment: Option<i32>,
  #[serde(alias = "accountsDataSlice")]
  pub accounts_data_slice: Option<Vec<JsAccountsDataSlice>>,
  pub ping: Option<JsPing>,
  #[serde(alias = "fromSlot")]
  pub from_slot: Option<u64>,
}

#[derive(Deserialize, Debug)]
pub struct JsAccountFilter {
  pub account: Option<Vec<String>>,
  pub owner: Option<Vec<String>>,
  pub filters: Option<Vec<JsAccountsFilter>>,
  #[serde(alias = "nonemptyTxnSignature")]
  pub nonempty_txn_signature: Option<bool>,
  // Add aliases for consistent interface matching transactions
  #[serde(alias = "accountInclude")]
  pub account_include: Option<Vec<String>>,
  #[serde(alias = "accountExclude")]
  pub account_exclude: Option<Vec<String>>,
  #[serde(alias = "accountRequired")]
  pub account_required: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct JsAccountsFilter {
  pub memcmp: Option<JsMemcmpFilter>,
  pub datasize: Option<u64>,
  #[serde(alias = "tokenAccountState")]
  pub token_account_state: Option<bool>,
  pub lamports: Option<JsLamportsFilter>,
}

#[derive(Deserialize, Debug)]
pub struct JsMemcmpFilter {
  pub offset: u64,
  pub bytes: Option<String>, // base64 encoded
  pub base58: Option<String>,
  pub base64: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct JsLamportsFilter {
  pub eq: Option<u64>,
  pub ne: Option<u64>,
  pub lt: Option<u64>,
  pub gt: Option<u64>,
}

#[derive(Deserialize, Debug)]
pub struct JsSlotFilter {
  #[serde(alias = "filterByCommitment")]
  pub filter_by_commitment: Option<bool>,
  #[serde(alias = "interslotUpdates")]
  pub interslot_updates: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct JsTransactionFilter {
  pub vote: Option<bool>,
  pub failed: Option<bool>,
  pub signature: Option<String>,
  #[serde(alias = "accountInclude")]
  pub account_include: Option<Vec<String>>,
  #[serde(alias = "accountExclude")]
  pub account_exclude: Option<Vec<String>>,
  #[serde(alias = "accountRequired")]
  pub account_required: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct JsBlockFilter {
  #[serde(alias = "accountInclude")]
  pub account_include: Option<Vec<String>>,
  #[serde(alias = "includeTransactions")]
  pub include_transactions: Option<bool>,
  #[serde(alias = "includeAccounts")]
  pub include_accounts: Option<bool>,
  #[serde(alias = "includeEntries")]
  pub include_entries: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct JsBlockMetaFilter {
  // Empty struct as per proto
}

#[derive(Deserialize, Debug)]
pub struct JsEntryFilter {
  // Empty struct as per proto
}

#[derive(Deserialize, Debug)]
pub struct JsAccountsDataSlice {
  pub offset: u64,
  pub length: u64,
}

#[derive(Deserialize, Debug)]
pub struct JsPing {
  pub id: i32,
}

// GetLatestBlockhash types
#[napi(object)]
#[derive(Deserialize, Debug)]
pub struct JsGetLatestBlockhashRequest {
  pub commitment: Option<i32>,
}

#[napi(object)]
#[derive(Debug)]
pub struct JsGetLatestBlockhashResponse {
  pub slot: String,
  pub blockhash: String,
  pub last_valid_block_height: String, // u64 as string for JS compatibility
}

// Ping types
#[napi(object)]
#[derive(Deserialize, Debug)]
pub struct JsPingRequest {
  pub count: i32,
}

#[napi(object)]
#[derive(Debug)]
pub struct JsPongResponse {
  pub count: i32,
}

// GetBlockHeight Types
#[napi(object)]
#[derive(Deserialize, Debug)]
pub struct JsGetBlockHeightRequest {
  pub commitment: Option<i32>,
}

#[napi(object)]
#[derive(Deserialize, Debug)]
pub struct JsGetBlockHeightResponse {
  pub block_height: String, // u64 as string for JS compatibility
}

// GetSlot types
#[napi(object)]
#[derive(Deserialize, Debug)]
pub struct JsGetSlotRequest {
  pub commitment: Option<i32>,
}

#[napi(object)]
#[derive(Debug)]
pub struct JsGetSlotResponse {
  pub slot: String, // u64 as string for JS compatibility
}

// IsBlockhashValid types
#[napi(object)]
#[derive(Deserialize, Debug)]
pub struct JsIsBlockhashValidRequest {
  pub blockhash: String,
  pub commitment: Option<i32>,
}

#[napi(object)]
#[derive(Debug)]
pub struct JsIsBlockhashValidResponse {
  pub slot: String,
  pub valid: bool,
}

// GetVersion types
#[napi(object)]
#[derive(Deserialize, Debug)]
pub struct JsGetVersionRequest {
  // Empty struct - no fields needed since the gRPC method takes no parameters
}

#[napi(object)]
#[derive(Debug)]
pub struct JsGetVersionResponse {
  pub version: String,
}
