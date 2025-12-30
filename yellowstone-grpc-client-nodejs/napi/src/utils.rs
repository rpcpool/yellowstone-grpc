use crate::bindings::{
  JsChannelOptions, JsCompressionAlgorithm, JsGetBlockHeightResponse, JsGetSlotResponse,
  JsGetVersionResponse, JsIsBlockhashValidResponse, JsSubscribeRequest,
};
use base64::{engine::general_purpose, Engine as _};
use napi::bindgen_prelude::*;
use prost_types::Timestamp;
use std::collections::HashMap;
use std::time::Duration;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder};
use yellowstone_grpc_proto::{
  geyser::subscribe_update::UpdateOneof,
  prelude::{Transaction, *},
  tonic::codec::CompressionEncoding,
};

//--------------------------------------------------------------------------------
// Utils
//--------------------------------------------------------------------------------

pub async fn get_client_builder(
  endpoint: String,
  x_token: Option<String>,
  channel_options: Option<JsChannelOptions>,
) -> Result<GeyserGrpcBuilder> {
  // Endpoint.
  let mut builder = match GeyserGrpcBuilder::from_shared(endpoint.clone()) {
    Ok(b) => b,
    Err(e) => return Err(napi::Error::new(Status::InvalidArg, e)),
  };

  // Credentials
  builder = match builder.x_token(x_token) {
    Ok(b) => b,
    Err(e) => return Err(napi::Error::new(Status::InvalidArg, e)),
  };

  // TLS
  if endpoint.starts_with("https://") {
    builder = match builder.tls_config(ClientTlsConfig::new().with_enabled_roots()) {
      Ok(b) => b,
      Err(e) => return Err(napi::Error::new(Status::InvalidArg, e)),
    };
  }

  // Process channel options.
  //
  // Apply sensible defaults.
  let default_opts = JsChannelOptions::default();
  builder = builder
    .connect_timeout(Duration::from_millis(
      default_opts
        .grpc_connect_timeout
        .expect("connect_timeout is defined in Default")
        .into(),
    ))
    .timeout(Duration::from_millis(
      default_opts
        .grpc_timeout
        .expect("timeout is defined in Default")
        .into(),
    ))
    .initial_stream_window_size(default_opts.grpc_initial_stream_window_size)
    .initial_connection_window_size(default_opts.grpc_initial_connection_window_size)
    .max_decoding_message_size(
      default_opts
        .grpc_max_decoding_message_size
        .expect("max_decoding_message_size is defined in Default") as usize,
    )
    .max_encoding_message_size(
      default_opts
        .grpc_max_encoding_message_size
        .expect("max_encoding_message_size is defined in Default") as usize,
    )
    .http2_adaptive_window(
      default_opts
        .grpc_http2_adaptive_window
        .expect("grpc_http2_adaptive_window is defined in Default"),
    )
    .tcp_nodelay(
      default_opts
        .grpc_tcp_nodelay
        .expect("grpc_tcp_nodelay is defined in Default"),
    );

  if let Some(opts) = channel_options {
    if let Some(timeout) = opts.grpc_connect_timeout {
      builder = builder.connect_timeout(Duration::from_millis(timeout as u64));
    }
    if let Some(size) = opts.grpc_buffer_size {
      builder = builder.buffer_size(size as usize);
    }
    if let Some(interval) = opts.grpc_http2_keep_alive_interval {
      builder = builder.http2_keep_alive_interval(Duration::from_millis(interval as u64));
    }
    if let Some(size) = opts.grpc_initial_connection_window_size {
      builder = builder.initial_connection_window_size(size);
    }
    if let Some(size) = opts.grpc_initial_stream_window_size {
      builder = builder.initial_stream_window_size(size);
    }
    if let Some(timeout) = opts.grpc_keep_alive_timeout {
      builder = builder.keep_alive_timeout(Duration::from_millis(timeout as u64));
    }
    if let Some(time) = opts.grpc_tcp_keepalive {
      builder = builder.tcp_keepalive(Some(Duration::from_millis(time as u64)));
    }
    if let Some(timeout) = opts.grpc_timeout {
      builder = builder.timeout(Duration::from_millis(timeout as u64));
    }
    if let Some(size) = opts.grpc_max_decoding_message_size {
      builder = builder.max_decoding_message_size(size as usize);
    }
    if let Some(size) = opts.grpc_max_encoding_message_size {
      builder = builder.max_encoding_message_size(size as usize);
    }
    if let Some(request_snapshot) = opts.grpc_set_x_request_snapshot {
      builder = builder.set_x_request_snapshot(request_snapshot);
    }
    if let Some(adaptive) = opts.grpc_http2_adaptive_window {
      builder = builder.http2_adaptive_window(adaptive);
    }
    if let Some(idle) = opts.grpc_keep_alive_while_idle {
      builder = builder.keep_alive_while_idle(idle);
    }
    if let Some(nodelay) = opts.grpc_tcp_nodelay {
      builder = builder.tcp_nodelay(nodelay);
    }
    if let Some(compression) = opts.grpc_default_compression_algorithm {
      let encoding = match compression {
        JsCompressionAlgorithm::Gzip => CompressionEncoding::Gzip,
        JsCompressionAlgorithm::Zstd => CompressionEncoding::Zstd,
      };

      builder = builder
        .send_compressed(encoding)
        .accept_compressed(encoding);
    }
  }

  Ok(builder)
}

/// Converts a protobuf GetLatestBlockhashResponse to JavaScript-compatible format.
pub fn get_latest_blockhash_response_to_js(
  pb_resp: yellowstone_grpc_proto::geyser::GetLatestBlockhashResponse,
) -> crate::bindings::JsGetLatestBlockhashResponse {
  crate::bindings::JsGetLatestBlockhashResponse {
    blockhash: pb_resp.blockhash,
    last_valid_block_height: pb_resp.last_valid_block_height.to_string(),
  }
}

/// Converts a protobuf PongResponse to JavaScript-compatible format.
pub fn pong_response_to_js(
  pb_resp: yellowstone_grpc_proto::geyser::PongResponse,
) -> crate::bindings::JsPongResponse {
  crate::bindings::JsPongResponse {
    count: pb_resp.count,
  }
}

pub fn get_block_height_response_to_js(
  pb_resp: GetBlockHeightResponse,
) -> JsGetBlockHeightResponse {
  JsGetBlockHeightResponse {
    block_height: pb_resp.block_height.to_string(),
  }
}

/// Converts a protobuf GetSlotResponse to JavaScript-compatible format.
pub fn get_slot_response_to_js(
  pb_resp: yellowstone_grpc_proto::geyser::GetSlotResponse,
) -> JsGetSlotResponse {
  JsGetSlotResponse {
    slot: pb_resp.slot.to_string(),
  }
}

/// Converts a protobuf IsBlockhashValidResponse to JavaScript-compatible format.
pub fn is_blockhash_valid_response_to_js(
  pb_resp: yellowstone_grpc_proto::geyser::IsBlockhashValidResponse,
) -> JsIsBlockhashValidResponse {
  JsIsBlockhashValidResponse {
    valid: pb_resp.valid,
  }
}

/// Converts a protobuf GetVersionResponse to JavaScript-compatible format.
pub fn get_version_response_to_js(
  pb_resp: yellowstone_grpc_proto::geyser::GetVersionResponse,
) -> JsGetVersionResponse {
  JsGetVersionResponse {
    version: pb_resp.version,
  }
}

pub fn js_to_subscribe_request(env: &Env, js_obj: Object) -> Result<SubscribeRequest> {
  let js_request: JsSubscribeRequest = env.from_js_value(js_obj)?;

  let mut request = SubscribeRequest::default();

  // Handle accounts with complete filter support
  if let Some(accounts) = js_request.accounts {
    let mut accounts_map = HashMap::new();
    for (key, filter) in accounts {
      let mut yellowstone_filter = SubscribeRequestFilterAccounts::default();

      // Handle account field (legacy interface)
      if let Some(account_list) = filter.account {
        yellowstone_filter.account = account_list;
      }

      // Handle accountInclude field (consistent interface)
      if let Some(account_include_list) = filter.account_include {
        yellowstone_filter.account = account_include_list;
      }

      if let Some(owner_list) = filter.owner {
        yellowstone_filter.owner = owner_list;
      }

      // Handle accountExclude - NOT directly supported by Yellowstone accounts filter
      // This would need to be implemented via complex filters, which is beyond scope
      if let Some(_account_exclude_list) = filter.account_exclude {
        // accountExclude not directly supported for account subscriptions
      }

      // Handle accountRequired - NOT directly supported by Yellowstone accounts filter
      if let Some(_account_required_list) = filter.account_required {
        // accountRequired not directly supported for account subscriptions
      }

      if let Some(nonempty_txn_signature) = filter.nonempty_txn_signature {
        yellowstone_filter.nonempty_txn_signature = Some(nonempty_txn_signature);
      }

      // Handle complete filters
      if let Some(filters) = filter.filters {
        let mut yellowstone_filters = Vec::new();
        for js_filter in filters {
          let mut yellowstone_accounts_filter = SubscribeRequestFilterAccountsFilter::default();

          if let Some(memcmp) = js_filter.memcmp {
            let mut memcmp_filter = SubscribeRequestFilterAccountsFilterMemcmp {
              offset: memcmp.offset,
              data: None,
            };

            if let Some(bytes_str) = memcmp.bytes {
              let bytes_data = general_purpose::STANDARD.decode(&bytes_str).map_err(|e| {
                napi::Error::new(Status::InvalidArg, format!("Invalid base64 bytes: {}", e))
              })?;
              memcmp_filter.data =
                Some(subscribe_request_filter_accounts_filter_memcmp::Data::Bytes(bytes_data));
            } else if let Some(base58_str) = memcmp.base58 {
              memcmp_filter.data =
                Some(subscribe_request_filter_accounts_filter_memcmp::Data::Base58(base58_str));
            } else if let Some(base64_str) = memcmp.base64 {
              memcmp_filter.data =
                Some(subscribe_request_filter_accounts_filter_memcmp::Data::Base64(base64_str));
            }

            yellowstone_accounts_filter.filter = Some(
              subscribe_request_filter_accounts_filter::Filter::Memcmp(memcmp_filter),
            );
          }

          if let Some(datasize) = js_filter.datasize {
            yellowstone_accounts_filter.filter = Some(
              subscribe_request_filter_accounts_filter::Filter::Datasize(datasize),
            );
          }

          if let Some(token_account_state) = js_filter.token_account_state {
            yellowstone_accounts_filter.filter = Some(
              subscribe_request_filter_accounts_filter::Filter::TokenAccountState(
                token_account_state,
              ),
            );
          }

          if let Some(lamports) = js_filter.lamports {
            let mut lamports_filter = SubscribeRequestFilterAccountsFilterLamports::default();

            if let Some(eq) = lamports.eq {
              lamports_filter.cmp = Some(
                subscribe_request_filter_accounts_filter_lamports::Cmp::Eq(eq),
              );
            } else if let Some(ne) = lamports.ne {
              lamports_filter.cmp = Some(
                subscribe_request_filter_accounts_filter_lamports::Cmp::Ne(ne),
              );
            } else if let Some(lt) = lamports.lt {
              lamports_filter.cmp = Some(
                subscribe_request_filter_accounts_filter_lamports::Cmp::Lt(lt),
              );
            } else if let Some(gt) = lamports.gt {
              lamports_filter.cmp = Some(
                subscribe_request_filter_accounts_filter_lamports::Cmp::Gt(gt),
              );
            }

            yellowstone_accounts_filter.filter = Some(
              subscribe_request_filter_accounts_filter::Filter::Lamports(lamports_filter),
            );
          }

          yellowstone_filters.push(yellowstone_accounts_filter);
        }
        yellowstone_filter.filters = yellowstone_filters;
      }

      accounts_map.insert(key, yellowstone_filter);
    }
    request.accounts = accounts_map;
  }

  // Handle slots with complete filter support
  if let Some(slots) = js_request.slots {
    let mut slots_map = HashMap::new();
    for (key, filter) in slots {
      let mut yellowstone_filter = SubscribeRequestFilterSlots::default();

      if let Some(filter_by_commitment) = filter.filter_by_commitment {
        yellowstone_filter.filter_by_commitment = Some(filter_by_commitment);
      }

      if let Some(interslot_updates) = filter.interslot_updates {
        yellowstone_filter.interslot_updates = Some(interslot_updates);
      }

      slots_map.insert(key, yellowstone_filter);
    }
    request.slots = slots_map;
  }

  // Handle transactions with complete filter support
  if let Some(transactions) = js_request.transactions {
    let mut transactions_map = HashMap::new();
    for (key, filter) in transactions {
      let mut yellowstone_filter = SubscribeRequestFilterTransactions::default();

      yellowstone_filter.vote = filter.vote;
      yellowstone_filter.failed = filter.failed;
      yellowstone_filter.signature = filter.signature;

      if let Some(account_include) = filter.account_include {
        yellowstone_filter.account_include = account_include;
      }

      if let Some(account_exclude) = filter.account_exclude {
        yellowstone_filter.account_exclude = account_exclude;
      }

      if let Some(account_required) = filter.account_required {
        yellowstone_filter.account_required = account_required;
      }

      transactions_map.insert(key, yellowstone_filter);
    }
    request.transactions = transactions_map;
  }

  // Handle transactions_status with complete filter support
  if let Some(transactions_status) = js_request.transactions_status {
    let mut transactions_status_map = HashMap::new();
    for (key, filter) in transactions_status {
      let mut yellowstone_filter = SubscribeRequestFilterTransactions::default();

      yellowstone_filter.vote = filter.vote;
      yellowstone_filter.failed = filter.failed;
      yellowstone_filter.signature = filter.signature;

      if let Some(account_include) = filter.account_include {
        yellowstone_filter.account_include = account_include;
      }

      if let Some(account_exclude) = filter.account_exclude {
        yellowstone_filter.account_exclude = account_exclude;
      }

      if let Some(account_required) = filter.account_required {
        yellowstone_filter.account_required = account_required;
      }

      transactions_status_map.insert(key, yellowstone_filter);
    }
    request.transactions_status = transactions_status_map;
  }

  // Handle blocks with complete filter support
  if let Some(blocks) = js_request.blocks {
    let mut blocks_map = HashMap::new();
    for (key, filter) in blocks {
      let mut yellowstone_filter = SubscribeRequestFilterBlocks::default();

      if let Some(account_include) = filter.account_include {
        yellowstone_filter.account_include = account_include;
      }

      yellowstone_filter.include_transactions = filter.include_transactions;
      yellowstone_filter.include_accounts = filter.include_accounts;
      yellowstone_filter.include_entries = filter.include_entries;

      blocks_map.insert(key, yellowstone_filter);
    }
    request.blocks = blocks_map;
  }

  // Handle blocks_meta
  if let Some(blocks_meta) = js_request.blocks_meta {
    let mut blocks_meta_map = HashMap::new();
    for (key, _filter) in blocks_meta {
      blocks_meta_map.insert(key, SubscribeRequestFilterBlocksMeta::default());
    }
    request.blocks_meta = blocks_meta_map;
  }

  // Handle entry
  if let Some(entry) = js_request.entry {
    let mut entry_map = HashMap::new();
    for (key, _filter) in entry {
      entry_map.insert(key, SubscribeRequestFilterEntry::default());
    }
    request.entry = entry_map;
  }

  // Handle commitment
  request.commitment = js_request.commitment;

  // Handle accounts_data_slice
  if let Some(accounts_data_slice) = js_request.accounts_data_slice {
    let mut yellowstone_slices = Vec::new();
    for slice in accounts_data_slice {
      yellowstone_slices.push(SubscribeRequestAccountsDataSlice {
        offset: slice.offset,
        length: slice.length,
      });
    }
    request.accounts_data_slice = yellowstone_slices;
  }

  // Handle ping
  if let Some(ping) = js_request.ping {
    request.ping = Some(SubscribeRequestPing { id: ping.id });
  }

  // Handle from_slot
  request.from_slot = js_request.from_slot;

  Ok(request)
}

// Leaving for future use.
#[allow(unused)]
fn bytes_to_js_ui_pubkey(bytes: Vec<u8>) -> String {
  bs58::encode(bytes).into_string()
}

fn bytes_to_js(bytes: Vec<u8>) -> Buffer {
  Buffer::from(bytes)
}

// Helper for Timestamp to Date
fn timestamp_to_js_date(env: &Env, ts: Timestamp) -> Result<Date> {
  let millis = (ts.seconds * 1000) as f64 + (ts.nanos as f64 / 1_000_000.0);
  env.create_date(millis)
}

// Convert SubscribeUpdateAccount to Object
fn to_js_account(env: &Env, acc: SubscribeUpdateAccount) -> Result<Object> {
  let mut obj = Object::new(env)?;
  if let Some(info) = acc.account {
    let info_obj = to_js_account_info(env, info)?;
    obj.set("account", info_obj)?;
  }
  obj.set("slot", acc.slot.to_string())?;
  obj.set("isStartup", acc.is_startup)?;
  Ok(obj)
}

// Convert SubscribeUpdateSlot to Object
fn to_js_slot(env: &Env, slot: SubscribeUpdateSlot) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("slot", slot.slot.to_string())?;
  if let Some(parent) = slot.parent {
    obj.set("parent", parent.to_string())?;
  }
  obj.set("status", slot.status() as i32)?;
  if let Some(dead_error) = slot.dead_error {
    obj.set("dead_error", dead_error)?;
  }
  Ok(obj)
}

// Convert SubscribeUpdateTransaction to Object
fn to_js_transaction(env: &Env, tx: SubscribeUpdateTransaction) -> Result<Object> {
  let mut obj = Object::new(env)?;
  if let Some(info) = tx.transaction {
    obj.set("transaction", to_js_transaction_info(env, info)?)?;
  }
  obj.set("slot", tx.slot.to_string())?;
  Ok(obj)
}

fn to_js_transaction_info(env: &Env, info: SubscribeUpdateTransactionInfo) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("signature", bytes_to_js(info.signature))?;
  obj.set("isVote", info.is_vote)?;
  if let Some(tx) = info.transaction {
    obj.set("transaction", to_js_sol_transaction(env, tx)?)?;
  }
  if let Some(meta) = info.meta {
    obj.set("meta", to_js_transaction_status_meta(env, meta)?)?;
  }
  obj.set("index", info.index.to_string())?;
  Ok(obj)
}

fn to_js_sol_transaction(env: &Env, tx: Transaction) -> Result<Object> {
  let mut obj = Object::new(env)?;
  let sigs_len = tx.signatures.len() as u32;
  let mut sigs_arr = env.create_array(sigs_len)?;
  for (i, sig) in tx.signatures.into_iter().enumerate() {
    sigs_arr.set(i as u32, bytes_to_js(sig))?;
  }
  obj.set("signatures", sigs_arr)?;
  if let Some(msg) = tx.message {
    obj.set("message", to_js_message(env, msg)?)?;
  }
  // TODO: Message hash from proto.
  // obj.set("message_hash", ???)?)
  Ok(obj)
}

fn to_js_message(env: &Env, msg: Message) -> Result<Object> {
  let mut obj = Object::new(env)?;
  if let Some(header) = msg.header {
    obj.set("header", to_js_message_header(env, header)?)?;
  }
  let accts_len = msg.account_keys.len() as u32;
  let mut accts_arr = env.create_array(accts_len)?;
  for (i, acct) in msg.account_keys.into_iter().enumerate() {
    accts_arr.set(i as u32, bytes_to_js(acct))?;
  }
  obj.set("accountKeys", accts_arr)?;
  obj.set("recentBlockhash", bytes_to_js(msg.recent_blockhash))?;
  let insts_len = msg.instructions.len() as u32;
  let mut insts_arr = env.create_array(insts_len)?;
  for (i, inst) in msg.instructions.into_iter().enumerate() {
    insts_arr.set(i as u32, to_js_compiled_instruction(env, inst)?)?;
  }
  obj.set("instructions", insts_arr)?;
  let lookups_len = msg.address_table_lookups.len() as u32;
  let mut lookups_arr = env.create_array(lookups_len)?;
  for (i, lookup) in msg.address_table_lookups.into_iter().enumerate() {
    lookups_arr.set(i as u32, to_js_address_table_lookup(env, lookup)?)?;
  }
  obj.set("addressTableLookups", lookups_arr)?;
  obj.set("versioned", msg.versioned)?;
  Ok(obj)
}

fn to_js_address_table_lookup(env: &Env, lookup: MessageAddressTableLookup) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("accountKey", bytes_to_js(lookup.account_key))?;
  let writable_len = lookup.writable_indexes.len() as u32;
  let mut writable_arr = env.create_array(writable_len)?;
  for (i, idx) in lookup.writable_indexes.into_iter().enumerate() {
    writable_arr.set(i as u32, idx as u32)?;
  }
  obj.set("writableIndexes", writable_arr)?;
  let readonly_len = lookup.readonly_indexes.len() as u32;
  let mut readonly_arr = env.create_array(readonly_len)?;
  for (i, idx) in lookup.readonly_indexes.into_iter().enumerate() {
    readonly_arr.set(i as u32, idx as u32)?;
  }
  obj.set("readonlyIndexes", readonly_arr)?;
  Ok(obj)
}

fn to_js_message_header(env: &Env, header: MessageHeader) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set(
    "numRequiredSignatures",
    header.num_required_signatures as u32,
  )?;
  obj.set(
    "numReadonlySignedAccounts",
    header.num_readonly_signed_accounts as u32,
  )?;
  obj.set(
    "numReadonlyUnsignedAccounts",
    header.num_readonly_unsigned_accounts as u32,
  )?;
  Ok(obj)
}

fn to_js_compiled_instruction(env: &Env, inst: CompiledInstruction) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("programIdIndex", inst.program_id_index as u32)?;
  obj.set("accounts", bytes_to_js(inst.accounts))?;
  obj.set("data", bytes_to_js(inst.data))?;
  Ok(obj)
}

fn to_js_transaction_status_meta(env: &Env, meta: TransactionStatusMeta) -> Result<Object> {
  let mut obj = Object::new(env)?;
  if let Some(err) = meta.err {
    obj.set("err", to_js_transaction_error(env, err)?)?;
  }
  obj.set("fee", meta.fee.to_string())?;
  let pre_bal_len = meta.pre_balances.len() as u32;
  let mut pre_bal_arr = env.create_array(pre_bal_len)?;
  for (i, bal) in meta.pre_balances.into_iter().enumerate() {
    pre_bal_arr.set(i as u32, bal.to_string())?;
  }
  obj.set("preBalances", pre_bal_arr)?;
  let post_bal_len = meta.post_balances.len() as u32;
  let mut post_bal_arr = env.create_array(post_bal_len)?;
  for (i, bal) in meta.post_balances.into_iter().enumerate() {
    post_bal_arr.set(i as u32, bal.to_string())?;
  }
  obj.set("postBalances", post_bal_arr)?;
  let inner_len = meta.inner_instructions.len() as u32;
  let mut inner_arr = env.create_array(inner_len)?;
  for (i, ii) in meta.inner_instructions.into_iter().enumerate() {
    inner_arr.set(i as u32, to_js_inner_instructions(env, ii)?)?;
  }
  obj.set("innerInstructions", inner_arr)?;
  obj.set("innerInstructionsNone", meta.inner_instructions_none)?;
  let logs_len = meta.log_messages.len() as u32;
  let mut logs_arr = env.create_array(logs_len)?;
  for (i, log) in meta.log_messages.into_iter().enumerate() {
    logs_arr.set(i as u32, log)?;
  }
  obj.set("logMessages", logs_arr)?;
  obj.set("logMessagesNone", meta.log_messages_none)?;
  let pre_tb_len = meta.pre_token_balances.len() as u32;
  let mut pre_tb_arr = env.create_array(pre_tb_len)?;
  for (i, tb) in meta.pre_token_balances.into_iter().enumerate() {
    pre_tb_arr.set(i as u32, to_js_token_balance(env, tb)?)?;
  }
  obj.set("preTokenBalances", pre_tb_arr)?;
  let post_tb_len = meta.post_token_balances.len() as u32;
  let mut post_tb_arr = env.create_array(post_tb_len)?;
  for (i, tb) in meta.post_token_balances.into_iter().enumerate() {
    post_tb_arr.set(i as u32, to_js_token_balance(env, tb)?)?;
  }
  obj.set("postTokenBalances", post_tb_arr)?;
  let rewards_len = meta.rewards.len() as u32;
  let mut rewards_arr = env.create_array(rewards_len)?;
  for (i, r) in meta.rewards.into_iter().enumerate() {
    rewards_arr.set(i as u32, to_js_reward(env, r)?)?;
  }
  obj.set("rewards", rewards_arr)?;

  let writable_len = meta.loaded_writable_addresses.len() as u32;
  let mut writable_arr = env.create_array(writable_len)?;
  for (i, addr) in meta.loaded_writable_addresses.into_iter().enumerate() {
    writable_arr.set(i as u32, bytes_to_js(addr))?;
  }
  obj.set("loadedWritableAddresses", writable_arr)?;
  let readonly_len = meta.loaded_readonly_addresses.len() as u32;
  let mut readonly_arr = env.create_array(readonly_len)?;
  for (i, addr) in meta.loaded_readonly_addresses.into_iter().enumerate() {
    readonly_arr.set(i as u32, bytes_to_js(addr))?;
  }
  obj.set("loadedReadonlyAddresses", readonly_arr)?;
  if let Some(rd) = meta.return_data {
    obj.set("returnData", to_js_return_data(env, rd)?)?;
  }
  obj.set("returnDataNone", meta.return_data_none)?;
  if let Some(cu) = meta.compute_units_consumed {
    obj.set("computeUnitsConsumed", cu.to_string())?;
  }
  if let Some(cost_units) = meta.cost_units {
    obj.set("costUnits", cost_units.to_string())?;
  }
  Ok(obj)
}

fn to_js_transaction_error(env: &Env, err: TransactionError) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("err", bytes_to_js(err.err))?;
  Ok(obj)
}

fn to_js_inner_instructions(env: &Env, ii: InnerInstructions) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("index", env.create_uint32(ii.index)?)?;
  let insts_len = ii.instructions.len() as u32;
  let mut insts_arr = env.create_array(insts_len)?;
  for (i, inst) in ii.instructions.into_iter().enumerate() {
    insts_arr.set(i as u32, to_js_inner_instruction(env, inst)?)?;
  }
  obj.set("instructions", insts_arr)?;
  Ok(obj)
}

fn to_js_inner_instruction(env: &Env, inst: InnerInstruction) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("programIdIndex", env.create_uint32(inst.program_id_index)?)?;
  let accts_len = inst.accounts.len() as u32;
  let mut accts_arr = env.create_array(accts_len)?;
  for (i, acct_idx) in inst.accounts.into_iter().enumerate() {
    accts_arr.set(i as u32, env.create_uint32(acct_idx as u32)?)?;
  }
  obj.set("accounts", accts_arr)?;
  obj.set("data", bytes_to_js(inst.data))?;
  if let Some(sh) = inst.stack_height {
    obj.set("stackHeight", env.create_uint32(sh)?)?;
  }
  Ok(obj)
}

fn to_js_token_balance(env: &Env, tb: TokenBalance) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("accountIndex", env.create_uint32(tb.account_index)?)?;
  obj.set("mint", bytes_to_js(tb.mint.into()))?;
  if let Some(uta) = tb.ui_token_amount {
    obj.set("uiTokenAmount", to_js_ui_token_amount(env, uta)?)?;
  }
  obj.set("owner", tb.owner)?;
  obj.set("programId", tb.program_id)?;
  Ok(obj)
}

fn to_js_ui_token_amount(env: &Env, uta: UiTokenAmount) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("uiAmount", uta.amount.clone())?;
  obj.set("decimals", env.create_uint32(uta.decimals)?)?;
  obj.set("amount", uta.amount)?;
  obj.set("uiAmountString", uta.ui_amount_string)?;
  Ok(obj)
}

fn to_js_reward(env: &Env, r: Reward) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("pubkey", r.pubkey)?;
  obj.set("lamports", format!("{}", r.lamports))?;
  obj.set("postBalance", r.post_balance.to_string())?;
  obj.set("rewardType", r.reward_type)?;
  obj.set("commission", r.commission)?;
  Ok(obj)
}

fn to_js_return_data(env: &Env, rd: ReturnData) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("programId", bytes_to_js(rd.program_id))?;
  obj.set("data", bytes_to_js(rd.data))?;
  Ok(obj)
}

fn to_js_account_info(env: &Env, info: SubscribeUpdateAccountInfo) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("pubkey", bytes_to_js(info.pubkey))?;
  obj.set("lamports", info.lamports.to_string())?;
  obj.set("owner", bytes_to_js(info.owner))?;
  obj.set("executable", info.executable)?;
  obj.set("rentEpoch", info.rent_epoch.to_string())?;
  obj.set("data", bytes_to_js(info.data))?;
  obj.set("writeVersion", info.write_version.to_string())?;
  if let Some(sig) = info.txn_signature {
    obj.set("txnSignature", bytes_to_js(sig))?;
  }
  Ok(obj)
}

// Convert SubscribeUpdateBlock to Object
fn to_js_block(env: &Env, block: SubscribeUpdateBlock) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("slot", block.slot.to_string())?;
  obj.set("blockhash", block.blockhash)?;
  obj.set("parentSlot", block.parent_slot.to_string())?;
  obj.set("parentBlockhash", block.parent_blockhash)?;
  obj.set(
    "executedTransactionCount",
    block.executed_transaction_count.to_string(),
  )?;
  obj.set(
    "updatedAccountCount",
    block.updated_account_count.to_string(),
  )?;
  obj.set("entriesCount", block.entries_count.to_string())?;
  if let Some(rewards) = block.rewards {
    let mut rewards_obj = Object::new(env)?;
    let rewards_len = rewards.rewards.len() as u32;
    let mut rewards_arr = env.create_array(rewards_len)?;
    for (i, r) in rewards.rewards.into_iter().enumerate() {
      rewards_arr.set(i as u32, to_js_reward(env, r)?)?;
    }
    rewards_obj.set("rewards", rewards_arr)?;
    obj.set("rewards", rewards_obj)?;
  }
  if let Some(block_time) = block.block_time {
    let mut block_time_obj = Object::new(env)?;
    block_time_obj.set("timestamp", format!("{}", block_time.timestamp))?;
    obj.set("blockTime", block_time_obj)?;
  }
  if let Some(block_height) = block.block_height {
    let mut block_height_obj = Object::new(env)?;
    block_height_obj.set("blockHeight", block_height.block_height.to_string())?;
    obj.set("blockHeight", block_height_obj)?;
  }
  let txs_len = block.transactions.len() as u32;
  let mut txs_arr = env.create_array(txs_len)?;
  for (i, tx_info) in block.transactions.into_iter().enumerate() {
    txs_arr.set(i as u32, to_js_transaction_info(env, tx_info)?)?;
  }
  obj.set("transactions", txs_arr)?;
  let accts_len = block.accounts.len() as u32;
  let mut accts_arr = env.create_array(accts_len)?;
  for (i, acct_info) in block.accounts.into_iter().enumerate() {
    accts_arr.set(i as u32, to_js_account_info(env, acct_info)?)?;
  }
  obj.set("accounts", accts_arr)?;
  let entries_len = block.entries.len() as u32;
  let mut entries_arr = env.create_array(entries_len)?;
  for (i, entry) in block.entries.into_iter().enumerate() {
    entries_arr.set(i as u32, to_js_entry(env, entry)?)?;
  }
  obj.set("entries", entries_arr)?;
  Ok(obj)
}

// Convert SubscribeUpdateEntry to Object
fn to_js_entry(env: &Env, entry: SubscribeUpdateEntry) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("slot", entry.slot.to_string())?;
  obj.set("index", entry.index.to_string())?;
  obj.set("numHashes", entry.num_hashes.to_string())?;
  obj.set("hash", bytes_to_js(entry.hash))?;
  obj.set(
    "executedTransactionCount",
    entry.executed_transaction_count.to_string(),
  )?;
  obj.set(
    "startingTransactionIndex",
    entry.starting_transaction_index.to_string(),
  )?;
  Ok(obj)
}

// Convert SubscribeUpdatePing to Object
fn to_js_ping(env: &Env, _ping: SubscribeUpdatePing) -> Result<Object> {
  let obj = Object::new(env)?;
  Ok(obj)
}

// Convert SubscribeUpdatePong to Object
fn to_js_pong(env: &Env, pong: SubscribeUpdatePong) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("id", env.create_int32(pong.id)?)?;
  Ok(obj)
}

// Convert SubscribeUpdateBlockMeta to Object
fn to_js_block_meta(env: &Env, meta: SubscribeUpdateBlockMeta) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("slot", meta.slot.to_string())?;
  obj.set("blockhash", meta.blockhash)?;
  if let Some(rewards) = meta.rewards {
    let mut rewards_obj = Object::new(env)?;
    let rewards_len = rewards.rewards.len() as u32;
    let mut rewards_arr = env.create_array(rewards_len)?;
    for (i, r) in rewards.rewards.into_iter().enumerate() {
      rewards_arr.set(i as u32, to_js_reward(env, r)?)?;
    }
    rewards_obj.set("rewards", rewards_arr)?;
    obj.set("rewards", rewards_obj)?;
  }
  if let Some(block_time) = meta.block_time {
    let mut block_time_obj = Object::new(env)?;
    block_time_obj.set("timestamp", format!("{}", block_time.timestamp))?;
    obj.set("blockTime", block_time_obj)?;
  }
  if let Some(block_height) = meta.block_height {
    let mut block_height_obj = Object::new(env)?;
    block_height_obj.set("blockHeight", block_height.block_height.to_string())?;
    obj.set("blockHeight", block_height_obj)?;
  }
  obj.set("parentSlot", meta.parent_slot.to_string())?;
  obj.set("parentBlockhash", meta.parent_blockhash)?;
  obj.set(
    "executedTransactionCount",
    meta.executed_transaction_count.to_string(),
  )?;
  obj.set("entriesCount", meta.entries_count.to_string())?;
  Ok(obj)
}

// Convert SubscribeUpdateTransactionStatus to Object
fn to_js_transaction_status(env: &Env, status: SubscribeUpdateTransactionStatus) -> Result<Object> {
  let mut obj = Object::new(env)?;
  obj.set("slot", status.slot.to_string())?;
  obj.set("signature", bytes_to_js(status.signature))?;
  obj.set("isVote", status.is_vote)?;
  obj.set("index", status.index.to_string())?;
  if let Some(err) = status.err {
    obj.set("err", to_js_transaction_error(env, err)?)?;
  }
  Ok(obj)
}

pub fn to_js_update(env: &Env, update: SubscribeUpdate) -> Result<Object> {
  let mut obj = Object::new(env)?;

  // Filters as array of strings
  let filters_len = update.filters.len() as u32;
  let mut filters_arr = env.create_array(filters_len)?;
  for (i, filter) in update.filters.into_iter().enumerate() {
    filters_arr.set(i as u32, filter)?;
  }
  obj.set("filters", filters_arr)?;

  // Created at as Date
  if let Some(ts) = update.created_at {
    obj.set("createdAt", timestamp_to_js_date(env, ts)?)?;
  }

  // Update oneof: set a property based on variant
  if let Some(oneof) = update.update_oneof {
    match oneof {
      UpdateOneof::Account(acc) => {
        obj.set("account", to_js_account(env, acc)?)?;
      }
      UpdateOneof::Slot(slot) => {
        obj.set("slot", to_js_slot(env, slot)?)?;
      }
      UpdateOneof::Transaction(tx) => {
        obj.set("transaction", to_js_transaction(env, tx)?)?;
      }
      UpdateOneof::Block(block) => {
        obj.set("block", to_js_block(env, block)?)?;
      }
      UpdateOneof::Entry(entry) => {
        obj.set("entry", to_js_entry(env, entry)?)?;
      }
      UpdateOneof::Ping(ping) => {
        obj.set("ping", to_js_ping(env, ping)?)?;
      }
      UpdateOneof::Pong(pong) => {
        obj.set("pong", to_js_pong(env, pong)?)?;
      }
      UpdateOneof::BlockMeta(block_meta) => {
        obj.set("blockMeta", to_js_block_meta(env, block_meta)?)?;
      }
      UpdateOneof::TransactionStatus(tx_status) => {
        obj.set(
          "transactionStatus",
          to_js_transaction_status(env, tx_status)?,
        )?;
      }
    }
  }

  Ok(obj)
}
