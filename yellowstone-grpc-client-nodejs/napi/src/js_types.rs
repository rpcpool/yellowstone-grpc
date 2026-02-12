use napi::bindgen_prelude::{BufferSlice, Date, Env};
use napi_derive::napi;
use yellowstone_grpc_proto::geyser::*;
use yellowstone_grpc_proto::prelude::*;
use yellowstone_grpc_proto::solana::storage::confirmed_block::*;
#[napi(object)]
pub struct JsSubscribeRequestFilterAccountsFilterFilter<'env> {
  pub memcmp: ::core::option::Option<JsSubscribeRequestFilterAccountsFilterMemcmp<'env>>,
  pub datasize: ::core::option::Option<String>,
  pub token_account_state: ::core::option::Option<bool>,
  pub lamports: ::core::option::Option<JsSubscribeRequestFilterAccountsFilterLamports>,
}
impl<'env> JsSubscribeRequestFilterAccountsFilterFilter<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: subscribe_request_filter_accounts_filter::Filter,
  ) -> napi::Result<Self> {
    match value {
      subscribe_request_filter_accounts_filter::Filter::Memcmp(oneof_variant_value) => Ok(Self {
        memcmp: Some(
          JsSubscribeRequestFilterAccountsFilterMemcmp::from_protobuf_to_js_type(
            env,
            oneof_variant_value,
          )?,
        ),
        datasize: None,
        token_account_state: None,
        lamports: None,
      }),
      subscribe_request_filter_accounts_filter::Filter::Datasize(oneof_variant_value) => Ok(Self {
        datasize: Some(Ok::<_, napi::Error>(oneof_variant_value.to_string())?),
        memcmp: None,
        token_account_state: None,
        lamports: None,
      }),
      subscribe_request_filter_accounts_filter::Filter::TokenAccountState(oneof_variant_value) => {
        Ok(Self {
          token_account_state: Some(Ok::<_, napi::Error>(oneof_variant_value)?),
          memcmp: None,
          datasize: None,
          lamports: None,
        })
      }
      subscribe_request_filter_accounts_filter::Filter::Lamports(oneof_variant_value) => Ok(Self {
        lamports: Some(
          JsSubscribeRequestFilterAccountsFilterLamports::from_protobuf_to_js_type(
            env,
            oneof_variant_value,
          )?,
        ),
        memcmp: None,
        datasize: None,
        token_account_state: None,
      }),
    }
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestFilterAccountsFilterLamportsCmp {
  pub eq: ::core::option::Option<String>,
  pub ne: ::core::option::Option<String>,
  pub lt: ::core::option::Option<String>,
  pub gt: ::core::option::Option<String>,
}
impl JsSubscribeRequestFilterAccountsFilterLamportsCmp {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: subscribe_request_filter_accounts_filter_lamports::Cmp,
  ) -> napi::Result<Self> {
    match value {
      subscribe_request_filter_accounts_filter_lamports::Cmp::Eq(oneof_variant_value) => Ok(Self {
        eq: Some(Ok::<_, napi::Error>(oneof_variant_value.to_string())?),
        ne: None,
        lt: None,
        gt: None,
      }),
      subscribe_request_filter_accounts_filter_lamports::Cmp::Ne(oneof_variant_value) => Ok(Self {
        ne: Some(Ok::<_, napi::Error>(oneof_variant_value.to_string())?),
        eq: None,
        lt: None,
        gt: None,
      }),
      subscribe_request_filter_accounts_filter_lamports::Cmp::Lt(oneof_variant_value) => Ok(Self {
        lt: Some(Ok::<_, napi::Error>(oneof_variant_value.to_string())?),
        eq: None,
        ne: None,
        gt: None,
      }),
      subscribe_request_filter_accounts_filter_lamports::Cmp::Gt(oneof_variant_value) => Ok(Self {
        gt: Some(Ok::<_, napi::Error>(oneof_variant_value.to_string())?),
        eq: None,
        ne: None,
        lt: None,
      }),
    }
  }
}
#[napi(object)]
pub struct JsSubscribeRequestFilterAccountsFilterMemcmpData<'env> {
  pub bytes: ::core::option::Option<BufferSlice<'env>>,
  pub base58: ::core::option::Option<::prost::alloc::string::String>,
  pub base64: ::core::option::Option<::prost::alloc::string::String>,
}
impl<'env> JsSubscribeRequestFilterAccountsFilterMemcmpData<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: subscribe_request_filter_accounts_filter_memcmp::Data,
  ) -> napi::Result<Self> {
    match value {
      subscribe_request_filter_accounts_filter_memcmp::Data::Bytes(oneof_variant_value) => {
        Ok(Self {
          bytes: Some(BufferSlice::copy_from(env, &oneof_variant_value)?),
          base58: None,
          base64: None,
        })
      }
      subscribe_request_filter_accounts_filter_memcmp::Data::Base58(oneof_variant_value) => {
        Ok(Self {
          base58: Some(Ok::<_, napi::Error>(oneof_variant_value)?),
          bytes: None,
          base64: None,
        })
      }
      subscribe_request_filter_accounts_filter_memcmp::Data::Base64(oneof_variant_value) => {
        Ok(Self {
          base64: Some(Ok::<_, napi::Error>(oneof_variant_value)?),
          bytes: None,
          base58: None,
        })
      }
    }
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateUpdateOneof<'env> {
  pub account: ::core::option::Option<JsSubscribeUpdateAccount<'env>>,
  pub slot: ::core::option::Option<JsSubscribeUpdateSlot>,
  pub transaction: ::core::option::Option<JsSubscribeUpdateTransaction<'env>>,
  pub transaction_status: ::core::option::Option<JsSubscribeUpdateTransactionStatus<'env>>,
  pub block: ::core::option::Option<JsSubscribeUpdateBlock<'env>>,
  pub ping: ::core::option::Option<JsSubscribeUpdatePing>,
  pub pong: ::core::option::Option<JsSubscribeUpdatePong>,
  pub block_meta: ::core::option::Option<JsSubscribeUpdateBlockMeta>,
  pub entry: ::core::option::Option<JsSubscribeUpdateEntry<'env>>,
}
impl<'env> JsSubscribeUpdateUpdateOneof<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: subscribe_update::UpdateOneof,
  ) -> napi::Result<Self> {
    match value {
      subscribe_update::UpdateOneof::Account(oneof_variant_value) => Ok(Self {
        account: Some(JsSubscribeUpdateAccount::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        slot: None,
        transaction: None,
        transaction_status: None,
        block: None,
        ping: None,
        pong: None,
        block_meta: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::Slot(oneof_variant_value) => Ok(Self {
        slot: Some(JsSubscribeUpdateSlot::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        account: None,
        transaction: None,
        transaction_status: None,
        block: None,
        ping: None,
        pong: None,
        block_meta: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::Transaction(oneof_variant_value) => Ok(Self {
        transaction: Some(JsSubscribeUpdateTransaction::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        account: None,
        slot: None,
        transaction_status: None,
        block: None,
        ping: None,
        pong: None,
        block_meta: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::TransactionStatus(oneof_variant_value) => Ok(Self {
        transaction_status: Some(
          JsSubscribeUpdateTransactionStatus::from_protobuf_to_js_type(env, oneof_variant_value)?,
        ),
        account: None,
        slot: None,
        transaction: None,
        block: None,
        ping: None,
        pong: None,
        block_meta: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::Block(oneof_variant_value) => Ok(Self {
        block: Some(JsSubscribeUpdateBlock::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        account: None,
        slot: None,
        transaction: None,
        transaction_status: None,
        ping: None,
        pong: None,
        block_meta: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::Ping(oneof_variant_value) => Ok(Self {
        ping: Some(JsSubscribeUpdatePing::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        account: None,
        slot: None,
        transaction: None,
        transaction_status: None,
        block: None,
        pong: None,
        block_meta: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::Pong(oneof_variant_value) => Ok(Self {
        pong: Some(JsSubscribeUpdatePong::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        account: None,
        slot: None,
        transaction: None,
        transaction_status: None,
        block: None,
        ping: None,
        block_meta: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::BlockMeta(oneof_variant_value) => Ok(Self {
        block_meta: Some(JsSubscribeUpdateBlockMeta::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        account: None,
        slot: None,
        transaction: None,
        transaction_status: None,
        block: None,
        ping: None,
        pong: None,
        entry: None,
      }),
      subscribe_update::UpdateOneof::Entry(oneof_variant_value) => Ok(Self {
        entry: Some(JsSubscribeUpdateEntry::from_protobuf_to_js_type(
          env,
          oneof_variant_value,
        )?),
        account: None,
        slot: None,
        transaction: None,
        transaction_status: None,
        block: None,
        ping: None,
        pong: None,
        block_meta: None,
      }),
    }
  }
}
#[napi(object)]
pub struct JsSubscribeRequest<'env> {
  pub accounts: ::std::collections::HashMap<
    ::prost::alloc::string::String,
    JsSubscribeRequestFilterAccounts<'env>,
  >,
  pub slots:
    ::std::collections::HashMap<::prost::alloc::string::String, JsSubscribeRequestFilterSlots>,
  pub transactions: ::std::collections::HashMap<
    ::prost::alloc::string::String,
    JsSubscribeRequestFilterTransactions,
  >,
  pub transactions_status: ::std::collections::HashMap<
    ::prost::alloc::string::String,
    JsSubscribeRequestFilterTransactions,
  >,
  pub blocks:
    ::std::collections::HashMap<::prost::alloc::string::String, JsSubscribeRequestFilterBlocks>,
  pub blocks_meta:
    ::std::collections::HashMap<::prost::alloc::string::String, JsSubscribeRequestFilterBlocksMeta>,
  pub entry:
    ::std::collections::HashMap<::prost::alloc::string::String, JsSubscribeRequestFilterEntry>,
  pub commitment: ::core::option::Option<i32>,
  pub accounts_data_slice: ::prost::alloc::vec::Vec<JsSubscribeRequestAccountsDataSlice>,
  pub ping: ::core::option::Option<JsSubscribeRequestPing>,
  pub from_slot: ::core::option::Option<String>,
}
impl<'env> JsSubscribeRequest<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: SubscribeRequest) -> napi::Result<Self> {
    Ok(Self {
      accounts: value
        .accounts
        .into_iter()
        .map(|(hash_map_entry_key, hash_map_entry_value)| {
          let converted_hash_map_value =
            JsSubscribeRequestFilterAccounts::from_protobuf_to_js_type(env, hash_map_entry_value)?;
          Ok::<_, napi::Error>((hash_map_entry_key, converted_hash_map_value))
        })
        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()?,
      slots: value
        .slots
        .into_iter()
        .map(|(hash_map_entry_key, hash_map_entry_value)| {
          let converted_hash_map_value =
            JsSubscribeRequestFilterSlots::from_protobuf_to_js_type(env, hash_map_entry_value)?;
          Ok::<_, napi::Error>((hash_map_entry_key, converted_hash_map_value))
        })
        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()?,
      transactions: value
        .transactions
        .into_iter()
        .map(|(hash_map_entry_key, hash_map_entry_value)| {
          let converted_hash_map_value =
            JsSubscribeRequestFilterTransactions::from_protobuf_to_js_type(
              env,
              hash_map_entry_value,
            )?;
          Ok::<_, napi::Error>((hash_map_entry_key, converted_hash_map_value))
        })
        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()?,
      transactions_status: value
        .transactions_status
        .into_iter()
        .map(|(hash_map_entry_key, hash_map_entry_value)| {
          let converted_hash_map_value =
            JsSubscribeRequestFilterTransactions::from_protobuf_to_js_type(
              env,
              hash_map_entry_value,
            )?;
          Ok::<_, napi::Error>((hash_map_entry_key, converted_hash_map_value))
        })
        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()?,
      blocks: value
        .blocks
        .into_iter()
        .map(|(hash_map_entry_key, hash_map_entry_value)| {
          let converted_hash_map_value =
            JsSubscribeRequestFilterBlocks::from_protobuf_to_js_type(env, hash_map_entry_value)?;
          Ok::<_, napi::Error>((hash_map_entry_key, converted_hash_map_value))
        })
        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()?,
      blocks_meta: value
        .blocks_meta
        .into_iter()
        .map(|(hash_map_entry_key, hash_map_entry_value)| {
          let converted_hash_map_value =
            JsSubscribeRequestFilterBlocksMeta::from_protobuf_to_js_type(
              env,
              hash_map_entry_value,
            )?;
          Ok::<_, napi::Error>((hash_map_entry_key, converted_hash_map_value))
        })
        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()?,
      entry: value
        .entry
        .into_iter()
        .map(|(hash_map_entry_key, hash_map_entry_value)| {
          let converted_hash_map_value =
            JsSubscribeRequestFilterEntry::from_protobuf_to_js_type(env, hash_map_entry_value)?;
          Ok::<_, napi::Error>((hash_map_entry_key, converted_hash_map_value))
        })
        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()?,
      commitment: value
        .commitment
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
      accounts_data_slice: value
        .accounts_data_slice
        .into_iter()
        .map(|vec_inner_value| {
          JsSubscribeRequestAccountsDataSlice::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      ping: value
        .ping
        .map(|option_inner_value| {
          JsSubscribeRequestPing::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      from_slot: value
        .from_slot
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value.to_string()))
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeRequestFilterAccounts<'env> {
  pub account: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
  pub owner: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
  pub filters: ::prost::alloc::vec::Vec<JsSubscribeRequestFilterAccountsFilter<'env>>,
  pub nonempty_txn_signature: ::core::option::Option<bool>,
}
impl<'env> JsSubscribeRequestFilterAccounts<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeRequestFilterAccounts,
  ) -> napi::Result<Self> {
    Ok(Self {
      account: value
        .account
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      owner: value
        .owner
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      filters: value
        .filters
        .into_iter()
        .map(|vec_inner_value| {
          JsSubscribeRequestFilterAccountsFilter::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      nonempty_txn_signature: value
        .nonempty_txn_signature
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeRequestFilterAccountsFilter<'env> {
  pub filter: ::core::option::Option<JsSubscribeRequestFilterAccountsFilterFilter<'env>>,
}
impl<'env> JsSubscribeRequestFilterAccountsFilter<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeRequestFilterAccountsFilter,
  ) -> napi::Result<Self> {
    Ok(Self {
      filter: value
        .filter
        .map(|option_inner_value| {
          JsSubscribeRequestFilterAccountsFilterFilter::from_protobuf_to_js_type(
            env,
            option_inner_value,
          )
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeRequestFilterAccountsFilterMemcmp<'env> {
  pub offset: String,
  pub data: ::core::option::Option<JsSubscribeRequestFilterAccountsFilterMemcmpData<'env>>,
}
impl<'env> JsSubscribeRequestFilterAccountsFilterMemcmp<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeRequestFilterAccountsFilterMemcmp,
  ) -> napi::Result<Self> {
    Ok(Self {
      offset: Ok::<_, napi::Error>(value.offset.to_string())?,
      data: value
        .data
        .map(|option_inner_value| {
          JsSubscribeRequestFilterAccountsFilterMemcmpData::from_protobuf_to_js_type(
            env,
            option_inner_value,
          )
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestFilterAccountsFilterLamports {
  pub cmp: ::core::option::Option<JsSubscribeRequestFilterAccountsFilterLamportsCmp>,
}
impl JsSubscribeRequestFilterAccountsFilterLamports {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeRequestFilterAccountsFilterLamports,
  ) -> napi::Result<Self> {
    Ok(Self {
      cmp: value
        .cmp
        .map(|option_inner_value| {
          JsSubscribeRequestFilterAccountsFilterLamportsCmp::from_protobuf_to_js_type(
            env,
            option_inner_value,
          )
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestFilterSlots {
  pub filter_by_commitment: ::core::option::Option<bool>,
  pub interslot_updates: ::core::option::Option<bool>,
}
impl JsSubscribeRequestFilterSlots {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeRequestFilterSlots,
  ) -> napi::Result<Self> {
    Ok(Self {
      filter_by_commitment: value
        .filter_by_commitment
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
      interslot_updates: value
        .interslot_updates
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestFilterTransactions {
  pub vote: ::core::option::Option<bool>,
  pub failed: ::core::option::Option<bool>,
  pub signature: ::core::option::Option<::prost::alloc::string::String>,
  pub account_include: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
  pub account_exclude: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
  pub account_required: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
impl JsSubscribeRequestFilterTransactions {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeRequestFilterTransactions,
  ) -> napi::Result<Self> {
    Ok(Self {
      vote: value
        .vote
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
      failed: value
        .failed
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
      signature: value
        .signature
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
      account_include: value
        .account_include
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      account_exclude: value
        .account_exclude
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      account_required: value
        .account_required
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestFilterBlocks {
  pub account_include: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
  pub include_transactions: ::core::option::Option<bool>,
  pub include_accounts: ::core::option::Option<bool>,
  pub include_entries: ::core::option::Option<bool>,
}
impl JsSubscribeRequestFilterBlocks {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeRequestFilterBlocks,
  ) -> napi::Result<Self> {
    Ok(Self {
      account_include: value
        .account_include
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      include_transactions: value
        .include_transactions
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
      include_accounts: value
        .include_accounts
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
      include_entries: value
        .include_entries
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestFilterBlocksMeta {}
impl JsSubscribeRequestFilterBlocksMeta {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeRequestFilterBlocksMeta,
  ) -> napi::Result<Self> {
    Ok(Self {})
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestFilterEntry {}
impl JsSubscribeRequestFilterEntry {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeRequestFilterEntry,
  ) -> napi::Result<Self> {
    Ok(Self {})
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestAccountsDataSlice {
  pub offset: String,
  pub length: String,
}
impl JsSubscribeRequestAccountsDataSlice {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeRequestAccountsDataSlice,
  ) -> napi::Result<Self> {
    Ok(Self {
      offset: Ok::<_, napi::Error>(value.offset.to_string())?,
      length: Ok::<_, napi::Error>(value.length.to_string())?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeRequestPing {
  pub id: i32,
}
impl JsSubscribeRequestPing {
  pub fn from_protobuf_to_js_type(env: &Env, value: SubscribeRequestPing) -> napi::Result<Self> {
    Ok(Self {
      id: Ok::<_, napi::Error>(value.id)?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdate<'env> {
  pub filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
  pub created_at: ::core::option::Option<Date<'env>>,
  pub update_oneof: ::core::option::Option<JsSubscribeUpdateUpdateOneof<'env>>,
}
impl<'env> JsSubscribeUpdate<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: SubscribeUpdate) -> napi::Result<Self> {
    Ok(Self {
      filters: value
        .filters
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      created_at: value
        .created_at
        .map(|option_inner_value| {
          let timestamp_value_for_date_conversion = option_inner_value;
          let timestamp_millis_for_date_conversion = (timestamp_value_for_date_conversion.seconds
            * 1000) as f64
            + (timestamp_value_for_date_conversion.nanos as f64 / 1_000_000.0);
          env.create_date(timestamp_millis_for_date_conversion)
        })
        .transpose()?,
      update_oneof: value
        .update_oneof
        .map(|option_inner_value| {
          JsSubscribeUpdateUpdateOneof::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateAccount<'env> {
  pub account: ::core::option::Option<JsSubscribeUpdateAccountInfo<'env>>,
  pub slot: String,
  pub is_startup: bool,
}
impl<'env> JsSubscribeUpdateAccount<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeUpdateAccount,
  ) -> napi::Result<Self> {
    Ok(Self {
      account: value
        .account
        .map(|option_inner_value| {
          JsSubscribeUpdateAccountInfo::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      is_startup: Ok::<_, napi::Error>(value.is_startup)?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateAccountInfo<'env> {
  pub pubkey: BufferSlice<'env>,
  pub lamports: String,
  pub owner: BufferSlice<'env>,
  pub executable: bool,
  pub rent_epoch: String,
  pub data: BufferSlice<'env>,
  pub write_version: String,
  pub txn_signature: ::core::option::Option<BufferSlice<'env>>,
}
impl<'env> JsSubscribeUpdateAccountInfo<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeUpdateAccountInfo,
  ) -> napi::Result<Self> {
    Ok(Self {
      pubkey: BufferSlice::copy_from(env, &value.pubkey)?,
      lamports: Ok::<_, napi::Error>(value.lamports.to_string())?,
      owner: BufferSlice::copy_from(env, &value.owner)?,
      executable: Ok::<_, napi::Error>(value.executable)?,
      rent_epoch: Ok::<_, napi::Error>(value.rent_epoch.to_string())?,
      data: BufferSlice::copy_from(env, &value.data)?,
      write_version: Ok::<_, napi::Error>(value.write_version.to_string())?,
      txn_signature: value
        .txn_signature
        .map(|option_inner_value| BufferSlice::copy_from(env, &option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeUpdateSlot {
  pub slot: String,
  pub parent: ::core::option::Option<String>,
  pub status: i32,
  pub dead_error: ::core::option::Option<::prost::alloc::string::String>,
}
impl JsSubscribeUpdateSlot {
  pub fn from_protobuf_to_js_type(env: &Env, value: SubscribeUpdateSlot) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      parent: value
        .parent
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value.to_string()))
        .transpose()?,
      status: Ok::<_, napi::Error>(value.status)?,
      dead_error: value
        .dead_error
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateTransaction<'env> {
  pub transaction: ::core::option::Option<JsSubscribeUpdateTransactionInfo<'env>>,
  pub slot: String,
}
impl<'env> JsSubscribeUpdateTransaction<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeUpdateTransaction,
  ) -> napi::Result<Self> {
    Ok(Self {
      transaction: value
        .transaction
        .map(|option_inner_value| {
          JsSubscribeUpdateTransactionInfo::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateTransactionInfo<'env> {
  pub signature: BufferSlice<'env>,
  pub is_vote: bool,
  pub transaction: ::core::option::Option<JsTransaction<'env>>,
  pub meta: ::core::option::Option<JsTransactionStatusMeta<'env>>,
  pub index: String,
}
impl<'env> JsSubscribeUpdateTransactionInfo<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeUpdateTransactionInfo,
  ) -> napi::Result<Self> {
    Ok(Self {
      signature: BufferSlice::copy_from(env, &value.signature)?,
      is_vote: Ok::<_, napi::Error>(value.is_vote)?,
      transaction: value
        .transaction
        .map(|option_inner_value| JsTransaction::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      meta: value
        .meta
        .map(|option_inner_value| {
          JsTransactionStatusMeta::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      index: Ok::<_, napi::Error>(value.index.to_string())?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateTransactionStatus<'env> {
  pub slot: String,
  pub signature: BufferSlice<'env>,
  pub is_vote: bool,
  pub index: String,
  pub err: ::core::option::Option<JsTransactionError<'env>>,
}
impl<'env> JsSubscribeUpdateTransactionStatus<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeUpdateTransactionStatus,
  ) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      signature: BufferSlice::copy_from(env, &value.signature)?,
      is_vote: Ok::<_, napi::Error>(value.is_vote)?,
      index: Ok::<_, napi::Error>(value.index.to_string())?,
      err: value
        .err
        .map(|option_inner_value| {
          JsTransactionError::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateBlock<'env> {
  pub slot: String,
  pub blockhash: ::prost::alloc::string::String,
  pub rewards: ::core::option::Option<JsRewards>,
  pub block_time: ::core::option::Option<JsUnixTimestamp>,
  pub block_height: ::core::option::Option<JsBlockHeight>,
  pub parent_slot: String,
  pub parent_blockhash: ::prost::alloc::string::String,
  pub executed_transaction_count: String,
  pub transactions: ::prost::alloc::vec::Vec<JsSubscribeUpdateTransactionInfo<'env>>,
  pub updated_account_count: String,
  pub accounts: ::prost::alloc::vec::Vec<JsSubscribeUpdateAccountInfo<'env>>,
  pub entries_count: String,
  pub entries: ::prost::alloc::vec::Vec<JsSubscribeUpdateEntry<'env>>,
}
impl<'env> JsSubscribeUpdateBlock<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeUpdateBlock,
  ) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      blockhash: Ok::<_, napi::Error>(value.blockhash)?,
      rewards: value
        .rewards
        .map(|option_inner_value| JsRewards::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      block_time: value
        .block_time
        .map(|option_inner_value| {
          JsUnixTimestamp::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      block_height: value
        .block_height
        .map(|option_inner_value| JsBlockHeight::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      parent_slot: Ok::<_, napi::Error>(value.parent_slot.to_string())?,
      parent_blockhash: Ok::<_, napi::Error>(value.parent_blockhash)?,
      executed_transaction_count: Ok::<_, napi::Error>(
        value.executed_transaction_count.to_string(),
      )?,
      transactions: value
        .transactions
        .into_iter()
        .map(|vec_inner_value| {
          JsSubscribeUpdateTransactionInfo::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      updated_account_count: Ok::<_, napi::Error>(value.updated_account_count.to_string())?,
      accounts: value
        .accounts
        .into_iter()
        .map(|vec_inner_value| {
          JsSubscribeUpdateAccountInfo::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      entries_count: Ok::<_, napi::Error>(value.entries_count.to_string())?,
      entries: value
        .entries
        .into_iter()
        .map(|vec_inner_value| {
          JsSubscribeUpdateEntry::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeUpdateBlockMeta {
  pub slot: String,
  pub blockhash: ::prost::alloc::string::String,
  pub rewards: ::core::option::Option<JsRewards>,
  pub block_time: ::core::option::Option<JsUnixTimestamp>,
  pub block_height: ::core::option::Option<JsBlockHeight>,
  pub parent_slot: String,
  pub parent_blockhash: ::prost::alloc::string::String,
  pub executed_transaction_count: String,
  pub entries_count: String,
}
impl JsSubscribeUpdateBlockMeta {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeUpdateBlockMeta,
  ) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      blockhash: Ok::<_, napi::Error>(value.blockhash)?,
      rewards: value
        .rewards
        .map(|option_inner_value| JsRewards::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      block_time: value
        .block_time
        .map(|option_inner_value| {
          JsUnixTimestamp::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      block_height: value
        .block_height
        .map(|option_inner_value| JsBlockHeight::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      parent_slot: Ok::<_, napi::Error>(value.parent_slot.to_string())?,
      parent_blockhash: Ok::<_, napi::Error>(value.parent_blockhash)?,
      executed_transaction_count: Ok::<_, napi::Error>(
        value.executed_transaction_count.to_string(),
      )?,
      entries_count: Ok::<_, napi::Error>(value.entries_count.to_string())?,
    })
  }
}
#[napi(object)]
pub struct JsSubscribeUpdateEntry<'env> {
  pub slot: String,
  pub index: String,
  pub num_hashes: String,
  pub hash: BufferSlice<'env>,
  pub executed_transaction_count: String,
  pub starting_transaction_index: String,
}
impl<'env> JsSubscribeUpdateEntry<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: SubscribeUpdateEntry,
  ) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      index: Ok::<_, napi::Error>(value.index.to_string())?,
      num_hashes: Ok::<_, napi::Error>(value.num_hashes.to_string())?,
      hash: BufferSlice::copy_from(env, &value.hash)?,
      executed_transaction_count: Ok::<_, napi::Error>(
        value.executed_transaction_count.to_string(),
      )?,
      starting_transaction_index: Ok::<_, napi::Error>(
        value.starting_transaction_index.to_string(),
      )?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeUpdatePing {}
impl JsSubscribeUpdatePing {
  pub fn from_protobuf_to_js_type(env: &Env, value: SubscribeUpdatePing) -> napi::Result<Self> {
    Ok(Self {})
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeUpdatePong {
  pub id: i32,
}
impl JsSubscribeUpdatePong {
  pub fn from_protobuf_to_js_type(env: &Env, value: SubscribeUpdatePong) -> napi::Result<Self> {
    Ok(Self {
      id: Ok::<_, napi::Error>(value.id)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeReplayInfoRequest {}
impl JsSubscribeReplayInfoRequest {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeReplayInfoRequest,
  ) -> napi::Result<Self> {
    Ok(Self {})
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsSubscribeReplayInfoResponse {
  pub first_available: ::core::option::Option<String>,
}
impl JsSubscribeReplayInfoResponse {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: SubscribeReplayInfoResponse,
  ) -> napi::Result<Self> {
    Ok(Self {
      first_available: value
        .first_available
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value.to_string()))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsPingRequest {
  pub count: i32,
}
impl JsPingRequest {
  pub fn from_protobuf_to_js_type(env: &Env, value: PingRequest) -> napi::Result<Self> {
    Ok(Self {
      count: Ok::<_, napi::Error>(value.count)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsPongResponse {
  pub count: i32,
}
impl JsPongResponse {
  pub fn from_protobuf_to_js_type(env: &Env, value: PongResponse) -> napi::Result<Self> {
    Ok(Self {
      count: Ok::<_, napi::Error>(value.count)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetLatestBlockhashRequest {
  pub commitment: ::core::option::Option<i32>,
}
impl JsGetLatestBlockhashRequest {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: GetLatestBlockhashRequest,
  ) -> napi::Result<Self> {
    Ok(Self {
      commitment: value
        .commitment
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetLatestBlockhashResponse {
  pub slot: String,
  pub blockhash: ::prost::alloc::string::String,
  pub last_valid_block_height: String,
}
impl JsGetLatestBlockhashResponse {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: GetLatestBlockhashResponse,
  ) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      blockhash: Ok::<_, napi::Error>(value.blockhash)?,
      last_valid_block_height: Ok::<_, napi::Error>(value.last_valid_block_height.to_string())?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetBlockHeightRequest {
  pub commitment: ::core::option::Option<i32>,
}
impl JsGetBlockHeightRequest {
  pub fn from_protobuf_to_js_type(env: &Env, value: GetBlockHeightRequest) -> napi::Result<Self> {
    Ok(Self {
      commitment: value
        .commitment
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetBlockHeightResponse {
  pub block_height: String,
}
impl JsGetBlockHeightResponse {
  pub fn from_protobuf_to_js_type(env: &Env, value: GetBlockHeightResponse) -> napi::Result<Self> {
    Ok(Self {
      block_height: Ok::<_, napi::Error>(value.block_height.to_string())?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetSlotRequest {
  pub commitment: ::core::option::Option<i32>,
}
impl JsGetSlotRequest {
  pub fn from_protobuf_to_js_type(env: &Env, value: GetSlotRequest) -> napi::Result<Self> {
    Ok(Self {
      commitment: value
        .commitment
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetSlotResponse {
  pub slot: String,
}
impl JsGetSlotResponse {
  pub fn from_protobuf_to_js_type(env: &Env, value: GetSlotResponse) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetVersionRequest {}
impl JsGetVersionRequest {
  pub fn from_protobuf_to_js_type(env: &Env, value: GetVersionRequest) -> napi::Result<Self> {
    Ok(Self {})
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsGetVersionResponse {
  pub version: ::prost::alloc::string::String,
}
impl JsGetVersionResponse {
  pub fn from_protobuf_to_js_type(env: &Env, value: GetVersionResponse) -> napi::Result<Self> {
    Ok(Self {
      version: Ok::<_, napi::Error>(value.version)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsIsBlockhashValidRequest {
  pub blockhash: ::prost::alloc::string::String,
  pub commitment: ::core::option::Option<i32>,
}
impl JsIsBlockhashValidRequest {
  pub fn from_protobuf_to_js_type(env: &Env, value: IsBlockhashValidRequest) -> napi::Result<Self> {
    Ok(Self {
      blockhash: Ok::<_, napi::Error>(value.blockhash)?,
      commitment: value
        .commitment
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsIsBlockhashValidResponse {
  pub slot: String,
  pub valid: bool,
}
impl JsIsBlockhashValidResponse {
  pub fn from_protobuf_to_js_type(
    env: &Env,
    value: IsBlockhashValidResponse,
  ) -> napi::Result<Self> {
    Ok(Self {
      slot: Ok::<_, napi::Error>(value.slot.to_string())?,
      valid: Ok::<_, napi::Error>(value.valid)?,
    })
  }
}
#[napi(object)]
pub struct JsConfirmedBlock<'env> {
  pub previous_blockhash: ::prost::alloc::string::String,
  pub blockhash: ::prost::alloc::string::String,
  pub parent_slot: String,
  pub transactions: ::prost::alloc::vec::Vec<JsConfirmedTransaction<'env>>,
  pub rewards: ::prost::alloc::vec::Vec<JsReward>,
  pub block_time: ::core::option::Option<JsUnixTimestamp>,
  pub block_height: ::core::option::Option<JsBlockHeight>,
  pub num_partitions: ::core::option::Option<JsNumPartitions>,
}
impl<'env> JsConfirmedBlock<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: ConfirmedBlock) -> napi::Result<Self> {
    Ok(Self {
      previous_blockhash: Ok::<_, napi::Error>(value.previous_blockhash)?,
      blockhash: Ok::<_, napi::Error>(value.blockhash)?,
      parent_slot: Ok::<_, napi::Error>(value.parent_slot.to_string())?,
      transactions: value
        .transactions
        .into_iter()
        .map(|vec_inner_value| {
          JsConfirmedTransaction::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      rewards: value
        .rewards
        .into_iter()
        .map(|vec_inner_value| JsReward::from_protobuf_to_js_type(env, vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      block_time: value
        .block_time
        .map(|option_inner_value| {
          JsUnixTimestamp::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      block_height: value
        .block_height
        .map(|option_inner_value| JsBlockHeight::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      num_partitions: value
        .num_partitions
        .map(|option_inner_value| {
          JsNumPartitions::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsConfirmedTransaction<'env> {
  pub transaction: ::core::option::Option<JsTransaction<'env>>,
  pub meta: ::core::option::Option<JsTransactionStatusMeta<'env>>,
}
impl<'env> JsConfirmedTransaction<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: ConfirmedTransaction,
  ) -> napi::Result<Self> {
    Ok(Self {
      transaction: value
        .transaction
        .map(|option_inner_value| JsTransaction::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      meta: value
        .meta
        .map(|option_inner_value| {
          JsTransactionStatusMeta::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsTransaction<'env> {
  pub signatures: ::prost::alloc::vec::Vec<BufferSlice<'env>>,
  pub message: ::core::option::Option<JsMessage<'env>>,
}
impl<'env> JsTransaction<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: Transaction) -> napi::Result<Self> {
    Ok(Self {
      signatures: value
        .signatures
        .into_iter()
        .map(|vec_inner_value| BufferSlice::copy_from(env, &vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      message: value
        .message
        .map(|option_inner_value| JsMessage::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsMessage<'env> {
  pub header: ::core::option::Option<JsMessageHeader>,
  pub account_keys: ::prost::alloc::vec::Vec<BufferSlice<'env>>,
  pub recent_blockhash: BufferSlice<'env>,
  pub instructions: ::prost::alloc::vec::Vec<JsCompiledInstruction<'env>>,
  pub versioned: bool,
  pub address_table_lookups: ::prost::alloc::vec::Vec<JsMessageAddressTableLookup<'env>>,
}
impl<'env> JsMessage<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: Message) -> napi::Result<Self> {
    Ok(Self {
      header: value
        .header
        .map(|option_inner_value| {
          JsMessageHeader::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      account_keys: value
        .account_keys
        .into_iter()
        .map(|vec_inner_value| BufferSlice::copy_from(env, &vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      recent_blockhash: BufferSlice::copy_from(env, &value.recent_blockhash)?,
      instructions: value
        .instructions
        .into_iter()
        .map(|vec_inner_value| {
          JsCompiledInstruction::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      versioned: Ok::<_, napi::Error>(value.versioned)?,
      address_table_lookups: value
        .address_table_lookups
        .into_iter()
        .map(|vec_inner_value| {
          JsMessageAddressTableLookup::from_protobuf_to_js_type(env, vec_inner_value)
        })
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsMessageHeader {
  pub num_required_signatures: u32,
  pub num_readonly_signed_accounts: u32,
  pub num_readonly_unsigned_accounts: u32,
}
impl JsMessageHeader {
  pub fn from_protobuf_to_js_type(env: &Env, value: MessageHeader) -> napi::Result<Self> {
    Ok(Self {
      num_required_signatures: Ok::<_, napi::Error>(value.num_required_signatures)?,
      num_readonly_signed_accounts: Ok::<_, napi::Error>(value.num_readonly_signed_accounts)?,
      num_readonly_unsigned_accounts: Ok::<_, napi::Error>(value.num_readonly_unsigned_accounts)?,
    })
  }
}
#[napi(object)]
pub struct JsMessageAddressTableLookup<'env> {
  pub account_key: BufferSlice<'env>,
  pub writable_indexes: BufferSlice<'env>,
  pub readonly_indexes: BufferSlice<'env>,
}
impl<'env> JsMessageAddressTableLookup<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: MessageAddressTableLookup,
  ) -> napi::Result<Self> {
    Ok(Self {
      account_key: BufferSlice::copy_from(env, &value.account_key)?,
      writable_indexes: BufferSlice::copy_from(env, &value.writable_indexes)?,
      readonly_indexes: BufferSlice::copy_from(env, &value.readonly_indexes)?,
    })
  }
}
#[napi(object)]
pub struct JsTransactionStatusMeta<'env> {
  pub err: ::core::option::Option<JsTransactionError<'env>>,
  pub fee: String,
  pub pre_balances: ::prost::alloc::vec::Vec<String>,
  pub post_balances: ::prost::alloc::vec::Vec<String>,
  pub inner_instructions: ::prost::alloc::vec::Vec<JsInnerInstructions<'env>>,
  pub inner_instructions_none: bool,
  pub log_messages: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
  pub log_messages_none: bool,
  pub pre_token_balances: ::prost::alloc::vec::Vec<JsTokenBalance>,
  pub post_token_balances: ::prost::alloc::vec::Vec<JsTokenBalance>,
  pub rewards: ::prost::alloc::vec::Vec<JsReward>,
  pub loaded_writable_addresses: ::prost::alloc::vec::Vec<BufferSlice<'env>>,
  pub loaded_readonly_addresses: ::prost::alloc::vec::Vec<BufferSlice<'env>>,
  pub return_data: ::core::option::Option<JsReturnData<'env>>,
  pub return_data_none: bool,
  pub compute_units_consumed: ::core::option::Option<String>,
  pub cost_units: ::core::option::Option<String>,
}
impl<'env> JsTransactionStatusMeta<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: TransactionStatusMeta,
  ) -> napi::Result<Self> {
    Ok(Self {
      err: value
        .err
        .map(|option_inner_value| {
          JsTransactionError::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      fee: Ok::<_, napi::Error>(value.fee.to_string())?,
      pre_balances: value
        .pre_balances
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value.to_string()))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      post_balances: value
        .post_balances
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value.to_string()))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      inner_instructions: value
        .inner_instructions
        .into_iter()
        .map(|vec_inner_value| JsInnerInstructions::from_protobuf_to_js_type(env, vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      inner_instructions_none: Ok::<_, napi::Error>(value.inner_instructions_none)?,
      log_messages: value
        .log_messages
        .into_iter()
        .map(|vec_inner_value| Ok::<_, napi::Error>(vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      log_messages_none: Ok::<_, napi::Error>(value.log_messages_none)?,
      pre_token_balances: value
        .pre_token_balances
        .into_iter()
        .map(|vec_inner_value| JsTokenBalance::from_protobuf_to_js_type(env, vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      post_token_balances: value
        .post_token_balances
        .into_iter()
        .map(|vec_inner_value| JsTokenBalance::from_protobuf_to_js_type(env, vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      rewards: value
        .rewards
        .into_iter()
        .map(|vec_inner_value| JsReward::from_protobuf_to_js_type(env, vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      loaded_writable_addresses: value
        .loaded_writable_addresses
        .into_iter()
        .map(|vec_inner_value| BufferSlice::copy_from(env, &vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      loaded_readonly_addresses: value
        .loaded_readonly_addresses
        .into_iter()
        .map(|vec_inner_value| BufferSlice::copy_from(env, &vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      return_data: value
        .return_data
        .map(|option_inner_value| JsReturnData::from_protobuf_to_js_type(env, option_inner_value))
        .transpose()?,
      return_data_none: Ok::<_, napi::Error>(value.return_data_none)?,
      compute_units_consumed: value
        .compute_units_consumed
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value.to_string()))
        .transpose()?,
      cost_units: value
        .cost_units
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value.to_string()))
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsTransactionError<'env> {
  pub err: BufferSlice<'env>,
}
impl<'env> JsTransactionError<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: TransactionError) -> napi::Result<Self> {
    Ok(Self {
      err: BufferSlice::copy_from(env, &value.err)?,
    })
  }
}
#[napi(object)]
pub struct JsInnerInstructions<'env> {
  pub index: u32,
  pub instructions: ::prost::alloc::vec::Vec<JsInnerInstruction<'env>>,
}
impl<'env> JsInnerInstructions<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: InnerInstructions) -> napi::Result<Self> {
    Ok(Self {
      index: Ok::<_, napi::Error>(value.index)?,
      instructions: value
        .instructions
        .into_iter()
        .map(|vec_inner_value| JsInnerInstruction::from_protobuf_to_js_type(env, vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
    })
  }
}
#[napi(object)]
pub struct JsInnerInstruction<'env> {
  pub program_id_index: u32,
  pub accounts: BufferSlice<'env>,
  pub data: BufferSlice<'env>,
  pub stack_height: ::core::option::Option<u32>,
}
impl<'env> JsInnerInstruction<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: InnerInstruction) -> napi::Result<Self> {
    Ok(Self {
      program_id_index: Ok::<_, napi::Error>(value.program_id_index)?,
      accounts: BufferSlice::copy_from(env, &value.accounts)?,
      data: BufferSlice::copy_from(env, &value.data)?,
      stack_height: value
        .stack_height
        .map(|option_inner_value| Ok::<_, napi::Error>(option_inner_value))
        .transpose()?,
    })
  }
}
#[napi(object)]
pub struct JsCompiledInstruction<'env> {
  pub program_id_index: u32,
  pub accounts: BufferSlice<'env>,
  pub data: BufferSlice<'env>,
}
impl<'env> JsCompiledInstruction<'env> {
  pub fn from_protobuf_to_js_type(
    env: &'env Env,
    value: CompiledInstruction,
  ) -> napi::Result<Self> {
    Ok(Self {
      program_id_index: Ok::<_, napi::Error>(value.program_id_index)?,
      accounts: BufferSlice::copy_from(env, &value.accounts)?,
      data: BufferSlice::copy_from(env, &value.data)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsTokenBalance {
  pub account_index: u32,
  pub mint: ::prost::alloc::string::String,
  pub ui_token_amount: ::core::option::Option<JsUiTokenAmount>,
  pub owner: ::prost::alloc::string::String,
  pub program_id: ::prost::alloc::string::String,
}
impl JsTokenBalance {
  pub fn from_protobuf_to_js_type(env: &Env, value: TokenBalance) -> napi::Result<Self> {
    Ok(Self {
      account_index: Ok::<_, napi::Error>(value.account_index)?,
      mint: Ok::<_, napi::Error>(value.mint)?,
      ui_token_amount: value
        .ui_token_amount
        .map(|option_inner_value| {
          JsUiTokenAmount::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
      owner: Ok::<_, napi::Error>(value.owner)?,
      program_id: Ok::<_, napi::Error>(value.program_id)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsUiTokenAmount {
  pub ui_amount: f64,
  pub decimals: u32,
  pub amount: ::prost::alloc::string::String,
  pub ui_amount_string: ::prost::alloc::string::String,
}
impl JsUiTokenAmount {
  pub fn from_protobuf_to_js_type(env: &Env, value: UiTokenAmount) -> napi::Result<Self> {
    Ok(Self {
      ui_amount: Ok::<_, napi::Error>(value.ui_amount)?,
      decimals: Ok::<_, napi::Error>(value.decimals)?,
      amount: Ok::<_, napi::Error>(value.amount)?,
      ui_amount_string: Ok::<_, napi::Error>(value.ui_amount_string)?,
    })
  }
}
#[napi(object)]
pub struct JsReturnData<'env> {
  pub program_id: BufferSlice<'env>,
  pub data: BufferSlice<'env>,
}
impl<'env> JsReturnData<'env> {
  pub fn from_protobuf_to_js_type(env: &'env Env, value: ReturnData) -> napi::Result<Self> {
    Ok(Self {
      program_id: BufferSlice::copy_from(env, &value.program_id)?,
      data: BufferSlice::copy_from(env, &value.data)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsReward {
  pub pubkey: ::prost::alloc::string::String,
  pub lamports: String,
  pub post_balance: String,
  pub reward_type: i32,
  pub commission: ::prost::alloc::string::String,
}
impl JsReward {
  pub fn from_protobuf_to_js_type(env: &Env, value: Reward) -> napi::Result<Self> {
    Ok(Self {
      pubkey: Ok::<_, napi::Error>(value.pubkey)?,
      lamports: Ok::<_, napi::Error>(value.lamports.to_string())?,
      post_balance: Ok::<_, napi::Error>(value.post_balance.to_string())?,
      reward_type: Ok::<_, napi::Error>(value.reward_type)?,
      commission: Ok::<_, napi::Error>(value.commission)?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsRewards {
  pub rewards: ::prost::alloc::vec::Vec<JsReward>,
  pub num_partitions: ::core::option::Option<JsNumPartitions>,
}
impl JsRewards {
  pub fn from_protobuf_to_js_type(env: &Env, value: Rewards) -> napi::Result<Self> {
    Ok(Self {
      rewards: value
        .rewards
        .into_iter()
        .map(|vec_inner_value| JsReward::from_protobuf_to_js_type(env, vec_inner_value))
        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()?,
      num_partitions: value
        .num_partitions
        .map(|option_inner_value| {
          JsNumPartitions::from_protobuf_to_js_type(env, option_inner_value)
        })
        .transpose()?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsUnixTimestamp {
  pub timestamp: String,
}
impl JsUnixTimestamp {
  pub fn from_protobuf_to_js_type(env: &Env, value: UnixTimestamp) -> napi::Result<Self> {
    Ok(Self {
      timestamp: Ok::<_, napi::Error>(value.timestamp.to_string())?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsBlockHeight {
  pub block_height: String,
}
impl JsBlockHeight {
  pub fn from_protobuf_to_js_type(env: &Env, value: BlockHeight) -> napi::Result<Self> {
    Ok(Self {
      block_height: Ok::<_, napi::Error>(value.block_height.to_string())?,
    })
  }
}
#[napi(object)]
#[derive(Debug, Clone)]
pub struct JsNumPartitions {
  pub num_partitions: String,
}
impl JsNumPartitions {
  pub fn from_protobuf_to_js_type(env: &Env, value: NumPartitions) -> napi::Result<Self> {
    Ok(Self {
      num_partitions: Ok::<_, napi::Error>(value.num_partitions.to_string())?,
    })
  }
}
