use {
  napi::Status,
  napi_derive::napi,
  prost011::Message as Prost11Message,
  serde::Serialize,
  solana_storage_proto::convert::generated::{
    Transaction as StorageTransaction, TransactionStatusMeta as StorageTransactionStatusMeta,
  },
  solana_transaction::versioned::VersionedTransaction,
  solana_transaction_error::TransactionError as TransactionErrorSolana,
  solana_transaction_status::{
    EncodableWithMeta, EncodedTransaction, TransactionWithStatusMeta, UiTransactionEncoding,
    VersionedTransactionWithStatusMeta,
  },
  yellowstone_grpc_proto::{
    prelude::{SubscribeUpdateDeshredTransactionInfo, SubscribeUpdateTransactionInfo},
    prost::Message as Prost14Message,
  },
};

fn to_napi_cause(status: Status, source: &dyn std::error::Error) -> napi::Error {
  let mut cause = napi::Error::new(status, source.to_string());
  if let Some(next) = source.source() {
    cause.set_cause(to_napi_cause(status, next));
  }
  cause
}

fn napi_error_with_cause(
  status: Status,
  reason: impl Into<String>,
  cause: &dyn std::error::Error,
) -> napi::Error {
  let mut error = napi::Error::new(status, reason.into());
  error.set_cause(to_napi_cause(status, cause));
  error
}

#[napi]
#[derive(Debug, Clone, Copy)]
pub enum WasmUiTransactionEncoding {
  Binary = 0,
  Base64 = 1,
  Base58 = 2,
  Json = 3,
  JsonParsed = 4,
}

#[napi]
impl From<WasmUiTransactionEncoding> for UiTransactionEncoding {
  fn from(encoding: WasmUiTransactionEncoding) -> Self {
    match encoding {
      WasmUiTransactionEncoding::Binary => UiTransactionEncoding::Binary,
      WasmUiTransactionEncoding::Base64 => UiTransactionEncoding::Base64,
      WasmUiTransactionEncoding::Base58 => UiTransactionEncoding::Base58,
      WasmUiTransactionEncoding::Json => UiTransactionEncoding::Json,
      WasmUiTransactionEncoding::JsonParsed => UiTransactionEncoding::JsonParsed,
    }
  }
}

#[napi]
pub fn encode_tx(
  data: &[u8],
  encoding: WasmUiTransactionEncoding,
  max_supported_transaction_version: Option<u8>,
  show_rewards: bool,
) -> napi::Result<String> {
  let tx = SubscribeUpdateTransactionInfo::decode(data).map_err(|error| {
    napi_error_with_cause(
      Status::InvalidArg,
      "failed to decode SubscribeUpdateTransactionInfo payload",
      &error,
    )
  })?;

  let transaction_proto = tx
    .transaction
    .ok_or_else(|| napi::Error::from_reason("failed to get transaction payload"))?;
  let transaction =
    <StorageTransaction as Prost11Message>::decode(transaction_proto.encode_to_vec().as_slice())
      .map_err(|error| {
        napi_error_with_cause(
          Status::InvalidArg,
          "failed to decode transaction payload",
          &error,
        )
      })?;
  let transaction = transaction.into();
  let meta_proto = tx
    .meta
    .ok_or_else(|| napi::Error::from_reason("failed to get transaction meta"))?;
  let meta =
    <StorageTransactionStatusMeta as Prost11Message>::decode(meta_proto.encode_to_vec().as_slice())
      .map_err(|error| {
        napi_error_with_cause(
          Status::InvalidArg,
          "failed to decode transaction meta",
          &error,
        )
      })?
      .try_into()
      .map_err(|error| {
        napi_error_with_cause(
          Status::InvalidArg,
          "failed to decode transaction meta",
          &error,
        )
      })?;

  let tx_with_meta =
    TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta { transaction, meta });

  if let TransactionWithStatusMeta::Complete(tx) = tx_with_meta {
    serde_json::to_string(
      &tx
        .encode(
          encoding.into(),
          max_supported_transaction_version,
          show_rewards,
        )
        .map_err(|error| {
          napi_error_with_cause(
            Status::InvalidArg,
            "failed to encode transaction with selected encoding",
            &error,
          )
        })?,
    )
    .map_err(|error| {
      napi_error_with_cause(
        Status::InvalidArg,
        "failed to serialize encoded transaction as JSON",
        &error,
      )
    })
  } else {
    Err(napi::Error::from_reason("tx with missing metadata"))
  }
}

#[napi]
pub fn decode_tx_error(err: Vec<u8>) -> napi::Result<String> {
  let tx_error = wincode::deserialize::<TransactionErrorSolana>(&err).map_err(|error| {
    napi_error_with_cause(
      Status::InvalidArg,
      "failed to decode TransactionError",
      &error,
    )
  })?;

  serde_json::to_string(&tx_error).map_err(|error| {
    napi_error_with_cause(
      Status::InvalidArg,
      "failed to serialize TransactionError as JSON",
      &error,
    )
  })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EncodedDeshredTransaction {
  signature: String,
  is_vote: bool,
  transaction: EncodedTransaction,
  loaded_writable_addresses: Vec<String>,
  loaded_readonly_addresses: Vec<String>,
}

#[napi]
pub fn encode_deshred_tx(data: &[u8], encoding: WasmUiTransactionEncoding) -> napi::Result<String> {
  let tx = SubscribeUpdateDeshredTransactionInfo::decode(data).map_err(|error| {
    napi_error_with_cause(
      Status::InvalidArg,
      "failed to decode SubscribeUpdateDeshredTransactionInfo payload",
      &error,
    )
  })?;

  let transaction_proto = tx
    .transaction
    .ok_or_else(|| napi::Error::from_reason("failed to get deshred transaction payload"))?;
  let transaction =
    <StorageTransaction as Prost11Message>::decode(transaction_proto.encode_to_vec().as_slice())
      .map_err(|error| {
        napi_error_with_cause(
          Status::InvalidArg,
          "failed to decode deshred transaction payload",
          &error,
        )
      })?;
  let transaction: VersionedTransaction = transaction.into();

  let meta: solana_transaction_status::TransactionStatusMeta = StorageTransactionStatusMeta {
    loaded_writable_addresses: tx.loaded_writable_addresses.clone(),
    loaded_readonly_addresses: tx.loaded_readonly_addresses.clone(),
    inner_instructions_none: true,
    log_messages_none: true,
    return_data_none: true,
    ..StorageTransactionStatusMeta::default()
  }
  .try_into()
  .map_err(|error| {
    napi_error_with_cause(
      Status::InvalidArg,
      "failed to build deshred transaction status meta",
      &error,
    )
  })?;

  let encoded = EncodedDeshredTransaction {
    signature: bs58::encode(tx.signature).into_string(),
    is_vote: tx.is_vote,
    transaction: transaction.encode_with_meta(encoding.into(), &meta),
    loaded_writable_addresses: tx
      .loaded_writable_addresses
      .into_iter()
      .map(|address| bs58::encode(address).into_string())
      .collect(),
    loaded_readonly_addresses: tx
      .loaded_readonly_addresses
      .into_iter()
      .map(|address| bs58::encode(address).into_string())
      .collect(),
  };

  serde_json::to_string(&encoded).map_err(|error| {
    napi_error_with_cause(
      Status::InvalidArg,
      "failed to serialize encoded deshred transaction as JSON",
      &error,
    )
  })
}

#[cfg(test)]
mod tests {
  use super::{encode_deshred_tx, WasmUiTransactionEncoding};
  use yellowstone_grpc_proto::{
    prelude::SubscribeUpdateDeshredTransactionInfo, prost::Message as Prost14Message,
  };

  #[test]
  fn encode_deshred_tx_rejects_invalid_bytes_payload() {
    let error = encode_deshred_tx(&[0xFF, 0x00, 0xAA], WasmUiTransactionEncoding::Json)
      .expect_err("invalid protobuf bytes should be rejected");

    assert!(
      error.to_string().to_lowercase().contains("decode"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for protobuf decode error"
    );
  }

  #[test]
  fn encode_deshred_tx_rejects_missing_transaction_payload() {
    let data = SubscribeUpdateDeshredTransactionInfo {
      signature: vec![1, 2, 3],
      is_vote: false,
      transaction: None,
      loaded_writable_addresses: Vec::new(),
      loaded_readonly_addresses: Vec::new(),
    }
    .encode_to_vec();

    let error = encode_deshred_tx(&data, WasmUiTransactionEncoding::Json)
      .expect_err("missing transaction payload should be rejected");

    assert!(
      error
        .to_string()
        .contains("failed to get deshred transaction payload"),
      "unexpected error message: {error}"
    );
  }
}
