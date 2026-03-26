use {
  napi_derive::napi,
  prost011::Message as Prost11Message,
  serde::Serialize,
  solana_storage_proto::convert::generated::{
    Transaction as StorageTransaction, TransactionStatusMeta as StorageTransactionStatusMeta,
  },
  solana_transaction::versioned::VersionedTransaction,
  solana_transaction_error::TransactionError as TransactionErrorSolana,
  solana_transaction_status::{
    Encodable, EncodedTransaction, TransactionWithStatusMeta, UiTransactionEncoding,
    VersionedTransactionWithStatusMeta,
  },
  std::panic::{catch_unwind, AssertUnwindSafe},
  yellowstone_grpc_proto::{
    prelude::{SubscribeUpdateDeshredTransactionInfo, SubscribeUpdateTransactionInfo},
    prost::Message as Prost14Message,
  },
};

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
  let tx = SubscribeUpdateTransactionInfo::decode(data)
    .map_err(|e| napi::Error::from_reason(e.to_string()))?;

  let transaction_proto = tx
    .transaction
    .ok_or_else(|| napi::Error::from_reason("failed to get transaction payload"))?;
  let transaction =
    <StorageTransaction as Prost11Message>::decode(transaction_proto.encode_to_vec().as_slice())
      .map_err(|e| {
        napi::Error::from_reason(format!("failed to decode transaction payload: {e}"))
      })?;
  let transaction = catch_unwind(AssertUnwindSafe(|| transaction.into()))
    .map_err(|_| napi::Error::from_reason("failed to decode transaction payload"))?;
  let meta_proto = tx
    .meta
    .ok_or_else(|| napi::Error::from_reason("failed to get transaction meta"))?;
  let meta =
    <StorageTransactionStatusMeta as Prost11Message>::decode(meta_proto.encode_to_vec().as_slice())
      .map_err(|e| napi::Error::from_reason(format!("failed to decode transaction meta: {e}")))?
      .try_into()
      .map_err(|e| napi::Error::from_reason(format!("failed to decode transaction meta: {e}")))?;

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
        .map_err(|e| napi::Error::from_reason(e.to_string()))?,
    )
    .map_err(|e| napi::Error::from_reason(e.to_string()))
  } else {
    Err(napi::Error::from_reason("tx with missing metadata"))
  }
}

#[napi]
pub fn decode_tx_error(err: Vec<u8>) -> napi::Result<String> {
  let tx_error = catch_unwind(AssertUnwindSafe(|| {
    wincode::deserialize::<TransactionErrorSolana>(&err)
  }))
  .map_err(|_| napi::Error::from_reason("failed to decode TransactionError"))?
  .map_err(|e| napi::Error::from_reason(format!("failed to decode TransactionError: {e}")))?;

  serde_json::to_string(&tx_error).map_err(Into::into)
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
  let tx = SubscribeUpdateDeshredTransactionInfo::decode(data)
    .map_err(|e| napi::Error::from_reason(e.to_string()))?;

  let transaction_proto = tx
    .transaction
    .ok_or_else(|| napi::Error::from_reason("failed to get deshred transaction payload"))?;
  let transaction =
    <StorageTransaction as Prost11Message>::decode(transaction_proto.encode_to_vec().as_slice())
      .map_err(|e| {
        napi::Error::from_reason(format!("failed to decode deshred transaction payload: {e}"))
      })?;
  let transaction: VersionedTransaction = catch_unwind(AssertUnwindSafe(|| transaction.into()))
    .map_err(|_| napi::Error::from_reason("failed to decode deshred transaction payload"))?;

  let encoded = EncodedDeshredTransaction {
    signature: bs58::encode(tx.signature).into_string(),
    is_vote: tx.is_vote,
    transaction: transaction.encode(encoding.into()),
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

  serde_json::to_string(&encoded).map_err(Into::into)
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
