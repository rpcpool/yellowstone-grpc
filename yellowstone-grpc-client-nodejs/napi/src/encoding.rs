use {
  napi_derive::napi,
  solana_transaction_status::{TransactionWithStatusMeta, UiTransactionEncoding},
  yellowstone_grpc_proto::{
    convert_from,
    prelude::{SubscribeUpdateTransactionInfo, TransactionError as TransactionErrorProto},
    prost::Message,
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

  if let TransactionWithStatusMeta::Complete(tx) =
    convert_from::create_tx_with_meta(tx).map_err(|e| napi::Error::from_reason(e.to_string()))?
  {
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
  match convert_from::create_tx_error(Some(&TransactionErrorProto { err })) {
    Ok(Some(err)) => serde_json::to_string(&err).map_err(Into::into),
    Ok(None) => Err(napi::Error::from_reason("unexpected")),
    Err(error) => Err(napi::Error::from_reason(error)),
  }
}
