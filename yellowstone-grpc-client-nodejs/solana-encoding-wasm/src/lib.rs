use {
    solana_transaction_status::{TransactionWithStatusMeta, UiTransactionEncoding},
    wasm_bindgen::prelude::*,
    yellowstone_grpc_proto::{
        convert_from,
        prelude::{SubscribeUpdateTransactionInfo, TransactionError as TransactionErrorProto},
        prost::Message,
    },
};

#[wasm_bindgen]
#[derive(Debug, Clone, Copy)]
pub enum WasmUiTransactionEncoding {
    Binary = 0,
    Base64 = 1,
    Base58 = 2,
    Json = 3,
    JsonParsed = 4,
}

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

#[wasm_bindgen]
pub fn encode_tx(
    data: &[u8],
    encoding: WasmUiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
    show_rewards: bool,
) -> Result<String, JsError> {
    let tx = SubscribeUpdateTransactionInfo::decode(data)?;
    if let TransactionWithStatusMeta::Complete(tx) =
        convert_from::create_tx_with_meta(tx).map_err(JsError::new)?
    {
        serde_json::to_string(&tx.encode(
            encoding.into(),
            max_supported_transaction_version,
            show_rewards,
        )?)
        .map_err(Into::into)
    } else {
        Err(JsError::new("tx with missing metadata"))
    }
}

#[wasm_bindgen]
pub fn decode_tx_error(err: Vec<u8>) -> Result<String, JsError> {
    match convert_from::create_tx_error(Some(&TransactionErrorProto { err })) {
        Ok(Some(err)) => serde_json::to_string(&err).map_err(Into::into),
        Ok(None) => Err(JsError::new("unexpected")),
        Err(error) => Err(JsError::new(error)),
    }
}
