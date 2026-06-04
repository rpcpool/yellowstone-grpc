use {
  napi::bindgen_prelude::{Buffer, Result},
  napi_derive::napi,
  prost::Message,
  solana_pubkey::Pubkey,
  std::sync::Mutex,
  yellowstone_grpc_proto::{
    cuckoo::CompressedAccountFilterSet as NativeCompressedAccountFilterSet,
    geyser::CuckooFilter as ProtoCuckooFilter,
  },
};

fn to_napi_cause(status: napi::Status, source: &dyn std::error::Error) -> napi::Error {
  let mut cause = napi::Error::new(status, source.to_string());
  if let Some(next) = source.source() {
    cause.set_cause(to_napi_cause(status, next));
  }
  cause
}

fn napi_error_with_cause(
  status: napi::Status,
  reason: impl Into<String>,
  cause: &dyn std::error::Error,
) -> napi::Error {
  let mut error = napi::Error::new(status, reason.into());
  error.set_cause(to_napi_cause(status, cause));
  error
}

fn napi_error(status: napi::Status, reason: impl Into<String>) -> napi::Error {
  let reason = reason.into();
  let mut error = napi::Error::new(status, reason.clone());
  error.set_cause(napi::Error::new(status, reason));
  error
}

fn parse_pubkey_bytes(bytes: &[u8]) -> Result<Pubkey> {
  let pubkey_bytes: [u8; 32] = bytes.try_into().map_err(|_| {
    napi_error(
      napi::Status::InvalidArg,
      format!(
        "invalid pubkey bytes: expected 32 bytes, got {}",
        bytes.len()
      ),
    )
  })?;

  Ok(Pubkey::new_from_array(pubkey_bytes))
}

fn parse_pubkey_string(pubkey: &str) -> Result<Pubkey> {
  let bytes = bs58::decode(pubkey).into_vec().map_err(|error| {
    napi_error_with_cause(
      napi::Status::InvalidArg,
      "invalid pubkey: expected base58-encoded 32-byte public key",
      &error,
    )
  })?;

  parse_pubkey_bytes(&bytes)
}

#[napi]
pub struct CompressedAccountFilterSet {
  inner: Mutex<NativeCompressedAccountFilterSet>,
}

#[napi]
impl CompressedAccountFilterSet {
  #[napi(constructor)]
  pub fn new(max_capacity: u32) -> Result<Self> {
    let inner =
      NativeCompressedAccountFilterSet::with_capacity(max_capacity as usize).map_err(|error| {
        napi_error_with_cause(
          napi::Status::InvalidArg,
          "failed to build compressed account filter set",
          &error,
        )
      })?;

    Ok(Self {
      inner: Mutex::new(inner),
    })
  }

  #[napi]
  pub fn insert(&self, pubkey: String) -> Result<bool> {
    self.insert_pubkey(parse_pubkey_string(&pubkey)?)
  }

  #[napi]
  pub fn insert_bytes(&self, pubkey: Buffer) -> Result<bool> {
    self.insert_pubkey(parse_pubkey_bytes(pubkey.as_ref())?)
  }

  #[napi]
  pub fn remove(&self, pubkey: String) -> Result<bool> {
    self.remove_pubkey(parse_pubkey_string(&pubkey)?)
  }

  #[napi]
  pub fn remove_bytes(&self, pubkey: Buffer) -> Result<bool> {
    self.remove_pubkey(parse_pubkey_bytes(pubkey.as_ref())?)
  }

  #[napi]
  pub fn contains(&self, pubkey: String) -> Result<bool> {
    self.contains_pubkey(parse_pubkey_string(&pubkey)?)
  }

  #[napi]
  pub fn contains_bytes(&self, pubkey: Buffer) -> Result<bool> {
    self.contains_pubkey(parse_pubkey_bytes(pubkey.as_ref())?)
  }

  #[napi]
  pub fn len(&self) -> Result<u32> {
    let filter = self.lock_filter()?;
    u32::try_from(filter.len()).map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "filter length exceeds u32",
        &error,
      )
    })
  }

  #[napi]
  pub fn capacity(&self) -> Result<u32> {
    let filter = self.lock_filter()?;
    u32::try_from(filter.capacity()).map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "filter capacity exceeds u32",
        &error,
      )
    })
  }

  #[napi]
  pub fn is_empty(&self) -> Result<bool> {
    let filter = self.lock_filter()?;
    Ok(filter.is_empty())
  }

  #[napi]
  pub fn is_dirty(&self) -> Result<bool> {
    let filter = self.lock_filter()?;
    Ok(filter.is_dirty())
  }

  #[napi]
  pub fn take_dirty(&self) -> Result<bool> {
    let mut filter = self.lock_filter()?;
    Ok(filter.take_dirty())
  }

  #[napi]
  pub fn to_proto(&self) -> Result<Buffer> {
    Ok(Buffer::from(self.to_proto_message()?.encode_to_vec()))
  }

  #[napi]
  pub fn to_account_filter(&self) -> Result<Buffer> {
    let filter = self.lock_filter()?;
    Ok(Buffer::from(filter.to_account_filter().encode_to_vec()))
  }

  #[napi]
  pub fn to_block_filter(&self) -> Result<Buffer> {
    let filter = self.lock_filter()?;
    Ok(Buffer::from(filter.to_block_filter().encode_to_vec()))
  }
}

impl CompressedAccountFilterSet {
  fn lock_filter(&self) -> Result<std::sync::MutexGuard<'_, NativeCompressedAccountFilterSet>> {
    self.inner.lock().map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "failed to acquire compressed account filter lock",
        &error,
      )
    })
  }

  fn insert_pubkey(&self, pubkey: Pubkey) -> Result<bool> {
    let mut filter = self.lock_filter()?;
    filter.insert(pubkey).map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "compressed account filter is full; rebuild with a larger maxCapacity",
        &error,
      )
    })
  }

  fn remove_pubkey(&self, pubkey: Pubkey) -> Result<bool> {
    let mut filter = self.lock_filter()?;
    Ok(filter.remove(pubkey))
  }

  fn contains_pubkey(&self, pubkey: Pubkey) -> Result<bool> {
    let filter = self.lock_filter()?;
    Ok(filter.contains(pubkey))
  }

  fn to_proto_message(&self) -> Result<ProtoCuckooFilter> {
    let filter = self.lock_filter()?;
    Ok(filter.to_proto())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn key(seed: u8) -> Vec<u8> {
    vec![seed; 32]
  }

  #[test]
  fn insert_contains_and_remove_bytes_are_exact() {
    let filter = CompressedAccountFilterSet::new(10).unwrap();

    assert!(filter.insert_bytes(Buffer::from(key(1))).unwrap());
    assert!(!filter.insert_bytes(Buffer::from(key(1))).unwrap());
    assert!(filter.contains_bytes(Buffer::from(key(1))).unwrap());
    assert!(!filter.contains_bytes(Buffer::from(key(2))).unwrap());
    assert_eq!(filter.len().unwrap(), 1);

    assert!(filter.remove_bytes(Buffer::from(key(1))).unwrap());
    assert!(!filter.contains_bytes(Buffer::from(key(1))).unwrap());
    assert_eq!(filter.len().unwrap(), 0);
  }

  #[test]
  fn remove_missing_pubkey_does_not_mutate_existing_member() {
    let filter = CompressedAccountFilterSet::new(10).unwrap();

    filter.insert_bytes(Buffer::from(key(1))).unwrap();

    assert!(!filter.remove_bytes(Buffer::from(key(2))).unwrap());
    assert!(filter.contains_bytes(Buffer::from(key(1))).unwrap());
  }

  #[test]
  fn dirty_flag_tracks_successful_mutations() {
    let filter = CompressedAccountFilterSet::new(10).unwrap();

    assert!(!filter.is_dirty().unwrap());
    assert!(!filter.take_dirty().unwrap());

    filter.insert_bytes(Buffer::from(key(1))).unwrap();
    assert!(filter.is_dirty().unwrap());
    assert!(filter.take_dirty().unwrap());
    assert!(!filter.is_dirty().unwrap());

    assert!(!filter.insert_bytes(Buffer::from(key(1))).unwrap());
    assert!(!filter.is_dirty().unwrap());

    assert!(filter.remove_bytes(Buffer::from(key(1))).unwrap());
    assert!(filter.take_dirty().unwrap());
  }

  #[test]
  fn rejects_wrong_length_pubkey_bytes() {
    let filter = CompressedAccountFilterSet::new(10).unwrap();
    let error = filter
      .insert_bytes(Buffer::from(vec![1; 31]))
      .expect_err("short pubkey should be rejected");

    assert!(
      error.to_string().contains("expected 32 bytes"),
      "unexpected error: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on validation error");
  }

  #[test]
  fn accepts_base58_pubkey_strings() {
    let filter = CompressedAccountFilterSet::new(10).unwrap();
    let encoded = bs58::encode(key(1)).into_string();

    assert!(filter.insert(encoded.clone()).unwrap());
    assert!(filter.contains(encoded.clone()).unwrap());
    assert!(filter.remove(encoded).unwrap());
  }

  #[test]
  fn rejects_invalid_base58_pubkey_strings() {
    let filter = CompressedAccountFilterSet::new(10).unwrap();
    let error = filter
      .insert("not a base58 pubkey".to_string())
      .expect_err("invalid base58 pubkey should be rejected");

    assert!(
      error.to_string().contains("invalid pubkey"),
      "unexpected error: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on base58 error");
  }

  #[test]
  fn proto_output_carries_cuckoo_metadata() {
    let filter = CompressedAccountFilterSet::new(10).unwrap();
    filter.insert_bytes(Buffer::from(key(1))).unwrap();

    let proto = filter.to_proto_message().unwrap();

    assert!(!proto.data.is_empty());
    assert!(proto.bucket_count.is_power_of_two());
    assert_eq!(proto.entries_per_bucket, 4);
    assert_eq!(proto.fingerprint_bits, 16);
    assert_eq!(
      proto.hash_algorithm,
      yellowstone_grpc_proto::geyser::CuckooHashAlgorithm::SipHash as i32
    );
  }
}
