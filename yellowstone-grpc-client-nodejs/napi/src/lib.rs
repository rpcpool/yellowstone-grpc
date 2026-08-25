//! N-API stream engine for Yellowstone subscriptions.
//!
//! This module exposes a `DuplexStream` type used by the JS SDK wrapper.
//! The Rust side owns the gRPC subscribe task and bridges:
//! - JS writes (`SubscribeRequest`) -> gRPC sink
//! - gRPC stream (`SubscribeUpdate`) -> JS reads
//!
//! Design goals:
//! - Keep JS-facing API small and stable (`read` / `write_raw`)
//! - Pass protobuf payload bytes over the N-API boundary
//! - Stop worker tasks deterministically when JS drops stream handles
mod bindings;
mod client;
mod cuckoo;
mod encoding;
mod subscribe_request_validation;
mod utils;

use futures::{future::poll_fn, Sink, Stream, TryStream, TryStreamExt};
use futures_util::{SinkExt, StreamExt};
use napi::{bindgen_prelude::*, Env};
use napi_derive::napi;
use prost::Message;
use std::{
  sync::{Arc, Mutex as StdMutex, Once},
  task::Poll,
};
use yellowstone_grpc_client::{
  GeyserStream, SubscribeDeshredRequestSink, SubscribeDeshredStream, SubscribeRequestSink,
};
use yellowstone_grpc_proto::prelude::*;

use crate::{client::GrpcClient, subscribe_request_validation::validate_subscribe_request};

static INITIALIZE_CRYPTO_PROVIDER: Once = Once::new();

#[napi(js_name = "AUTORECONNECT_FILTER_KEY")]
pub const AUTORECONNECT_FILTER_KEY: &str = "__autoreconnect";

/// Initialize crypto provider once.
fn init_crypto_provider() {
  INITIALIZE_CRYPTO_PROVIDER.call_once(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
  });
}

fn to_napi_cause(status: napi::Status, source: &dyn std::error::Error) -> napi::Error {
  let mut cause = napi::Error::new(status, source.to_string());
  if let Some(next) = source.source() {
    cause.set_cause(to_napi_cause(status, next));
  }
  cause
}

fn capture_terminal_error(terminal_error: &Arc<StdMutex<Option<napi::Error>>>, error: napi::Error) {
  // First terminal failure wins: preserve the earliest causal error and avoid
  // replacing it with follow-up shutdown noise from the same worker.
  let mut error_guard = match terminal_error.lock() {
    Ok(guard) => guard,
    Err(poisoned) => poisoned.into_inner(),
  };
  if error_guard.is_none() {
    *error_guard = Some(error);
  }
}

fn get_terminal_error(terminal_error: &Arc<StdMutex<Option<napi::Error>>>) -> Option<napi::Error> {
  // Recover poisoned state and still return any stored terminal error so JS
  // observes the native failure instead of a silent EOF.
  let mut error_guard = match terminal_error.lock() {
    Ok(guard) => guard,
    Err(poisoned) => poisoned.into_inner(),
  };
  error_guard.take()
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

///
/// Stream decorator that encodes each item to protobuf bytes on `poll_next()`.
struct ProtoEncodedSt<St> {
  wrapped: St,
}

impl<St> Stream for ProtoEncodedSt<St>
where
  St: TryStream + Unpin,
  St::Ok: prost::Message + Send + 'static,
  St::Error: std::error::Error + Send + Sync + 'static,
{
  type Item = std::result::Result<Vec<u8>, St::Error>;

  fn poll_next(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    let this = self.get_mut();

    match futures::ready!(this.wrapped.try_poll_next_unpin(cx)) {
      Some(result) => Poll::Ready(Some(result.map(|message| message.encode_to_vec()))),
      None => Poll::Ready(None),
    }
  }
}

///
/// Thread-safe wrapper around a [`Stream`] that can be cloned and polled from multiple threads.
/// Each clone shares the same underlying stream state.
///
struct SharedStream<St> {
  inner: Arc<StdMutex<St>>,
}

impl<St> Clone for SharedStream<St> {
  fn clone(&self) -> Self {
    Self {
      inner: Arc::clone(&self.inner),
    }
  }
}

impl<St> SharedStream<St> {
  fn new(inner: St) -> Self {
    Self {
      inner: Arc::new(StdMutex::new(inner)),
    }
  }
}

impl<St> Stream for SharedStream<St>
where
  St: Stream + Unpin,
{
  type Item = St::Item;

  fn poll_next(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    let this = self.get_mut();
    let mut guard = this.inner.lock().expect("state lock");
    guard.poll_next_unpin(cx)
  }
}
/// Shared engine behind `DuplexStream`/`DuplexStreamDeshred`.
///
/// Not itself `#[napi]` — napi-rs structs can't be generic — so each of
/// those wraps one concrete instantiation (`Sk` = its gRPC sink type, `St` =
/// its gRPC stream type) and forwards `read`/`close`/`write_raw` here.
struct DuplexStreamInner<Sk, St> {
  /// Read side consumed by `read()`. Polls the gRPC stream directly and
  /// encodes each update to protobuf bytes.
  readable: SharedStream<ProtoEncodedSt<St>>,
  /// Write side used by `write_raw()`. Requests are sent directly to the
  /// gRPC sink.
  ///
  /// The mutex protects a close-state transition, not sender sharing:
  /// - `close()` sets the state to `None` (revokes future writes).
  /// - `write_raw()` reads/clones under the same lock.
  ///
  /// The sink being cheap-`Clone` is true, but clone alone does not provide
  /// an atomic "disable writes now" transition.
  writable: Arc<StdMutex<Option<Sk>>>,
  /// Terminal error captured from a gRPC send failure.
  ///
  /// `read()` surfaces this to JS once the gRPC stream ends.
  terminal_error: Arc<StdMutex<Option<napi::Error>>>,
}

impl<Sk, St> DuplexStreamInner<Sk, St> {
  fn new(sink: Sk, stream: St) -> Self {
    Self {
      readable: SharedStream::new(ProtoEncodedSt { wrapped: stream }),
      writable: Arc::new(StdMutex::new(Some(sink))),
      terminal_error: Arc::new(StdMutex::new(None)),
    }
  }

  /// Close the stream and reject future writes. "Failed to acquire writable
  /// lock" is identical wording in both wrappers, so this needs no
  /// per-type message parameter.
  fn close(&self) -> Result<()> {
    let mut writable_guard = self.writable.lock().map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "Failed to acquire writable lock",
        &error,
      )
    })?;
    // Dropping the last sink closes the sender's side of the channel it
    // wraps, which signals the server the client is done sending. `writable`
    // being `None` *is* the closed state — there is no separate flag to
    // desync from it.
    *writable_guard = None;

    Ok(())
  }

  /// Synchronous close-state guard: clones out the sink to send on, or
  /// rejects if the stream is closed. No `Env` needed, so this stays
  /// directly unit-testable. `closed_message` lets each wrapper keep its own
  /// exact wording.
  fn take_sink_for_write(&self, closed_message: &str) -> Result<Sk>
  where
    Sk: Clone,
  {
    self
      .writable
      .lock()
      .map_err(|error| {
        napi_error_with_cause(
          napi::Status::GenericFailure,
          "Failed to acquire writable lock",
          &error,
        )
      })?
      .as_ref()
      .cloned()
      .ok_or_else(|| napi_error(napi::Status::GenericFailure, closed_message))
  }

  async fn send_subscribe_request<Req>(
    mut sink: Sk,
    request: Req,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
    failure_message: &str,
  ) -> Result<()>
  where
    Sk: Sink<Req> + Unpin,
    Sk::Error: std::error::Error + Send + Sync + 'static,
  {
    sink.send(request).await.map_err(|error| {
      capture_terminal_error(
        &terminal_error,
        napi_error_with_cause(napi::Status::GenericFailure, failure_message, &error),
      );
      napi_error_with_cause(napi::Status::GenericFailure, failure_message, &error)
    })
  }

  async fn recv_update_or_error(
    mut readable: SharedStream<ProtoEncodedSt<St>>,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
    failure_message: &str,
  ) -> Result<Option<Vec<u8>>>
  where
    St: TryStream + Unpin,
    St::Ok: prost::Message + Send + 'static,
    St::Error: std::error::Error + Send + Sync + 'static,
  {
    let read_fut = poll_fn(|cx| readable.poll_next_unpin(cx));
    match read_fut.await {
      Some(Ok(update_bytes)) => Ok(Some(update_bytes)),
      Some(Err(status)) => Err(napi_error_with_cause(
        napi::Status::GenericFailure,
        failure_message,
        &status,
      )),
      // Stream end. If a send failure captured a terminal error first,
      // surface that instead of a silent graceful EOF.
      None => match get_terminal_error(&terminal_error) {
        Some(error) => Err(error),
        None => Ok(None),
      },
    }
  }
}

/// DuplexStream Engine
///
/// The inner engine for a custom implementation of stream.Duplex
/// on the JS runtime.
///
/// This is not meant to be directly interacted with by the user
/// rather an underlying stream implementation where stream.Duplex
/// will `_read()` from and `_write()` to.
#[napi]
struct DuplexStream {
  inner: DuplexStreamInner<SubscribeRequestSink, GeyserStream>,
}

/// Opens a subscribe stream on `grpc_client` and assembles a `DuplexStream`
/// around it.
///
/// `DuplexStream` itself only knows how to read/write/close an already-open
/// stream; opening the gRPC connection is this factory's job, not the
/// struct's.
fn subscribe_duplex_stream<'env>(
  env: &'env Env,
  grpc_client: &GrpcClient,
  initial_request_bytes: Option<Buffer>,
) -> Result<PromiseRaw<'env, DuplexStream>> {
  let initial_request = match initial_request_bytes {
    Some(request_bytes) => {
      let request = SubscribeRequest::decode(request_bytes.as_ref()).map_err(|error| {
        napi_error_with_cause(
          napi::Status::InvalidArg,
          "invalid SubscribeRequest payload",
          &error,
        )
      })?;
      validate_subscribe_request(&request).map_err(|error| {
        napi_error_with_cause(napi::Status::InvalidArg, error.to_string(), &error)
      })?;
      Some(request)
    }
    None => None,
  };
  let mut client = grpc_client.client.clone();

  // Open the gRPC stream before returning to JS so connection/protocol errors
  // reject the Promise and bubble to TypeScript callers.
  env.spawn_future_with_callback(
    async move {
      let (stream_tx, stream_rx) = client
        .subscribe_with_request(initial_request)
        .await
        .map_err(|error| {
          napi_error_with_cause(
            napi::Status::GenericFailure,
            "failed to open subscribe stream",
            &error,
          )
        })?;

      Ok(DuplexStream {
        inner: DuplexStreamInner::new(stream_tx, stream_rx),
      })
    },
    move |_environment, stream| Ok(stream),
  )
}

#[napi]
impl DuplexStream {
  /// Read JS Accesspoint.
  ///
  /// Retrieve one encoded `SubscribeUpdate` payload from the gRPC stream.
  #[napi]
  #[allow(dead_code)]
  pub fn read<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, Option<Buffer>>> {
    let readable = self.inner.readable.clone();
    let terminal_error = self.inner.terminal_error.clone();

    env.spawn_future_with_callback(
      async move { Self::recv_update_or_error(readable, terminal_error).await },
      move |_environment, update_bytes_opt| Ok(update_bytes_opt.map(Buffer::from)),
    )
  }

  /// Close the stream and reject future writes.
  #[napi]
  #[allow(dead_code)]
  pub fn close(&self) -> Result<()> {
    self.inner.close()
  }

  #[napi]
  #[allow(dead_code)]
  pub fn write_raw<'env>(
    &self,
    env: &'env Env,
    request_bytes: Buffer,
  ) -> Result<PromiseRaw<'env, ()>> {
    let protobuf_subscribe_request = Self::decode_and_validate_subscribe_request(request_bytes)?;
    let sink = self.take_sink_for_write()?;
    let terminal_error = self.inner.terminal_error.clone();

    env.spawn_future_with_callback(
      Self::send_subscribe_request(sink, protobuf_subscribe_request, terminal_error),
      move |_environment, ()| Ok(()),
    )
  }

  /// Decode + validate a raw `SubscribeRequest` payload. No `Env` needed, so
  /// this stays directly unit-testable.
  fn decode_and_validate_subscribe_request(request_bytes: Buffer) -> Result<SubscribeRequest> {
    let protobuf_subscribe_request =
      SubscribeRequest::decode(request_bytes.as_ref()).map_err(|error| {
        napi_error_with_cause(
          napi::Status::InvalidArg,
          "invalid SubscribeRequest payload",
          &error,
        )
      })?;

    validate_subscribe_request(&protobuf_subscribe_request).map_err(|error| {
      napi_error_with_cause(napi::Status::InvalidArg, error.to_string(), &error)
    })?;

    Ok(protobuf_subscribe_request)
  }

  fn take_sink_for_write(&self) -> Result<SubscribeRequestSink> {
    self
      .inner
      .take_sink_for_write("Cannot write to a closed subscription stream")
  }

  async fn send_subscribe_request(
    sink: SubscribeRequestSink,
    protobuf_subscribe_request: SubscribeRequest,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  ) -> Result<()> {
    DuplexStreamInner::<SubscribeRequestSink, GeyserStream>::send_subscribe_request(
      sink,
      protobuf_subscribe_request,
      terminal_error,
      "subscribe stream send failed",
    )
    .await
  }

  async fn recv_update_or_error(
    readable: SharedStream<ProtoEncodedSt<GeyserStream>>,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  ) -> Result<Option<Vec<u8>>> {
    DuplexStreamInner::<SubscribeRequestSink, GeyserStream>::recv_update_or_error(
      readable,
      terminal_error,
      "subscribe stream receive failed",
    )
    .await
  }
}

/// DuplexStreamDeshred Engine.
///
/// Similar to `DuplexStream`, but targets the deshred pre-execution stream.
#[napi]
struct DuplexStreamDeshred {
  inner: DuplexStreamInner<SubscribeDeshredRequestSink, SubscribeDeshredStream>,
}

/// Opens a deshred subscribe stream on `grpc_client` and assembles a
/// `DuplexStreamDeshred` around it.
///
/// `DuplexStreamDeshred` itself only knows how to read/write/close an
/// already-open stream; opening the gRPC connection is this factory's job,
/// not the struct's.
fn subscribe_duplex_stream_deshred<'env>(
  env: &'env Env,
  grpc_client: &GrpcClient,
) -> Result<PromiseRaw<'env, DuplexStreamDeshred>> {
  let mut client = grpc_client.client.clone();

  // Open the gRPC stream before returning to JS so connection/protocol errors
  // (e.g. UNIMPLEMENTED) reject the Promise and bubble to TypeScript callers.
  env.spawn_future_with_callback(
    async move {
      let (stream_tx, stream_rx) = client.subscribe_deshred().await.map_err(|error| {
        napi_error_with_cause(
          napi::Status::GenericFailure,
          "failed to open deshred subscribe stream",
          &error,
        )
      })?;

      Ok(DuplexStreamDeshred {
        inner: DuplexStreamInner::new(stream_tx, stream_rx),
      })
    },
    move |_environment, stream| Ok(stream),
  )
}

#[napi]
impl DuplexStreamDeshred {
  /// Retrieve one encoded `SubscribeUpdateDeshred` payload.
  #[napi]
  #[allow(dead_code)]
  pub fn read<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, Option<Buffer>>> {
    let readable = self.inner.readable.clone();
    let terminal_error = self.inner.terminal_error.clone();

    env.spawn_future_with_callback(
      async move { Self::recv_update_or_error(readable, terminal_error).await },
      move |_environment, update_bytes_opt| Ok(update_bytes_opt.map(Buffer::from)),
    )
  }

  #[napi]
  #[allow(dead_code)]
  pub fn close(&self) -> Result<()> {
    self.inner.close()
  }

  #[napi]
  #[allow(dead_code)]
  pub fn write_raw<'env>(
    &self,
    env: &'env Env,
    request_bytes: Buffer,
  ) -> Result<PromiseRaw<'env, ()>> {
    let protobuf_subscribe_request = Self::decode_subscribe_deshred_request(request_bytes)?;
    let sink = self.take_sink_for_write()?;
    let terminal_error = self.inner.terminal_error.clone();

    env.spawn_future_with_callback(
      Self::send_subscribe_request(sink, protobuf_subscribe_request, terminal_error),
      move |_environment, ()| Ok(()),
    )
  }

  /// Decode a raw `SubscribeDeshredRequest` payload. No `Env` needed, so
  /// this stays directly unit-testable.
  fn decode_subscribe_deshred_request(request_bytes: Buffer) -> Result<SubscribeDeshredRequest> {
    SubscribeDeshredRequest::decode(request_bytes.as_ref()).map_err(|error| {
      napi_error_with_cause(
        napi::Status::InvalidArg,
        "invalid SubscribeDeshredRequest payload",
        &error,
      )
    })
  }

  fn take_sink_for_write(&self) -> Result<SubscribeDeshredRequestSink> {
    self
      .inner
      .take_sink_for_write("Cannot write to a closed deshred subscription stream")
  }

  async fn send_subscribe_request(
    sink: SubscribeDeshredRequestSink,
    protobuf_subscribe_request: SubscribeDeshredRequest,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  ) -> Result<()> {
    DuplexStreamInner::<SubscribeDeshredRequestSink, SubscribeDeshredStream>::send_subscribe_request(
      sink,
      protobuf_subscribe_request,
      terminal_error,
      "deshred stream send failed",
    )
    .await
  }

  async fn recv_update_or_error(
    readable: SharedStream<ProtoEncodedSt<SubscribeDeshredStream>>,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  ) -> Result<Option<Vec<u8>>> {
    DuplexStreamInner::<SubscribeDeshredRequestSink, SubscribeDeshredStream>::recv_update_or_error(
      readable,
      terminal_error,
      "deshred stream receive failed",
    )
    .await
  }
}

#[cfg(test)]
mod tests {
  use crate::{DuplexStream, DuplexStreamDeshred, DuplexStreamInner, ProtoEncodedSt, SharedStream};
  use futures::channel::mpsc as futures_mpsc;
  use futures::StreamExt;
  use napi::bindgen_prelude::Buffer;
  use napi::Status;
  use prost::Message;
  use std::collections::HashMap;
  use std::sync::{Arc, Mutex as StdMutex};
  use tokio::time::{timeout, Duration};
  use yellowstone_grpc_client::{
    GeyserStream, SubscribeDeshredRequestSink, SubscribeDeshredStream, SubscribeRequestSink,
  };
  use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter, subscribe_request_filter_accounts_filter_lamports,
    subscribe_request_filter_accounts_filter_memcmp,
  };
  use yellowstone_grpc_proto::prelude::{
    SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
    SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterDeshredTransactions,
    SubscribeRequestPing, SubscribeUpdate, SubscribeUpdateDeshred,
  };

  /// `DuplexStreamInner`'s two type params fixed to the `subscribe` pairing —
  /// used by every test below that exercises shared logic. `DuplexStream`'s
  /// own tests reuse this directly instead of a full `DuplexStream`, since
  /// its wrapper methods are just thin forwarders to these.
  type TestInner = DuplexStreamInner<SubscribeRequestSink, GeyserStream>;
  /// Same idea, fixed to the `deshred` pairing.
  type TestDeshredInner = DuplexStreamInner<SubscribeDeshredRequestSink, SubscribeDeshredStream>;

  fn subscribe_request_with_memcmp_filter() -> SubscribeRequest {
    let mut accounts = HashMap::new();
    accounts.insert(
      "client".to_string(),
      SubscribeRequestFilterAccounts {
        account: vec![],
        owner: vec![],
        filters: vec![SubscribeRequestFilterAccountsFilter {
          filter: Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
            SubscribeRequestFilterAccountsFilterMemcmp {
              offset: 4,
              data: Some(
                subscribe_request_filter_accounts_filter_memcmp::Data::Bytes(vec![9, 9, 9]),
              ),
            },
          )),
        }],
        nonempty_txn_signature: None,
        cuckoo_accounts_filter: None,
      },
    );

    SubscribeRequest {
      accounts,
      slots: HashMap::new(),
      transactions: HashMap::new(),
      transactions_status: HashMap::new(),
      blocks: HashMap::new(),
      blocks_meta: HashMap::new(),
      entry: HashMap::new(),
      commitment: Some(1),
      accounts_data_slice: Vec::new(),
      ping: None,
      from_slot: None,
    }
  }

  fn subscribe_request_with_memcmp_base58_filter() -> SubscribeRequest {
    let mut request = subscribe_request_with_memcmp_filter();
    request.accounts.get_mut("client").unwrap().filters[0].filter =
      Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
        SubscribeRequestFilterAccountsFilterMemcmp {
          offset: 4,
          data: Some(
            subscribe_request_filter_accounts_filter_memcmp::Data::Base58(
              "11111111111111111111111111111111".to_string(),
            ),
          ),
        },
      ));
    request
  }

  fn subscribe_request_with_memcmp_base64_filter() -> SubscribeRequest {
    let mut request = subscribe_request_with_memcmp_filter();
    request.accounts.get_mut("client").unwrap().filters[0].filter =
      Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
        SubscribeRequestFilterAccountsFilterMemcmp {
          offset: 4,
          data: Some(
            subscribe_request_filter_accounts_filter_memcmp::Data::Base64("AQID".to_string()),
          ),
        },
      ));
    request
  }

  fn subscribe_request_with_lamports_filter(
    cmp: subscribe_request_filter_accounts_filter_lamports::Cmp,
  ) -> SubscribeRequest {
    let mut request = subscribe_request_with_memcmp_filter();
    request.accounts.get_mut("client").unwrap().filters[0].filter =
      Some(subscribe_request_filter_accounts_filter::Filter::Lamports(
        SubscribeRequestFilterAccountsFilterLamports { cmp: Some(cmp) },
      ));
    request
  }

  fn terminal_error_with_cause(reason: &str, cause_message: &str) -> napi::Error {
    let mut error = napi::Error::new(Status::GenericFailure, reason.to_string());
    error.set_cause(napi::Error::new(
      Status::GenericFailure,
      cause_message.to_string(),
    ));
    error
  }

  fn subscribe_update_with_filters(filters: &[&str]) -> SubscribeUpdate {
    SubscribeUpdate {
      filters: filters.iter().map(|filter| (*filter).to_string()).collect(),
      update_oneof: None,
      created_at: None,
    }
  }

  fn make_test_inner() -> (
    TestInner,
    futures_mpsc::Receiver<SubscribeRequest>,
    tokio::sync::mpsc::Sender<std::result::Result<SubscribeUpdate, tonic::Status>>,
  ) {
    let (mock_tx, mock_rx) = tokio::sync::mpsc::channel(16);
    let (writable_tx, writable_rx) = futures_mpsc::channel::<SubscribeRequest>(16);
    (
      DuplexStreamInner::new(
        SubscribeRequestSink::mock(writable_tx),
        GeyserStream::mock(mock_rx),
      ),
      writable_rx,
      mock_tx,
    )
  }

  fn make_test_deshred_inner() -> (
    TestDeshredInner,
    futures_mpsc::UnboundedReceiver<SubscribeDeshredRequest>,
    tokio::sync::mpsc::Sender<std::result::Result<SubscribeUpdateDeshred, tonic::Status>>,
  ) {
    let (mock_tx, mock_rx) = tokio::sync::mpsc::channel(16);
    let (writable_tx, writable_rx) = futures_mpsc::unbounded::<SubscribeDeshredRequest>();
    (
      DuplexStreamInner::new(
        SubscribeDeshredRequestSink::mock(writable_tx),
        SubscribeDeshredStream::mock(mock_rx),
      ),
      writable_rx,
      mock_tx,
    )
  }

  /// Just the read side, independent of a writable sink — used by the
  /// `recv_*` tests below, which don't touch `writable` at all.
  fn make_test_readable() -> (
    SharedStream<ProtoEncodedSt<GeyserStream>>,
    tokio::sync::mpsc::Sender<std::result::Result<SubscribeUpdate, tonic::Status>>,
  ) {
    let (mock_tx, mock_rx) = tokio::sync::mpsc::channel(16);
    (
      SharedStream::new(ProtoEncodedSt {
        wrapped: GeyserStream::mock(mock_rx),
      }),
      mock_tx,
    )
  }

  // ---------------------------------------------------------------------
  // Shared `DuplexStreamInner` behavior — tested once, through the
  // `subscribe` pairing. `DuplexStream`/`DuplexStreamDeshred`'s own methods
  // are thin forwarders into this, so there is nothing type-specific left
  // to re-verify per wrapper.
  // ---------------------------------------------------------------------

  #[tokio::test]
  async fn close_drops_sink_and_receiver_observes_shutdown() {
    let (inner, mut writable_rx, _mock_tx) = make_test_inner();

    inner.close().expect("close should succeed");

    let shutdown_observed = timeout(Duration::from_millis(200), writable_rx.next())
      .await
      .expect("receiver await should not time out");

    assert!(
      shutdown_observed.is_none(),
      "receiver should observe channel close when stream is closed"
    );
  }

  #[tokio::test]
  async fn take_sink_for_write_fails_after_close() {
    let (inner, _writable_rx, _mock_tx) = make_test_inner();

    inner.close().expect("close should succeed");

    let error = inner
      .take_sink_for_write("stream is closed")
      .err()
      .expect("take_sink_for_write should fail after close");

    assert!(
      error.to_string().contains("stream is closed"),
      "unexpected error message: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on close-state error");
  }

  #[tokio::test]
  async fn write_before_close_is_delivered_to_receiver() {
    let (inner, mut writable_rx, _mock_tx) = make_test_inner();

    let sink = inner
      .take_sink_for_write("stream is closed")
      .expect("sink should be available before close");
    TestInner::send_subscribe_request(
      sink,
      SubscribeRequest::default(),
      inner.terminal_error.clone(),
      "send failed",
    )
    .await
    .expect("send should succeed before close");

    let received = timeout(Duration::from_millis(200), writable_rx.next())
      .await
      .expect("receiver await should not time out");

    assert!(
      received.is_some(),
      "receiver should get request written before close"
    );

    inner.close().expect("close should succeed");
  }

  #[tokio::test]
  async fn send_failure_when_receiver_dropped_populates_terminal_error() {
    let (inner, writable_rx, _mock_tx) = make_test_inner();
    drop(writable_rx);

    let sink = inner
      .take_sink_for_write("stream is closed")
      .expect("sink should still be available; only the paired receiver was dropped");
    let terminal_error = inner.terminal_error.clone();

    let error = TestInner::send_subscribe_request(
      sink,
      SubscribeRequest::default(),
      terminal_error.clone(),
      "send failed",
    )
    .await
    .expect_err("send should fail when receiver is dropped");

    assert!(
      error.to_string().contains("send failed"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for channel error"
    );
    assert!(
      terminal_error
        .lock()
        .expect("terminal_error lock should be available")
        .is_some(),
      "a failed send should also populate terminal_error, so reads reflect the dead connection"
    );
  }

  #[tokio::test]
  async fn lock_poisoning_fails_close_and_take_sink_for_write() {
    let (inner, _writable_rx, _mock_tx) = make_test_inner();
    {
      let writable_poison = inner.writable.clone();
      let _ = std::panic::catch_unwind(move || {
        let _guard = writable_poison
          .lock()
          .expect("lock should be available before intentional poison");
        panic!("intentional poison");
      });
    }

    let close_error = inner
      .close()
      .expect_err("close should fail when writable lock is poisoned");
    assert!(
      close_error
        .to_string()
        .contains("Failed to acquire writable lock"),
      "unexpected error message: {close_error}"
    );
    assert!(
      close_error.cause.is_some(),
      "expected nested cause for poisoned lock error"
    );

    let write_error = inner
      .take_sink_for_write("stream is closed")
      .err()
      .expect("take_sink_for_write should fail when writable lock is poisoned");
    assert!(
      write_error
        .to_string()
        .contains("Failed to acquire writable lock"),
      "unexpected error message: {write_error}"
    );
    assert!(
      write_error.cause.is_some(),
      "expected nested cause for poisoned lock error"
    );
  }

  #[tokio::test]
  async fn close_is_idempotent() {
    let (inner, _writable_rx, _mock_tx) = make_test_inner();

    inner.close().expect("first close should succeed");
    inner.close().expect("second close should succeed");

    let error = inner
      .take_sink_for_write("stream is closed")
      .err()
      .expect("writes should stay rejected after repeated close");
    assert!(
      error.to_string().contains("stream is closed"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn concurrent_close_write_race_is_stable_and_stream_ends_closed() {
    for _ in 0..32 {
      let (inner, _writable_rx, _mock_tx) = make_test_inner();
      let inner = Arc::new(inner);
      let inner_for_close = inner.clone();
      let inner_for_write = inner.clone();

      let (close_result, write_result) =
        tokio::join!(async move { inner_for_close.close() }, async move {
          inner_for_write.take_sink_for_write("stream is closed")
        });

      close_result.expect("close should never fail");
      if let Err(error) = write_result {
        assert!(
          error.to_string().contains("stream is closed"),
          "unexpected race error message: {error}"
        );
      }

      let post_close_error = inner
        .take_sink_for_write("stream is closed")
        .err()
        .expect("writes after close/write race should be rejected");
      assert!(
        post_close_error.to_string().contains("stream is closed"),
        "unexpected post-race error message: {post_close_error}"
      );
    }
  }

  #[tokio::test]
  async fn recv_returns_none_when_channel_closed_without_terminal_error() {
    let (readable, mock_tx) = make_test_readable();
    drop(mock_tx);
    let terminal_error = Arc::new(StdMutex::new(None));

    let result = TestInner::recv_update_or_error(readable, terminal_error, "recv failed")
      .await
      .expect("closed channel without error should map to None");

    assert!(result.is_none());
  }

  #[tokio::test]
  async fn recv_returns_terminal_error_when_channel_closed_after_send_failure() {
    let (readable, mock_tx) = make_test_readable();
    drop(mock_tx);
    let terminal_error = Arc::new(StdMutex::new(Some(terminal_error_with_cause(
      "subscribe stream receive failed: channel closed",
      "upstream grpc status unavailable",
    ))));

    let error = TestInner::recv_update_or_error(readable, terminal_error, "recv failed")
      .await
      .expect_err("terminal error should be propagated to caller");

    assert!(
      error
        .to_string()
        .contains("subscribe stream receive failed: channel closed"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected terminal error cause to propagate through read()"
    );
  }

  #[tokio::test]
  async fn recv_returns_terminal_error_when_terminal_error_lock_is_poisoned() {
    let (readable, mock_tx) = make_test_readable();
    drop(mock_tx);
    let terminal_error = Arc::new(StdMutex::new(None));
    {
      let terminal_error_poison = terminal_error.clone();
      let _ = std::panic::catch_unwind(move || {
        let mut guard = terminal_error_poison
          .lock()
          .expect("lock should be available before intentional poison");
        *guard = Some(napi::Error::from_reason(
          "subscribe stream receive failed: poisoned lock",
        ));
        panic!("intentional poison");
      });
    }

    let error = TestInner::recv_update_or_error(readable, terminal_error, "recv failed")
      .await
      .expect_err("terminal error should still propagate from poisoned lock");

    assert!(
      error
        .to_string()
        .contains("subscribe stream receive failed: poisoned lock"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn recv_returns_encoded_bytes_for_stream_item() {
    let (readable, mock_tx) = make_test_readable();
    let terminal_error = Arc::new(StdMutex::new(None));
    let update = subscribe_update_with_filters(&["client"]);

    mock_tx
      .send(Ok(update.clone()))
      .await
      .expect("mock channel should accept item");

    let result = TestInner::recv_update_or_error(readable, terminal_error, "recv failed")
      .await
      .expect("stream item should be delivered")
      .expect("stream item should not be None");

    assert_eq!(result, update.encode_to_vec());
  }

  #[tokio::test]
  async fn recv_returns_error_for_stream_status_error() {
    let (readable, mock_tx) = make_test_readable();
    let terminal_error = Arc::new(StdMutex::new(None));

    mock_tx
      .send(Err(tonic::Status::unavailable("upstream unavailable")))
      .await
      .expect("mock channel should accept item");

    let error = TestInner::recv_update_or_error(readable, terminal_error, "recv failed")
      .await
      .expect_err("stream status error should be surfaced");

    assert!(
      error.to_string().contains("recv failed"),
      "unexpected error message: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on stream error");
  }

  // ---------------------------------------------------------------------
  // `DuplexStream`-specific: decode/validate is the only logic that isn't
  // shared with `DuplexStreamDeshred`.
  // ---------------------------------------------------------------------

  #[test]
  fn decode_and_validate_subscribe_request_rejects_invalid_bytes() {
    let error =
      DuplexStream::decode_and_validate_subscribe_request(Buffer::from(vec![0xFF, 0x00, 0xAA]))
        .expect_err("invalid protobuf bytes should be rejected");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("invalid subscriberequest payload"),
      "unexpected error message: {error}"
    );
  }

  #[test]
  fn decode_and_validate_subscribe_request_rejects_filter_without_variant() {
    let mut request = subscribe_request_with_memcmp_filter();
    request.accounts.get_mut("client").unwrap().filters[0].filter = None;

    let error =
      DuplexStream::decode_and_validate_subscribe_request(Buffer::from(request.encode_to_vec()))
        .expect_err("missing filter variant should be rejected");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("filter should be defined"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for validation error"
    );
  }

  #[tokio::test]
  async fn decode_and_validate_subscribe_request_accepts_each_supported_filter_variant() {
    let cases: Vec<(&str, SubscribeRequest)> = vec![
      ("memcmp bytes", subscribe_request_with_memcmp_filter()),
      (
        "memcmp base58",
        subscribe_request_with_memcmp_base58_filter(),
      ),
      (
        "memcmp base64",
        subscribe_request_with_memcmp_base64_filter(),
      ),
      (
        "lamports eq",
        subscribe_request_with_lamports_filter(
          subscribe_request_filter_accounts_filter_lamports::Cmp::Eq(1),
        ),
      ),
      (
        "lamports ne",
        subscribe_request_with_lamports_filter(
          subscribe_request_filter_accounts_filter_lamports::Cmp::Ne(2),
        ),
      ),
      (
        "lamports lt",
        subscribe_request_with_lamports_filter(
          subscribe_request_filter_accounts_filter_lamports::Cmp::Lt(3),
        ),
      ),
      (
        "lamports gt",
        subscribe_request_with_lamports_filter(
          subscribe_request_filter_accounts_filter_lamports::Cmp::Gt(4),
        ),
      ),
    ];

    for (label, request) in cases {
      let (inner, mut writable_rx, _mock_tx) = make_test_inner();

      let decoded =
        DuplexStream::decode_and_validate_subscribe_request(Buffer::from(request.encode_to_vec()))
          .unwrap_or_else(|error| panic!("{label} should be accepted: {error}"));
      assert_eq!(
        decoded, request,
        "{label}: decode should round-trip exactly"
      );

      let sink = inner
        .take_sink_for_write("stream is closed")
        .expect("sink should be available");
      TestInner::send_subscribe_request(sink, decoded, inner.terminal_error.clone(), "send failed")
        .await
        .unwrap_or_else(|error| panic!("{label}: send should succeed: {error}"));

      let received = timeout(Duration::from_millis(200), writable_rx.next())
        .await
        .expect("receiver await should not time out")
        .unwrap_or_else(|| panic!("{label}: receiver should get one request"));

      assert_eq!(
        received, request,
        "{label}: receiver should get the exact request"
      );
    }
  }

  // ---------------------------------------------------------------------
  // `DuplexStreamDeshred`-specific: decode (no validation step) is the only
  // logic that isn't shared with `DuplexStream`.
  // ---------------------------------------------------------------------

  #[test]
  fn decode_subscribe_deshred_request_rejects_invalid_bytes() {
    let error =
      DuplexStreamDeshred::decode_subscribe_deshred_request(Buffer::from(vec![0xAA, 0xBB, 0xCC]))
        .expect_err("invalid deshred protobuf bytes should be rejected");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("invalid subscribedeshredrequest payload"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn deshred_write_raw_delivers_request_with_filters_and_ping_to_receiver() {
    let (inner, mut writable_rx, _mock_tx) = make_test_deshred_inner();

    let mut deshred_transactions = HashMap::new();
    deshred_transactions.insert(
      "client".to_string(),
      SubscribeRequestFilterDeshredTransactions {
        vote: Some(false),
        account_include: vec!["acc1".to_string()],
        account_exclude: vec!["acc2".to_string()],
        account_required: vec!["acc3".to_string()],
      },
    );

    let request = SubscribeDeshredRequest {
      deshred_transactions,
      ping: Some(SubscribeRequestPing { id: 99 }),
      slots: HashMap::new(),
    };

    let decoded =
      DuplexStreamDeshred::decode_subscribe_deshred_request(Buffer::from(request.encode_to_vec()))
        .expect("decode should succeed for valid deshred request");

    let sink = inner
      .take_sink_for_write("stream is closed")
      .expect("sink should be available");
    TestDeshredInner::send_subscribe_request(
      sink,
      decoded,
      inner.terminal_error.clone(),
      "send failed",
    )
    .await
    .expect("send should succeed for valid deshred request");

    let received = timeout(Duration::from_millis(200), writable_rx.next())
      .await
      .expect("receiver await should not time out")
      .expect("receiver should get one request");

    assert_eq!(
      received
        .deshred_transactions
        .get("client")
        .and_then(|filter| filter.vote),
      Some(false)
    );
    assert_eq!(
      received
        .deshred_transactions
        .get("client")
        .unwrap()
        .account_include,
      vec!["acc1".to_string()]
    );
    assert_eq!(
      received
        .deshred_transactions
        .get("client")
        .unwrap()
        .account_exclude,
      vec!["acc2".to_string()]
    );
    assert_eq!(
      received
        .deshred_transactions
        .get("client")
        .unwrap()
        .account_required,
      vec!["acc3".to_string()]
    );
    assert_eq!(received.ping.unwrap().id, 99);
  }
}
