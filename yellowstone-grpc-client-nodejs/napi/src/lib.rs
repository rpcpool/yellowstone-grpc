//! N-API stream engine for Yellowstone subscriptions.
//!
//! This module exposes a `DuplexStream` type used by the JS SDK wrapper.
//! The Rust side owns the gRPC subscribe task and bridges:
//! - JS writes (`SubscribeRequest`) -> gRPC sink
//! - gRPC stream (`SubscribeUpdate`) -> JS reads
//!
//! Design goals:
//! - Keep JS-facing API small and stable (`read` / `write`)
//! - Convert all protobuf <-> JS objects through generated `js_types`
//! - Stop worker tasks deterministically when JS drops stream handles
mod bindings;
mod client;
mod encoding;
mod subscribe_request_validation;
mod utils;

use futures_util::{SinkExt, StreamExt};
use napi::{bindgen_prelude::*, Env};
use napi_derive::napi;
use prost::Message;
use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc, Mutex as StdMutex, Once,
};
use tokio::sync::{
  mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
  Mutex,
};
use yellowstone_grpc_proto::prelude::*;

use crate::{
  client::GrpcClient,
  js_types::{
    JsSubscribeDeshredRequest, JsSubscribeRequest, JsSubscribeUpdate, JsSubscribeUpdateDeshred,
  },
  subscribe_request_validation::validate_subscribe_request,
};

pub mod js_types;

static INITIALIZE_CRYPTO_PROVIDER: Once = Once::new();

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
  /// Read side consumed by `read()`. Each message is delivered exactly once.
  readable: Arc<Mutex<UnboundedReceiver<SubscribeUpdate>>>,
  /// Write side used by `write()`. Requests are forwarded to gRPC task.
  ///
  /// The mutex protects a close-state transition, not sender sharing:
  /// - `close()` sets the state to `None` (revokes future writes).
  /// - `write()` reads/clones under the same lock.
  ///
  /// `UnboundedSender` being cheap-`Clone` is true, but clone alone does not
  /// provide an atomic "disable writes now" transition.
  writable: Arc<StdMutex<Option<UnboundedSender<SubscribeRequest>>>>,
  /// Terminal worker error captured from gRPC send/recv failures.
  ///
  /// `read()` surfaces this to JS once the update channel is closed.
  terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  /// Set once JS has started destroying this stream.
  is_closing: Arc<AtomicBool>,
}

#[napi]
impl DuplexStream {
  // #[napi]
  pub fn subscribe<'env>(
    env: &'env Env,
    grpc_client: &GrpcClient,
  ) -> Result<PromiseRaw<'env, Self>> {
    let client_holder = grpc_client.holder.clone();

    // Open the gRPC stream before returning to JS so connection/protocol errors
    // reject the Promise and bubble to TypeScript callers.
    env.spawn_future_with_callback(
      async move {
        let holder = client_holder
          .downcast_ref::<crate::client::internal::ClientHolder<
            yellowstone_grpc_client::InterceptorXToken,
          >>()
          .ok_or_else(|| napi_error(napi::Status::GenericFailure, "Invalid client type"))?;

        // Acquire lock, call subscribe, and immediately release the lock.
        let (mut stream_tx, mut stream_rx) = {
          let mut client = holder.client.lock().await;
          client.subscribe().await.map_err(|error| {
            napi_error_with_cause(
              napi::Status::GenericFailure,
              "failed to open subscribe stream",
              &error,
            )
          })?
        };

        // TODO : Fine tune unbounded channels.
        let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdate>();
        let (writable_tx, mut writable_rx) = unbounded_channel::<SubscribeRequest>();
        let writable = Arc::new(StdMutex::new(Some(writable_tx)));
        let terminal_error = Arc::new(StdMutex::new(None));
        let terminal_error_worker = terminal_error.clone();
        let is_closing = Arc::new(AtomicBool::new(false));
        let is_closing_worker = is_closing.clone();

        // Worker lifecycle:
        // - Runs until JS closes the writable side, gRPC side ends, or a
        //   non-graceful gRPC send/recv failure occurs.
        // - For non-graceful failures, captures terminal error and exits.
        // - Dropping `readable_tx` on exit closes JS read channel.
        tokio::spawn(async move {
          loop {
            tokio::select! {
              // 1. SubscribeRequest is received from self.write().
              // 2. SubscribeRequest is propagated to Geyser client's sender.
              req_option = writable_rx.recv() => {
                if let Some(request) = req_option {
                  if let Err(error) = stream_tx.send(request).await {
                    if is_closing_worker.load(Ordering::Acquire) {
                      // JS initiated close; treat send failure during teardown
                      // as expected shutdown, not a user-facing stream error.
                      break;
                    }
                    capture_terminal_error(
                      &terminal_error_worker,
                      napi_error_with_cause(
                        napi::Status::GenericFailure,
                        "subscribe stream send failed",
                        &error,
                      ),
                    );
                    break;
                  }
                } else {
                  // JS writable side dropped: no more requests can be sent.
                  // Exit worker so the upstream gRPC stream is torn down as well.
                  break;
                }
              },

              // 1. SubscribeUpdate is received from Geyser client's receiver.
              // 2. SubscribeUpdate is propagated to self.read() for NodeJS consumption.
              maybe_update_result = stream_rx.next() => {
                match maybe_update_result {
                  Some(Ok(update)) => {
                    // JS reader side disappeared; no point continuing the worker.
                    if readable_tx.send(update).is_err() {
                      break;
                    }
                  }
                  Some(Err(error)) => {
                    if is_closing_worker.load(Ordering::Acquire) {
                      // JS initiated close; treat recv failure during teardown
                      // as expected shutdown, not a user-facing stream error.
                      break;
                    }
                    capture_terminal_error(
                      &terminal_error_worker,
                      napi_error_with_cause(
                        napi::Status::GenericFailure,
                        "subscribe stream receive failed",
                        &error,
                      ),
                    );
                    break;
                  }
                  None => break,
                }
              }
            }
          }
        });

        Ok(Self {
          readable: Arc::new(Mutex::new(readable_rx)),
          writable,
          terminal_error,
          is_closing,
        })
      },
      move |_environment, stream| Ok(stream),
    )
  }

  /// Read JS Accesspoint.
  ///
  /// Retrieve one `SubscribeUpdate` from the worker and convert it to
  /// the generated N-API JS representation (`JsSubscribeUpdate`).
  #[napi]
  pub fn read<'env>(
    &self,
    env: &'env Env,
  ) -> Result<PromiseRaw<'env, Option<JsSubscribeUpdate<'env>>>> {
    let readable = self.readable.clone();
    let terminal_error = self.terminal_error.clone();

    env.spawn_future_with_callback(
      async move { Self::recv_update_or_error(readable, terminal_error).await },
      move |environment, subscribe_update_opt| match subscribe_update_opt {
        Some(subscribe_update) => {
          JsSubscribeUpdate::from_protobuf_to_js_type(environment, subscribe_update).map(Some)
        }
        None => Ok(None),
      },
    )
  }

  /// Write JS Accesspoint.
  ///
  /// Accept a JS request object, convert to protobuf, then enqueue for the
  /// worker to forward to the gRPC request sink.
  #[napi]
  pub fn close(&self) -> Result<()> {
    self.is_closing.store(true, Ordering::Release);

    let mut writable_guard = self.writable.lock().map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "Failed to acquire writable lock",
        &error,
      )
    })?;
    // Dropping the last sender closes the channel and causes
    // `writable_rx.recv()` to return `None` in the worker.
    *writable_guard = None;

    Ok(())
  }

  #[napi]
  pub fn write(&self, request: JsSubscribeRequest) -> Result<()> {
    let protobuf_subscribe_request: SubscribeRequest = request.from_js_to_protobuf_type()?;
    validate_subscribe_request(&protobuf_subscribe_request).map_err(|error| {
      napi_error_with_cause(napi::Status::InvalidArg, error.to_string(), &error)
    })?;

    self.enqueue_subscribe_request(protobuf_subscribe_request)
  }

  #[napi]
  pub fn write_raw(&self, request_bytes: Buffer) -> Result<()> {
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

    self.enqueue_subscribe_request(protobuf_subscribe_request)
  }

  fn enqueue_subscribe_request(&self, protobuf_subscribe_request: SubscribeRequest) -> Result<()> {
    if self.is_closing.load(Ordering::Acquire) {
      return Err(napi_error(
        napi::Status::GenericFailure,
        "Cannot write to a closing subscription stream",
      ));
    }

    let writable = self
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
      .ok_or_else(|| {
        napi_error(
          napi::Status::GenericFailure,
          "Cannot write to a closed subscription stream",
        )
      })?;

    if let Err(e) = writable.send(protobuf_subscribe_request) {
      return Err(napi_error_with_cause(
        napi::Status::GenericFailure,
        e.to_string(),
        &e,
      ));
    }
    Ok(())
  }

  async fn recv_update_or_error(
    readable: Arc<Mutex<UnboundedReceiver<SubscribeUpdate>>>,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  ) -> Result<Option<SubscribeUpdate>> {
    match readable.lock().await.recv().await {
      Some(update) => Ok(Some(update)),
      // Channel close indicates worker termination. If worker captured a native
      // terminal error, surface it to JS instead of silently ending the stream.
      None => {
        // EOF without terminal error => graceful end-of-stream.
        // EOF with terminal error => reject read promise so TS emits `error`.
        let error = get_terminal_error(&terminal_error);

        if let Some(error) = error {
          Err(error)
        } else {
          Ok(None)
        }
      }
    }
  }
}

/// DuplexStreamDeshred Engine.
///
/// Similar to `DuplexStream`, but targets the deshred pre-execution stream.
#[napi]
struct DuplexStreamDeshred {
  /// Read side consumed by `read()`. Each message is delivered exactly once.
  readable: Arc<Mutex<UnboundedReceiver<SubscribeUpdateDeshred>>>,
  /// Write side used by `write()`. Requests are forwarded to gRPC task.
  writable: Arc<StdMutex<Option<UnboundedSender<SubscribeDeshredRequest>>>>,
  /// Terminal worker error captured from gRPC send/recv failures.
  ///
  /// `read()` surfaces this to JS once the update channel is closed.
  terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  /// Set once JS has started destroying this stream.
  is_closing: Arc<AtomicBool>,
}

#[napi]
impl DuplexStreamDeshred {
  pub fn subscribe<'env>(
    env: &'env Env,
    grpc_client: &GrpcClient,
  ) -> Result<PromiseRaw<'env, Self>> {
    let client_holder = grpc_client.holder.clone();

    // Open the gRPC stream before returning to JS so connection/protocol errors
    // (e.g. UNIMPLEMENTED) reject the Promise and bubble to TypeScript callers.
    env.spawn_future_with_callback(
      async move {
        let holder = client_holder
          .downcast_ref::<crate::client::internal::ClientHolder<
            yellowstone_grpc_client::InterceptorXToken,
          >>()
          .ok_or_else(|| napi_error(napi::Status::GenericFailure, "Invalid client type"))?;

        // Acquire lock, open stream, and release lock immediately.
        let (mut stream_tx, mut stream_rx) = {
          let mut client = holder.client.lock().await;
          client.subscribe_deshred().await.map_err(|error| {
            napi_error_with_cause(
              napi::Status::GenericFailure,
              "failed to open deshred subscribe stream",
              &error,
            )
          })?
        };

        let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdateDeshred>();
        let (writable_tx, mut writable_rx) = unbounded_channel::<SubscribeDeshredRequest>();
        let writable = Arc::new(StdMutex::new(Some(writable_tx)));
        let terminal_error = Arc::new(StdMutex::new(None));
        let terminal_error_worker = Arc::clone(&terminal_error);
        let is_closing = Arc::new(AtomicBool::new(false));
        let is_closing_worker = Arc::clone(&is_closing);

        // Same lifecycle contract as `DuplexStream`:
        // propagate first non-graceful gRPC error through `terminal_error`,
        // then close channel so JS `read()` can map it to a rejected promise.
        tokio::spawn(async move {
          loop {
            tokio::select! {
              req_option = writable_rx.recv() => {
                if let Some(request) = req_option {
                  if let Err(error) = stream_tx.send(request).await {
                    if is_closing_worker.load(Ordering::Acquire) {
                      // Closing path: avoid surfacing expected teardown errors.
                      break;
                    }
                    capture_terminal_error(
                      &terminal_error_worker,
                      napi_error_with_cause(
                        napi::Status::GenericFailure,
                        "deshred stream send failed",
                        &error,
                      ),
                    );
                    break;
                  }
                } else {
                  break;
                }
              },
              maybe_update_result = stream_rx.next() => {
                match maybe_update_result {
                  Some(Ok(update)) => {
                    if readable_tx.send(update).is_err() {
                      break;
                    }
                  }
                  Some(Err(error)) => {
                    if is_closing_worker.load(Ordering::Acquire) {
                      // Closing path: avoid surfacing expected teardown errors.
                      break;
                    }
                    capture_terminal_error(
                      &terminal_error_worker,
                      napi_error_with_cause(
                        napi::Status::GenericFailure,
                        "deshred stream receive failed",
                        &error,
                      ),
                    );
                    break;
                  }
                  None => break,
                }
              }
            }
          }
        });

        Ok(Self {
          readable: Arc::new(Mutex::new(readable_rx)),
          writable,
          terminal_error,
          is_closing,
        })
      },
      move |_environment, stream| Ok(stream),
    )
  }

  /// Retrieve one `SubscribeUpdateDeshred` and convert it to generated N-API JS shape.
  #[napi]
  pub fn read<'env>(
    &self,
    env: &'env Env,
  ) -> Result<PromiseRaw<'env, Option<JsSubscribeUpdateDeshred<'env>>>> {
    let readable = self.readable.clone();
    let terminal_error = self.terminal_error.clone();

    env.spawn_future_with_callback(
      async move { Self::recv_update_or_error(readable, terminal_error).await },
      move |environment, subscribe_update_opt| match subscribe_update_opt {
        Some(subscribe_update) => {
          JsSubscribeUpdateDeshred::from_protobuf_to_js_type(environment, subscribe_update)
            .map(Some)
        }
        None => Ok(None),
      },
    )
  }

  #[napi]
  pub fn close(&self) -> Result<()> {
    self.is_closing.store(true, Ordering::Release);

    let mut writable_guard = self.writable.lock().map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "Failed to acquire writable lock",
        &error,
      )
    })?;
    *writable_guard = None;

    Ok(())
  }

  #[napi]
  pub fn write(&self, request: JsSubscribeDeshredRequest) -> Result<()> {
    let protobuf_subscribe_request: SubscribeDeshredRequest = request.from_js_to_protobuf_type()?;
    self.enqueue_subscribe_request(protobuf_subscribe_request)
  }

  #[napi]
  pub fn write_raw(&self, request_bytes: Buffer) -> Result<()> {
    let protobuf_subscribe_request = SubscribeDeshredRequest::decode(request_bytes.as_ref())
      .map_err(|error| {
        napi_error_with_cause(
          napi::Status::InvalidArg,
          "invalid SubscribeDeshredRequest payload",
          &error,
        )
      })?;

    self.enqueue_subscribe_request(protobuf_subscribe_request)
  }

  fn enqueue_subscribe_request(
    &self,
    protobuf_subscribe_request: SubscribeDeshredRequest,
  ) -> Result<()> {
    if self.is_closing.load(Ordering::Acquire) {
      return Err(napi_error(
        napi::Status::GenericFailure,
        "Cannot write to a closing deshred subscription stream",
      ));
    }

    let writable = self
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
      .ok_or_else(|| {
        napi_error(
          napi::Status::GenericFailure,
          "Cannot write to a closed deshred subscription stream",
        )
      })?;

    if let Err(e) = writable.send(protobuf_subscribe_request) {
      return Err(napi_error_with_cause(
        napi::Status::GenericFailure,
        e.to_string(),
        &e,
      ));
    }
    Ok(())
  }

  async fn recv_update_or_error(
    readable: Arc<Mutex<UnboundedReceiver<SubscribeUpdateDeshred>>>,
    terminal_error: Arc<StdMutex<Option<napi::Error>>>,
  ) -> Result<Option<SubscribeUpdateDeshred>> {
    match readable.lock().await.recv().await {
      Some(update) => Ok(Some(update)),
      None => {
        // Match regular subscribe semantics: graceful EOF when no terminal
        // error, otherwise reject and bubble root cause to TypeScript caller.
        let error = get_terminal_error(&terminal_error);

        if let Some(error) = error {
          Err(error)
        } else {
          Ok(None)
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::{
    js_types::{
      JsSubscribeDeshredRequest, JsSubscribeRequest, JsSubscribeRequestAccountsDataSlice,
      JsSubscribeRequestFilterDeshredTransactions, JsSubscribeRequestPing,
    },
    DuplexStream, DuplexStreamDeshred,
  };
  use napi::bindgen_prelude::Buffer;
  use napi::Status;
  use prost::Message;
  use std::collections::HashMap;
  use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex as StdMutex,
  };
  use tokio::sync::mpsc::unbounded_channel;
  use tokio::sync::Mutex;
  use tokio::time::{timeout, Duration};
  use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter, subscribe_request_filter_accounts_filter_lamports,
    subscribe_request_filter_accounts_filter_memcmp,
  };
  use yellowstone_grpc_proto::prelude::{
    SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
    SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterDeshredTransactions,
    SubscribeUpdate, SubscribeUpdateDeshred,
  };

  fn empty_subscribe_request() -> JsSubscribeRequest<'static> {
    JsSubscribeRequest {
      accounts: HashMap::new(),
      slots: HashMap::new(),
      transactions: HashMap::new(),
      transactions_status: HashMap::new(),
      blocks: HashMap::new(),
      blocks_meta: HashMap::new(),
      entry: HashMap::new(),
      commitment: None,
      accounts_data_slice: Vec::new(),
      ping: None,
      from_slot: None,
    }
  }

  fn empty_subscribe_deshred_request() -> JsSubscribeDeshredRequest {
    JsSubscribeDeshredRequest {
      deshred_transactions: HashMap::new(),
      ping: None,
    }
  }

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

  fn make_test_stream() -> (
    DuplexStream,
    tokio::sync::mpsc::UnboundedReceiver<SubscribeRequest>,
  ) {
    let (_readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdate>();
    let (writable_tx, writable_rx) = unbounded_channel::<SubscribeRequest>();

    (
      DuplexStream {
        readable: Arc::new(Mutex::new(readable_rx)),
        writable: Arc::new(StdMutex::new(Some(writable_tx))),
        terminal_error: Arc::new(StdMutex::new(None)),
        is_closing: Arc::new(AtomicBool::new(false)),
      },
      writable_rx,
    )
  }

  fn make_test_deshred_stream() -> (
    DuplexStreamDeshred,
    tokio::sync::mpsc::UnboundedReceiver<SubscribeDeshredRequest>,
  ) {
    let (_readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdateDeshred>();
    let (writable_tx, writable_rx) = unbounded_channel::<SubscribeDeshredRequest>();

    (
      DuplexStreamDeshred {
        readable: Arc::new(Mutex::new(readable_rx)),
        writable: Arc::new(StdMutex::new(Some(writable_tx))),
        terminal_error: Arc::new(StdMutex::new(None)),
        is_closing: Arc::new(AtomicBool::new(false)),
      },
      writable_rx,
    )
  }

  fn terminal_error_with_cause(reason: &str, cause_message: &str) -> napi::Error {
    let mut error = napi::Error::new(Status::GenericFailure, reason.to_string());
    error.set_cause(napi::Error::new(
      Status::GenericFailure,
      cause_message.to_string(),
    ));
    error
  }

  #[tokio::test]
  async fn close_drops_sender_and_worker_receiver_observes_shutdown() {
    let (stream, mut writable_rx) = make_test_stream();

    stream.close().expect("close should succeed");

    let shutdown_observed = timeout(Duration::from_millis(200), writable_rx.recv())
      .await
      .expect("receiver await should not time out");

    assert!(
      shutdown_observed.is_none(),
      "receiver should observe channel close when stream is closed"
    );
  }

  #[tokio::test]
  async fn write_after_close_is_rejected() {
    let (stream, _writable_rx) = make_test_stream();

    stream.close().expect("close should succeed");

    let error = stream
      .write(empty_subscribe_request())
      .expect_err("write should fail after close");

    assert!(
      error
        .to_string()
        .contains("Cannot write to a closing subscription stream"),
      "unexpected error message: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on close-state error");
  }

  #[tokio::test]
  async fn worker_loop_style_receiver_exits_after_close() {
    let (stream, mut writable_rx) = make_test_stream();
    let processed_requests = Arc::new(AtomicUsize::new(0));
    let processed_requests_worker = processed_requests.clone();

    let worker = tokio::spawn(async move {
      while writable_rx.recv().await.is_some() {
        processed_requests_worker.fetch_add(1, Ordering::SeqCst);
      }
    });

    stream
      .write(empty_subscribe_request())
      .expect("initial write should succeed");
    stream.close().expect("close should succeed");

    timeout(Duration::from_secs(1), worker)
      .await
      .expect("worker did not terminate after close")
      .expect("worker join failed");

    assert_eq!(
      processed_requests.load(Ordering::SeqCst),
      1,
      "expected exactly one enqueued request before channel shutdown"
    );
  }

  #[tokio::test]
  async fn write_before_close_is_delivered_to_receiver() {
    let (stream, mut writable_rx) = make_test_stream();

    stream
      .write(empty_subscribe_request())
      .expect("write before close should succeed");

    let received = timeout(Duration::from_millis(200), writable_rx.recv())
      .await
      .expect("receiver await should not time out");

    assert!(
      received.is_some(),
      "receiver should get request written before close"
    );

    stream.close().expect("close should succeed");
  }

  #[tokio::test]
  async fn write_after_receiver_drop_returns_channel_closed_error() {
    let (stream, writable_rx) = make_test_stream();
    drop(writable_rx);

    let error = stream
      .write(empty_subscribe_request())
      .expect_err("write should fail when receiver is dropped");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("channel closed"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for channel error"
    );
  }

  #[tokio::test]
  async fn close_returns_lock_error_when_writable_lock_is_poisoned() {
    let (stream, _writable_rx) = make_test_stream();
    {
      let writable_poison = stream.writable.clone();
      let _ = std::panic::catch_unwind(move || {
        let _guard = writable_poison
          .lock()
          .expect("lock should be available before intentional poison");
        panic!("intentional poison");
      });
    }

    let error = stream
      .close()
      .expect_err("close should fail when writable lock is poisoned");
    assert!(
      error
        .to_string()
        .contains("Failed to acquire writable lock"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for poisoned lock error"
    );
  }

  #[tokio::test]
  async fn write_returns_lock_error_when_writable_lock_is_poisoned() {
    let (stream, _writable_rx) = make_test_stream();
    {
      let writable_poison = stream.writable.clone();
      let _ = std::panic::catch_unwind(move || {
        let _guard = writable_poison
          .lock()
          .expect("lock should be available before intentional poison");
        panic!("intentional poison");
      });
    }

    let error = stream
      .write(empty_subscribe_request())
      .expect_err("write should fail when writable lock is poisoned");
    assert!(
      error
        .to_string()
        .contains("Failed to acquire writable lock"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for poisoned lock error"
    );
  }

  #[tokio::test]
  async fn close_is_idempotent() {
    let (stream, _writable_rx) = make_test_stream();

    stream.close().expect("first close should succeed");
    stream.close().expect("second close should succeed");

    let error = stream
      .write(empty_subscribe_request())
      .expect_err("writes should stay rejected after repeated close");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("closing") || message.contains("closed"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn concurrent_close_write_race_is_stable_and_stream_ends_closed() {
    for _ in 0..32 {
      let (stream, _writable_rx) = make_test_stream();
      let stream = Arc::new(stream);
      let stream_for_close = stream.clone();
      let stream_for_write = stream.clone();

      let (close_result, write_result) =
        tokio::join!(async move { stream_for_close.close() }, async move {
          stream_for_write.write(empty_subscribe_request())
        });

      close_result.expect("close should never fail");
      if let Err(error) = write_result {
        let message = error.to_string().to_lowercase();
        assert!(
          message.contains("closing")
            || message.contains("closed")
            || message.contains("channel closed"),
          "unexpected race error message: {error}"
        );
      }

      let post_close_error = stream
        .write(empty_subscribe_request())
        .expect_err("writes after close/write race should be rejected");
      let post_close_message = post_close_error.to_string().to_lowercase();
      assert!(
        post_close_message.contains("closing") || post_close_message.contains("closed"),
        "unexpected post-race error message: {post_close_error}"
      );
    }
  }

  #[tokio::test]
  async fn write_raw_delivers_decoded_request_to_receiver() {
    let (stream, mut writable_rx) = make_test_stream();
    let request = subscribe_request_with_memcmp_filter();
    let encoded_request = request.encode_to_vec();

    stream
      .write_raw(Buffer::from(encoded_request))
      .expect("write_raw should succeed for valid payload");

    let received = timeout(Duration::from_millis(200), writable_rx.recv())
      .await
      .expect("receiver await should not time out")
      .expect("receiver should get one request");

    assert_eq!(received, request);
  }

  #[tokio::test]
  async fn write_raw_rejects_invalid_bytes_payload() {
    let (stream, _writable_rx) = make_test_stream();

    let error = stream
      .write_raw(Buffer::from(vec![0xFF, 0x00, 0xAA]))
      .expect_err("invalid protobuf bytes should be rejected");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("invalid subscriberequest payload"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn write_rejects_invalid_u64_input_with_nested_parse_cause() {
    let (stream, _writable_rx) = make_test_stream();
    let mut request = empty_subscribe_request();
    request.accounts_data_slice = vec![JsSubscribeRequestAccountsDataSlice {
      offset: "not-a-number".to_string(),
      length: "1".to_string(),
    }];

    let error = stream
      .write(request)
      .expect_err("invalid u64 input should be rejected");
    assert!(
      error.to_string().contains("Invalid u64 value"),
      "unexpected error message: {error}"
    );
    let cause_message = error
      .cause
      .as_ref()
      .map(ToString::to_string)
      .unwrap_or_default()
      .to_lowercase();
    assert!(
      cause_message.contains("invalid digit"),
      "expected parse error cause details, got: {cause_message}"
    );
  }

  #[tokio::test]
  async fn write_raw_rejects_filter_without_variant() {
    let (stream, _writable_rx) = make_test_stream();
    let mut request = subscribe_request_with_memcmp_filter();
    request.accounts.get_mut("client").unwrap().filters[0].filter = None;

    let error = stream
      .write_raw(Buffer::from(request.encode_to_vec()))
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
  async fn write_raw_rejects_lamports_filter_without_cmp() {
    let (stream, _writable_rx) = make_test_stream();
    let mut request = subscribe_request_with_memcmp_filter();
    request.accounts.get_mut("client").unwrap().filters[0].filter =
      Some(subscribe_request_filter_accounts_filter::Filter::Lamports(
        SubscribeRequestFilterAccountsFilterLamports { cmp: None },
      ));

    let error = stream
      .write_raw(Buffer::from(request.encode_to_vec()))
      .expect_err("missing lamports comparator should be rejected");
    let message = error.to_string().to_lowercase();
    assert!(
      message.contains("lamports comparator should be defined"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for validation error"
    );
  }

  #[tokio::test]
  async fn write_raw_accepts_memcmp_base58_variant() {
    let (stream, mut writable_rx) = make_test_stream();
    let request = subscribe_request_with_memcmp_base58_filter();

    stream
      .write_raw(Buffer::from(request.encode_to_vec()))
      .expect("memcmp base58 should be accepted");

    let received = timeout(Duration::from_millis(200), writable_rx.recv())
      .await
      .expect("receiver await should not time out")
      .expect("receiver should receive request");
    assert_eq!(received, request);
  }

  #[tokio::test]
  async fn write_raw_accepts_memcmp_base64_variant() {
    let (stream, mut writable_rx) = make_test_stream();
    let request = subscribe_request_with_memcmp_base64_filter();

    stream
      .write_raw(Buffer::from(request.encode_to_vec()))
      .expect("memcmp base64 should be accepted");

    let received = timeout(Duration::from_millis(200), writable_rx.recv())
      .await
      .expect("receiver await should not time out")
      .expect("receiver should receive request");
    assert_eq!(received, request);
  }

  #[tokio::test]
  async fn write_raw_accepts_each_lamports_comparator_variant() {
    let lamports_cmp_variants = vec![
      subscribe_request_filter_accounts_filter_lamports::Cmp::Eq(1),
      subscribe_request_filter_accounts_filter_lamports::Cmp::Ne(2),
      subscribe_request_filter_accounts_filter_lamports::Cmp::Lt(3),
      subscribe_request_filter_accounts_filter_lamports::Cmp::Gt(4),
    ];

    for cmp in lamports_cmp_variants {
      let (stream, mut writable_rx) = make_test_stream();
      let request = subscribe_request_with_lamports_filter(cmp);

      stream
        .write_raw(Buffer::from(request.encode_to_vec()))
        .expect("lamports comparator variant should be accepted");

      let received = timeout(Duration::from_millis(200), writable_rx.recv())
        .await
        .expect("receiver await should not time out")
        .expect("receiver should receive request");
      assert_eq!(received, request);
    }
  }

  #[tokio::test]
  async fn write_raw_after_close_is_rejected() {
    let (stream, _writable_rx) = make_test_stream();
    stream.close().expect("close should succeed");

    let error = stream
      .write_raw(Buffer::from(
        subscribe_request_with_memcmp_filter().encode_to_vec(),
      ))
      .expect_err("write_raw after close should fail");
    let message = error.to_string().to_lowercase();
    assert!(
      message.contains("closing") || message.contains("closed"),
      "unexpected error message: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on close-state error");
  }

  #[tokio::test]
  async fn subscribe_read_returns_none_when_channel_closed_without_terminal_error() {
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdate>();
    drop(readable_tx);
    let readable = Arc::new(Mutex::new(readable_rx));
    let terminal_error = Arc::new(StdMutex::new(None));

    let result = DuplexStream::recv_update_or_error(readable, terminal_error)
      .await
      .expect("closed channel without error should map to None");

    assert!(result.is_none());
  }

  #[tokio::test]
  async fn subscribe_read_returns_terminal_error_when_channel_closed_after_worker_failure() {
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdate>();
    drop(readable_tx);
    let readable = Arc::new(Mutex::new(readable_rx));
    let terminal_error = Arc::new(StdMutex::new(Some(terminal_error_with_cause(
      "subscribe stream receive failed: channel closed",
      "upstream grpc status unavailable",
    ))));

    let error = DuplexStream::recv_update_or_error(readable, terminal_error)
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
  async fn subscribe_read_returns_terminal_error_when_terminal_error_lock_is_poisoned() {
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdate>();
    drop(readable_tx);
    let readable = Arc::new(Mutex::new(readable_rx));
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

    let error = DuplexStream::recv_update_or_error(readable, terminal_error)
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
  async fn deshred_write_raw_delivers_decoded_request_to_receiver() {
    let (stream, mut writable_rx) = make_test_deshred_stream();

    let mut deshred_transactions = HashMap::new();
    deshred_transactions.insert(
      "client".to_string(),
      SubscribeRequestFilterDeshredTransactions {
        vote: Some(true),
        account_include: vec!["acc1".to_string()],
        account_exclude: vec!["acc2".to_string()],
        account_required: vec!["acc3".to_string()],
      },
    );
    let request = SubscribeDeshredRequest {
      deshred_transactions,
      ping: None,
    };

    stream
      .write_raw(Buffer::from(request.encode_to_vec()))
      .expect("write_raw should succeed for valid deshred payload");

    let received = timeout(Duration::from_millis(200), writable_rx.recv())
      .await
      .expect("receiver await should not time out")
      .expect("receiver should get one request");

    assert_eq!(received, request);
  }

  #[tokio::test]
  async fn deshred_write_delivers_js_request_to_receiver() {
    let (stream, mut writable_rx) = make_test_deshred_stream();

    let mut deshred_transactions = HashMap::new();
    deshred_transactions.insert(
      "client".to_string(),
      JsSubscribeRequestFilterDeshredTransactions {
        vote: Some(false),
        account_include: vec!["acc1".to_string()],
        account_exclude: vec!["acc2".to_string()],
        account_required: vec!["acc3".to_string()],
      },
    );

    let request = JsSubscribeDeshredRequest {
      deshred_transactions,
      ping: Some(JsSubscribeRequestPing { id: 99 }),
    };

    stream
      .write(request)
      .expect("write should succeed for valid JS deshred request");

    let received = timeout(Duration::from_millis(200), writable_rx.recv())
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

  #[tokio::test]
  async fn deshred_write_raw_rejects_invalid_bytes_payload() {
    let (stream, _writable_rx) = make_test_deshred_stream();

    let error = stream
      .write_raw(Buffer::from(vec![0xAA, 0xBB, 0xCC]))
      .expect_err("invalid deshred protobuf bytes should be rejected");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("invalid subscribedeshredrequest payload"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn deshred_write_after_close_is_rejected() {
    let (stream, _writable_rx) = make_test_deshred_stream();
    stream.close().expect("close should succeed");

    let error = stream
      .write(empty_subscribe_deshred_request())
      .expect_err("write should fail after close");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("closing") || message.contains("closed"),
      "unexpected error message: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on close-state error");
  }

  #[tokio::test]
  async fn deshred_write_after_receiver_drop_returns_channel_closed_error() {
    let (stream, writable_rx) = make_test_deshred_stream();
    drop(writable_rx);

    let error = stream
      .write(empty_subscribe_deshred_request())
      .expect_err("write should fail when deshred receiver is dropped");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("channel closed"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for channel error"
    );
  }

  #[tokio::test]
  async fn deshred_write_raw_after_close_is_rejected() {
    let (stream, _writable_rx) = make_test_deshred_stream();
    stream.close().expect("close should succeed");

    let error = stream
      .write_raw(Buffer::from(
        SubscribeDeshredRequest {
          deshred_transactions: HashMap::new(),
          ping: None,
        }
        .encode_to_vec(),
      ))
      .expect_err("write_raw after close should fail");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("closing") || message.contains("closed"),
      "unexpected error message: {error}"
    );
    assert!(error.cause.is_some(), "expected cause on close-state error");
  }

  #[tokio::test]
  async fn deshred_close_is_idempotent() {
    let (stream, _writable_rx) = make_test_deshred_stream();

    stream.close().expect("first close should succeed");
    stream.close().expect("second close should succeed");

    let error = stream
      .write(empty_subscribe_deshred_request())
      .expect_err("writes should stay rejected after repeated close");
    let message = error.to_string().to_lowercase();

    assert!(
      message.contains("closing") || message.contains("closed"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn deshred_concurrent_close_write_race_is_stable_and_stream_ends_closed() {
    for _ in 0..32 {
      let (stream, _writable_rx) = make_test_deshred_stream();
      let stream = Arc::new(stream);
      let stream_for_close = stream.clone();
      let stream_for_write = stream.clone();

      let (close_result, write_result) =
        tokio::join!(async move { stream_for_close.close() }, async move {
          stream_for_write.write(empty_subscribe_deshred_request())
        });

      close_result.expect("close should never fail");
      if let Err(error) = write_result {
        let message = error.to_string().to_lowercase();
        assert!(
          message.contains("closing")
            || message.contains("closed")
            || message.contains("channel closed"),
          "unexpected race error message: {error}"
        );
      }

      let post_close_error = stream
        .write(empty_subscribe_deshred_request())
        .expect_err("writes after close/write race should be rejected");
      let post_close_message = post_close_error.to_string().to_lowercase();
      assert!(
        post_close_message.contains("closing") || post_close_message.contains("closed"),
        "unexpected post-race error message: {post_close_error}"
      );
    }
  }

  #[tokio::test]
  async fn deshred_close_returns_lock_error_when_writable_lock_is_poisoned() {
    let (stream, _writable_rx) = make_test_deshred_stream();
    {
      let writable_poison = stream.writable.clone();
      let _ = std::panic::catch_unwind(move || {
        let _guard = writable_poison
          .lock()
          .expect("lock should be available before intentional poison");
        panic!("intentional poison");
      });
    }

    let error = stream
      .close()
      .expect_err("close should fail when writable lock is poisoned");
    assert!(
      error
        .to_string()
        .contains("Failed to acquire writable lock"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for poisoned lock error"
    );
  }

  #[tokio::test]
  async fn deshred_write_returns_lock_error_when_writable_lock_is_poisoned() {
    let (stream, _writable_rx) = make_test_deshred_stream();
    {
      let writable_poison = stream.writable.clone();
      let _ = std::panic::catch_unwind(move || {
        let _guard = writable_poison
          .lock()
          .expect("lock should be available before intentional poison");
        panic!("intentional poison");
      });
    }

    let error = stream
      .write(empty_subscribe_deshred_request())
      .expect_err("write should fail when writable lock is poisoned");
    assert!(
      error
        .to_string()
        .contains("Failed to acquire writable lock"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected nested cause for poisoned lock error"
    );
  }

  #[tokio::test]
  async fn deshred_read_returns_none_when_channel_closed_without_terminal_error() {
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdateDeshred>();
    drop(readable_tx);
    let readable = Arc::new(Mutex::new(readable_rx));
    let terminal_error = Arc::new(StdMutex::new(None));

    let result = DuplexStreamDeshred::recv_update_or_error(readable, terminal_error)
      .await
      .expect("closed channel without error should map to None");

    assert!(result.is_none());
  }

  #[tokio::test]
  async fn deshred_read_returns_terminal_error_when_channel_closed_after_worker_failure() {
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdateDeshred>();
    drop(readable_tx);
    let readable = Arc::new(Mutex::new(readable_rx));
    let terminal_error = Arc::new(StdMutex::new(Some(terminal_error_with_cause(
      "deshred stream receive failed: channel closed",
      "upstream grpc status unavailable",
    ))));

    let error = DuplexStreamDeshred::recv_update_or_error(readable, terminal_error)
      .await
      .expect_err("terminal error should be propagated to caller");

    assert!(
      error
        .to_string()
        .contains("deshred stream receive failed: channel closed"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected terminal error cause to propagate through deshred read()"
    );
  }

  #[tokio::test]
  async fn deshred_read_returns_terminal_error_when_terminal_error_lock_is_poisoned() {
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdateDeshred>();
    drop(readable_tx);
    let readable = Arc::new(Mutex::new(readable_rx));
    let terminal_error = Arc::new(StdMutex::new(None));
    {
      let terminal_error_poison = terminal_error.clone();
      let _ = std::panic::catch_unwind(move || {
        let mut guard = terminal_error_poison
          .lock()
          .expect("lock should be available before intentional poison");
        *guard = Some(napi::Error::from_reason(
          "deshred stream receive failed: poisoned lock",
        ));
        panic!("intentional poison");
      });
    }

    let error = DuplexStreamDeshred::recv_update_or_error(readable, terminal_error)
      .await
      .expect_err("terminal error should still propagate from poisoned lock");

    assert!(
      error
        .to_string()
        .contains("deshred stream receive failed: poisoned lock"),
      "unexpected error message: {error}"
    );
  }
}
