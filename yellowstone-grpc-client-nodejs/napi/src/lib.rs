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
mod utils;

use futures_util::{SinkExt, StreamExt};
use napi::{bindgen_prelude::*, Env};
use napi_derive::napi;
use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc, Mutex as StdMutex, Once,
};
use tokio::sync::{
  mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
  oneshot, Mutex,
};
use yellowstone_grpc_proto::prelude::*;

use crate::{
  client::GrpcClient,
  js_types::{JsSubscribeRequest, JsSubscribeUpdate},
};

pub mod js_types;
#[cfg(test)]
mod js_types_tests;

static INITIALIZE_CRYPTO_PROVIDER: Once = Once::new();

/// Initialize crypto provider once.
fn init_crypto_provider() {
  INITIALIZE_CRYPTO_PROVIDER.call_once(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
  });
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
  writable: Arc<StdMutex<Option<UnboundedSender<SubscribeRequest>>>>,
  /// One-shot shutdown signal for the worker spawned by `subscribe()`.
  shutdown_tx: Arc<StdMutex<Option<oneshot::Sender<()>>>>,
  /// Set once JS has started destroying this stream.
  is_closing: Arc<AtomicBool>,
}

#[napi]
impl DuplexStream {
  // #[napi]
  pub fn subscribe(env: &Env, grpc_client: &GrpcClient) -> Result<Self> {
    let client_holder = grpc_client.holder.clone();

    // TODO : Fine tune unbounded channels.
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdate>();
    let (writable_tx, mut writable_rx) = unbounded_channel::<SubscribeRequest>();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let writable = Arc::new(StdMutex::new(Some(writable_tx)));
    let shutdown_tx = Arc::new(StdMutex::new(Some(shutdown_tx)));
    let is_closing = Arc::new(AtomicBool::new(false));
    let is_closing_worker = is_closing.clone();

    // Spawn one worker per `subscribe()` call. The worker owns both sides of
    // the gRPC bidirectional stream and forwards data between JS and gRPC.
    env.spawn_future(async move {
      let holder = client_holder
      .downcast_ref::<crate::client::internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
      .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

      // Acquire lock, call subscribe, and immediately release the lock
      // The returned streams are independent and don't need the client lock
      let (mut stream_tx, mut stream_rx) = {
        let mut client = holder.client.lock().await;
        match client.subscribe().await {
          Ok(stream) => stream,
          Err(e) => return Err(napi::Error::from_reason(e.to_string()))
        }
      }; // Lock is dropped here when client goes out of scope

      loop {
        tokio::select! {
          // Explicit shutdown from JS destroy/close path.
          _ = &mut shutdown_rx => {
            break;
          },

          // 1. SubscribeRequest is received from self.write().
          // 2. SubscribeRequest is propagated to Geyser client's sender.
          req_option = writable_rx.recv() => {
            if let Some(request) = req_option {
              if let Err(e) = stream_tx.send(request).await {
                if is_closing_worker.load(Ordering::Relaxed) {
                  break;
                }
                return Err(napi::Error::from_reason(e.to_string()))
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
              Some(Err(e)) => {
                if is_closing_worker.load(Ordering::Relaxed) {
                  break;
                }
                return Err(napi::Error::from_reason(e.to_string()))
              }
              None => break,
            }
          }
        }
      }

      Ok(())
    })?;

    Ok(Self {
      readable: Arc::new(Mutex::new(readable_rx)),
      writable,
      shutdown_tx,
      is_closing,
    })
  }

  /// Read JS Accesspoint.
  ///
  /// Retrieve one `SubscribeUpdate` from the worker and convert it to
  /// the generated N-API JS representation (`JsSubscribeUpdate`).
  #[napi]
  pub fn read<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, JsSubscribeUpdate<'env>>> {
    let readable = self.readable.clone();

    env.spawn_future_with_callback(
      async move {
        match readable.lock().await.recv().await {
          Some(update) => Ok(update),
          // Channel close indicates worker termination; JS wrapper treats this
          // as stream end and destroys the Node duplex stream.
          None => Err(napi::Error::from_reason("No update available")),
        }
      },
      move |environment, subscribe_update| {
        JsSubscribeUpdate::from_protobuf_to_js_type(environment, subscribe_update)
      },
    )
  }

  /// Write JS Accesspoint.
  ///
  /// Accept a JS request object, convert to protobuf, then enqueue for the
  /// worker to forward to the gRPC request sink.
  #[napi]
  pub fn close(&self) -> Result<()> {
    self.is_closing.store(true, Ordering::Relaxed);

    let mut shutdown_guard = self
      .shutdown_tx
      .lock()
      .map_err(|_| napi::Error::from_reason("Failed to acquire shutdown lock"))?;
    if let Some(shutdown_tx) = shutdown_guard.take() {
      let _ = shutdown_tx.send(());
    }
    drop(shutdown_guard);

    let mut writable_guard = self
      .writable
      .lock()
      .map_err(|_| napi::Error::from_reason("Failed to acquire writable lock"))?;
    *writable_guard = None;

    Ok(())
  }

  #[napi]
  pub fn write(&self, request: JsSubscribeRequest) -> Result<()> {
    if self.is_closing.load(Ordering::Relaxed) {
      return Err(napi::Error::from_reason(
        "Cannot write to a closing subscription stream",
      ));
    }

    let protobuf_subscribe_request: SubscribeRequest = request.from_js_to_protobuf_type()?;

    let writable = self
      .writable
      .lock()
      .map_err(|_| napi::Error::from_reason("Failed to acquire writable lock"))?
      .as_ref()
      .cloned()
      .ok_or_else(|| napi::Error::from_reason("Cannot write to a closed subscription stream"))?;

    if let Err(e) = writable.send(protobuf_subscribe_request) {
      return Err(napi::Error::from_reason(e.to_string()));
    }
    Ok(())
  }
}
