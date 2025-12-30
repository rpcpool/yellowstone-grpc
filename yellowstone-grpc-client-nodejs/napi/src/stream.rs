//! N-API stream.Duplex Engine
//!
//! This module provides the gateway for the JS
//! runtime to interact with Rust's async runtime.
mod bindings;
mod client;
mod utils;

use futures_util::{SinkExt, StreamExt};
use napi::{bindgen_prelude::*, Env};
use napi_derive::napi;
use std::sync::Arc;
use std::sync::Once;
use tokio::{
  sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
  sync::Mutex,
};
use yellowstone_grpc_proto::prelude::*;

use crate::client::GrpcClient;

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
  readable: Arc<Mutex<UnboundedReceiver<SubscribeUpdate>>>,
  writable: UnboundedSender<SubscribeRequest>,
}

#[napi]
impl DuplexStream {
  // #[napi]
  pub fn subscribe(env: &Env, grpc_client: &GrpcClient) -> Result<Self> {
  
    let client_holder = grpc_client.holder.clone();

    // TODO : Fine tune unbounded channels.
    let (readable_tx, readable_rx) = unbounded_channel::<SubscribeUpdate>();
    let (writable_tx, mut writable_rx) = unbounded_channel::<SubscribeRequest>();

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
          // 1. SubscribeRequest is received from self.write().
          // 2. SubscribeRequest is propagated to Geyser client's sender.
          req_option = writable_rx.recv() => {
            if let Some(request) = req_option {
              if let Err(e) = stream_tx.send(request).await {
                return Err(napi::Error::from_reason(e.to_string()))
              }
            }
          },

          // 1. SubscribeUpdate is received from Geyser client's receiver.
          // 2. SubscribeUpdate is propagated to self.read() for NodeJS consumption.
          Some(update_result) = stream_rx.next() => {
            let update = match update_result {
              Ok(u) => u,
              Err(e) => return Err(napi::Error::from_reason(e.to_string()))
            };

            if let Err(e) = readable_tx.send(update) {
              return Err(napi::Error::from_reason(e.to_string()))
            }
          }

          else => { break; }
        }
      }

      Ok(())
    })?;

    Ok(Self {
      readable: Arc::new(Mutex::new(readable_rx)),
      writable: writable_tx,
    })
  }

  /// Read JS Accesspoint.
  ///
  /// Retrieve one SubscribeUpdate from the worker.
  #[napi]
  pub fn read<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, Object<'env>>> {
    let readable = self.readable.clone();

    env.spawn_future_with_callback(
      async move {
        match readable.lock().await.recv().await {
          Some(update) => Ok(update),
          None => Err(napi::Error::from_reason("No update available")),
        }
      },
      move |env, update| utils::to_js_update(env, update),
    )
  }

  /// Write JS Accesspoint.
  ///
  /// Take in SubscribeRequest and send to the worker.
  #[napi]
  pub fn write(&self, env: &Env, chunk: Object) -> Result<()> {
    let request: SubscribeRequest = utils::js_to_subscribe_request(env, chunk)?;
    if let Err(e) = self.writable.send(request) {
      return Err(napi::Error::from_reason(e.to_string()));
    }
    Ok(())
  }
}
