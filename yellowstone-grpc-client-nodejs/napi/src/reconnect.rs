use futures::stream::BoxStream;
use futures::StreamExt;
use napi::{bindgen_prelude::*, Env};
use napi_derive::napi;
use prost::Message;
use yellowstone_grpc_client::{BankRef, ReconnectEvent, SubscribeRequestSink};
use yellowstone_grpc_proto::tonic::Status;

use crate::{client::GrpcClient, napi_error_with_cause, DuplexStream, DuplexStreamInner};

/// A bank: its slot and its `bank_id`. Both are uint64, sent as decimal strings.
#[napi(object)]
pub struct JsBankRef {
  pub slot: String,
  pub bank_id: String,
}

impl From<BankRef> for JsBankRef {
  fn from(bank: BankRef) -> Self {
    Self {
      slot: bank.slot.to_string(),
      bank_id: bank.bank_id.to_string(),
    }
  }
}

/// An item of a `ReconnectDuplexStream`.
#[napi]
pub enum JsReconnectEvent {
  /// An encoded `SubscribeUpdate`.
  Update { update: Buffer },
  /// The connection dropped. Discard `partial_banks`; the stream reconnects from
  /// `replay_from_slot`, or at the live tip without it.
  Disconnected {
    partial_banks: Vec<JsBankRef>,
    replay_from_slot: Option<String>,
  },
}

impl From<ReconnectEvent> for JsReconnectEvent {
  fn from(event: ReconnectEvent) -> Self {
    match event {
      ReconnectEvent::Update(update) => Self::Update {
        update: update.encode_to_vec().into(),
      },
      ReconnectEvent::Disconnected(disconnected) => Self::Disconnected {
        partial_banks: disconnected
          .partial_banks
          .into_iter()
          .map(JsBankRef::from)
          .collect(),
        replay_from_slot: disconnected.replay_from_slot.map(|slot| slot.to_string()),
      },
    }
  }
}

type EventStream = BoxStream<'static, std::result::Result<ReconnectEvent, Status>>;
type ReconnectInner = DuplexStreamInner<SubscribeRequestSink, EventStream>;

/// A subscription that reconnects after a recoverable failure.
#[napi]
pub struct ReconnectDuplexStream {
  inner: ReconnectInner,
  closed: tokio::sync::watch::Sender<bool>,
}

pub(crate) fn subscribe_with_reconnect<'env>(
  env: &'env Env,
  grpc_client: &GrpcClient,
  initial_request_bytes: Option<Buffer>,
) -> Result<PromiseRaw<'env, ReconnectDuplexStream>> {
  let request = initial_request_bytes
    .map(DuplexStream::decode_and_validate_subscribe_request)
    .transpose()?;
  let mut client = grpc_client.client.clone();
  env.spawn_future_with_callback(
    async move {
      let (sink, stream) = client
        .subscribe_with_reconnect(request)
        .await
        .map_err(|error| {
          napi_error_with_cause(
            napi::Status::GenericFailure,
            "failed to open reconnect subscription",
            &error,
          )
        })?;
      Ok(ReconnectDuplexStream {
        inner: ReconnectInner::new(sink, stream.boxed()),
        closed: tokio::sync::watch::channel(false).0,
      })
    },
    |_env, stream| Ok(stream),
  )
}

#[napi]
impl ReconnectDuplexStream {
  #[napi]
  pub fn read<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, Option<JsReconnectEvent>>> {
    let readable = self.inner.readable.clone();
    let terminal_error = self.inner.terminal_error.clone();
    let mut closed = self.closed.subscribe();
    env.spawn_future_with_callback(
      async move {
        if *closed.borrow() {
          return Ok(None);
        }
        tokio::select! {
          biased;
          _ = closed.changed() => Ok(None),
          event = ReconnectInner::recv_item_or_error(readable, terminal_error, "reconnect stream receive failed") => event.map(|event| event.map(JsReconnectEvent::from)),
        }
      },
      |_env, event| Ok(event),
    )
  }

  #[napi]
  pub fn close(&self) -> Result<()> {
    self.closed.send_replace(true);
    self.inner.close()?;
    self.inner.readable.state.lock().expect("state lock").stream = futures::stream::empty().boxed();
    Ok(())
  }

  /// Sends a new request on the live connection. A reconnect subscribes with the latest one.
  #[napi]
  pub fn write_raw<'env>(
    &self,
    env: &'env Env,
    request_bytes: Buffer,
  ) -> Result<PromiseRaw<'env, ()>> {
    let request = DuplexStream::decode_and_validate_subscribe_request(request_bytes)?;
    let sink = self
      .inner
      .take_sink_for_write("Cannot write to a closed subscription stream")?;
    let terminal_error = self.inner.terminal_error.clone();
    env.spawn_future_with_callback(
      ReconnectInner::send_subscribe_request(
        sink,
        request,
        terminal_error,
        "reconnect stream send failed",
      ),
      |_env, ()| Ok(()),
    )
  }
}

impl Drop for ReconnectDuplexStream {
  fn drop(&mut self) {
    let _ = self.close();
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use yellowstone_grpc_client::Disconnected;
  use yellowstone_grpc_proto::prelude::SubscribeUpdate;

  #[test]
  fn disconnected_keeps_uint64_banks_and_replay_slot() {
    let event = JsReconnectEvent::from(ReconnectEvent::Disconnected(Disconnected {
      partial_banks: vec![BankRef {
        slot: u64::MAX - 1,
        bank_id: u64::MAX,
      }],
      replay_from_slot: Some(u64::MAX - 2),
    }));
    let JsReconnectEvent::Disconnected {
      partial_banks,
      replay_from_slot,
    } = event
    else {
      panic!("expected Disconnected");
    };
    assert_eq!(partial_banks[0].slot, (u64::MAX - 1).to_string());
    assert_eq!(partial_banks[0].bank_id, u64::MAX.to_string());
    assert_eq!(replay_from_slot, Some((u64::MAX - 2).to_string()));
  }

  #[test]
  fn disconnected_without_replay_has_no_replay_slot() {
    let event = JsReconnectEvent::from(ReconnectEvent::Disconnected(Disconnected {
      partial_banks: vec![],
      replay_from_slot: None,
    }));
    assert!(matches!(
      event,
      JsReconnectEvent::Disconnected { partial_banks, replay_from_slot: None } if partial_banks.is_empty()
    ));
  }

  #[test]
  fn update_is_the_encoded_subscribe_update() {
    let update = SubscribeUpdate {
      filters: vec!["accounts".to_owned()],
      ..Default::default()
    };
    let JsReconnectEvent::Update { update: bytes } =
      JsReconnectEvent::from(ReconnectEvent::Update(update.clone()))
    else {
      panic!("expected Update");
    };
    assert_eq!(SubscribeUpdate::decode(bytes.as_ref()).unwrap(), update);
  }

  #[tokio::test]
  async fn close_releases_the_stream_and_rejects_writes() {
    let (sink, _requests) = futures::channel::mpsc::channel(1);
    let (updates, stream) = tokio::sync::mpsc::channel(1);
    let duplex = ReconnectDuplexStream {
      inner: ReconnectInner::new(
        SubscribeRequestSink::mock(sink),
        tokio_stream::wrappers::ReceiverStream::new(stream).boxed(),
      ),
      closed: tokio::sync::watch::channel(false).0,
    };
    let mut closed = duplex.closed.subscribe();
    duplex.close().unwrap();
    closed.changed().await.unwrap();
    assert!(*closed.borrow());
    assert!(updates.is_closed());
    assert!(duplex.inner.take_sink_for_write("closed").is_err());
    assert!(ReconnectInner::recv_item_or_error(
      duplex.inner.readable.clone(),
      duplex.inner.terminal_error.clone(),
      "receive failed"
    )
    .await
    .unwrap()
    .is_none());
  }
}
