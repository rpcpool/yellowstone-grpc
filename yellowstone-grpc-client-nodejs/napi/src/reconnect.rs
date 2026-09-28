use futures::stream::BoxStream;
use futures::StreamExt;
use napi::{bindgen_prelude::*, Env};
use napi_derive::napi;
use prost::Message;
use yellowstone_grpc_client::{DiscardReason, ReconnectEvent, SlotWinner, SubscribeRequestSink};
use yellowstone_grpc_proto::tonic::Status;

use crate::{client::GrpcClient, napi_error_with_cause, DuplexStream, DuplexStreamInner};

#[napi(object)]
pub struct JsBankRef {
  pub generation: String,
  pub slot: String,
  pub bank_id: String,
}

#[napi(object)]
pub struct JsReplacementReplay {
  pub from_slot: String,
  pub generation: String,
}

#[napi]
pub enum JsSlotWinner {
  Unknown { slot: String },
  Finalized { slot: String, blockhash: String },
}

#[napi(string_enum)]
pub enum JsDiscardReason {
  IncompleteDelivery,
}

#[napi]
pub enum JsReconnectEvent {
  Update {
    generation: String,
    update: Buffer,
  },
  DiscardBanks {
    banks: Vec<JsBankRef>,
    reason: JsDiscardReason,
    replacement: JsReplacementReplay,
    winners: Vec<JsSlotWinner>,
  },
}

impl From<ReconnectEvent> for JsReconnectEvent {
  fn from(event: ReconnectEvent) -> Self {
    match event {
      ReconnectEvent::Update { generation, update } => Self::Update {
        generation: generation.to_string(),
        update: update.encode_to_vec().into(),
      },
      ReconnectEvent::DiscardBanks {
        banks,
        reason,
        replacement,
        winners,
      } => Self::DiscardBanks {
        banks: banks
          .into_iter()
          .map(|bank| JsBankRef {
            generation: bank.generation.to_string(),
            slot: bank.slot.to_string(),
            bank_id: bank.bank_id.to_string(),
          })
          .collect(),
        reason: match reason {
          DiscardReason::IncompleteDelivery => JsDiscardReason::IncompleteDelivery,
        },
        replacement: JsReplacementReplay {
          from_slot: replacement.from_slot.to_string(),
          generation: replacement.generation.to_string(),
        },
        winners: winners
          .into_iter()
          .map(|winner| match winner {
            SlotWinner::Unknown { slot } => JsSlotWinner::Unknown {
              slot: slot.to_string(),
            },
            SlotWinner::Finalized { slot, blockhash } => JsSlotWinner::Finalized {
              slot: slot.to_string(),
              blockhash,
            },
          })
          .collect(),
      },
    }
  }
}

type EventStream = BoxStream<'static, std::result::Result<ReconnectEvent, Status>>;
type ReconnectInner = DuplexStreamInner<SubscribeRequestSink, EventStream>;

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
  use yellowstone_grpc_client::{BankRef, ReplacementReplay};

  #[test]
  fn discard_preserves_uint64_bank_identities_and_winner_variants() {
    let event = JsReconnectEvent::from(ReconnectEvent::DiscardBanks {
      banks: vec![BankRef {
        generation: u64::MAX,
        slot: u64::MAX - 1,
        bank_id: u64::MAX - 2,
      }],
      reason: DiscardReason::IncompleteDelivery,
      replacement: ReplacementReplay {
        from_slot: u64::MAX - 1,
        generation: u64::MAX,
      },
      winners: vec![
        SlotWinner::Finalized {
          slot: u64::MAX - 1,
          blockhash: "winner".into(),
        },
        SlotWinner::Unknown { slot: u64::MAX },
      ],
    });
    let JsReconnectEvent::DiscardBanks {
      banks,
      reason,
      replacement,
      winners,
    } = event
    else {
      panic!("expected DiscardBanks");
    };
    assert_eq!(banks[0].generation, u64::MAX.to_string());
    assert_eq!(banks[0].slot, (u64::MAX - 1).to_string());
    assert_eq!(banks[0].bank_id, (u64::MAX - 2).to_string());
    assert!(matches!(reason, JsDiscardReason::IncompleteDelivery));
    assert_eq!(replacement.from_slot, (u64::MAX - 1).to_string());
    assert_eq!(replacement.generation, u64::MAX.to_string());
    assert!(
      matches!(&winners[0], JsSlotWinner::Finalized { slot, blockhash }
      if slot == &(u64::MAX - 1).to_string() && blockhash == "winner")
    );
    assert!(matches!(&winners[1], JsSlotWinner::Unknown { slot } if slot == &u64::MAX.to_string()));
  }

  #[tokio::test]
  async fn close_releases_recovery_stream_and_rejects_writes() {
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
