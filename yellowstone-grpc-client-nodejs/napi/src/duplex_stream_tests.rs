use crate::{js_types::JsSubscribeRequest, DuplexStream};
use std::collections::HashMap;
use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc, Mutex as StdMutex,
};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeUpdate};

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
      is_closing: Arc::new(AtomicBool::new(false)),
    },
    writable_rx,
  )
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
