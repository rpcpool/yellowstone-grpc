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

    let (close_result, write_result) = tokio::join!(
      async move { stream_for_close.close() },
      async move { stream_for_write.write(empty_subscribe_request()) }
    );

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
