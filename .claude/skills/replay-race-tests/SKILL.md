---
name: replay-race-tests
description: How to write deterministic unit tests for the geyser plugin's from_slot replay and live-stream join (geyser_loop, block_reconstruction_loop, client_loop, ReplayPhase) — forcing a slot to seal during a replay request, holding one batch between its broadcast and its handover, and checking every update arrives once. Use when adding or changing a test for replay duplicates, gaps or ordering in yellowstone-grpc-geyser.
---

# Deterministic replay and live-join tests

The replay and live-join bugs are races between three tasks: `geyser_loop`, which broadcasts
raw batches and then hands them over; `block_reconstruction_loop`, which builds the replay;
and `client_loop`, which subscribes and asks for the replay. A test must force one
interleaving every run. Use these five tools, then check the test fails with the fix
disabled.

Existing examples, in `yellowstone-grpc-geyser/src/grpc.rs`, module
`tests::from_slot_partial_slot`:

- `subscribe_while_a_slot_seals` — a slot seals while the replay request is held.
- `processed_client_gets_a_batch_handed_over_after_the_replay` — one batch held between its
  broadcast and its handover.
- `processed_client_gets_a_bank_waiting_for_its_status_once` — a bank sealed but waiting for
  its Processed status when the replay is built.

`ReplayPhase` alone is unit-tested in `yellowstone-grpc-geyser/src/geyser_subscriber.rs`, with
a plain `broadcast::channel` and hand-built `ReplayCut`s, by collecting what `next` hands out.

## 1. One batch per send: the `current_thread` runtime

`#[tokio::test]` runs on a `current_thread` runtime, so no task runs until the test awaits.
Messages sent to `geyser_loop` without an `await` in between therefore reach it together, and
it broadcasts them as one batch. Put everything that must be one batch into one `Vec` and send
it before awaiting.

## 2. Hold a replay request: a stub on the replay channel

Give `client_loop` its own replay sender and forward the request to block reconstruction only
when released. This holds the window between the client's subscription (L) and the replay
(R) open while the test sends more data:

```rust
let (client_replay_tx, mut client_replay_rx) = mpsc::channel::<ReplayStoredSlotsRequest>(1);
let (requested_tx, requested_rx) = oneshot::channel();
let (release_tx, release_rx) = oneshot::channel::<()>();
tokio::spawn(async move {
    let request = client_replay_rx.recv().await.expect("replay request");
    let _ = requested_tx.send(());
    release_rx.await.expect("release");
    replay_tx.send(request).await.expect("block reconstruction open");
});
```

`requested_rx` resolving means the client has subscribed and is waiting for its replay.

## 3. Hold a batch after its broadcast: a gate before block reconstruction

Put a task between `geyser_loop`'s hand-over channel and block reconstruction that holds
messages while closed. A batch sent while it is closed is broadcast but not taken in: the
race `ReplayCut::next_taken_in` exists for.

```rust
let (block_reconstruction_tx, mut gate_rx) = mpsc::unbounded_channel();
let (gate_tx, block_reconstruction_rx) = mpsc::unbounded_channel();
let (closed, mut is_closed) = watch::channel(false);
tokio::spawn(async move {
    while let Some(message) = gate_rx.recv().await {
        if is_closed.wait_for(|closed| !closed).await.is_err()
            || gate_tx.send(message).is_err()
        {
            return;
        }
    }
});
```

Close it, send the batch, wait for its broadcast, subscribe the client and read the replay,
then open it.

## 4. Block reconstruction behind: a queue longer than tokio's budget

To test what a replay does with a long backlog, drive `block_reconstruction_loop` alone. Fill
its queue and its replay channel before spawning it, so it starts with everything waiting:

- **Size:** each queue read takes up to 1024 items. Only the first goes through the budgeted
  `poll_recv`; the rest use `try_recv`, which tokio does not budget. So the budget runs out
  after about 128 × 1024 items, and the queue must be longer than that, such as 140,000
  one-transaction `BroadcastBatch`es.
- **Check:** count the replay's messages. All of them must be there.

Example: `replay_takes_in_the_whole_queue_however_long`. With a `now_or_never` drain instead
of the biased `select!`, it gets 130,047 of 140,000, every run.

## 5. Something happens between two reads: a stub queue

`block_reconstruction_loop` is generic over its `BatchStream`, so a test can wrap the queue
and run code at an exact point of the loop. `StaleEmptyRead` runs a closure the first time the
queue reads empty: it hands over two batches and sends the replay request, as other threads
could between that read and the request read. Check the replay holds both batches.

Example: `replay_holds_what_was_handed_over_before_its_request_was_read`. With the replay
built in the request arm, it gets neither batch, every run.

## Waiting and reading

- Wait for a broadcast with `wait_broadcast` on a receiver the test subscribed itself, not
  with sleeps.
- Read the client stream with `read_until_confirmed`, which records transactions, CreatedBank
  and Processed statuses in `Received`.
- A processed client that replayed waits for the next raw batch before going live: after the
  last statuses, send the next slot's raw data or the test times out.
- Stop at a point the live stream must reach after every duplicate, such as the next slot's
  Confirmed status, or a duplicate arriving later goes unseen.

## Prove the test

1. Run it 20–50 times in a loop; it must pass every run.
2. Disable the fix, for example `&& false` on the guard, and run it again in a loop. It must
   fail every run, with the expected `left:`. A test that passes some runs without the fix
   does not pin the interleaving.

## Diagram

Document each test with a left-to-right timeline: a `channel` row with L and R marked, then
aligned `replay`, `live` and `client` rows, overlap or gap marked, and a key of the batches.
See the tests in `geyser_subscriber.rs`.
