# Streaming latency vs. subscriber count — analysis & optimization targets

## Symptom

The geyser plugin gets slower and streaming latency increases as the number of
subscribers grows, and it degrades over time.

## Verdict

This is evident in the code. It is not a single bug but **several compounding
`O(number_of_subscribers)` costs that all sit on the critical path of every
message**.

## Architecture (relevant part)

`geyser_loop` (`src/grpc.rs:741`) is a **single task** that ingests all geyser
messages, pre-encodes each batch **once** (`parallel_encoder.encode`,
`src/grpc.rs:999`), and fans them out through one `tokio::sync::broadcast`
channel (`src/grpc.rs:605`; sends at `:1001` / `:1010` / `:1015`). Each
subscriber gets its own `client_loop` task (spawned at `src/grpc.rs:1669`) that
receives from `broadcast_tx.subscribe()` (`:1676`) and independently filters
every message via `session.filter.get_updates(...)` (`src/grpc.rs:1281`).

The pre-encode-once design is good and deliberately avoids per-subscriber
encoding. But everything *downstream* of it scales with subscriber count.

## Why it slows down as subscribers grow

### 1. `tokio::sync::broadcast` wakes *every* receiver on *every* send — O(N) on the producer's critical path
This is the dominant factor. tokio's broadcast channel, on each `send`, locks
its internal tail mutex and drains/wakes the entire waiter list. With N parked
`client_loop` receivers, each of the three sends per message batch
(Processed/Confirmed/Finalized, `src/grpc.rs:999-1015`) wakes all N tasks under
that lock. The single `geyser_loop` therefore spends time linear in subscriber
count *per message*. As N rises, send latency rises, messages back up in the
unbounded `messages_rx`, and end-to-end streaming latency grows.

### 2. All three commitment levels are broadcast to all clients — ~3× wake amplification
There is one broadcast channel carrying Processed + Confirmed + Finalized. A
client subscribed at `processed` is still woken for confirmed/finalized batches
and only then discards them via the cheap
`commitment == session.filter.get_commitment_level()` check
(`src/grpc.rs:1279`). The filtering is skipped, but the per-task wakeup/recv
cost (factor #1) is paid regardless.

### 3. Per-subscriber filtering competes for a fixed 8-thread runtime
`get_updates` (`src/plugin/filter/filter.rs:236`) — match + build
`FilteredUpdate` + `encoded_len()` (`src/grpc.rs:1282`) — runs once per
subscriber per message. Total filtering CPU is O(N × message_rate), but
`config.json` pins `worker_threads: 8` with core affinity. Once N × rate exceeds
what 8 threads sustain, per-client mpsc queues (capacity 100_000) fill,
`queue_size` climbs, latency rises, and eventually clients are dropped with
`"client_channel_full"` (`src/grpc.rs:1288-1294`). Note: `encoded_len()` is cheap
for pre-encoded accounts but is **recomputed fully per subscriber** whenever a
`data_slice` is set (`src/plugin/filter/message.rs:531-537`), since data-slicing
bypasses the shared pre-encoded bytes.

### 4. Prometheus `with_label_values` runs twice per message per subscriber
For every message sent, the loop calls
`incr_grpc_message_sent_counter(&subscriber_id)` and
`incr_grpc_bytes_sent(&subscriber_id, …)` (`src/grpc.rs:1285-1286`,
`:1239-1240`), each doing `with_label_values(&[subscriber_id])`
(`src/metrics.rs:473-483`) — a label hash plus a read-lock on a shared
`IntCounterVec` map. Plus `set_subscriber_queue_size` every loop iteration
(`src/grpc.rs:1160`). At high message rates × many subscribers this is
significant per-message CPU and shared-map lock traffic.

### 5. The "over time" part: unbounded per-subscriber metric cardinality
The per-`subscriber_id` metrics — `GRPC_MESSAGE_SENT`, `GRPC_BYTES_SENT`,
`GRPC_SUBSCRIBER_QUEUE_SIZE`, `GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD`,
`GRPC_CLIENT_DISCONNECTS` — are **never removed**. On disconnect,
`ClientSession::drop` only sets queue size to 0 (`src/grpc.rs:483`); it does not
call `remove_label_values`. Only the per-IP traffic and per-TCP-connection
metrics are actually removed (`src/metrics.rs:618`, `:631-634`). So as distinct
subscribers churn through over time, these label maps grow without bound —
increasing memory, lookup cost on the hot path (#4), and `/metrics` scrape cost.
This is the mechanism by which the plugin degrades *over time*, not just *with
concurrent N*.

## Summary

| Factor | Cost | On critical path? |
|---|---|---|
| broadcast wake-all per send | O(N) per message, single producer task | Yes — serializes all output |
| 3 commitment levels to all clients | ~3× wakeups | Yes |
| per-subscriber filtering | O(N), capped by 8 worker threads | Yes |
| `with_label_values` ×2 per msg/sub | per-message hashing + shared lock | Yes |
| per-subscriber metric label leak | grows unbounded over time | Yes (degrades #4 + scrapes) |

The single-producer broadcast (#1) is the architectural ceiling; the
metric-label leak (#5) best explains the "gets worse over time" symptom
specifically.

## Optimization directions

- **Shard the fan-out.** Replace the single broadcast with multiple broadcast
  channels / dedicated sender tasks, or group subscribers by commitment so each
  receiver only gets its own commitment level (kills factors #1 and #2).
- **Cache per-subscriber metric handles.** Resolve the `IntCounter`/`IntGauge`
  once per `ClientSession` instead of calling `with_label_values` per message
  (factor #4).
- **Clean up metrics on disconnect.** Call `remove_label_values` for all
  per-`subscriber_id` metrics in `ClientSession::drop` (factor #5).
- **Reconsider the fixed thread pool** sizing relative to expected
  subscriber × message-rate load (factor #3).
