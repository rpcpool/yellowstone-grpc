# AGENTS.md

## Scope: non-leader RPC nodes only

The geyser plugin is meant to run only on RPC nodes that don't take part in consensus:
unstaked, never leader, with RPC enabled. It is not supported on a validator that votes or
produces blocks.

- **What it can rely on:** every bank it sees is a replayed bank, built from shreds the node
  received. RPC is on, so Processed, Confirmed and Finalized statuses always arrive.
- **What it does not handle:** orderings that only a leader produces. Examples are a leader
  bank's `entries_count` of 0, the Alpenglow leader footer arriving after BlockMeta, the final
  tick queued after freeze, and a leader `BankFreezeTimeout` bank with no footer.
- **In reviews:** don't report a leader-only case as a bug, and don't add code for one. If the
  plugin must ever run on a leader, that is a new requirement: revisit block reconstruction's
  completion checks first. The leader paths are described in
  `.claude/rules/geyser-ordering.md`.

## Invariants

### A subscription replays at most once, on its first request

Only the first `SubscribeRequest` that asks for something can start a `from_slot` replay. Two
kinds of request don't count, and the server keeps them away from `client_loop`:

- **Pings:** the server answers them itself.
- **Empty requests before the first real one:** no filter and no `from_slot` (`asks_for_nothing`
  in `grpc.rs`; a commitment or an accounts data slice alone asks for nothing). The Rust and
  TS SDKs' `subscribe()` send one before the user's own request, so taking it as the first
  request would silently ignore the user's `from_slot`. After the first real request, an empty
  request is an ordinary filter update and goes through.

The server ignores `from_slot` on every later request, so a subscription replays at most once,
right after its first real request, and only if that request sets `from_slot`.

- **Why:** `client_loop` subscribes to no live channel until the first request, so the
  replay can only start before anything live has reached the client. `ReplayPhase` then
  joins the two once: it hands out the replay and the live backlog that follows it, and only
  then gives the live receiver back, so the live path is a plain `recv` with no check.
- **Where:** the `subscribe` request task (which skips the empty requests), `client_loop` and
  `block_reconstruction_loop` in `yellowstone-grpc-geyser/src/grpc.rs`, and `ReplayPhase` in
  `yellowstone-grpc-geyser/src/geyser_subscriber.rs`.
- **Snapshots:** a subscription with a startup snapshot never replays. The snapshot loop takes
  the first request.
- **Keep it:** do not honour `from_slot` on a later request. A replay in the middle of a live
  stream sends the banks that sealed between the client's live receiver and the replay twice.
  A client that wants a replay must put `from_slot` in its first real request.
- **Don't make it stricter:** don't count an empty request as the first request again. Every
  `subscribe()` then `sink.send(from_slot)` client loses its replay without an error. Checked
  end to end by `replay-after-sdk-subscribe`; the unit tests drive `client_loop` directly and
  skip the request task.

### Each bank arrives in the same order, at every commitment

Whatever the subscription's commitment, live or replayed, a bank reaches the client in this
order:

```text
time ──────────────────────────────────────────────────────────────────────►
content (any order)   block footer   block meta   Processed   Confirmed   Finalized
```

- **Content** arrives in any order: account updates, transactions, entries, and the bank's
  CreatedBank.
- **Then** the block footer, then the block meta. The reconstructed Block, for `blocks`
  subscribers, comes just before the footer.
- **Then** the commitment statuses the bank has reached, in the order Processed, Confirmed,
  Finalized. A subscriber below Finalized still gets the higher statuses, when the bank
  reaches them.
- **Lifecycle events** are not held back. FirstShredReceived, Completed and Dead name no bank:
  they go out as they land, at every commitment (block reconstruction sends them to confirmed
  and finalized). CreatedBank goes out as it lands at processed, and with the bank's content at
  confirmed and finalized, never as it lands there: block reconstruction must not send it with
  them, or confirmed subscribers get it twice.

Where: `GrpcService::take_in` in `yellowstone-grpc-geyser/src/grpc.rs`. A commitment status
is never broadcast on a channel before that channel's copy of the bank's content and block
meta. Block reconstruction holds a bank's footer, block meta and statuses until the bank
seals (`BlockReconstructionMessage::SealGated`), then sends, per channel:

- the channel the bank just reached: its CreatedBank and content (confirmed and finalized),
  its Block, footer and block meta, then every status up to this one, in order;
- the lower channels, which already have the content: this status only;
- the higher channels: nothing yet; they get it all when the bank reaches them.

The replay keeps the same order for each bank. Tested by
`live_order_is_content_then_meta_then_statuses_at_every_commitment`, and end to end by
`replay-ordering[Processed|Confirmed|Finalized]`.

### The replay and the live stream overlap, never leave a gap

A `from_slot` client gets the replay, then the live stream. The replay is built at R, after
the client's live receiver subscribed at L, so the two share what went out between L and R:

```text
time ──────────────────────────────►
channel:   b1   b2   b3   b4   b5
         L              R
replay:    b1   b2   b3
live:      b1   b2   b3   b4   b5
           └─ overlap ──┘           dropped by `ReplayPhase`
client:                   b4   b5
```

An overlap is dropped; a gap cannot be recovered. Any change to the replay path must keep the
two overlapping. `ReplayCut` tells the client where R falls on its receiver: `last_taken_in`
(the last raw batch block reconstruction took in), `last_sent` (the last batch it sent)
and `next_taken_in` (the one raw batch that can be in neither, see below). The cuts are the
very `Arc`s that went out, found with `Arc::ptr_eq`: no lock, counter or atomic. Keep it that
way: the broadcast send and the live receive are the hot path.

End to end, `replay-ordering[…]` and `replay-contiguous[…]` check it (see "E2E tests" below).

### geyser_loop broadcasts a raw batch, then hands it over, with nothing in between

`next_taken_in` relies on this, in `geyser_loop`:

1. it is the only task that broadcasts raw batches and hands them to block reconstruction;
2. it broadcasts each batch, then hands it over, with no `await` in between, so a batch is
   handed over before the next one is broadcast;
3. the client subscribes its live receiver before asking for the replay.

Then at most one raw batch is in neither the replay nor the live stream: broadcast before L,
handed over after block reconstruction built the replay. It is the first raw batch block
reconstruction takes in afterward, so `next_taken_in` delivers it. A second producer, an
`await` between the two steps, or handing over before broadcasting breaks this and can lose
data silently.

### Block reconstruction builds a replay only after the queue reads empty, after the request

Every batch in block reconstruction's queue went out live, maybe before the client
subscribed, so the replay must hold all of them; `next_taken_in` recovers only one.
`block_reconstruction_loop` uses a `biased` `select!` with four arms, in this order:

1. the queue;
2. the build arm, `consume_budget()` while a request is pending: it builds the replay;
3. the route changes;
4. the request arm, while none is pending: it only stores the request.

So a replay is built only when a queue read made after its request was taken reads `Pending`.

- **Why not build in the request arm:** the arms are read one after the other, not at once.
  Between an empty queue read and the request read, geyser_loop can hand over two batches and
  the client subscribe and send its request. A replay built then misses both, and the second
  is lost silently. Tested by `replay_holds_what_was_handed_over_before_its_request_was_read`,
  whose queue runs that interleaving on its first empty read.
- **Tokio's budget:** a tokio channel read also returns `Pending` when the task's cooperative
  budget (128 reads per poll) runs out, with items still queued. The build arm is safe only
  because `consume_budget()` is on the same budget: it reads `Pending` too, and the task
  yields and comes back with a fresh budget. The request channel is a tokio channel on the
  same budget as well.
- **Keep it:** don't add an arm that is always ready after the queue arm, and don't drain the
  queue with `now_or_never` before a replay. The drain loop that did lost every item past
  about 127 × 1024 queued ones when block reconstruction was behind. Tested by
  `replay_takes_in_the_whole_queue_however_long` (140,000 queued transactions, all in the
  replay).
- **Cost:** while block reconstruction is behind, a replay waits until it catches up. If it
  waits long enough for the client's live receiver to lag, the replay fails with an error;
  nothing is lost silently.

### Block reconstruction sends only through `BroadcastPosition::send`

`last_sent` is the client's cut for block reconstruction's own batches. A send that bypasses
it leaves it stale, and clients then keep repeated batches as new. A replay's answer goes on
the relay to `geyser_loop` too, behind the batches relayed before it, so the client's receiver
holds `last_sent` when the answer arrives.

### `BlockReconstructionMessage` is named by whether its content was broadcast

`BroadcastBatch` is the very `Arc` `geyser_loop` broadcast, so it is a cut and updates
`last_taken_in`. `SealGated` (block metas, footers, commitment statuses) is never in a raw
batch, so it is never a cut. If a broadcast event is ever sent another way (say a
`BroadcastSingle`), it must update `last_taken_in` too.

### A sealed bank is not out at processed until its Processed status arrives

Block reconstruction sends a bank's Block, BlockMeta and Processed status only when the
status arrives, which can be after the bank sealed. A processed replay therefore sends only
the data of a bank not in `BroadcastPosition::sent_at_processed`, and leaves its block and
statuses to the live stream. Including them sends them twice.

### Testing these races

Use the `replay-race-tests` skill in `.claude/skills/` to write deterministic tests for the
replay and live join.

## E2E tests

The scenarios live in `yellowstone-grpc-e2e-test/src/scenarios/` and run with the
`yellowstone-e2e` binary against a cluster running the plugin.

### Replay scenarios

- `replay-ordering[Processed|Confirmed|Finalized]` checks every bank from `from_slot` across
  the switch from replay to live: CreatedBank once and before the block meta, no duplicate
  transaction or entry, the whole bank (counts match the block meta), Block, footer and block
  meta in order and once, statuses after the meta in order.
- `replay-contiguous[Processed|Confirmed|Finalized]` replays from one slot behind the tip and
  checks that every slot up to ten past the processed tip arrives. It assumes no skipped
  slot: a slot its leader skipped fails it although nothing was lost. Check the slot on the
  cluster before reading such a failure as data loss.
- `replay-after-sdk-subscribe` asks for a replay the way SDK users do: `subscribe()`, which
  sends an empty request, then the request with `from_slot` on the sink. The first block meta
  must be from near `from_slot`, which only the replay can send.

### Start a replay from the commitment's own tip

A replay test at confirmed or finalized replays from `get_slot(Some(commitment))` minus a few
slots, not from the processed tip. From the processed tip, a shallow replay holds no bank
that already reached the commitment, and a deep one reaches past the server's replay buffer
(`replay_stored_slots`, 150 slots by default) and fails with "not available".

### Running

- The block footer scenarios (`scenarios/default/block_footer/`, one subfile per topic) are
  part of the `default` module; `--tags block-footer` runs them alone, `--exclude-tags
  block-footer` skips them.
- `--tags` and `--exclude-tags` take comma-separated lists, and can be repeated:
  `yellowstone-e2e all --exclude-tags block-footer,gossip`.
