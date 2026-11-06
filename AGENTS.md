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
- **Where:** the request task, `GrpcService::read_requests` (which skips the empty requests),
  `client_loop` and `block_reconstruction_loop` in `yellowstone-grpc-geyser/src/grpc.rs`, and
  `ReplayPhase` in `yellowstone-grpc-geyser/src/geyser_subscriber.rs`.
- **Snapshots:** a subscription with a startup snapshot never replays. The snapshot loop takes
  the first request.
- **Later requests:** the request task (`GrpcService::read_requests`) posts at most one filter
  change per `filter_apply_interval`, and a newer waiting change replaces an older one. It
  posts the first real request at once, so coalescing never delays or drops a replay. A later
  `from_slot` waits like any change, and `client_loop` still ignores it. During a snapshot or a
  replay, `client_loop` reads no request: the snapshot or replay, and its backlog, use the
  first request's filter. The request task posts no change until `client_loop` drops the
  session's `client_live_tx`, where the live stream starts, so it compiles and holds no filter
  that cannot apply yet. The newest change sent meanwhile then posts, at once or when its
  interval ends, and its filter error comes then. Pings are still answered, and no request is
  dropped. Tested by `changes_sent_during_a_replay_apply_after_it_and_only_the_newest` and
  `the_live_signal_fires_only_when_the_live_stream_starts`.
- **Keep it:** do not honour `from_slot` on a later request. A replay in the middle of a live
  stream sends the banks that sealed between the client's live receiver and the replay twice.
  A client that wants a replay must put `from_slot` in its first real request.
- **Don't make it stricter:** don't count an empty request as the first request again. Every
  `subscribe()` then `sink.send(from_slot)` client loses its replay without an error. Checked
  end to end by `replay-after-sdk-subscribe`, and in the request task by
  `empty_requests_before_the_first_neither_post_nor_start_the_interval`.

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
- **Lifecycle events** wait for no bank. FirstShredReceived, Completed and Dead name no bank:
  they go out as they land at processed. At confirmed and finalized, block reconstruction
  sends them as it takes them in, behind the work queued before them. CreatedBank goes out as
  it lands at processed, and with the bank's content at confirmed and finalized, never as it
  lands there: block reconstruction leaves CreatedBank out of the lifecycle events it
  forwards, or confirmed subscribers get it twice.

Where: `GrpcService::take_in` in `yellowstone-grpc-geyser/src/grpc.rs`. Block reconstruction
never sends a commitment status on a channel before that channel's copy of the bank's content
and block meta. It holds a bank's footer, block meta and statuses until the bank seals
(`BlockReconstructionMessage::SealGated`), then sends, per channel:

- the channel the bank just reached: its CreatedBank and content (confirmed and finalized),
  its Block, footer and block meta, then every status up to this one, in order;
- the lower channels, which already have the content: this status only;
- the higher channels: nothing yet; they get it all when the bank reaches them.

The replay keeps the same order for each bank. Tested by
`live_order_is_content_then_meta_then_statuses_at_every_commitment`, and end to end by
`replay-ordering[Processed|Confirmed|Finalized]`.

### Each commitment level has one owner, its only sender

`geyser_loop` sends everything on the processed channel, and block reconstruction everything
on confirmed and finalized, each through its own `OwnerRoutes`. Block reconstruction relays its
processed batches and its replays to `geyser_loop` (`RelayedItem`). An owner applies each
`RouteChange` between two of its sends, so a client that moves between its route and the
broadcast at one level neither misses nor repeats a batch. Send each batch through its
owner's `OwnerRoutes`: a batch sent any other way never reaches a routed client.

A client joins a route only after its replay phase, so `ReplayPhase` always reads a broadcast
receiver. A routed client gets only what its route keys find: give a new filter kind route keys
in `Filter::route_keys`, or have it return `None`. Tested by the `route/mod.rs` tests,
`a_client_switching_between_routed_and_broadcast_gets_what_its_twin_gets` and
`each_level_gets_the_commitment_statuses_after_the_lifecycle_statuses`.

A replay relies on the same route keys. The bank thread (`BankThread`, named `solGrpcBankDrop`)
builds a `BankRouteKeyIndex` for each bank that seals while replay is on: the positions of the
bank's messages under the hash of each key `for_each_route_key` lists for them. Block
reconstruction puts the index in the bank's `SealedBank` replay items. A replaying client with
route keys gets from such an item only the messages its keys find, in bank order. It still
visits every other replayed message, and every message of a bank with no index yet, and
`Filter::get_updates` still makes the exact match. Keep `for_each_route_key` the one list of
the keys a message carries, for the live router and the index alike: a key either one misses
loses that update. Tested by `a_bank_route_key_index_finds_every_message_that_carries_a_key`,
`a_replay_with_route_keys_hands_out_what_a_full_scan_hands_out`,
`block_reconstruction_hands_a_replay_each_sealed_bank_with_its_route_key_index` and
`a_routed_replay_of_an_indexed_bank_gets_what_its_full_scan_twin_gets`.

A client joins its route in two steps, after its replay phase. It sends `RouteChange::Prepare`
and keeps reading and filtering its broadcast receiver while the owner claims the route's keys.
When the owner answers, the client sends `RouteChange::Join` with the receiver. It sends each
step at a read that finds its receiver and its requests empty, so a queued filter change
applies first. The owner delivers what the receiver got since, then makes the route live,
between two sends. So a joining client gets every update from its first batch on, and the owner
never drains a long backlog. A filter change to other keys or another level leaves the route
with a receiver request and joins again the same way. A change that keeps the key list replaces
the live route's filter in place. Tested by
`a_joining_client_reads_its_broadcast_until_its_route_is_prepared`,
`a_routed_client_that_changes_keys_reads_the_broadcast_from_the_next_batch` and
`a_client_switching_between_routed_and_broadcast_gets_what_its_twin_gets`. Sending only at an
empty read is tested by `a_client_hands_its_receiver_over_only_after_reading_it_to_the_end`,
`a_client_prepares_only_once_it_has_read_its_receiver_to_the_end` and
`a_change_that_comes_with_the_answer_applies_before_the_hand_over`.

Each owner takes in at most 64 queued route changes at the top of every pass, so a change waits
for one pass or one replay build, plus one pass per 64 changes queued ahead of it. Taking a
change in is O(1), except that a hand-over drains what the client's receiver got since its last
read, and a join the owner did not prepare claims all its keys at once (`MAX_ROUTE_KEYS` at
most). A leave or a disconnect removes the route at once and releases its per-kind keys at
once; its other keys wait for release. Claims and releases run in index slices: at most one per
`route_index_interval`, each at most `route_index_budget` plus one step of 64 keys and one
shard growth, and at most eight answered preparations. A slice runs only when the owner's other
arms read `Pending`, or at the top of a pass once index work has waited ten intervals. A pass
of block reconstruction that seals a bank at processed relays to `geyser_loop` first, then
wakes itself and returns `Pending` once (`let_woken_task_run_first`), and sends its confirmed
and finalized batches after that. Any other pass relays only at its end, after its confirmed
and finalized sends, and then wakes itself and returns `Pending` once, before the next pass
takes in a change or runs a slice. Keep all of it: a change that waits for the queue to read
empty holds a joining client's backlog, index work done whole stalls every client's delivery,
a block meta relayed after a bank's routing waits for that routing and then for `geyser_loop`
to run again, and a task woken from a worker waits in that worker's LIFO slot, which no other
worker steals from, until the waking task's poll ends.
`tokio::task::yield_now` does not end the wait in time: it defers the wake until the worker
parks. `geyser_loop`'s relay arm sends every relayed item queued when it reads the first one,
for one unit of tokio's budget, as a read of the plugin's next batch does. Keep it: with one
unit per item, a burst of replay answers spends the budget of 128 units per poll, and a task
whose budget is spent waits in its worker's defer list until the worker parks or runs its
maintenance, with the plugin's batches behind it. Tested by
`geyser_loop_sends_a_burst_of_relayed_items_in_one_poll`. Every route key of a live route holds
a claim for its session id. A route goes live only once all its keys are claimed, and a release
removes one claim, so claims and releases can run in any order. Tested by
`releases_never_remove_a_claim_that_a_live_route_needs`,
`routed_clients_match_their_twins_under_random_route_work`,
`each_pass_takes_in_at_most_64_route_changes`,
`a_batch_ready_when_an_index_slice_is_due_goes_out_before_the_slice`,
`a_pass_that_seals_a_bank_at_processed_relays_before_its_confirmed_sends`,
`a_pass_that_only_confirms_a_bank_relays_after_its_confirmed_sends`,
`block_reconstruction_lets_geyser_loop_run_right_after_it_relays`,
`a_pass_that_relays_at_its_end_lets_geyser_loop_run_before_the_next_pass`,
`block_reconstruction_takes_in_at_most_64_route_changes_per_pass`,
`a_routed_client_behind_a_backlog_keeps_up_as_its_twin_does` and
`a_routed_client_that_ends_behind_a_backlog_ends_its_stream_at_the_next_pass`.

When a client's task ends, `LeaveOnDrop` sends its last leave only to the owner that holds the
client's route, staged or live, and sends none when no owner holds one. It sends it after the
session drops, so the owner is the last task the client's end wakes, and runs next on that
worker. Keep both: a wake from a worker takes the worker's LIFO slot and moves the task woken
before it to the back of the run queue, so an owner woken earlier in the client's end waits
behind the client's other tasks, and in a reconnect storm hundreds of clients end at once.
Tested by `a_client_that_ends_leaves_only_the_owner_that_holds_its_route` and
`the_owner_that_holds_the_route_runs_right_after_its_client_ends`.

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
`block_reconstruction_loop` uses a `biased` `select!` with five arms, in this order:

1. the queue;
2. the build arm, `consume_budget()` while a request is pending: it builds the replay;
3. a route change that arrives while the loop waits;
4. the request arm, while none is pending: it only stores the request;
5. the index slice timer, while index work waits: it runs one slice of route index work. It is
   a tokio `Sleep`, so like the queue it reads `Pending` when the budget is spent.

So a replay is built only when a queue read made after its request was taken reads `Pending`.
Before the `select!`, the loop takes in at most 64 queued route changes, and may run an
overdue index slice. A pass that relayed to `geyser_loop` returns `Pending` once before it ends.
Neither reads the queue or the requests, or spends budget, so they change none of this.

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
it leaves it stale, and clients then keep repeated batches as new. `last_sent` is also out on
its channel before the client gets the replay. `take_in` sends its held batches before it
returns: the confirmed and finalized ones through its `OwnerRoutes`, then the processed ones on
the relay. Every replay goes on the relay (`RelayedItem::Answer`), so at processed it arrives
behind the batches relayed before it. A replay that arrives before `last_sent` is out makes
clients keep repeated batches as new too.

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
