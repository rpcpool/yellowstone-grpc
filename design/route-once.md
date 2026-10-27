# Route once: filter each update once, wake only the streams it matches

Branch `feat/latency-harness`. The change is one commit on top of the harness (`perf(geyser): route each update once to the subscribers it matches`). `yellowstone-grpc-bench` measures it.

## Problem

- Every Subscribe stream has a `client_loop` task. It receives every batch of its commitment level from a tokio broadcast and runs `Filter::get_updates` on every message.
- With N streams, each batch costs N task wakes and N filter passes, even when no stream wants it. The wakes queue up on the runtime workers, and processed p99 grows with N.

## Master

```
agave callbacks ──mpsc──► geyser_loop ──batch──► processed broadcast ──┐
                              │                                         │ every stream wakes
                              ▼                                         ▼ for every batch
                    block reconstruction ──sealed──► confirmed / finalized broadcasts
agave status callbacks ─────────────────────────────► (CreatedBank, Dead, ...)
                                                                        │
                                   client_loop × N: get_updates on every message
                                                                        │ try_send
                                                                        ▼
                                   stream queue × N ──► response task: encode + compress
```

## This branch

```
agave callbacks ──mpsc──► geyser_loop = owner of the processed router
                              │  per message: index ─► candidate streams
                              │               candidate's own Filter::get_updates (exact match)
                              │               try_send into that stream's queue
                              ├──────────────────────► stream queue (matching streams only)
                              ├──► raw ring ─────────► client_loop (streams the router does not filter)
                              ▼
                    block reconstruction ──processed sealed batches──► geyser_loop input channel
                                        ──confirmed / finalized───► router task (one per level)
agave status callbacks ─────────────────────────────────────────────► router task inputs
client_loop ──route changes: Add / Update / GoLive / Remove──────────► owner's input channel
```

## Where the table lives

- One `Router` per commitment level, with exactly one owner. `geyser_loop` owns the processed router. A task owns each of the confirmed and finalized routers.
- Nothing else touches a router, so there is no lock. The owner reads one input channel that carries batches and route changes. It applies route changes between batches, in order.
- A router holds the routes (one slot per stream), the index, and a raw ring (a tokio broadcast sized by the existing broadcast capacity config).

## The index

- Key maps: account pubkey → streams, memcmp → streams, owner → streams, transaction key → streams. `Filter::keys` gives the keys:
  - an account filter: its pubkeys; or else the offset and first 32 bytes of a memcmp of at least 32 bytes, such as the mint of token accounts; or else its owners;
  - a transaction filter: one required key, or else its include keys.
- The memcmp key keeps the common "owner = token program, memcmp = mint" filter from becoming a candidate for every token account.
- One bitset of streams per update kind: entries, update parent, slots, blocks, block meta, block footer, and "any account" / "any transaction" for filters without keys.
- The candidates for a message are the OR of the bitsets and the key hits. They are a superset of the matches.
- Each candidate's own `Filter::get_updates` makes the exact match, so streams get exactly the updates they get on master. The per-subscriber filter code, including the per-filter bitset index, stays as it is. The router adds a coarse index over subscribers on top of it.
- Randomized tests check the superset property across every filter option, commitment and message kind.

## Filter updates

- `client_loop` sends `Update` with the new filter and its keys to the owner, then awaits the reply. The owner diffs the sorted key lists and touches only the keys that changed. A re-sent identical filter costs almost nothing.
- An update takes effect at the owner's next iteration.
- A commitment switch awaits `Remove` on the old router before `Add` on the new one, so no update of the old level follows one of the new level.

## Streams the router does not filter

- The router filters a stream only when every account and transaction filter names keys, with at most 8192 keys, 64 account filters and 8 transaction filters, and with no blocks filter and no `token_accounts` expansion.
- Any other stream reads the raw ring and filters its own batches on its `client_loop`, as on master. This covers firehose filters, cuckoo filters, blocks filters (they walk whole blocks), `token_accounts` filters (they parse every transaction's token balances), and very large or very many filters. Their cost stays off the shared path, and a slow one lags and is dropped on its own.
- The limits also bound the work one route change costs the owner, at about 1 ms. Key counts are checked before keys are collected, so a filter with hundreds of thousands of keys costs nothing extra.
- A filter that panics closes only its own stream.

## from_slot replay

- With `from_slot`, the route switches to the raw ring in-band. `client_loop` sends the replay, then filters raw batches with the held-bank logic it has on master.
- A held bank is released when its block seals. A bank that never seals is released once a slot 350 or more slots above it is finalized: block reconstruction has swept it by then.
- When `client_loop` has read its raw backlog, it hands its raw receiver to the owner (`GoLive`). The owner filters the few batches that arrived meanwhile (at most 4) into the stream, and the route goes live. With more, the receiver goes back untouched. Nothing is reordered at the switch.
- A client that took the startup snapshot gets a route right after it.

## What does not change

- Filter semantics and code, the replay content, encoding and compression in the response task, the stream queues and their disconnect rules (`client_channel_full`, `client_broadcast_lag`), deshred and the unary methods.

## Results (harness)

`ab` scenario: 400 local streams with a realistic filter mix, 1500 paced slots, 3 repeats with the variant order rotated. Master against this branch:

| | master | this branch |
|---|---|---|
| probe p99 account / entry | 3.7-5.3 / 7.7 ms | 0.26-0.30 / 0.94-1.0 ms |
| probe p99.9 account / entry | 7.5-8.5 / 10.5-10.6 ms | 1.1-1.9 / 1.8-1.9 ms |
| confirmed account p99 | 10.0-10.4 ms | 2.3-2.6 ms |
| paired per-slot p99, processed account / entry | | -68% to -69% / -79% to -80% |
| paired per-slot p99, confirmed account / entry | | -50% to -57% / -45% to -46% |
| plugin CPU | 1.96 cores | 0.69-0.70 cores |

Every paired confidence interval excludes 0. Both variants delivered the same messages and bytes (87k msg/s, 36.6 MiB/s). The `quick` scenario (60 streams, churn with `from_slot`, emulated links) shows -23% to -56% paired per-slot p99 against the commit before routing.

## Costs and open points

- All processed matching runs serially on `geyser_loop`. The per-message cost is the index lookups, plus one exact match and one `try_send` per candidate stream. A key matched by many streams costs one send per stream; that cost is unavoidable. If it becomes the bottleneck, shard the router by stream across a few owners.
- `geyser_loop` hands each batch to block reconstruction before it routes it, so sealing does not wait for routing.
- A transaction queued before any stream encoded it weighs 1 in the stream queue, since the encoded size is not known yet.
- The owners' input channels are unbounded, as the broadcasts were.

## Next: one encode for many streams

- The router knows, per message, the set of streams that get it. Message payloads are already encoded once (`pre_encoded`). Each stream still encodes its envelope (filter names, `created_at`) and compresses separately.
- Streams that get identical bytes (same filter names, data slice and compression) can share one encoded, and compressed, frame. This needs a pre-encoded queue item and a pass-through codec. Filter names differ per client, so how often frames can be shared depends on the clients.
- With fewer wakes, frames for many streams can then be written with fewer syscalls.

## Review plan

Smaller PRs instead of one big change:

1. `Filter::keys` and `FilterKeys`, a pure function, with the randomized parity tests.
2. `route.rs`: index, router and owners, with their tests, not wired in.
3. Wiring: `geyser_loop` owns the processed router, block reconstruction and the status callbacks send through the routers, `client_loop` uses routes, and the broadcasts go.
4. Later: shared encoding.
