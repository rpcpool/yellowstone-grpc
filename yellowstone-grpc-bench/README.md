# yellowstone-grpc-bench (`ygb`)

`ygb` measures processed-commitment delivery latency of the Yellowstone gRPC geyser plugin with many subscribers. It replays a recorded mainnet firehose into the full plugin, loaded in-process from its shared library the way agave loads it, and measures what subscribers receive.

## Commands

| Command | What it does |
|---|---|
| `ygb record` | Subscribes to a live node at processed on two connections and writes raw frames into a fixture. Main stream: accounts, transactions (votes and failed included), entries with update-parent markers, slots with interslot updates, block meta, and block footers. Deshred stream: deshred transactions, update-parent markers, and slots. |
| `ygb inspect` | Reads a fixture, checks every bank, and prints rates, recording lag, timing facts, and the heaviest slots. |
| `ygb run` | Loads a plugin `.so`, replays a fixture window into it, and writes `driver.json` with lane lag and callback durations. With `--fleet-profile`, it starts `ygb fleet` when prefill ends and stops it when the replay ends. |
| `ygb fleet` | Runs subscribers on a raw HTTP/2 gRPC client. Probes timestamp each message when its last DATA frame arrives, before decompression, and write `samples.bin`. Load subscribers only count bytes. |
| `ygb profiles` | Generates a production-shaped subscriber profile. Filter shapes come from fra215 `filter_stats`, and pubkeys are drawn from the fixture. It picks the sampling exponent whose predicted per-stream rates best match production. |
| `ygb report` | Prints probe latency by probe class, compression, and kind, plus per-slot p99 and stall counts. With a traced plugin it also prints the plugin's stage waterfall. Reference times: processed data uses `created_at`. Slot statuses, and everything on confirmed or finalized streams, use the driver's callback time for that slot's status (`driver-statuses.json`). |
| `ygb compare` | Paired A/B of two runs over the same window. The unit is the slot: per-slot p99 per probe group, paired by slot, with a 95% CI from a 10-slot block bootstrap on the median difference. |

Record from nyc226 (the endpoint needs TLS with SNI `nyc226.nodes.rpcpool.com`). From the codespace, `--split-accounts` puts accounts on their own connection, because one connection's receive window (about 2.8 MB at 98 ms) caps it near 28 MB/s:

```bash
ygb record --out /workspaces/fixtures/ygb/<name> --slots 10000 --tail-secs 40 --split-accounts
ygb inspect --fixture /workspaces/fixtures/ygb/<name>
```

Build, generate a profile, and sweep the subscriber count:

```bash
yellowstone-grpc-bench/tools/build-plugin.sh public-fork   # rpcpool tonic fork, as in production
ygb profiles --fixture <fixture> --streams 600 --out profiles/prod-600.json
YGB=target/bench-fork/release/ygb PLUGIN=target/bench-fork/release/libyellowstone_grpc_geyser.so \
  FIXTURE=<fixture> PROFILE=profiles/prod-600.json RUNS=<runs dir> \
  yellowstone-grpc-bench/tools/sweep.sh base "1 50 150 400" --replay-stored-slots 300 --slots 1500
```

Build `ygb` and the plugin in one cargo invocation, as `build-plugin.sh` does. The plugin's trait objects cross the library boundary, so both sides need the same rustc and the same dependency builds. Rank changes only on `public-fork` builds. Upstream tonic creates a zstd context per message, which adds about 0.6 ms per compressed message.

## Plugin checkpoints (`latency-trace`)

`TRACE=1 build-plugin.sh public-fork` builds the plugin with the `latency-trace` feature into `target/bench-fork-trace`. `ygb run` sets `YELLOWSTONE_LATENCY_TRACE_DIR` to `<run>/trace`. The plugin then writes per-stage histograms there every second. Most stages record a message's age (`now - created_at`), so the stages line up as a waterfall:

| Stage | Where | Meaning |
|---|---|---|
| `dequeue_age_ns` | geyser_loop | callback to taken off the plugin channel |
| `batch_size`, `broadcast_send_ns` | geyser_loop | processed batch size; time of the send that wakes every processed subscriber |
| `client_recv_age_ns` | client_loop | callback to a subscriber task receiving the batch (1 in 16 batches) |
| `client_batch_loop_ns` | client_loop | one subscriber's pass over a batch (same sample) |
| `queue_residence_ns` | subscriber queue | time an update waited before the response stream took it |
| `reconstruction_pass_ns` | block reconstruction | one pass of the reconstruction loop |
| `worker_busy_permille`, `global_queue_depth` | runtime sampler | worker busy share and inject queue depth every 100 ms |

`ygb run` also writes `cpu.json`: cores used per thread group, busy cores on the plugin CPUs, and the share not explained by the plugin's own threads (other tenants).

## Fixture format

A fixture directory holds `manifest.json` and `chunks/NNNNN.ygf.zst`.

Each chunk is one zstd stream. It starts with the magic `YGF1`, followed by frames. A frame holds the stream id (u8: 0 main, 1 deshred), the receive time (u64 unix ns), the payload length (u32), and the payload. Integers are little-endian. The payload is the raw `SubscribeUpdate` or `SubscribeUpdateDeshred` protobuf.

The manifest lists the chunks with their slot ranges, plus counts per kind, the server version, the requests, and the end reason. The recorder rewrites the manifest at every chunk rotation, so an interrupted recording stays readable.

## How the replay is built

- Pacing uses each message's server `created_at`, never the receive time.
- Bank and slot statuses come from the deshred stream. The plugin forwards them to deshred subscribers inside the callback with the original `created_at`. The main stream re-creates Processed, Confirmed, and Finalized when the bank seals, so those times include the source node's own reconstruction delay.
- The scheduler merges both streams into one `created_at` order with a reorder buffer, because the streams lag each other by up to seconds in the recording. Processed sorts just after its bank's BlockMeta.
- The main lane calls account, transaction, entry, block-meta, update-parent, and status callbacks. The deshred lane calls the deshred callbacks.
- Account writes reference a stub transaction that carries only the recorded signature. The plugin reads nothing else from it.
- A run first prefills `--prefill-slots` slots unpaced, which fills the plugin's replay buffer. It then waits `--attach-secs` for subscribers and paces the rest.

## Known gaps against production

- Prod runs the private wrapper, which receives account writes in batches through `_extended_update_accounts`. The public plugin gets one callback per account.
- Prod builds with the rpcpool tonic fork and tokio 1.54.0.
- Agave's own work on `solEntryNotif` and `solTxStatusWrtr` is not reproduced.
- Block footers do not exist on mainnet yet, so they are not replayed.
