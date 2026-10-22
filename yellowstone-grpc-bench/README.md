# yellowstone-grpc-bench (`ygb`)

`ygb` measures how fast the Yellowstone gRPC geyser plugin delivers processed updates while many subscribers are connected. It loads the real plugin library (`.so`, `.dylib` on macOS) in-process the way agave does, replays a recorded mainnet firehose into the plugin callbacks at the recorded pace, and attaches a fleet of production-shaped subscribers. The subscribers use different filters, compression, commitment levels and emulated networks. Probe subscribers shaped like streamlab's measure latency from the plugin's `created_at` to receipt.

## Quick start

One command builds the plugin and `ygb` together, records a fixture when asked, and runs a scenario:

```bash
# Smoke test: about 2 minutes per variant after the build.
yellowstone-grpc-bench/bench.sh --scenario quick --fixture <fixture>

# A production-shaped sweep over the number of subscribers: about 30 minutes per variant.
yellowstone-grpc-bench/bench.sh --scenario prod-like --fixture <fixture>

# An A/B of two plugin builds: every variant gets this checkout's harness, so only the plugin differs.
yellowstone-grpc-bench/bench.sh --scenario ab --fixture <fixture> \
  --variant base=<base commit> --variant stack=. --set 'plugin.env.YELLOWSTONE_FANOUT_SHARDS="8"'

# No fixture yet: record 3000 slots first (about 8 GB, 12 minutes).
yellowstone-grpc-bench/bench.sh --scenario quick --fixture ~/fixtures/nyc226-3k --record 3000
```

On `feat/latency-harness`, `.` also carries two plugin experiments as its last two commits: per-session metric handles (always on) and sharded processed fan-out (on only with `YELLOWSTONE_FANOUT_SHARDS=K`). The commit before them builds the plugin as on master, so use it as `<base commit>`; `git log --oneline -3` shows it. A variant ref must exist in the clone: a `--single-branch` clone has no `origin/master`.

`bench.sh --help` lists every option. Results go to `../ygb-results/<scenario>-<UTC time>/` next to the repository, unless `--out` says otherwise. `summary.md` holds the headline table. A rerun with the same `--out` skips finished runs, so an interrupted bench resumes.

Prerequisites: `git`, rustup (the repository pins the toolchain), a C compiler and `perl`. `protoc` is vendored. It runs on Linux and on macOS; see "Running on another machine" for what macOS cannot do.

The default `public-fork` flavour patches in the rpcpool tonic fork that production uses, from the private repository `rpcpool/grpc-rust`. Cargo needs your GitHub credentials for it: `export CARGO_NET_GIT_FETCH_WITH_CLI=true` makes cargo fetch with your own git setup. `--flavour public` builds with upstream tonic instead, which adds about 0.6 ms per zstd message; do not rank changes on it.

## What a run simulates

| Part | What `ygb` does |
|---|---|
| Plugin | The real plugin library, loaded through `_create_plugin` with a production config (`configs/geyser-streaming.json`): 8 workers, gzip and zstd, `replay_stored_slots`, filter limits. Unprefixed jemalloc as in agave, on Linux. |
| Agave callers | Agave-shaped driver threads call the bank-scoped callbacks at the recorded pace, with agave's ordering guarantees (see "Driver lanes"). |
| Filters | Each load stream gets production-shaped filters: account filters by pubkey (1, 2-99 or 100-1000 keys) or owner, transaction filters with `account_include` (1 to 1000 keys), slots, blocks_meta, entries. Keys come from the fixture. The share of keys that never update is calibrated so per-stream rates match fra215. |
| Commitment | Processed, confirmed and finalized streams in the scenario's split. Full-block subscribers at confirmed and deshred subscribers are separate groups. |
| Compression | Each stream asks for none, zstd or gzip (`grpc-accept-encoding`) in the scenario's split. The plugin compresses per stream, as in production. |
| Customers | Streams are grouped into customers by `x-subscription-id`, about 3.7 streams per customer. A customer's streams share one network path. |
| Networks | Each customer draws a network profile: one-way delay, jitter, bandwidth, path queue and periodic stalls, emulated in both directions (see "Network emulation"). |
| Churn | A share of streams reconnects periodically and resumes with `from_slot` some slots back, which exercises the plugin's replay path. Load streams that the plugin drops reconnect after a backoff, as production clients do. |
| Slow readers | A share of streams sits behind a very thin link, so the plugin's per-subscriber queues fill and its drop logic runs. |
| Unary calls | GetSlot, GetLatestBlockhash, GetBlockHeight and Ping at production rates (nyc227 serves about 240/s of each of the first three). They run on the plugin's runtime and read its block-meta store. |
| Metrics scraping | The fleet scrapes the plugin's `/metrics` every 10 s, as production's agent does. The plugin renders the whole registry on a runtime worker; production registries hold 13-39k series on busy nodes. |
| Probes | 8 streamlab-shaped probes (half uncompressed, half zstd) and 2 at confirmed, on reserved CPUs. They stamp each message when its last DATA frame arrives, before decompression. |

## Scenarios

A scenario is a TOML file. `scenarios/` holds these:

| Scenario | Purpose |
|---|---|
| `quick` | Smoke test of every feature at 60 streams, about 2 minutes per variant. |
| `prod-like` | Production-shaped fleet on geographic links, swept over 50, 150, 300 and 450 streams, plus 6 full-block subscribers at 300. |
| `ab` | Decision run for two builds: 400 local streams, 3 repeats, variant order rotated per repeat. |
| `network` | The same 300 streams on different networks: all local, geographic, all far, stalling, 10% slow readers. |
| `compression` | The same 300 local streams uncompressed, all zstd, all gzip, and the production split. |
| `stress` | Known stall sources: reconnect storms with `from_slot`, 12 block subscribers, 16 deshred subscribers, 10% slow readers. |

Keys and defaults:

| Key | Default | Meaning |
|---|---|---|
| `window.start` | `"heaviest"` | Where pacing starts: `heaviest` (the window with the most bytes), `first`, or a slot. |
| `window.slots` / `prefill_slots` / `warmup_secs` | 1500 / 300 / 60 | Paced slots, unpaced slots that fill the replay buffer first, and seconds excluded from statistics. |
| `plugin.replay_stored_slots` | template (1000) | 300 fits a 48 GiB codespace. |
| `plugin.worker_threads`, `plugin.config_template` | template | Runtime size; another config file, relative to the scenario. |
| `plugin.env` | `{}` | Environment for the plugin, for experiment switches. |
| `plugin.fifo_priority` | unset | SCHED_FIFO priority for the plugin's runtime threads and the driver lanes. Needs CAP_SYS_NICE. |
| `driver.lanes` | `"agave"` | `agave` or `single` (see "Driver lanes"). |
| `driver.sched_lanes` | 4 | Scheduler-worker threads that deliver transaction account writes. |
| `driver.notifiers_on_plugin_cpus` | true | The entry-notifier and transaction-status threads share the plugin CPUs, as in production. |
| `mix.streams` | 400 | Load streams in the profile. Cases use the first N. |
| `mix.commitment` | processed 90, confirmed 7, finalized 3 | Weights. |
| `mix.compression` | none 40, zstd 50, gzip 10 | Weights. |
| `mix.blocks` / `mix.deshred` | 0 / 0 | Full-block subscribers at confirmed (a third include accounts, half use zstd) and deshred subscribers. |
| `mix.streams_per_id` | 3.7 | Mean streams per customer. |
| `mix.cold_key_share`, `rate_targets`, `total_per_stream`, `sample_slots` | `"auto"`, [7, 120, 4900], 220, 300 | Rate calibration against production per-stream p50/p90/p99 and mean messages per second. |
| `mix.seed` | 1 | Profile seed. Repeat r of a case uses fleet seed `seed + r` for jitter and churn timing. |
| `[[mix.network]]` | none (all local) | Network profiles: `name`, `weight`, `delay_ms = [min, max]`, `jitter_ms`, `bandwidth_mbps = [min, max]`, `buffer_kib` (256), `socket_rcvbuf_kib`, `stall_every_secs`, `stall_ms`. |
| `mix.churn` | none | `share` (percent of load streams), `every_secs` (mean interval, drawn from 0.5x to 1.5x), `from_slot_back`. |
| `mix.slow` | none | `share` (percent), `bandwidth_mbps`, `delay_ms`. |
| `mix.unary` | `{}` | Unary calls per second by method: `get_slot`, `get_latest_blockhash`, `get_block_height`, `get_version`, `ping`. |
| `mix.unary_connections` | 4 | Connections each method is spread over. |
| `probes.streamlab` / `probes.confirmed` | 8 / 2 | Probe counts. `probes.network` puts them on a named network profile. |
| `fleet.streams_per_connection` | 1 | Load streams of one customer and path that share an HTTP/2 connection. |
| `fleet.stream_window_mib` / `connection_window_mib` | 8 / 32 | HTTP/2 windows of the fleet. |
| `fleet.scrape_secs` | 10 | Seconds between scrapes of the plugin's metrics; 0 disables. |
| `[[cases]]` | required | `name`, `streams`, `blocks`, `deshred` (lists; every combination runs), `repeats`, `env`, and `mix` or `probes` tables that replace the scenario's keys for that case. |

A case that sets `mix` or `probes` gets its own profile. Keys listed there replace the scenario's keys of the same name. Draws that a key does not affect stay the same, so a case that changes only `compression` keeps every filter.

`--set KEY=VALUE` overrides any key for one machine without editing the file, for example `--set plugin.fifo_priority=40 --set plugin.replay_stored_slots=1000`. A value that is not valid TOML is taken as a string.

## Network emulation

Each emulated subscriber connects through a user-space link in the fleet process (`src/fleet/link.rs`). Both directions are delay lines:

- Bytes leave at the link rate, then arrive one delay plus jitter later. Jitter never reorders bytes, as on TCP.
- The downlink holds at most the bandwidth-delay product plus `buffer_kib` in flight. When that fills, the fleet stops reading the socket. TCP and HTTP/2 flow control then push back on the plugin, as a real far or thin link does.
- The uplink delays requests, WINDOW_UPDATE frames and pings by the same delay, so flow control runs at the emulated round trip.
- `stall_every_secs` and `stall_ms` stop delivery now and then, like a congested path or a paused client.

Limits: tokio timers have 1 ms resolution, so delays below about 2 ms are not faithful; same-metro clients stay local. The kernel's loopback socket buffers add to `buffer_kib`; `socket_rcvbuf_kib` bounds the client side. Each emulated stream runs four small tasks in the fleet, so check the fleet CPU in `fleet/fleet.json` for large emulated fleets.

## Driver lanes

`driver.lanes = "agave"` (the default) calls the plugin from threads shaped like agave's callers. Each lane keeps its own recorded pace, and the cross-lane waits keep the ordering guarantees in `.claude/rules/geyser-ordering.md`:

| Lane | Agave caller | Callbacks | Waits for earlier events of |
|---|---|---|---|
| `ygbReplay` | replay thread | CreatedBank, BlockMeta, account writes without a transaction (sysvars, rewards, freeze), Dead | scheduler workers |
| `ygbScHandle00..` | scheduler workers | transaction account writes, by pubkey | replay |
| `ygbEntryNotif` | `solEntryNotif` | entries, UpdateParent | replay |
| `ygbTxStatus` | `solTxStatusWrtr` | transactions | replay, scheduler workers |
| `ygbBankNotif` | `solBankNotif` | Processed, Confirmed, Rooted | replay, transaction status |
| `ygbShred` | shred insertion | deshred transactions and UpdateParent, FirstShredReceived, Completed | nothing |

So BlockMeta comes after every account write of its bank, and a transaction after its account writes. Processed comes after BlockMeta and the bank's transactions. One pubkey's writes keep their store order. Entries and transactions may still arrive after BlockMeta, as they can in agave. In production, `solEntryNotif` and `solTxStatusWrtr` run on the notifier cores next to the plugin's runtime, so these two lanes run on the plugin CPUs by default.

`driver.lanes = "single"` uses one thread for all bank data and statuses plus one for deshred. It serializes bursts that agave delivers in parallel.

`driver.json` reports, per lane, the lag behind the recorded pace, split into timer lag (the lane was idle), queue lag (behind its own earlier callbacks) and gate wait (waiting for another lane), plus callback durations.

## CPU layout

`ygb bench` pins every role. Without `--layout`, it reads the topology from `/sys` and uses only CPUs the process may run on. The plugin gets `--plugin-cores` whole cores (default 6, with their SMT siblings) on the smallest L3 domain that fits, which matches one production CCD. The probes (two cores on machines with 16 cores or more), driver (two cores with 12 or more) and decoder go outside that domain. The load fleet gets the rest. `ygb layout` prints the choice. `layouts/` holds fixed layouts for the codespace and nyc226.

## Variants and A/B decisions

A variant is a directory with a `ygb` and a plugin library (`.so`, `.dylib` on macOS) from one cargo invocation, so both sides of `dlopen` share rustc and dependency builds. `bench.sh --variant NAME=REF` checks REF out in `target/ygb-variants/NAME`, replaces its `yellowstone-grpc-bench` with this checkout's, and builds it. `NAME=.` builds this checkout as it is. `ygb bench --variant NAME=DIR` takes prebuilt directories.

Each repeat runs every variant on the same window with the same profile and fleet seed. The variant order rotates per repeat. `summary.md` compares each variant with the first one, pairing slot by slot: the median change of the per-slot p99 and a 95% CI from a 10-slot block bootstrap. Accept a change when the CI excludes zero and the effect exceeds both 5% and twice the A/A spread (about 9% at run level and about 1% paired on the codespace). Rank only `public-fork` builds; upstream tonic creates a zstd context per message, which adds about 0.6 ms per compressed message.

## Results

An output directory holds:

| Path | Content |
|---|---|
| `summary.md`, `summary.json` | One row per run: streamlab probe p50 / p99 / p99.9 / median per-slot p99 for accounts, transactions and entries, zstd accounts, confirmed accounts, plugin worker cores, delivered load MiB/s, ended streams, the slowest metrics scrape, unary calls per second with the slowest method's p99, validity. Then the paired comparisons. |
| `scenario.toml`, `layout.toml`, `bench-key.json` | What the runs used. A rerun into the same directory must match `bench-key.json`. |
| `profile-<name>.json` | The subscriber profile of each case mix. |
| `runs/<case>-n<N>-b<B>-d<D>-r<repeat>-<variant>/` | `run.log`, `geyser.json`, `driver.json`, `driver-statuses.json`, `cpu.json`, `report.json`, `fleet/fleet.json`, `fleet/samples.bin.zst`, and `trace/` for traced builds. |

A run is invalid, and is left out of comparisons, when any of these holds:

- a plugin callback returned an error;
- a driver lane's timer lag p99 exceeds `--max-timer-lag-us` (default 200 µs), so the replay lost the recorded pace;
- other tenants use more than `--max-foreign` (default 2%) of the plugin or probe CPUs, after the plugin's threads, the notifier lanes and interrupt time;
- a probe disconnected, or a local load stream ended (the fleet, not the network, fell behind);
- a probe thread used more than 70% of a core in any 500 ms window.

Streams on emulated links that the plugin drops are results, not errors; the summary counts them. A run that fails only an environment check (other tenants, timer lag, a busy probe thread) runs again up to `--retries` times (default 1). The failed attempt stays in `runs/<label>.attempt<N>/` without its sample file.

On a shared host such as the codespace, other tenants often take 2-3% of each CPU. Use `--max-foreign 0.05` there, and compare variants only within one bench, where the rotation spreads that noise over both.

`ygb report --run <dir> --stalls 5` adds a timeline of 100 ms windows in which processed probes waited more than 5 ms, next to the fleet's reconnects, stream ends and metrics scrapes from the second before each window. `ygb compare --a <run> --b <run>` pairs any two runs of the same window.

## Running on another machine

A fixture moves more easily cut down to the windows the scenarios use. `ygb trim` keeps the slots around the heaviest `prod-like` window and the slots that profiles sample. So `prod-like`, `ab` and `quick` replay the same slots with the same subscriber profile as on the full fixture. The 10k-slot fixture of 26 GB becomes 7.2 GB:

```bash
target/bench-fork/release/ygb trim --fixture /workspaces/fixtures/ygb/nyc226-10k-20261020 \
  --out /workspaces/fixtures/ygb/nyc226-trim-20261020
```

A trimmed fixture holds only `--prefill-slots` (default 300) slots before the window; for a longer prefill, trim with a matching `--prefill-slots`. The 1000-slot windows of `network`, `compression` and `stress` may land elsewhere than on the full fixture.

### macOS

`bench.sh` and `ygb` run on macOS with the system bash 3.2, Xcode's command line tools and rustup. macOS has no thread pinning, no SCHED_FIFO and no `/proc`, so there:

- every role shares every CPU, and the scheduler moves threads between performance and efficiency cores;
- the driver sleeps with relative sleeps instead of `clock_nanosleep`;
- there is no CPU accounting: plugin cores show as `-`, and the foreign-load guard is skipped;
- `bench.sh` runs `ygb` under `caffeinate -i`, so the Mac does not sleep during a run. Keep it on power with the lid open.

Use the Mac for functional runs and for A/Bs inside one bench, not for numbers to put next to production. macOS wakes sleeping threads less precisely; if runs fail the timer-lag guard, raise it with `--max-timer-lag-us 2000`. The `ygb run` process needs about 11 GiB at `replay_stored_slots = 300`. On a 16 GB Mac, add `--set plugin.replay_stored_slots=100`; this must stay above the scenario's `from_slot_back` (20 in `quick`, 30 in `prod-like`, 150 in `stress`), or churned streams cannot resume.

### nyc226

The same command works on any Linux host. For nyc226:

1. Clone the branch and record a fixture while the validator still runs, or copy the full fixture from the codespace: `yellowstone-grpc-bench/bench.sh --scenario quick --fixture ~/fixtures/nyc226 --record 3000 --dry-run` records and builds without running. The run in step 4 uses a 1000-slot prefill, which a default trimmed fixture does not hold; trim with `--prefill-slots 1000` instead.
2. Stop the validator, so the plugin under test has the machine to itself.
3. Check the layout: `lscpu -e`, then `target/bench-fork/release/ygb layout`. `layouts/nyc226.toml` puts the plugin on the production notifier cores (47-52 and 111-116).
4. Run with production settings:

```bash
yellowstone-grpc-bench/bench.sh --scenario prod-like --fixture ~/fixtures/nyc226 --setcap \
  --layout yellowstone-grpc-bench/layouts/nyc226.toml \
  --set plugin.fifo_priority=40 --set plugin.replay_stored_slots=1000 --set window.prefill_slots=1000
```

SCHED_FIFO needs CAP_SYS_NICE. `--setcap` grants it to each built `ygb` with `sudo setcap`, so the build and the results stay owned by you. With `kernel.sched_rt_runtime_us=-1`, a FIFO thread that never sleeps owns its CPU; a driver lane that waits on another spins for at most 20 µs, then sleeps in 20 µs steps. If something already listens on 127.0.0.1:10000 or 9123, pick other ports with `--grpc-port` and `--prometheus-port`.

Results from different machines are comparable only within one machine. Compare variants on the same host and fixture.

## Commands

| Command | What it does |
|---|---|
| `ygb bench` | Runs a scenario: layout, profiles, every case and variant, validity checks, `summary.md`. |
| `ygb record` | Subscribes to a live node at processed and writes raw frames into a fixture. Main stream: accounts, transactions (votes and failed included), entries with update-parent markers, slots with interslot updates, block meta, and block footers. Deshred stream: deshred transactions, update-parent markers, and slots. `--split-accounts` puts accounts on their own connection, because one connection's receive window caps WAN throughput near 28 MB/s. |
| `ygb inspect` | Checks every bank of a fixture and prints rates, recording lag, timing facts, and the heaviest slots. |
| `ygb run` | Loads a plugin library, replays a fixture window into it, and starts `ygb fleet` when prefill ends. |
| `ygb fleet` | Runs subscribers on a raw HTTP/2 gRPC client. Probes record every message; load subscribers count bytes and messages. |
| `ygb profiles` | Generates a subscriber profile from a fixture and a scenario's `[mix]`. |
| `ygb report` | Prints probe latency by class, compression and kind, with per-slot p99 and stall counts, the plugin's trace stages for traced builds, and the stall timeline with `--stalls`. |
| `ygb compare` | Paired A/B of two runs of the same window, by per-slot p99. |
| `ygb layout` | Prints the CPU layout `ygb bench` would pick here. |
| `ygb trim` | Copies the part of a fixture one scenario window needs, to move it to another machine. |

Reference times in reports: processed data uses the plugin's `created_at`. Slot statuses, and everything on confirmed or finalized streams, use the driver's callback time for that slot's status (`driver-statuses.json`).

## Plugin checkpoints (`latency-trace`)

`TRACE=1 bench.sh ...` (or `TRACE=1 tools/build-plugin.sh public-fork`) builds the plugin with the `latency-trace` feature into `target/bench-fork-trace`. `ygb run` sets `YELLOWSTONE_LATENCY_TRACE_DIR` to `<run>/trace`. The plugin then writes per-stage histograms there every second. Most stages record a message's age (`now - created_at`), so the stages line up as a waterfall:

| Stage | Where | Meaning |
|---|---|---|
| `dequeue_age_ns` | geyser_loop | callback to taken off the plugin channel |
| `batch_size`, `broadcast_send_ns` | geyser_loop | processed batch size; time of the send that wakes every processed subscriber |
| `client_recv_age_ns` | client_loop | callback to a subscriber task receiving the batch (1 in 16 batches) |
| `client_batch_loop_ns` | client_loop | one subscriber's pass over a batch (same sample) |
| `queue_residence_ns` | subscriber queue | time an update waited before the response stream took it |
| `reconstruction_pass_ns` | block reconstruction | one pass of the reconstruction loop |
| `worker_busy_permille`, `global_queue_depth` | runtime sampler | worker busy share and inject queue depth every 100 ms |

A traced build adds about 10-15% to p99, so compare traced runs only with traced runs.

## Fixture format

A fixture directory holds `manifest.json` and `chunks/NNNNN.ygf.zst`.

Each chunk is one zstd stream. It starts with the magic `YGF1`, followed by frames. A frame holds the stream id (u8: 0 main, 1 deshred, 2 main accounts with `--split-accounts`), the receive time (u64 unix ns), the payload length (u32), and the payload. Integers are little-endian. The payload is the raw `SubscribeUpdate` or `SubscribeUpdateDeshred` protobuf.

The manifest lists the chunks with their slot ranges, plus counts per kind, the server version, the requests, and the end reason. The recorder rewrites the manifest at every chunk rotation, so an interrupted recording stays readable.

## How the replay is built

- Pacing uses each message's server `created_at`, never the receive time.
- Bank and slot statuses come from the deshred stream. The plugin forwards them to deshred subscribers inside the callback with the original `created_at`. The main stream re-creates Processed, Confirmed, and Finalized when the bank seals, so those times include the source node's own reconstruction delay.
- The scheduler merges the streams into one `created_at` order with a reorder buffer, because the streams lag each other by up to seconds in the recording. Processed sorts just after its bank's BlockMeta.
- Account writes reference a stub transaction that carries only the recorded signature. The plugin reads nothing else from it.
- A run first prefills `prefill_slots` slots unpaced, which fills the plugin's replay buffer. It then waits 10 s for subscribers to attach and paces the rest.

## Known gaps against production

- Production runs the private wrapper, which receives account writes in batches through `_extended_update_accounts`. The public plugin gets one callback per account.
- Production builds with the rpcpool tonic fork and tokio 1.54.0.
- The driver lanes call the plugin at agave's times, but agave's own CPU work on the shared notifier cores is not reproduced.
- Block footers do not exist on mainnet yet, so they are not replayed.
- The network shares, the compression split and the client HTTP/2 windows are estimates, not measurements.
- Probes stamp a chunk when their task reads it. All probes get each burst at once, so on shared probe threads a probe can read its chunk up to a few probe handling times late. The load table shows the slowest probe's p99 handling time per chunk (10-20 µs), and the probes get two cores on machines with 16 cores or more. Paired comparisons cancel most of this.
