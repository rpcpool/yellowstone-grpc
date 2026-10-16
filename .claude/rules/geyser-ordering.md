# Geyser plugin ordering reference (agave 4.3, `bank_id` callbacks)

Derived from the agave source at `~/dev/agave`, branch `v4.3` at `a203300045` (4.3.2). It is
**not** a documented contract. The interface itself promises only two orderings in writing:

- the block footer "is ordered with entry notifications";
- entry notifications "may race" UpdateParent.

Everything else here describes what the code does today. Re-check it when the target agave version
moves. Paths are relative to the agave tree.

**Scope:** the callbacks that carry a `bank_id`. These are out of scope, and come up only where
they explain a gap:

- `update_slot_status`: FirstShredReceived, Completed, Dead;
- deshred transactions;
- snapshot-restore accounts;
- contact info.

## TL;DR

- **Key everything on `bank_id`.** One slot can see several banks: forks, Alpenglow UpdateParent
  restarts, duplicate-block dump-and-repair. Agave never holds two banks for one slot in
  BankForks at once, but notifications for an old bank and its replacement interleave on the wire.
- **A block starts at the first callback carrying a `bank_id` you have not seen.**
  - That callback is always an account write, never CreatedBank.
  - The bank constructor's account writes are delivered before CreatedBank on every path.
  - CreatedBank is still the only bank_id callback that carries the parent slot.
- **A block freezes at BlockMeta** (`notify_block_metadata_for_bank`).
  - BlockMeta is emitted synchronously right after `bank.freeze()`.
  - When it arrives, every account write of that bank has been delivered.
  - Entries, the footer and transactions travel through queues, so they **can still arrive
    after BlockMeta**.
- **A block is complete when:**
  - BlockMeta has arrived;
  - entries received ≥ `entry_count` (the count is 0 for leader banks, see below);
  - transactions received ≥ `executed_transaction_count`;
  - the footer has arrived (Alpenglow banks only).
- **Never use these as a freeze or completion signal:**
  - the next slot's CreatedBank;
  - Processed (needs RPC, crosses two queues, races the footer and entries);
  - Completed (no `bank_id`, and it is about shreds, not execution).
- **Abandonment is almost never announced.**
  - UpdateParent's `cleared_bank_id` is the only abandonment signal that carries a bank id.
  - Every other way a bank dies is either silent or a slot-only `Dead`.

## How each callback reaches the plugin

The plugin manager calls every plugin inline, on whichever thread calls the notifier
(`geyser-plugin-manager/src/*_notifier.rs`). So ordering depends on two things only:

- which thread produces the event;
- whether a channel sits between the producer and the plugin.

A channel with several producers preserves each producer's order and nothing more.

| Callback | Produced by | Plugin is called on | Queue |
|---|---|---|---|
| `update_account_for_bank` | `Accounts::_store_accounts` (`accounts-db/src/accounts.rs:540-570`), one account at a time, **before** the store | The storing thread. That is the replay thread, `solScHandle*` scheduler workers, banking workers, `solBlkCreatLoop`, or a `solBnkClcRwds*` rewards-pool worker for epoch feature activation (the caller blocks on it) | none |
| `update_bank_status(CreatedBank)` | `ReplayStage::new_bank_from_parent_with_notify` (`core/src/replay_stage.rs:5468`), **after** `Bank::new_from_parent_with_options` returns | The replay thread (fork generation, TowerBFT leader start) or `solBlkCreatLoop` (Alpenglow leader) | none |
| `notify_block_metadata_for_bank` | `process_replay_results` (`core/src/replay_stage.rs:4262`), after `bank.freeze()`. This covers leader banks too: replay freezes them | The replay thread | none |
| `notify_entry_for_bank`, `notify_block_footer`, `notify_entry_update_parent` | `EntryNotification` channel (`ledger/src/entry_notifier_service.rs`) | `solEntryNotif`, the only consumer | Unbounded, **several producers**: the replay thread (replayed entries and footer, replay-side UpdateParent), `solTpuEntry` (leader entries and footer), `solBlkCreatLoop` (leader UpdateParent) |
| `notify_transaction_for_bank` | `TransactionStatusService` (`rpc/src/transaction_status_service.rs`) | `solTxStatusWrtr`, the only consumer | Unbounded. Producers are scheduler workers (replay and startup) and banking workers (leader) |
| `update_bank_status(Processed)` | `BankNotification::Frozen`, sent by replay just before BlockMeta | `solBankNotif`, after passing through `solOpConfBnkTrk` | **two** queues |
| `update_bank_status(Confirmed)` | `BankNotification::OptimisticallyConfirmed`. TowerBFT sends it from the vote listener; Alpenglow sends it from the votor root path (`votor/src/root_utils.rs:80-95`), so it means *finalized* there | same | two queues |
| `update_bank_status(Rooted)` | `BankNotification::NewRootedChain` | same | two queues |

## One bank's life, in production order

"→ sync" means the plugin was called before the producer moved on. "→ queued" means the event was
only enqueued at that point.

### Replayed bank (the normal case for a non-leader RPC/geyser node)

1. **`Bank::new_from_parent_with_options`** → sync account writes with the new `bank_id`. These
   come from `runtime/src/bank.rs:2000-2040`; see "Construction-time writes" below.
2. **CreatedBank** → sync.
3. **For each entry batch,** the Entry is queued **before** its transactions are scheduled
   (`ledger/src/blockstore_processor.rs:1566`, vs `process_entries` at `:1715`).
4. **For each transaction,** on a scheduler worker:
   - its account writes run inside `Bank::commit_transactions` → sync;
   - its status batch is queued afterwards (`runtime/src/transaction_execution.rs:79-142`).
5. **The block footer (Alpenglow):**
   - replay first waits for the scheduler to drain;
   - the footer's account writes run → sync (Clock, `NANOSECOND_CLOCK_ACCOUNT`, and vote
     accounts for rewards; `runtime/src/block_component_processor.rs:767-787`);
   - BlockFooter is queued;
   - the final alpentick Entry is queued **after** the footer.
6. **The last tick** writes RecentBlockhashes → sync (`runtime/src/bank.rs:3655-3662`).
7. **`bank.freeze()`** → sync writes: fee payout, SlotHistory, incinerator
   (`runtime/src/bank.rs:3111-3113`). Alpenglow non-leader banks then check their bank hash
   against the footer; on a mismatch the slot goes Dead and steps 8-9 never happen.
8. **`BankNotification::Frozen`** is queued; it becomes Processed later.
9. **BlockMeta** → sync.

### Leader bank, TowerBFT

1. **Steps 1-2 as above** (`core/src/replay_stage.rs:3006`).
2. **Banking workers** run execute → record → commit → queue status batch, all under the bank's
   freeze lock (`core/src/banking_stage/consumer.rs:370-446`).
   - Recording is fire-and-forget.
   - The Entry is built later on `solPohTickProd` and notified via `solTpuEntry`.
   - So the leader Entry races its own transactions' commit. This is the opposite of replay.
3. **The final tick** is queued right after `register_tick` (`poh/src/poh_recorder.rs:664-677`), so
   it can lose the race with freeze.
4. **Freeze, Frozen and BlockMeta** run on the replay thread, the same code as replay, without
   hash verification.

### Leader bank, Alpenglow

`core/src/block_creation_loop.rs:665-835` and `poh/src/poh_recorder.rs:575-680`:

1. **Steps 1-2 as above** (`block_creation_loop.rs:1334`). The first Alpenglow block also writes
   `GENESIS_CERTIFICATE_ACCOUNT` **after** CreatedBank.
2. **Transactions** are the same as on the TowerBFT leader; the Entry is built on
   `solBlkCreatLoop`.
3. **Pending records are drained.** Every transaction Entry is queued before the footer.
4. **The footer's account writes** run on `solBlkCreatLoop` → sync.
5. **The last tick is registered.** Then:
   - replay freezes the bank → Frozen → **BlockMeta**;
   - **only then** is BlockFooter queued, carrying the bank hash;
   - then the final tick Entry is queued.
6. **The footer is therefore always produced after BlockMeta on Alpenglow leader blocks.**

### Construction-time writes (all delivered before CreatedBank)

| When | What is written, in order |
|---|---|
| Every bank | SlotHashes; Clock (Alpenglow writes only the slot here, and the footer writes Clock again with the time); LastRestartSlot, only when its value changes |
| First bank of a new epoch **on that fork** (not necessarily the epoch's first slot) | Feature accounts being activated, plus the Rent sysvar or builtin/core-BPF migration accounts if those features activate. Alpenglow only: `REWARD_EPOCH_DELEGATED_STAKES_ACCOUNT`, the VAT burn (vote accounts and incinerator), then the vote-account commission payouts, the EpochRewards sysvar, and `VOTE_REWARD_ACCOUNT_ADDR`. Then StakeHistory with the per-bank sysvars above |
| Partitioned-rewards window: the next `num_partitions` blocks **by block height**, per fork | A batch of stake accounts, then EpochRewards (written again inactive on the last partition). This runs **before** SlotHashes and Clock, so the first bank_id event of these banks is a non-sysvar stake account |

**When the per-bank sysvars are written:**

| Sysvar | Written | Before CreatedBank? |
|---|---|---|
| Clock, SlotHashes | at bank construction (`runtime/src/bank.rs:2024-2036`) | yes |
| RecentBlockhashes | at the last tick (`runtime/src/bank.rs:3655-3662`) | no |
| SlotHistory | inside `freeze()` (`runtime/src/bank.rs:3112`) | no |

**One pubkey can be written several times in one bank,** with increasing `write_version`:

- EpochRewards: 2-3 times;
- vote accounts: VAT burn, then commission;
- incinerator: VAT burn, then freeze;
- Alpenglow Clock: at construction, then from the footer.

## What you can rely on (per `bank_id`)

1. **Construction-time account writes come before CreatedBank.** The constructor does all its
   stores synchronously before returning; nothing at an epoch boundary stores asynchronously.
2. **CreatedBank comes before every execution-time event of that bank:** entries, transactions,
   footer, UpdateParent for it, Processed, BlockMeta, and post-construction account writes. All of
   these are produced after the synchronous CreatedBank call returns.
3. **Every account write comes before BlockMeta, and none can come after it.** Freeze asserts
   that no store has started (`runtime/src/bank.rs:4364`, `:4801`, `:4844`), and in-flight
   execution is drained before freeze.
4. **A transaction's account writes come before its transaction notification.** The store
   happens inside commit, and the status batch is queued after commit, on both paths.
5. **For one pubkey within one bank, writes arrive in store order.**
   - Account locks prevent two transactions that write the same account from committing at the
     same time.
   - The unified scheduler unlocks after the handler returns; banking unlocks when the batch
     drops after commit.
   - `write_version` increases in the same order. Use it, not arrival order, when merging.
6. **One bank's entries arrive in index order, and the footer follows every transaction Entry.**
   The footer is never the last entry-channel event: the final tick or alpentick Entry follows
   it.
7. **Processed comes after every transaction notification of that bank,** but only when RPC is
   on and transaction notifications exist. `solOpConfBnkTrk` waits on a dependency tracker
   (`rpc/src/optimistically_confirmed_bank_tracker.rs:301-305`). Entries and the footer get no
   such wait.
8. **Rooted arrives in ascending slot order, with each parent threaded through, and only for
   slots above the last root notified.** Confirmed walks `parents_inclusive()` upward and also
   only notifies new slots. Confirmed always has `parent = None`.

## What you must not rely on

1. **BlockMeta before the bank's last entries, footer or transactions.**
   - BlockMeta does not wait for `solEntryNotif` or `solTxStatusWrtr`.
   - Footer before BlockMeta on replayed slots is the usual case, not a guarantee.
   - On Alpenglow leader blocks the footer is produced after BlockMeta.
2. **Any order between entries, transactions and account writes of one bank.** They travel on
   three different paths.
3. **Transactions in `transaction_index` order.** The unified scheduler runs each transaction as
   its own batch on parallel workers, and banking workers run in parallel too. Use
   `transaction_index`.
4. **Processed relative to BlockMeta, the footer or entries.** Frozen is queued just before
   BlockMeta is called, then crosses two threads.
5. **Processed before Confirmed (TowerBFT).** The tracker checks `bank.is_frozen()` live. An
   OptimisticallyConfirmed dequeued after `freeze()`, but before that bank's Frozen message, emits
   Confirmed first.
6. **Any ordering across banks.**
   - Replay processes several banks per loop iteration (in parallel with
     `--replay-forks-threads > 1`).
   - The leader bank executes concurrently with replay.
   - In Alpenglow the replay thread and `solBlkCreatLoop` can construct banks at the same time.
7. **UpdateParent ordered against the cleared bank's entries.**
   - The leader sends UpdateParent from `solBlkCreatLoop` straight into the channel, while the
     cleared bank's entries may still be waiting in `solTpuEntry`
     (`core/src/block_creation_loop.rs:960-1000`).
   - Expect `Entry{bank_id = cleared}` **after** `UpdateParent{cleared_bank_id}`.
   - The replacement bank's CreatedBank and construction writes are synchronous, so they can
     arrive **before** the UpdateParent that explains them.
8. **`bank_id` order as slot order.** Siblings are built in HashMap order, replacements get
   newer ids, and two threads build banks. The only ordering is that a child's id is greater
   than its parent's.

## Start of a block

- **Open the block on the first event with an unseen `bank_id`, and buffer from there.**
  - Do not require CreatedBank to come first.
  - Do not require a sysvar to come first: on rewards-window banks the first event is a stake
    account.
  - The every-bank writes, SlotHashes then Clock, are always present before CreatedBank. Their
    absence before an Entry, Transaction or BlockMeta means the stream joined mid-bank.
- **Take the parent from CreatedBank.** BlockMeta also carries `parent_slot`, but it arrives at
  the end.
- **Treat `bank_id` as scoped to one validator process.**
  - The counter is shared along the fork and incremented per bank
    (`runtime/src/bank.rs:1428-1438`).
  - Root banks (genesis, snapshot root) start at 0.
  - Ids restart from 1 after every validator restart, so reset all bank_id state on
    reconnect.

## Freeze and completion

**BlockMeta is the freeze signal.** It is the only bank_id event emitted after `freeze()` for
every frozen bank, leader or not. Startup replay is the exception (see below).

| Check | Caveat |
|---|---|
| Entries ≥ `entry_count` | Leader banks report `entry_count = 0`. The count comes from replay progress (`core/src/replay_stage.rs:4282`), and leader banks skip replay, so the check passes trivially. There is no count-based way to know all leader entries arrived. |
| Transactions ≥ `executed_transaction_count` | This counts committed transactions, failed and vote included (`runtime/src/bank.rs:4376`). It matches the number of transaction notifications: TSS skips only uncommitted results, which a valid block cannot contain. |
| Footer received (Alpenglow) | An Alpenglow bank writes `NANOSECOND_CLOCK_ACCOUNT` while applying its footer, before freeze (`update_clock_from_footer`, `runtime/src/bank.rs:3469-3513`). On a leader `BankFreezeTimeout`, replay can still freeze the bank, so you get BlockMeta (and Processed) but never a footer. |

## Abandoned banks

| What happens | What the plugin sees |
|---|---|
| UpdateParent restart (Alpenglow) | `notify_entry_update_parent(cleared_bank_id)`, but only to plugins with `entry_notifications_enabled()`. Footer-only plugins never see it. Descendants cleared with it get nothing, and the event races entries (see above). |
| Replay failure, or a bank-hash mismatch after freeze | `update_slot_status(Dead)`, which has a slot and parent but **no `bank_id`**. On a hash mismatch, freeze's account writes and the footer were already delivered and there is no BlockMeta. Dead is not final for the slot: after dump-and-repair the slot can come back with a new `bank_id`. |
| Soft-dead (Alpenglow fast handover) | Nothing until it resolves, as UpdateParent plus a new CreatedBank, or as Dead. |
| Duplicate dump-and-repair (TowerBFT), Alpenglow duplicate-block switch, Alpenglow enablement purge | Nothing. The bank may already have had CreatedBank, Processed and BlockMeta. |
| Leader `insert_bank` stale rejection, `abort_working_bank` | Nothing after CreatedBank and the construction writes. |
| Fork pruned when a new root is set | Nothing. Infer it: any bank at or below a Rooted slot that is not the rooted chain's `bank_id` for that slot is dead. |

## Startup and restarts

1. **Snapshot restore:** `update_account_from_snapshot` (no `bank_id`), then
   `notify_end_of_startup`.
   - `notify_end_of_startup` fires even with snapshot notifications off.
   - On a start from genesis it **never fires**, and the genesis accounts arrive as
     `update_account_for_bank` with `bank_id` 0.
2. **Local-ledger replay, from the snapshot root to the blockstore tip:**
   - full slots only, always after `notify_end_of_startup`;
   - delivers account, entry, transaction and footer notifications with `bank_id`;
   - delivers **no CreatedBank, no BlockMeta, no Processed and no Dead**
     (`ledger/src/blockstore_processor.rs:1908`, `:2419`);
   - banks are built before they are processed, and some are discarded silently.
3. **Live replay starts.** Partial slots at the tip get CreatedBank as usual.
4. **The first rooted chain after boot** can produce Rooted and Confirmed for startup banks
   (including the snapshot root, `bank_id` 0) that never had CreatedBank.
5. **Under `--warp-slot`,** the warp bank writes accounts and freezes without any status
   notification.

## Configuration that changes what you get

- **RPC on/off (`--rpc-port`):**
  - Processed, Confirmed and Rooted exist only with RPC enabled. The tracker is created inside
    `if let Some(..) = config.rpc_addrs` (`core/src/validator.rs:1267`).
  - The Processed-after-transactions wait (guarantee 7) also needs RPC, plus a transaction
    notifier.
- **Entry vs footer opt-in:** the entry channel exists if any plugin wants entries **or**
  footers. UpdateParent is dispatched only to plugins with entry notifications enabled
  (`geyser-plugin-manager/src/entry_notifier.rs:101`).
- **Leader schedule:** a node with no stake is never leader, so it only ever sees the
  replayed-bank path. The leader caveats apply only to nodes that produce blocks.
