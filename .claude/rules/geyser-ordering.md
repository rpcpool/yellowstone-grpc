# Geyser plugin ordering reference (agave 4.5, `bank_id` callbacks)

Derived from the agave source (`anza-xyz/agave`), tag `v4.5.0-beta.0` (`ef54b258ef`), with `agave-geyser-plugin-interface` 6.0.0. Every claim was re-checked against that tree, coming from the 4.4 revision (`6eecb8bc58`); what changed is listed at the end. It is **not** a documented contract. The interface itself promises only two orderings in writing:

- the block footer "is ordered with entry notifications";
- entry notifications "may race" UpdateParent.

Everything else here describes what the code does today. Re-check it when the target agave version moves. Paths and line numbers are relative to the agave tree at that tag; in an agave checkout, `git show v4.5.0-beta.0:<path>` reads a file without switching branches.

**Scope:** the callbacks that carry a `bank_id`. These are out of scope, and come up only where they explain a gap:

- `update_slot_status`: FirstShredReceived, Completed;
- deshred transactions;
- snapshot-restore accounts;
- contact info.

## TL;DR

- **Key everything on `bank_id`.** One slot can see several banks: forks, Alpenglow UpdateParent restarts, duplicate-block dump-and-repair, Alpenglow duplicate-block switches. Agave never holds two banks for one slot in BankForks at once, but notifications for an old bank and its replacement interleave on the wire.
- **A block starts at the first callback carrying a `bank_id` you have not seen.**
  - With account notifications on, that callback is always an account write, never CreatedBank.
  - The bank constructor's account writes are delivered before CreatedBank on every path.
  - CreatedBank is the only callback at the start of a block that carries the parent slot. BlockMeta, Processed, Rooted and Dead carry it too, but they arrive later.
  - Remember the `bank_id`s of dead and cleared banks, so their late events do not open a block.
- **A block freezes at BlockMeta** (`notify_block_metadata_for_bank`).
  - BlockMeta is emitted synchronously right after `bank.freeze()`.
  - When it arrives, every account write of that bank has been delivered.
  - Entries, the footer and transactions travel through queues, so they **can still arrive after BlockMeta**.
- **A block is complete when:**
  - BlockMeta has arrived;
  - entries received ≥ `entry_count` (the count is 0 for leader banks, see below);
  - transactions received ≥ `executed_transaction_count`;
  - the footer has arrived (Alpenglow banks only).
- **Never use these as a freeze or completion signal:**
  - the next slot's CreatedBank;
  - Processed (needs RPC, crosses two queues, races the footer and entries);
  - Completed (no `bank_id`, and it is about shreds, not execution).
- **Dead names the bank.** It arrives as `update_bank_status(slot, Some(parent), Dead(err), bank_id)`, never through `update_slot_status`. A plugin that overrides `update_bank_status` handles Dead there. Dead ends the bank, but it is not the bank's last event: its queued entries, footer and transactions can still follow.
- **Dead and UpdateParent's `cleared_bank_id` are the only abandonment signals.** Both name the bank. Every other way a bank dies is silent.
- **A slow callback slows the validator.**
  - Entry and footer callbacks: the entry channel is bounded and its producers block when it is full, so a plugin that falls behind on `solEntryNotif` stalls replay and block production rather than growing memory.
  - Transaction callbacks: during an Alpenglow block switch, and for UpdateParent slots in startup replay, replay waits for `solTxStatusWrtr` to finish a purge request queued behind the pending transaction notifications.

## How each callback reaches the plugin

The plugin manager calls every plugin inline, on whichever thread calls the notifier (`geyser-plugin-manager/src/*_notifier.rs`). So ordering depends on two things only:

- which thread produces the event;
- whether a channel sits between the producer and the plugin.

A channel with several producers preserves each producer's order and nothing more.

| Callback | Produced by | Plugin is called on | Queue |
|---|---|---|---|
| `update_account_for_bank` | `Accounts::store_accounts` (`accounts-db/src/accounts.rs:504-535`), one account at a time, **before** the store | The storing thread. That is the replay thread, `solScHandle*` scheduler workers, `solReplayFork*` workers (with `--replay-forks-threads` > 1), banking workers, `solPohTickProd` (TowerBFT leader's last tick), `solBlkCreatLoop`, or a `solBnkClcRwds*` rewards-pool worker for epoch feature activation (the caller blocks on it) | none |
| `update_bank_status(CreatedBank)` | `ReplayStage::new_bank_from_parent_with_notify` (`core/src/replay_stage.rs:5648`), **after** `Bank::new_from_parent_with_options` returns | The replay thread (fork generation `:5564`, TowerBFT leader start `:3134`) or `solBlkCreatLoop` (Alpenglow leader, `core/src/block_creation_loop.rs:1430`) | none |
| `notify_block_metadata_for_bank` | `process_replay_results` (`core/src/replay_stage.rs:4401`), after `bank.freeze()`. This covers leader banks too: replay freezes them | The replay thread | none |
| `update_bank_status(Dead)` | `mark_hard_dead_slot_notifications` (`core/src/replay_stage/dead_slots.rs:242`), for a replay error (`replay_stage.rs:4123`), a completion error (`:4155`), a bank-hash mismatch after freeze (`:4219`), a soft-dead bank that cannot recover (`replay_stage/update_parent.rs:320`), or an unusable UpdateParent (`:422`). Soft-dead banks get nothing | The replay thread | none. It needs no RPC |
| `notify_entry_for_bank`, `notify_block_footer`, `notify_entry_update_parent` | `EntryNotification` channel (`ledger/src/entry_notifier_service.rs`) | `solEntryNotif`, the only consumer | **Bounded at 2,048, producers block when full** (`send_entry_notification`, `:39-58`), **several producers**: the replay thread or `solReplayFork*` (replayed entries and footer), the replay thread (replay-side UpdateParent), `solTpuEntry` (leader entries and footer), `solBlkCreatLoop` (leader UpdateParent). Leader entries and footers first cross PohRecorder's working-bank channel, also bounded at 2,048 and blocking (`poh/src/poh_recorder.rs:66`) |
| `notify_transaction_for_bank` | `TransactionStatusService` (`rpc/src/transaction_status_service.rs:352`) | `solTxStatusWrtr`, the only consumer | Unbounded. Producers are scheduler workers (replay and startup) and banking workers (leader). Replay's history purge requests share this queue (`:442`) |
| `update_bank_status(Processed)` | `BankNotification::Frozen`, sent by replay just before BlockMeta (`replay_stage.rs:4372`) | `solBankNotif`, after passing through `solOpConfBnkTrk` | **two** queues |
| `update_bank_status(Confirmed)` | `BankNotification::OptimisticallyConfirmed`. TowerBFT sends it from the vote listener; Alpenglow sends it from the votor root path (`votor/src/root_utils.rs:71-87`), so it means *finalized* there | same | two queues |
| `update_bank_status(Rooted)` | `BankNotification::NewRootedChain` (`votor/src/root_utils.rs:186`) | same | two queues |

## One bank's life, in production order

"→ sync" means the plugin was called before the producer moved on. "→ queued" means the event was only enqueued at that point.

### Replayed bank (the normal case for a non-leader RPC/geyser node)

1. **`Bank::new_from_parent_with_options`** → sync account writes with the new `bank_id`. These come from `runtime/src/bank.rs:2079-2129`; see "Construction-time writes" below.
2. **CreatedBank** → sync.
3. **For each entry batch,** every Entry is queued **before** any of its transactions is scheduled (`ledger/src/blockstore_processor.rs:1619`, vs `schedule_entries` at `:1727`).
4. **For each transaction,** on a scheduler worker:
   - its account writes run inside `Bank::commit_transactions` → sync;
   - its status batch is queued afterwards (`runtime/src/transaction_execution.rs:123-182`).
5. **The block footer (Alpenglow):**
   - replay first waits for the scheduler to drain (inside `BlockComponentProcessor::on_footer`, `runtime/src/block_component_processor.rs:646`);
   - the footer's account writes run → sync (Clock, `NANOSECOND_CLOCK_ACCOUNT`, and vote accounts for rewards; `update_bank_with_footer_fields`, `runtime/src/block_component_processor.rs:783-800`);
   - BlockFooter is queued (`blockstore_processor.rs:1549-1555`);
   - the final alpentick Entry is queued **after** the footer.
6. **The last tick** writes RecentBlockhashes → sync (`runtime/src/bank.rs:3847-3854`).
7. **`bank.freeze()`** → sync writes (`runtime/src/bank.rs:3309-3311`):
   - the fee payout: to the leader (its identity, or its vote account's block-revenue collector once `custom_commission_collector` is active) and, once `block_revenue_sharing` is active, the delegators' share to the leader's vote account (`runtime/src/bank/fee_distribution.rs`);
   - SlotHistory;
   - the incinerator.

   Alpenglow non-leader banks then check their bank hash against the footer (`freeze_and_verify_bank_hash`, `runtime/src/bank.rs:3326-3338`). On a mismatch, replay sends Dead for this `bank_id` (`replay_stage.rs:4219`), and steps 8-9 never happen.
8. **`BankNotification::Frozen`** is queued (`replay_stage.rs:4372`); it becomes Processed later.
9. **BlockMeta** → sync (`replay_stage.rs:4401`).

### Leader bank, TowerBFT

1. **Steps 1-2 as above** (`core/src/replay_stage.rs:3134`).
2. **Banking workers** run execute → record → commit → queue status batch (`core/src/banking_stage/consumer.rs:341-515`).
   - Execute runs under the bank's transaction-execution guard; record and commit run under its freeze lock (`:438-515`).
   - Recording is fire-and-forget.
   - The Entry is built later on `solPohTickProd` and notified via `solTpuEntry`.
   - So the leader Entry races its own transactions' commit. This is the opposite of replay.
3. **The final tick** is queued right after `register_tick` (`poh/src/poh_recorder.rs:720-736`), so it can lose the race with freeze.
4. **Freeze, Frozen and BlockMeta** run on the replay thread, the same code as replay, without hash verification.

### Leader bank, Alpenglow

`core/src/block_creation_loop.rs:773-940` and `poh/src/poh_recorder.rs:630-736`:

1. **Steps 1-2 as above** (`block_creation_loop.rs:1430`). The first Alpenglow block also writes `GENESIS_CERTIFICATE_ACCOUNT` **after** CreatedBank (`set_alpenglow_genesis_certificate`, `runtime/src/bank.rs:3662-3677`). A replayed first Alpenglow block writes it after CreatedBank too (`runtime/src/block_component_processor.rs:540`).
2. **Transactions** are the same as on the TowerBFT leader; the Entry is built on `solBlkCreatLoop`.
3. **Pending records are drained.** Every transaction Entry is queued before the footer.
4. **The footer's account writes** run on `solBlkCreatLoop` → sync, after `wait_for_inflight_commits`, under the bank's execution guard.
5. **The last tick is registered.** Then:
   - replay freezes the bank;
   - `solBlkCreatLoop` spins until `bank.is_frozen()`, which turns true when `freeze()` sets the hash (`wait_for_freeze_and_send_footer`, `poh_recorder.rs:630`);
   - it queues BlockFooter, carrying the bank hash, then the final tick Entry;
   - meanwhile, after `freeze()` returns, replay sends Frozen and calls BlockMeta.
6. **The footer is produced after freeze, and races BlockMeta.** Both start from the end of `freeze()`: BlockMeta follows on the replay thread after the Frozen send and vote bookkeeping, while the footer crosses the working-bank channel, `solTpuEntry` and the entry channel. So the footer usually reaches the plugin after BlockMeta, but nothing orders them.

### Construction-time writes (all delivered before CreatedBank)

| When | What is written, in order |
|---|---|
| Every bank | SlotHashes; StakeHistory, only on the first bank of a new epoch; Clock (Alpenglow writes only the slot here, and the footer writes Clock again with the time); LastRestartSlot, only when its value changes |
| First bank of a new epoch **on that fork** (not necessarily the epoch's first slot) | Feature accounts being activated. Then, for features that activate: the Rent sysvar (SIMD-0437/0438 only), builtin and precompile accounts, and core-BPF program upgrades. The SIMD-0567 p-ATA upgrade writes the ATA program, its programdata, then its buffer account zeroed, and nothing if its build-hash check fails (`runtime/src/bank.rs:6430-6444`). Alpenglow only: `REWARD_EPOCH_DELEGATED_STAKES_ACCOUNT`, the VAT burn (vote accounts and incinerator), then the epoch-boundary accounts in one store (`store_epoch_boundary_accounts_partitioned`): commission payouts, then vote accounts whose pending delegator rewards were swept (`block_revenue_sharing`). Payouts go to vote accounts, or, once `custom_commission_collector` (SIMD-0232) is active, to each validator's collector, which may be a system account or the incinerator. Then the EpochRewards sysvar and `VOTE_REWARD_ACCOUNT_ADDR`. Then the every-bank writes above |
| Partitioned-rewards window: the next `num_partitions` blocks **by block height**, per fork | A batch of stake accounts, then EpochRewards (written again inactive on the last partition). This runs **before** SlotHashes and Clock (`runtime/src/bank.rs:2097` vs `:2116`), so the first bank_id event of these banks is a non-sysvar stake account |

**When the per-bank sysvars are written:**

| Sysvar | Written | Before CreatedBank? |
|---|---|---|
| Clock, SlotHashes | at bank construction (`runtime/src/bank.rs:2116-2129`) | yes |
| RecentBlockhashes | at the last tick (`runtime/src/bank.rs:3847-3854`) | no |
| SlotHistory | inside `freeze()` (`runtime/src/bank.rs:3310`) | no |

**One pubkey can be written several times in one bank,** with increasing `write_version`:

- EpochRewards: 2-3 times;
- vote accounts: VAT burn, then commission or sweep at the epoch boundary; on Alpenglow, footer vote rewards and then, for the leader's vote account, the delegators' fee share at freeze;
- incinerator: VAT burn, a commission routed to it, then freeze;
- Alpenglow Clock: at construction, then from the footer.

## What you can rely on (per `bank_id`)

1. **Construction-time account writes come before CreatedBank.** The constructor does all its stores synchronously before returning; nothing at an epoch boundary stores asynchronously.
2. **CreatedBank comes before every execution-time event of that bank:** entries, transactions, footer, UpdateParent for it, Processed, Dead, BlockMeta, and post-construction account writes. All of these are produced after the synchronous CreatedBank call returns.
3. **Every account write comes before BlockMeta, and none can come after it.** Freeze asserts that no store has started (`runtime/src/bank.rs:4611`, `:5023`, `:5068`), and in-flight execution is drained before freeze.
4. **A transaction's account writes come before its transaction notification.** The store happens inside commit, and the status batch is queued after commit, on both paths.
5. **For one pubkey within one bank, writes arrive in store order.**
   - Account locks prevent two transactions that write the same account from committing at the same time.
   - The unified scheduler unlocks after the handler returns; banking unlocks when the batch drops after commit.
   - `write_version` increases in the same order. Use it, not arrival order, when merging.
6. **One bank's entries arrive in index order, and the footer follows every transaction Entry.** The footer is never the last entry-channel event: the final tick or alpentick Entry follows it.
7. **Processed comes after every transaction notification of that bank,** but only when RPC is on and transaction notifications exist. `solOpConfBnkTrk` waits on a dependency tracker (`rpc/src/optimistically_confirmed_bank_tracker.rs:307-310`). Entries and the footer get no such wait.
8. **Rooted arrives in ascending slot order, with each parent threaded through, and only for slots above the last root notified.** Confirmed always has `parent = None`, and each slot is confirmed at most once. Confirmed walks `parents_inclusive()` upward and is ascending, **except** for a confirmation that arrived before its bank froze and was overtaken: if a later slot is confirmed while that bank is still unfrozen, its Confirmed is delivered when it freezes, below slots already confirmed.
9. **A bank's Dead comes before every callback of a later bank for the same slot.** Replay sends Dead while that bank is the one BankForks holds for the slot, and builds a same-slot replacement only after it leaves BankForks, all on the replay thread. The one gap is theoretical: on the node's own leader slot, `solBlkCreatLoop` can build a bank between its existence check and replay's insert, so that bank's CreatedBank and construction writes precede a later Dead of the replayed bank.
10. **A bank gets at most one Dead, and never after its BlockMeta.** A slot can get several Deads, one per `bank_id`. On a bank-hash mismatch the plugin sees freeze's account writes, then Dead, and no Processed or BlockMeta.

## What you must not rely on

1. **BlockMeta before the bank's last entries, footer or transactions.**
   - BlockMeta does not wait for `solEntryNotif` or `solTxStatusWrtr`.
   - Footer before BlockMeta on replayed slots is the usual case, not a guarantee.
   - On Alpenglow leader blocks the footer is produced after freeze and races BlockMeta.
2. **Any order between entries, transactions and account writes of one bank.** They travel on three different paths.
3. **Transactions in `transaction_index` order.** The unified scheduler runs each transaction as its own batch on parallel workers, and banking workers run in parallel too. Use `transaction_index`.
4. **Processed relative to BlockMeta, the footer or entries.** Frozen is queued just before BlockMeta is called, then crosses two threads.
5. **Processed before Confirmed (TowerBFT).** The tracker checks `bank.is_frozen()` live. An OptimisticallyConfirmed dequeued after `freeze()`, but before that bank's Frozen message, emits Confirmed first.
6. **Any ordering across banks.**
   - Replay processes several banks per loop iteration (in parallel with `--replay-forks-threads > 1`).
   - The leader bank executes concurrently with replay.
   - In Alpenglow the replay thread and `solBlkCreatLoop` can construct banks at the same time.
7. **UpdateParent ordered against the cleared bank's entries, on the leader path.**
   - The leader drains its record receiver, then sends UpdateParent from `solBlkCreatLoop` straight into the entry channel, while the cleared bank's entries may still be waiting in the working-bank channel or `solTpuEntry` (`core/src/block_creation_loop.rs:1065-1121`).
   - Expect `Entry{bank_id = cleared}` **after** `UpdateParent{cleared_bank_id}`.
   - On the replay path, UpdateParent is queued after the cleared bank's replayed entries, so only its transactions can follow it.
   - The replacement bank's CreatedBank and construction writes are synchronous, so they can arrive **before** the UpdateParent that explains them.
8. **`bank_id` order as slot order.** Siblings are built in HashMap order, replacements get newer ids, and two threads build banks. The only ordering is that a child's id is greater than its parent's.
9. **Confirmed in slot order.** See guarantee 8: an overtaken confirmation arrives late.
10. **Dead as a bank's last event.** Its queued entries (including entries after the failure point, which never ran), footer and transactions can arrive after it. When replay itself detects the error (load, tick, hash, verification or block-component errors), it does not drain the scheduler, so transactions already scheduled keep committing and their account writes can follow Dead too. This last point comes from reading `mark_replay_dead_slot`; no test covers it.

## Start of a block

- **Open the block on the first event with an unseen `bank_id`, and buffer from there.**
  - Do not require CreatedBank to come first.
  - Do not require a sysvar to come first: on rewards-window banks the first event is a stake account.
  - The every-bank writes, SlotHashes then Clock, are always present before CreatedBank. Their absence before an Entry, Transaction or BlockMeta means the stream joined mid-bank.
  - Keep the `bank_id`s of dead and cleared banks, and drop their events. Late events of those banks otherwise open a block that never completes.
- **Take the parent from CreatedBank.** BlockMeta also carries `parent_slot`, but it arrives at the end.
- **Treat `bank_id` as scoped to one validator process.**
  - Every bank takes its id from one counter shared from the startup root (`runtime/src/bank.rs:1441-1451`, `accounts-db/src/bank_id.rs:28-36`), so ids are unique within the process.
  - Root banks (genesis, snapshot root) start at 0.
  - Ids restart from 1 after every validator restart, so reset all bank_id state on reconnect.

## Freeze and completion

**BlockMeta is the freeze signal.** It is the only bank_id event emitted after `freeze()` for every frozen bank, leader or not, except an Alpenglow bank whose hash does not match its footer: that bank gets Dead instead. Startup replay is the other exception (see below).

| Check | Caveat |
|---|---|
| Entries ≥ `entry_count` | Leader banks report `entry_count = 0`. The count comes from replay progress (`core/src/replay_stage.rs:4411`), and leader banks skip replay, so the check passes trivially. There is no count-based way to know all leader entries arrived. |
| Transactions ≥ `executed_transaction_count` | This counts committed transactions, failed and vote included (`runtime/src/bank.rs:4623`). It matches the number of transaction notifications: TSS skips only uncommitted results (`rpc/src/transaction_status_service.rs:308`), which a valid block cannot contain. |
| Footer received (Alpenglow) | An Alpenglow bank writes `NANOSECOND_CLOCK_ACCOUNT` while applying its footer, before freeze (`update_clock_from_footer`, `runtime/src/bank.rs:3680-3720`). On a leader `BankFreezeTimeout`, the block creation loop asks replay to clear the bank. If replay froze it first, you get BlockMeta (and Processed) but never a footer, then a silent drop. If the clear wins, there is no BlockMeta. |

## Abandoned banks

Every path that drops an unfrozen bank first quiesces it (`Bank::quiesce_transaction_execution`, `runtime/src/bank.rs:2489`): admitted execution and commits finish, and new execution is refused. So no account write for a dropped bank is produced after it is dropped. Its already-queued entries and transactions are still delivered.

| What happens | What the plugin sees |
|---|---|
| UpdateParent restart (Alpenglow) | `notify_entry_update_parent(cleared_bank_id)`, but only to plugins with `entry_notifications_enabled()` (`geyser-plugin-manager/src/entry_notifier.rs:180`). Footer-only plugins never see it. Descendants cleared with it get nothing, and on the leader path the event races entries (see above). If the UpdateParent marker is unusable, replay sends Dead for the bank instead, and no UpdateParent (`core/src/replay_stage/update_parent.rs:416-430`). |
| Replay failure, or a bank-hash mismatch after freeze | `update_bank_status(Dead)` with the bank's `bank_id` and parent. On a hash mismatch, freeze's account writes were already delivered, the footer was queued, and there is no BlockMeta. Dead is not final for the slot: after dump-and-repair or an Alpenglow duplicate-block switch, the slot can come back with a new `bank_id`. |
| Soft-dead (Alpenglow fast handover) | Nothing while the bank is soft-dead. It resolves as UpdateParent plus a new CreatedBank, as Dead for the bank (`update_parent.rs:296-321`), or silently: when the slot is already dead in blockstore (for example after Alpenglow duplicate-shred marking), or when root pruning or a switch removes the bank first. |
| Duplicate dump-and-repair (TowerBFT), Alpenglow duplicate-block switch, Alpenglow enablement purge | Nothing. Only frozen or dead banks are dumped: a dead one already had its Dead, and a frozen one may already have had CreatedBank, Processed and BlockMeta. |
| Leader `insert_bank` stale rejection (also when the parent slot's bank in BankForks has a different `bank_id`), or `abort_working_bank` after a failed slot-start or genesis-certificate send | CreatedBank and the construction writes, then nothing. When the parent is being quiesced, the block creation loop does not build the bank, so there is no CreatedBank either. |
| Other `abort_working_bank` calls, or a leader bank retired by root advancement during block completion | Nothing more. The bank may already have streamed transactions and entries, and after a `BankFreezeTimeout` even BlockMeta. |
| Fork pruned when a new root is set | Nothing. Infer it: any bank at or below a Rooted slot that is not the rooted chain's `bank_id` for that slot is dead, and so is any bank above the root that does not descend from it (`runtime/src/bank_forks.rs:713-727`). TowerBFT and Alpenglow both prune through `votor/src/root_utils.rs:203-275`, inline and silently. |

## Startup and restarts

1. **Snapshot restore:** `update_account_from_snapshot` (no `bank_id`), then `notify_end_of_startup`.
   - `notify_end_of_startup` fires even with snapshot notifications off.
   - On a start from genesis it **never fires**, and the genesis accounts arrive as `update_account_for_bank` with `bank_id` 0.
2. **Local-ledger replay, from the snapshot root to the blockstore tip:**
   - full slots only, always after `notify_end_of_startup`;
   - delivers account, entry, transaction and footer notifications with `bank_id`;
   - delivers **no CreatedBank, no BlockMeta, no Processed and no Dead** (`ledger/src/blockstore_processor.rs:1997`, `:2521`);
   - banks are built before they are processed, and some are discarded silently; a bank that fails is quiesced, then cleared, silently (`:2162`);
   - for a slot with an UpdateParent marker, replay waits for `solTxStatusWrtr` to purge that slot's earlier transaction history.
3. **Live replay starts.** Partial slots at the tip get CreatedBank as usual.
4. **The first rooted chain after boot** can produce Rooted and Confirmed for startup banks (including the snapshot root, `bank_id` 0) that never had CreatedBank.
5. **Under `--warp-slot`,** the warp bank writes accounts and freezes without any status notification.
6. **On graceful shutdown,** `solTxStatusWrtr` drains its queue before the plugins unload, while `solEntryNotif` and the status threads stop on exit. The last callbacks can be transactions of banks whose entries, BlockMeta or status never arrive.

## Configuration that changes what you get

- **RPC on/off (`--rpc-port`):**
  - Processed, Confirmed and Rooted exist only with RPC enabled. The tracker is created inside `if let Some(..) = config.rpc_addrs` (`core/src/validator.rs:1292`). CreatedBank, Dead and BlockMeta need no RPC.
  - The Processed-after-transactions wait (guarantee 7) also needs RPC, plus a transaction notifier.
- **Entry vs footer opt-in:** the entry channel exists if any plugin wants entries **or** footers. UpdateParent is dispatched only to plugins with entry notifications enabled (`geyser-plugin-manager/src/entry_notifier.rs:180`).
- **Leader schedule:** a node with no stake is never leader, so it only ever sees the replayed-bank path. The leader caveats apply only to nodes that produce blocks.
- **Feature gates that change which accounts are written** (not their order): `block_revenue_sharing` (freeze writes the leader's vote account; epoch boundary sweeps vote accounts) and `custom_commission_collector` (commissions and the leader's fee go to collector accounts).

## What changed from the 4.4 revision

- **Dead names the bank** (anza #16156, `bd60ddefd6`). `notify_dead_bank` sends `update_bank_status(slot, Some(parent), Dead(err), bank_id)`, and `update_slot_status` gets only FirstShredReceived and Completed. The 6.0.0 interface docs still list Dead under `update_slot_status`, and its default `update_bank_status` forwards to `update_slot_status`, so only plugins that override `update_bank_status` see the move. Dead has a table row, guarantees 9-10, must-not-rely item 10 and new abandonment rows.
- **Replay can wait on `solTxStatusWrtr`** (#14432): during an Alpenglow block switch, and in startup replay for UpdateParent slots.
- **`solTxStatusWrtr` drains its queue on graceful shutdown** (#16227).
- **A replayed transaction that pushes its bank over the cost limit is notified before the bank's Dead** (#15640). 4.4 never notified it.
- **Construction writes:** the SIMD-0567 p-ATA upgrade is new; the p-token and stake-v5 upgrades and the rent-threshold Rent write are gone. The order is unchanged.
- **BlockMeta rewards** list the block-revenue share as a separate `BlockRevenueSharing` entry (#16130), so one stake account can appear twice, and `Staking` holds inflation only.
- **Sysvar writes carry `rent_epoch = u64::MAX`** (#16387).
- **The leader stale-insert check** also rejects a bank whose parent slot's bank has a different `bank_id`, and the block creation loop does not build on a parent being quiesced (#15712).
- **Startup replay quiesces and clears a failed bank, silently** (#16180).
- **Corrections of the 4.4 revision:** CreatedBank is not the only callback with the parent; the hash-mismatch footer was queued, not delivered; `abort_working_bank` and root-retired leader banks may already have streamed transactions, entries or BlockMeta; root pruning also removes banks above the root that are off its chain; the storing threads include `solPohTickProd` and `solReplayFork*`; a replayed first Alpenglow block also writes `GENESIS_CERTIFICATE_ACCOUNT`; `bank_id`s are unique per process, not only per fork.
- Line references were updated throughout; everything not listed here was re-checked and holds.
