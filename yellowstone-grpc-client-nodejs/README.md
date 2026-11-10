# Yellowstone Node.js gRPC client

This library implements a client for streaming account updates for backend applications.

You can find more information and documentation on the [Triton One website](https://docs.triton.one/project-yellowstone/introduction).

## Prerequisites

You need to have the latest version of `protoc` installed.
Please refer to the [installation guide](https://grpc.io/docs/protoc-installation/) on the Protobuf website.

## Usage

Install required dependencies by running

```bash
npm install
```

Build the project (this will generate the gRPC client and compile TypeScript):

```
npm run build
```

Please refer to [examples/typescript](../examples/typescript/README.md) for some usage examples.

### Reconnecting subscriptions

`subscribeWithReconnect()` opens a subscription that reconnects by itself after a recoverable
failure. Ordinary `subscribe()` and `subscribeDeshred()` streams never reconnect. The fourth
constructor argument configures it:

```ts
import Client, { ReconnectEvent } from "@triton-one/yellowstone-grpc";

const client = new Client(endpoint, xToken, channelOptions, {
  backoff: { initialIntervalMs: 100, multiplier: 2, maxRetries: 10 },
  policy: "Replay", // or "NoReplay"
  slotsBehind: 0,
});

await client.connect();
const stream = await client.subscribeWithReconnect(request);

stream.on("data", (event: ReconnectEvent) => {
  if (event.type === "Update") {
    applyUpdate(event.update);
  } else {
    // The connection dropped: drop what you hold of these banks.
    for (const bank of event.partialBanks) {
      removeBank(bank.slot, bank.bankId);
    }
  }
});
stream.on("error", (error) => console.error("subscription failed", error));
```

The application supplies `applyUpdate` and `removeBank`. The stream yields two kinds of events:

- `Update`: a `SubscribeUpdate`, as `subscribe()` yields it.
- `Disconnected`: the connection dropped, and every later event comes from the new connection.
  `partialBanks` lists the banks you received part of (account writes, transactions,
  transaction statuses, entries or a CreatedBank status) but not their Block, block footer or
  block meta. The server sends those after all of a bank's content, so these are the only banks
  you may hold in part: discard them. `replayFromSlot` is where the new connection replays
  from, or `null` when it resumes at the live tip. Slots and bank IDs are decimal strings, to
  keep uint64 precision.

With `policy: "Replay"` (the default), a reconnect replays from `slotsBehind` slots before the
last confirmed slot the stream saw, or from the first request's `fromSlot` before any confirmed
slot is known. The replay resends every partial bank whole, and also
banks you already hold whole: handle duplicates. A replayed bank carries only the latest write of
each account (the server keeps one write per account and bank), so it can have fewer account
updates than the same bank delivered live, with the same final state. With `policy: "NoReplay"`, a reconnect
resumes at the live tip, and what the server sent during the outage is lost. Either way, the
stream drops whatever a later connection sends of a partial bank that no replay resends.

The stream adds block meta and slot filters named `RECONNECT_FILTER_KEY` (`"__reconn"`) to every
request, and hides the updates only they match: no filter of yours may use that name. A request
written to the stream (`stream.write(request)`) changes the filters of the live connection and of
every reconnect. Put `fromSlot` in the request passed to `subscribeWithReconnect()`: the server
ignores it on later writes. The stream ends with an error on an unrecoverable status, on a failed
reconnect, or once its retry budget is spent.

The retry budget is `backoff.maxRetries` reconnects in a row. Each reconnect waits a delay
first: `initialIntervalMs`, then multiplied by `multiplier` for each next one in a row. A
connection that delivers a bank's Block, block footer or block meta refills the budget; a ping or
a slot status alone does not, so a server that accepts and then drops cannot keep the stream
reconnecting forever.

To see reconnects against a real endpoint, run `node try-reconnect.js` after `npm run build:dev`.
It reads `TEST_ENDPOINT` (and `TEST_TOKEN`) from `.env`, connects through a local proxy that cuts
the connection every few seconds, and prints each `Disconnected` event, the partial banks it
drops, and the banks a replay resends. `--no-replay` and `--cut-every <seconds>` change the
policy and the cut interval.

### Compressed account filters

For large account sets, use `CompressedAccountFilterSet` to send a compact
cuckoo filter instead of a full explicit account list. The local set keeps exact
membership for false-positive filtering.

```ts
import Client, {
  CompressedAccountFilterSet,
  SubscribeRequest,
  TokenAccountExpansionControlFlag,
} from "@triton-one/yellowstone-grpc";

const accounts = new CompressedAccountFilterSet(2_000_000);
for (const pubkey of trackedPubkeys) {
  accounts.insert(pubkey); // base58 string, Buffer, or Uint8Array
}

const request: SubscribeRequest = {
  accounts: {},
  slots: {},
  transactions: {},
  transactionsStatus: {},
  blocks: {},
  blocksMeta: {},
  entry: {},
  accountsDataSlice: [],
};

accounts.insertIntoSubscribeRequest(request, "tracked");
const stream = await client.subscribe(request);

stream.on("data", (update) => {
  const pubkey = update.account?.account?.pubkey;
  if (pubkey && accounts.contains(pubkey)) {
    // exact local match
  }
});

accounts.insert(newPubkey);
accounts.remove(oldPubkey);
accounts.insertIntoSubscribeRequest(request, "tracked");
stream.write(request);
```

Use `insertIntoBlockSubscribeRequest(request, name)` when filtering account
includes inside block subscriptions.

Use `insertIntoTransactionSubscribeRequest(request, name)` for full transaction
updates and `insertIntoTransactionStatusSubscribeRequest(request, name)` for
transaction status updates:

```ts
accounts.insertIntoTransactionSubscribeRequest(request, "trackedTransactions");
accounts.insertIntoTransactionStatusSubscribeRequest(
  request,
  "trackedTransactionStatuses",
);
```

The compressed include filter and `accountInclude` use OR logic. Other fields,
such as `vote`, `failed`, `accountExclude`, and `accountRequired`, are applied as
separate conditions. Add them after creating the compressed filter:

```ts
request.transactions.trackedTransactions = {
  ...accounts.toTransactionFilter(),
  vote: false,
  failed: false,
  accountExclude: blockedPubkeys,
};
```

Transaction filters can also match token account owners.

- `ALL` matches an owner listed before or after the transaction.
- `BALANCE_CHANGED` matches an owner when its token balance changes. It also
  matches when a token account is created or closed.

This works with transaction and transaction status subscriptions. It applies to
`accountInclude`, `accountExclude`, and `accountRequired`.

```ts
request.transactions.trackedTransactions = {
  ...accounts.toTransactionFilter(),
  tokenAccounts: TokenAccountExpansionControlFlag.BALANCE_CHANGED,
};

request.transactionsStatus.trackedTransactionStatuses = {
  ...accounts.toTransactionFilter(),
  tokenAccounts: TokenAccountExpansionControlFlag.ALL,
};
```

If you do not set `tokenAccounts`, filters only match transaction account keys.

A compressed filter can match an account that you did not add. Before using a
full transaction update, check its account keys against your local set. If you
set `tokenAccounts`, also check the token account owners:

```ts
stream.on("data", (update) => {
  const info = update.transaction?.transaction;
  if (!info) return;

  const accountKeys = [
    ...(info.transaction?.message?.accountKeys ?? []),
    ...(info.meta?.loadedWritableAddresses ?? []),
    ...(info.meta?.loadedReadonlyAddresses ?? []),
  ];

  if (!accountKeys.some((pubkey) => accounts.contains(pubkey))) {
    return; // compressed-filter false positive
  }

  // exact local match
});
```

Transaction status updates do not include account keys. A false positive from a
`transactionsStatus` filter cannot be removed from that update alone. Use a full
transaction subscription or another transaction data source when exact local
matching is required.

## Troubleshooting

### For macOS:

You might have to run `npm run build` with `RUSTFLAGS="-Clink-arg=-undefined -Clink-arg=dynamic_lookup"` to skip the strict linkers from failing the build step and resolve `dylib`s via runtime.

```bash
RUSTFLAGS="-Clink-arg=-undefined -Clink-arg=dynamic_lookup" npm run build
```

## Working

Since the start, the `@triton-one/yellowstone-grpc` package has used the `@grpc/grpc-js` lib for gRPC types enforcement, connection and subscription management. This hit a bottleneck, described in [this blog](https://blog.triton.one/supercharging-the-javascript-sdk-with-napi/)

From `v5.0.0` the [napi-rs](https://github.com/napi-rs/napi-rs) framework is used for gRPC connection and subscription management. It's described into [this blog](https://blog.triton.one/supercharging-the-javascript-sdk-with-napi/)

These changes are internal to the SDK and do not have any breaking changes for client code. If you face any issues, please open an issue

The [napi-rs](https://github.com/napi-rs/napi-rs) based implementation is inspired from the implemenation of the [LaserStream SDK](https://github.com/helius-labs/laserstream-sdk)

### Type Compatibility

The public SDK always returns the generated protobuf-compatible types from
`src/grpc/geyser.ts`.

- Unary methods return generated response objects (for example `PongResponse`,
  `GetSlotResponse`, `GetVersionResponse`) instead of raw N-API wrapper shapes.
- Ordinary subscription stream updates are normalized to `SubscribeUpdate` with top-level oneof fields (`account`, `slot`, `transaction`, etc). Reconnecting subscriptions emit `ReconnectEvent` objects; their `Update` variant holds a `SubscribeUpdate`.
- The internal N-API `Js...` objects are an implementation detail and are
  converted automatically by the SDK wrapper.

This allows existing user code typed against the generated `src/grpc` types to
remain stable while using the N-API backend.

## Development

### Local Testing

When building for local testing at the root of the project where the `Makefile` resides, you must:

1. Clean build artifacts if any with `make clean`

2. Navigate to the SDK (where this README resides) and install dependencies with `npm install` and `npm run build:dev`. Make sure to use `build:dev` to reflect local changes in your test runs and NOT `build`.

3. Navigate to `examples/typescript` folder and install dependencies with `npm install`.

4. Run `client.ts` with an example subscription request below:
`tsx examples/typescript/src/client.ts --endpoint <ENDPOINT> --x-token <X-TOKEN> --commitment processed subscribe --transactions TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA`
