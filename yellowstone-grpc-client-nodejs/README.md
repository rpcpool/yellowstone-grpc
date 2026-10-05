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

### Auto reconnect

Use `subscribeWithReconnect()` to receive bank-aware recovery events. The optional fourth constructor argument configures the retry backoff. Ordinary `subscribe()` and `subscribeDeshred()` streams do not reconnect.

```ts
import Client, { ReconnectEvent } from "@triton-one/yellowstone-grpc";

const client = new Client(endpoint, xToken, channelOptions, {
  backoff: {
    initialIntervalMs: 100,
    multiplier: 2,
    maxRetries: 10,
  },
});

await client.connect();
const stream = await client.subscribeWithReconnect(request);

stream.on("data", (event: ReconnectEvent) => {
  if (event.type === "Update") {
    applyUpdate(event.generation, event.update);
  } else {
    for (const bank of event.banks) {
      removeBank(bank.generation, bank.slot, bank.bankId);
    }
    recordRecovery(event.replacement, event.winners);
  }
});
stream.on("error", (error) => console.error("Recovery failed", error));
```

The application supplies `applyUpdate`, `removeBank`, and `recordRecovery`. Keep state by `(generation, slot, bankId)`. Bank IDs identify banks within one connection; they are not stable across connections. All generation, slot, and bank ID values are decimal strings to preserve uint64 precision.

Apply each `DiscardBanks` event before processing the following replacement updates. Remove only the listed bank identities. `IncompleteDelivery` means that delivery was interrupted; it does not mean that a bank lost consensus. `replacement` gives the inclusive replay boundary and the connection generation that supplies replacement updates. Each slot winner is either `{ type: "Finalized", slot, blockhash }` or `{ type: "Unknown", slot }` for a proven skipped slot.

Recovery requires processed commitment, no startup snapshots, and no initial `fromSlot`. It waits for finalized winner evidence before emitting discards and replacement updates. Recovery buffers updates without a size cap while waiting for this evidence. Failed replay or exhausted retries terminates the stream with an error. Request writes and pings use the same `stream.write(request)` API as ordinary subscriptions.

To migrate reconnecting subscriptions, replace `subscribe()` with `subscribeWithReconnect()` and handle both event variants. Remove `enabled`, `policy`, and `slotRetention` from the fourth constructor argument; these options are rejected because they do not configure bank recovery. Omit the fourth argument to use the default backoff with `subscribeWithReconnect()`.

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

`signerInclude` and `signerExclude` match the accounts that signed a
transaction: the first `numRequiredSignatures` static account keys. A key the
transaction only references, or loads from a lookup table, does not match, and
`tokenAccounts` does not apply to them. Deshred filters accept them too.

```ts
request.transactions.signedByWallet = {
  vote: false,
  accountInclude: [],
  accountExclude: [],
  accountRequired: [],
  signerInclude: [walletPubkey],
  signerExclude: [],
};
```

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
- Ordinary subscription stream updates are normalized to `SubscribeUpdate` with top-level oneof fields (`account`, `slot`, `transaction`, etc). Reconnecting subscriptions emit `ReconnectEvent` objects whose `Update` variant contains a `SubscribeUpdate`.
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
