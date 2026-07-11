# TypeScript client example

## DISCLAIMER

This example can contains errors or be behind of the latest stable version, please use it only as an example of how your subscription can looks like. If you want well tested production ready example, please check our implementation on Rust.

<hr>

This is a sample client for Solana geyser gRPC written in TypeScript.

This can be used in the following way:

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token <token> \
  subscribe \
  --accounts --accounts-account SysvarC1ock11111111111111111111111111111111
```

### subscribe to account updates

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token "<token>" \
  subscribe \
  --accounts --accounts-account "<Pubkey>"
```

### subscribe to account updates with a compressed account set

Use the same `subscribe` command and `--accounts-account` inputs. Adding
`--accounts-compressed` sends those pubkeys as a compressed cuckoo filter instead
of an explicit account list. Account updates are still printed as `data ...`;
the example drops cuckoo false positives with the local exact account set before
printing.

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token "<token>" \
  subscribe \
  --accounts \
  --accounts-compressed \
  --accounts-compressed-capacity 100000 \
  --accounts-account "<Pubkey>"
```

### subscribe to slot updates

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token "<token>" \
  subscribe \
  --slots
```

### subscribe to slot updates (public endpoint, no token)

```shell
npm start -- --endpoint http://sg131.rpcpool.wg:10000 \
  subscribe \
  --slots
```

### subscribe with auto-reconnect

Auto-reconnect is opt-in and applies to standard `subscribe` streams. It reconnects after recoverable stream failures, replays from the last completed slot checkpoint, and deduplicates replayed updates before they reach the TypeScript stream.

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token "<token>" \
  --autoreconnect \
  subscribe \
  --slots
```

Backoff and slot retention can be configured from the example CLI:

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token "<token>" \
  --autoreconnect \
  --autoreconnect-initial-interval-ms 100 \
  --autoreconnect-multiplier 2 \
  --autoreconnect-max-retries 10 \
  --autoreconnect-replay-policy from-checkpoint \
  --autoreconnect-checkpoint-buffer "2" \
  --autoreconnect-slot-retention 250 \
  subscribe \
  --slots
```

Use fresh reconnect to reconnect from the current stream point without checkpoint replay:

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token "<token>" \
  --autoreconnect \
  --autoreconnect-replay-policy fresh \
  subscribe \
  --slots
```

The same setup in application code passes reconnect options as the fourth `Client` constructor argument. Passing the request to `subscribe(request)` sends the initial subscription when the stream opens.

```ts
const client = new Client(
  "https://api.rpcpool.com",
  "<token>",
  { grpcMaxDecodingMessageSize: 64 * 1024 * 1024 },
  {
    backoff: {
      initialIntervalMs: 100,
      multiplier: 2,
      maxRetries: 10,
    },
    replayPolicy: {
      fromCheckpoint: {
        checkpointBuffer: "2",
      },
    },
    slotRetention: 250,
  },
);

await client.connect();

const stream = await client.subscribe({
  accounts: {},
  slots: { client: { filterByCommitment: false } },
  transactions: {},
  transactionsStatus: {},
  entry: {},
  blocks: {},
  blocksMeta: {},
  accountsDataSlice: [],
  commitment: undefined,
  ping: undefined,
});
```

If reconnect options are omitted, the client keeps the original non-reconnecting behavior. `subscribeDeshred` is not covered by auto-reconnect.

### subscribe to slot updates, commitment processed

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  --commitment processed \
  subscribe \
  --slots
```

### subscribe to transaction updates

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  subscribe \
  --transactions \
  --transactions-vote false \
  --transactions-failed false \
  --transactions-account-include "<Pubkey>"
```

### subscribe to block updates with a compressed account include set

Use the same `--blocks-account-include` input as the explicit block filter.
Adding `--blocks-compressed` sends the included accounts as a compressed cuckoo
filter.

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  subscribe \
  --blocks \
  --blocks-compressed \
  --blocks-compressed-capacity 100000 \
  --blocks-account-include "<Pubkey>"
```

### subscribe to deshred transaction updates

```shell
npm start -- -e="http://sg131.rpcpool.wg:10000" \
  subscribeDeshred \
  --deshred-parsed \
  --deshred-vote=false \
  --deshred-account-include "<Pubkey>"
```

### unary Ping

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  ping
```

```text
response: 1
```

### unary GetLatestBlockhash

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  get-latest-blockhash
```

```text
response: {
  slot: 5188,
  blockhash: '5N5v1HQq5EFui4yaPRBAN8cF23KWdJWhvvTnNu97JEH8',
  lastValidBlockHeight: 5175
}
```

### unary GetBlockHeight

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  get-block-height
```

```text
response: 5188
```

### unary GetSlot

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  get-slot
```

```text
response: 196214563
```

### unary IsBlockhashValid

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  is-blockhash-valid --blockhash "<blockhash>"
```

```text
response: { slot: 196214563, valid: true }
```

### unary GetVersion

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  get-version
```

```text
response: { version: "{\"version\":\"0.7.0+solana.1.15.2\",\"proto\":\"1.2.0+solana.1.15.2\",\"solana\":\"1.15.2\",\"git\":\"e03a47c-modified\",\"rustc\":\"1.68.0-nightly\",\"buildts\":\"2023-05-27T08:20:15.440278Z\"}" }
```

### unary SubscribeReplayInfo

```shell
npm start -- -e="https://api.rpcpool.com" \
  --x-token "<token>" \
  subscribe-replay-info
```

```text
response: { firstAvailable: '390356345' }
```
