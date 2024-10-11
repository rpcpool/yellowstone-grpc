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

### subscribe to slot updates

```shell
npm start -- --endpoint https://api.rpcpool.com \
  --x-token "<token>" \
  subscribe \
  --slots
```

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
