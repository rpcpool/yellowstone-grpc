# Rust client for Solana gRPC interface

This is a sample client for Solana geyser gRPC written in Rust.

This can be used in the following way:

```
$ cargo run --bin client -- --endpoint https://api.rpcpool.com --x-token <token> subscribe --accounts --accounts-account SysvarC1ock11111111111111111111111111111111
    Finished dev [unoptimized + debuginfo] target(s) in 0.69s
     Running `target/debug/client --accounts --account SysvarC1ock11111111111111111111111111111111`
stream opened
new message: SubscribeUpdate { filters: ["client"], update_oneof: Some(Account(SubscribeUpdateAccount { account: Some(SubscribeUpdateAccountInfo { pubkey: [6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0], lamports: 1169280, owner: [6, 167, 213, 23, 24, 117, 247, 41, 199, 61, 147, 64, 143, 33, 97, 32, 6, 126, 216, 140, 118, 224, 140, 40, 127, 193, 148, 96, 0, 0, 0, 0], executable: false, rent_epoch: 0, data: [57, 29, 0, 0, 0, 0, 0, 0, 165, 160, 80, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 221, 189, 80, 99, 0, 0, 0, 0], write_version: 53718 }), slot: 7481, is_startup: false })) }
new message: SubscribeUpdate { filters: ["client"], update_oneof: Some(Account(SubscribeUpdateAccount { account: Some(SubscribeUpdateAccountInfo { pubkey: [6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0], lamports: 1169280, owner: [6, 167, 213, 23, 24, 117, 247, 41, 199, 61, 147, 64, 143, 33, 97, 32, 6, 126, 216, 140, 118, 224, 140, 40, 127, 193, 148, 96, 0, 0, 0, 0], executable: false, rent_epoch: 0, data: [58, 29, 0, 0, 0, 0, 0, 0, 165, 160, 80, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 222, 189, 80, 99, 0, 0, 0, 0], write_version: 53725 }), slot: 7482, is_startup: false })) }
^C
```

### subscribe to account updates
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  subscribe \
  --accounts --accounts-account "<Pubkey>"
```

### subscribe to slot updates
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  subscribe \
  --slots
```

### subscribe to slot updates, commitment processed
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  --commitment processed \
  subscribe \
  --slots
```

### subscribe to transaction updates
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  subscribe \
  --transactions \
  --transactions-vote false \
  --transactions-failed false \
  --transactions-account-include "<Pubkey>"
```

### unary Ping
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  ping
```
```text
response: PongResponse { count: 1 }
```

### unary GetLatestBlockhash
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  get-latest-blockhash
```
```text
response: GetLatestBlockhashResponse { slot: 196214003, blockhash: "EYo9Ct3k8nPXJfFxcmEWxN59RuvLehQ7WKqmodBJ9cRf", last_valid_block_height: 178985293 }
```

### unary GetBlockHeight
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  get-block-height
```
```text
response: GetBlockHeightResponse { block_height: 178985715 }
```

### unary GetSlot
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  get-slot
```
```text
response: GetSlotResponse { slot: 196214563 }
```

### unary IsBlockhashValid
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  is-blockhash-valid --blockhash "<blockhash>"
```
```text
response: IsBlockhashValidResponse { slot: 196214563, valid: true }
```

### unary GetVersion
```shell
cargo run --bin client -- -e "https://api.rpcpool.com" \
  --x-token "<token>" \
  get-version
```
```text
response: GetVersionResponse { version: "{\"version\":\"0.7.0+solana.1.15.2\",\"proto\":\"1.2.0+solana.1.15.2\",\"solana\":\"1.15.2\",\"git\":\"e03a47c-modified\",\"rustc\":\"1.68.0-nightly\",\"buildts\":\"2023-05-27T08:20:15.440278Z\"}" }
```
