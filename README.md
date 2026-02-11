# Yellowstone Dragon's Mouth - a Geyser based gRPC interface for Solana

This repo contains a fully functional gRPC interface for Solana, built and maintained by [Triton One](https://triton.one). It is built around Solana's Geyser interface. In this repo, we have the plugin and sample clients for multiple languages.

It provides the ability to get slots, blocks, transactions, and account update notifications over a standardised path.

For additional documentation,  please see: https://docs.triton.one/rpc-pool/grpc-subscriptions

#### Known bugs

Block reconstruction inside gRPC plugin is based on information provided by BlockMeta, unfortunately, the number of entries for blocks generated on validators is always equal to zero. These blocks will always have zero entries. See issue on GitHub: https://github.com/solana-labs/solana/issues/33823

### Validator

```bash
solana-validator --geyser-plugin-config yellowstone-grpc-geyser/config.json
```

### Plugin config check

```bash
cargo-fmt && cargo run --bin config-check -- --config yellowstone-grpc-geyser/config.json
```

### Block reconstruction

Geyser interface on block update does not provide detailed information about transactions and account updates. To provide this information with a block message, we must collect all messages and expect a specified order. By default, if we failed to reconstruct full block, we log an error message and increase the `invalid_full_blocks_total` counter in prometheus metrics. If you want to panic on invalid reconstruction, change the option `block_fail_action` in config to `panic` (default value is `log`).

### Filters for streamed data

Please check [yellowstone-grpc-proto/proto/geyser.proto](yellowstone-grpc-proto/proto/geyser.proto) for details.

   - `commitment` — commitment level: `processed` / `confirmed` / `finalized`
   - `accounts_data_slice` — array of objects `{ offset: uint64, length: uint64 }`, allow to receive only required data from accounts
   - `ping` — optional boolean field. Some cloud providers (like Cloudflare, Fly.io) close the stream if the client doesn't send anything during some time. You can send the same filter every N seconds as a workaround, but this would not be optimal since you need to keep this filter. Instead, you can send a subscribe request with `ping` field set to `true` and ignore the rest of the fields in the request. Since we sent a `Ping` message every 15s from the server, you can send a subscribe request with `ping` as a reply and receive a `Pong` message.

#### Slots

   - `filter_by_commitment` — by default, slots are sent for all commitment levels, but with this filter, you can receive only the selected commitment level

#### Account

Accounts can be filtered by:

   - `account` — account Pubkey, match to any Pubkey from the array
   - `owner` — account owner Pubkey, match to any Pubkey from the array
   - `filters` — same as `getProgramAccounts` filters, array of `dataSize` or `Memcmp` (bytes, base58, base64 are supported)

If all fields are empty, then all accounts are broadcast. Otherwise, fields work as logical `AND` and values in arrays as logical `OR` (except values in `filters` that works as logical `AND`).

#### Transactions

   - `vote` — enable/disable broadcast `vote` transactions
   - `failed` — enable/disable broadcast `failed` transactions
   - `signature` — match only specified transaction
   - `account_include` — filter transactions that use any account from the list
   - `account_exclude` — opposite to `account_include`
   - `account_required` — require all accounts from the list to be used in the transaction

If all fields are empty, then all transactions are broadcast. Otherwise, fields work as logical `AND` and values in arrays as logical `OR`.

#### Entries

Currently, we do not have filters for the entries, all entries are broadcast.

#### Blocks

   - `account_include` — filter transactions and accounts that use any account from the list
   - `include_transactions` — include all transactions
   - `include_accounts` — include all accounts updates
   - `include_entries` — include all entries

#### Blocks meta

Same as `Blocks` but without `transactions`, `accounts`, and entries. Currently, we do not have filters for block meta, all messages are broadcast.

### Limit filters

It's possible to add limits for filters in the config. If the `filters` field is omitted, then filters don't have any limits.

```json
"grpc": {
   "filters": {
      "accounts": {
         "max": 1,
         "any": false,
         "account_max": 10,
         "account_reject": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
         "owner_max": 10,
         "owner_reject": ["11111111111111111111111111111111"]
      },
      "slots": {
         "max": 1
      },
      "transactions": {
         "max": 1,
         "any": false,
         "account_include_max": 10,
         "account_include_reject": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
         "account_exclude_max": 10,
         "account_required_max": 10
      },
      "blocks": {
         "max": 1,
         "account_include_max": 10,
         "account_include_any": false,
         "account_include_reject": ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
         "include_transactions": true,
         "include_accounts" : false,
         "include_entries" : false
      },
      "blocks_meta": {
         "max": 1
      },
      "entry": {
         "max": 1
      }
   }
}
```

### Unary gRPC methods

#### Ping

#### GetLatestBlockhash

#### GetBlockHeight

#### GetSlot

#### IsBlockhashValid

#### GetVersion

### System Tuning

For high-throughput subscriptions, increase the Linux TCP receive buffer sizes. This can improve bandwidth by 5x+ on high-latency connections.

Apply immediately:

```bash
sudo sysctl -w net.core.rmem_max=67108864 net.ipv4.tcp_rmem="4096 87380 67108864"
```

To persist across reboots, add to `/etc/sysctl.conf`:

```
net.core.rmem_max=67108864
net.ipv4.tcp_rmem=4096 87380 67108864
```

### Examples

   - [Go](examples/golang)
   - [Rust](examples/rust)
   - [TypeScript](examples/typescript)

> [!NOTE]
> Some load balancers will terminate gRPC connections if no messages are sent from the client for a period of time.
In order to mitigate this, you need to send a message periodically. The `ping` field in the SubscribeRequest is used for this purpose.
The gRPC server already sends pings to the client, so you can reply with a ping, and your connection will remain open.
You can see in the rust example how to reply to the ping from the server with the client.

### Projects based on Geyser gRPC

- https://github.com/rpcpool/yellowstone-grpc-kafka — forward gRPC stream to Kafka, dedup, read stream from Kafka with gRPC server
