# Yellowstone Dragon's Mouth - a Geyser based gRPC interface for Solana

This repo contains a fully functional gRPC interface for Solana, built and maintained by [Triton One](https://triton.one). It is built around Solana's Geyser interface. In this repo, we have the plugin and sample clients for multiple languages.

It provides the ability to get slots, blocks, transactions, deshred pre-execution transactions, and account update notifications over a standardised path.

For additional documentation, please see: https://docs.triton.one/rpc-pool/grpc-subscriptions

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

### gRPC listen, TLS, and auth configuration

The recommended way to configure gRPC listeners is `grpc.listen`.

`grpc.listen` is an array where each item can define:

- `address`: required
- `tls`: optional, per-listener TLS config
- `auth`: optional, per-listener authentication mode

#### Listen address formats

`address` accepts:

- TCP socket as string, for example: `"0.0.0.0:10000"`
- Unix domain socket as string, for example: `"unix:///var/run/geyser.sock"`
- Unix domain socket object with explicit permissions mode:

```json
{
   "address": {
      "path": "unix:///var/run/geyser.sock",
      "mode": 432
   }
}
```

`mode` is decimal (example above is `0o660`).

#### TLS options (`listen[].tls`)

You can configure TLS in two ways:

1. Identity pair (`cert_path` + `key_path`)
2. Certificate directory (`cert_dir`) with PEM files

```json
{
   "tls": {
      "identity": {
         "cert_path": "/etc/yellowstone/tls/server.crt",
         "key_path": "/etc/yellowstone/tls/server.key"
      }
   }
}
```

```json
{
   "tls": {
      "cert_dir": "/etc/yellowstone/certs"
   }
}
```

Both forms accept an optional `watch_file` flag (default `false`). When `true`, the server watches the relevant path(s) on disk and hot-swaps the served certificate in place, without dropping existing connections or requiring a restart:

- Identity pair: watches `cert_path` and `key_path` individually and rebuilds the identity when either changes.
- Certificate directory: watches `cert_dir` recursively and rebuilds the SNI resolver when any file under it changes.

```json
{
   "tls": {
      "cert_dir": "/etc/yellowstone/certs",
      "watch_file": true
   }
}
```

Notes:

- TLS is ignored for Unix domain sockets.
- ALPN is configured for HTTP/2 automatically.

#### Auth options (`listen[].auth`)

Auth is configured per listener with `type`.

`type: "http"`

- Validates requests using an external HTTP resolver.
- Expected request query params to resolver: `host` and `token`.
- Resolver success payload should include `subscription_id` and rate limits (`rate_limits` or `ratelimits`).

HTTP resolver contract:

- Method: `GET`
- URL: `<subscription_resolver_url>?host=<request-host>&token=<x-token>`
- Required query params:
   - `host`: host extracted from the incoming gRPC request URI
   - `token`: value from the incoming `x-token` header
- Expected status codes:
   - `200 OK`: valid `(host, token)` pair, returns subscription info JSON
   - `404 Not Found`: invalid `(host, token)` pair

The `404` behavior is important: the gRPC server treats it as an unauthenticated request for that host/token pair.

Example request:

```text
GET http://127.0.0.1:8080/?host=api.example.com&token=test
```

Example `200 OK` response body:

```json
{
    "subscription_id": "test-sub",
    "ratelimits": {
         "methods": {
            "/geyser.Geyser/Subscribe": 2000,
            "/geyser.Geyser/SubscribeReplayInfo": 2000,
            "/geyser.Geyser/Ping": 2000,
            "/geyser.Geyser/GetLatestBlockhash": 2000,
            "/geyser.Geyser/GetBlockHeight": 2000,
            "/geyser.Geyser/GetSlot": 2000,
            "/geyser.Geyser/IsBlockhashValid": 2000,
            "/geyser.Geyser/GetVersion": 2000
         }
    }
}
```

```json
{
   "auth": {
      "type": "http",
      "subscription_resolver_url": "http://127.0.0.1:8080/",
      "subscription_resolution_cache_ttl": "30s",
      "max_concurrent_auth_requests": 1000
   }
}
```

`type: "file"`

- Validates requests from a local JSON mapping file.
- The file should contain an array of objects with `token`, `host`, and `subscription_info`.

```json
{
   "auth": {
      "type": "file",
      "subscription_resolver_path": "/etc/yellowstone/subscriptions.json"
   }
}
```

Example `/etc/yellowstone/subscriptions.json`:

```json
[
   {
      "token": "test",
      "host": "127.0.0.1",
      "subscription_info": {
         "subscription_id": "test-sub",
         "ratelimits": {
            "methods": {
               "/geyser.Geyser/Subscribe": 2000,
               "/geyser.Geyser/SubscribeReplayInfo": 2000,
               "/geyser.Geyser/Ping": 2000,
               "/geyser.Geyser/GetLatestBlockhash": 2000,
               "/geyser.Geyser/GetBlockHeight": 2000,
               "/geyser.Geyser/GetSlot": 2000,
               "/geyser.Geyser/IsBlockhashValid": 2000,
               "/geyser.Geyser/GetVersion": 2000
            }
         }
      }
   },
   {
      "token": "prod-token-1",
      "host": "api.example.com",
      "subscription_info": {
         "subscription_id": "prod-sub-1",
         "ratelimits": {
            "methods": {
               "/geyser.Geyser/Subscribe": 2000,
               "/geyser.Geyser/SubscribeReplayInfo": 2000,
               "/geyser.Geyser/Ping": 2000,
               "/geyser.Geyser/GetLatestBlockhash": 2000,
               "/geyser.Geyser/GetBlockHeight": 2000,
               "/geyser.Geyser/GetSlot": 2000,
               "/geyser.Geyser/IsBlockhashValid": 2000,
               "/geyser.Geyser/GetVersion": 2000
            }
         }
      }
   }
]
```

**NOTE**: method rate-limit is not yet implemented.

`type: "trusted-metadata"`

- Trusts the incoming `x-subscription-id` header and skips external auth checks.

```json
{
   "auth": {
      "type": "trusted-metadata"
   }
}
```

#### Full `listen` example

```json
{
   "grpc": {
      "listen": [
         {
            "address": "0.0.0.0:10000",
            "auth": {
               "type": "http",
               "subscription_resolver_url": "http://127.0.0.1:8080/",
               "subscription_resolution_cache_ttl": "30s",
               "max_concurrent_auth_requests": 1000
            }
         },
         {
            "address": "0.0.0.0:10001",
            "tls": {
               "identity": {
                  "cert_path": "/etc/yellowstone/tls/server.crt",
                  "key_path": "/etc/yellowstone/tls/server.key"
               },
               "watch_file": true
            },
            "auth": {
               "type": "trusted-metadata"
            }
         },
         {
            "address": {
               "path": "unix:///var/run/geyser.sock",
               "mode": 432
            },
            "auth": {
               "type": "file",
               "subscription_resolver_path": "/etc/yellowstone/subscriptions.json"
            }
         }
      ]
   }
}
```

#### Legacy fields

`grpc.address`, `grpc.tls_config`, and `grpc.cert_dir` are legacy and deprecated in favor of `grpc.listen`.

Use `grpc.listen` for new configurations, especially if you need different TLS/auth behavior per endpoint.

### Pre-commit hooks

Install repository hooks:

```bash
make install-hooks
```

The pre-commit hook will:

- ensure commit signing is enabled (`commit.gpgsign=true`)
- run `cargo fmt --all -- --check` and print a warning if formatting fails

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

#### Deshred transactions

`SubscribeDeshred` is a separate bi-directional stream for pre-execution transactions. Instead of waiting for Replay to execute a transaction and produce `TransactionStatusMeta`, the server reconstructs entries from incoming shreds and streams the decoded transaction as soon as it is available.

This gives you an earlier signal than the regular `transactions` stream, but it comes with less context:

   - available fields — `slot`, `signature`, `is_vote`, raw `transaction`, `loaded_writable_addresses`, `loaded_readonly_addresses`
   - unavailable fields — execution status, error details, logs, inner instructions, balances, compute usage, `TransactionStatusMeta`

`loaded_writable_addresses` and `loaded_readonly_addresses` contain addresses resolved from address lookup tables, so deshred filters can match both static account keys and dynamically loaded addresses.

Availability:

   - the protobuf API and Rust client expose `SubscribeDeshred`
   - this RPC is only available on Triton extension servers
   - the open-source `yellowstone-grpc-geyser` server in this repository currently returns `UNIMPLEMENTED` for `SubscribeDeshred`
   - the implemented version currently lives on the [`master-triton-ext` branch](https://github.com/rpcpool/yellowstone-grpc/tree/master-triton-ext)

The deshred transaction filter supports:

   - `vote` — enable/disable broadcast `vote` transactions
   - `account_include` — match transactions that mention any listed account, including ALT-loaded addresses
   - `account_exclude` — exclude transactions that mention any listed account, including ALT-loaded addresses
   - `account_required` — require all listed accounts to be present, including ALT-loaded addresses

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

### Examples

- [Go](examples/golang)
- [Rust](examples/rust)
- [TypeScript](examples/typescript)

For a `SubscribeDeshred` CLI example, see [examples/rust](examples/rust).

> [!NOTE]
> Some load balancers will terminate gRPC connections if no messages are sent from the client for a period of time.
> In order to mitigate this, you need to send a message periodically. The `ping` field in the SubscribeRequest is used for this purpose.
> The gRPC server already sends pings to the client, so you can reply with a ping, and your connection will remain open.
> You can see in the rust example how to reply to the ping from the server with the client.

### Projects based on Geyser gRPC

- https://github.com/rpcpool/yellowstone-grpc-kafka — forward gRPC stream to Kafka, dedup, read stream from Kafka with gRPC server
