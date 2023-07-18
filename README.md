# Yellowstone Dragon's Mouth - a Geyser based gRPC interface for Solana

This repo contains a fully functional gRPC interface for Solana. It is built around Solana's Geyser interface. In this repo we have the plugin as well as sample clients for multiple languages.

It provides the ability to get slots, blocks, transactions, and account update notifications over a standardised path. 

For additional documentation,  please see: https://docs.triton.one/rpc-pool/grpc-subscriptions

### Validator

```bash
$ solana-validator --geyser-plugin-config yellowstone-grpc-geyser/config.json
```

### Plugin config check

```
cargo-fmt && cargo run --bin config-check -- --config yellowstone-grpc-geyser/config.json
```

### Filters

See [yellowstone-grpc-proto/proto/geyser.proto](yellowstone-grpc-proto/proto/geyser.proto).

#### Slots

Currently all slots are broadcasted.

#### Account

Accounts can be filtered by:

   - `account` — acount Pubkey, match to any Pubkey from the array
   - `owner` — account owner Pubkey, match to any Pubkey from the array
   - `filters` — same as `getProgramAccounts` filters, array of `dataSize` or `Memcmp` (bytes, base58, base64 are supported)

If all fields are empty then all accounts are broadcasted. Otherwise fields works as logical `AND` and values in arrays as logical `OR` (except values in `filters` which works as logical `AND`).

#### Transactions

   - `vote` — enable/disable broadcast `vote` transactions
   - `failed` — enable/disable broadcast `failed` transactions
   - `signature` — match only specified transaction
   - `account_include` — filter transactions which use any account
   - `account_exclude` — filter transactions which do not use any account
   - `account_required` — require all accounts used in transaction

If all fields are empty then all transactions are broadcasted. Otherwise fields works as logical `AND` and values in arrays as logical `OR`.

#### Blocks

   - `account_include` — filter transactions and accounts which use any of listed accounts
   - `include_transactions` — include all transactions
   - `include_accounts` — include all accounts updates

Currently all blocks are broadcasted.

#### Blocks meta

Same as `Blocks` but without `transactions`.

### Limit filters

It's possible to add limits for filters in config. If `filters` field is omitted then filters doesn't have any limits.

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
         "account_exclude_max": 10
      },
      "blocks": {
         "max": 1
      }
   }
}
```

### Examples

   - [Go](examples/golang)
   - [Node.js](examples/nodejs)
   - [Rust](examples/rust)
   - [TypeScript](examples/typescript)

### License

This project and all source code in this repository is licensed as follows:

   Copyright 2023 Triton One Limited
   
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
