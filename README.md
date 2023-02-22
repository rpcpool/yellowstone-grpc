# Solana GRPC interface

This repo contains a fully functional gRPC interface for Solana. It provides the ability to get slots, blocks, transactions, and account update notifications over a standardised path. 

For additional documentation,  please see: https://docs.triton.one/rpc-pool/grpc-subscriptions

It is built around a Geyser plugin for the Solana interface.

### Validator

```bash
$ solana-validator --geyser-plugin-config ./config.json
```

### Plugin config check

```
cargo-fmt && cargo run --bin config-check -- --config config.json
```

### Filters

See [proto/geyser.proto](proto/geyser.proto).

#### Slots

Currently all slots are broadcasted.

#### Account

Accounts can be filtered by:

   - `account` — acount Pubkey, match to any Pubkey from the array
   - `owner` — account owner Pubkey, match to any Pubkey from the array

If all fields are empty then all accounts are broadcasted. Otherwise fields works as logical `AND` and values in arrays as logical `OR`.

#### Transactions

   - `vote` — enable/disable broadcast `vote` transactions
   - `failed` — enable/disable broadcast `failed` transactions
   - `account_include` — filter transactions which use any account
   - `account_exclude` — filter transactions which do not use any account

If all fields are empty then all transactions are broadcasted. Otherwise fields works as logical `AND` and values in arrays as logical `OR`.

#### Blocks

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
