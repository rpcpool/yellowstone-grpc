# Public GRPC interface to Geyser

### Validator

```bash
$ solana-validator --geyser-plugin-config ./config.json
```

### Client

- Always broadcast new slots
- Accounts can be filtered by `pubkey` and `owner` fields, also all accounts can be broadcasted with `*`

```
$ cargo run --bin client -- --accounts --account SysvarC1ock11111111111111111111111111111111
    Finished dev [unoptimized + debuginfo] target(s) in 0.69s
     Running `target/debug/client --accounts --account SysvarC1ock11111111111111111111111111111111`
stream opened
new message: SubscribeUpdate { filters: ["client"], update_oneof: Some(Account(SubscribeUpdateAccount { account: Some(SubscribeUpdateAccountInfo { pubkey: [6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0], lamports: 1169280, owner: [6, 167, 213, 23, 24, 117, 247, 41, 199, 61, 147, 64, 143, 33, 97, 32, 6, 126, 216, 140, 118, 224, 140, 40, 127, 193, 148, 96, 0, 0, 0, 0], executable: false, rent_epoch: 0, data: [57, 29, 0, 0, 0, 0, 0, 0, 165, 160, 80, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 221, 189, 80, 99, 0, 0, 0, 0], write_version: 53718 }), slot: 7481, is_startup: false })) }
new message: SubscribeUpdate { filters: ["client"], update_oneof: Some(Account(SubscribeUpdateAccount { account: Some(SubscribeUpdateAccountInfo { pubkey: [6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0], lamports: 1169280, owner: [6, 167, 213, 23, 24, 117, 247, 41, 199, 61, 147, 64, 143, 33, 97, 32, 6, 126, 216, 140, 118, 224, 140, 40, 127, 193, 148, 96, 0, 0, 0, 0], executable: false, rent_epoch: 0, data: [58, 29, 0, 0, 0, 0, 0, 0, 165, 160, 80, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 222, 189, 80, 99, 0, 0, 0, 0], write_version: 53725 }), slot: 7482, is_startup: false })) }
^C
```

### Filters

See [proto/geyser.proto](proto/geyser.proto).

#### Slots

   - `enabled` — broadcast slots updates

#### Account

Accounts can be filtered by:

   - `any` — stream all accounts, if active all other filters should be disabled
   - `account` — acount Pubkey, match to any Pubkey from the array
   - `owner` — account owner Pubkey, match to any Pubkey from the array

All fields in filter are optional but at least 1 is required. Fields works as logical `AND`. Values in the arrays works as logical `OR`.

#### Transactions

   - `any` — stream all transactions
   - `vote` — enable/disable broadcast `vote` transactions
   - `failed` — enable/disable broadcast `failed` transactions
