# Public GRPC interface to Geyser

### Validator

```bash
$ solana-validator --geyser-plugin-config ./config.json
```

### Client

- Always broadcast new slots
- Accounts can be filtered by `pubkey` and `owner` fields, also all accounts can be broadcasted with `*`

```
 cargo run --bin client -- --accounts SysvarC1ock11111111111111111111111111111111
    Finished dev [unoptimized + debuginfo] target(s) in 0.21s
     Running `target/debug/client --accounts SysvarC1ock11111111111111111111111111111111`
stream opened
new message: Ok(SubscribeUpdate { update_oneof: Some(Slot(SubscribeUpdateSlot { slot: 3159, parent: Some(3158), status: Processed })) })
new message: Ok(SubscribeUpdate { update_oneof: Some(Slot(SubscribeUpdateSlot { slot: 3128, parent: Some(3127), status: Rooted })) })
new message: Ok(SubscribeUpdate { update_oneof: Some(Account(SubscribeUpdateAccount { account: Some(SubscribeUpdateAccountInfo { pubkey: [6, 167, 213, 23, 24, 199, 116, 201, 40, 86, 99, 152, 105, 29, 94, 182, 139, 94, 184, 163, 155, 75, 109, 92, 115, 85, 91, 33, 0, 0, 0, 0], lamports: 1169280, owner: [6, 167, 213, 23, 24, 117, 247, 41, 199, 61, 147, 64, 143, 33, 97, 32, 6, 126, 216, 140, 118, 224, 140, 40, 127, 193, 148, 96, 0, 0, 0, 0], executable: false, rent_epoch: 0, data: [88, 12, 0, 0, 0, 0, 0, 0, 205, 209, 77, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 22, 215, 77, 99, 0, 0, 0, 0], write_version: 22722 }), slot: 3160, is_startup: false })) })
new message: Ok(SubscribeUpdate { update_oneof: Some(Slot(SubscribeUpdateSlot { slot: 3159, parent: None, status: Confirmed })) })
^C
```
