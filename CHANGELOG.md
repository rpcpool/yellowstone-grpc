# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Fixes

### Features

### Breaking

## 2025-05-01

- @triton-one/yellowstone-grpc@4.1.0
- yellowstone-grpc-client-simple-6.1.0
- yellowstone-grpc-client-6.1.0
- yellowstone-grpc-geyser-6.1.0
- yellowstone-grpc-proto-6.1.0

### Features

- proto: add unary method `SubscribeReplayInfo` ([#578](https://github.com/rpcpool/yellowstone-grpc/pull/578))

## 2025-03-17

- yellowstone-grpc-client-simple-6.0.1

### Fixes

- client: fix `SlotStatus` ([#557](https://github.com/rpcpool/yellowstone-grpc/pull/557))

## 2025-03-10

- @triton-one/yellowstone-grpc@4.0.0
- yellowstone-grpc-client-simple-6.0.0
- yellowstone-grpc-client-6.0.0
- yellowstone-grpc-geyser-6.0.0
- yellowstone-grpc-proto-6.0.0

### Breaking

- geyser: upgrade to v2.2 ([#554](https://github.com/rpcpool/yellowstone-grpc/pull/554))

## 2025-02-28

- yellowstone-grpc-geyser-5.0.1

### Fixes

- geyser: deserialize config param `replay_stored_slots` from the `String` ([#541](https://github.com/rpcpool/yellowstone-grpc/pull/541))

## 2025-02-06

- @triton-one/yellowstone-grpc@3.0.0
- yellowstone-grpc-client-simple-5.0.0
- yellowstone-grpc-client-5.0.0
- yellowstone-grpc-geyser-5.0.0
- yellowstone-grpc-proto-5.0.0

### Fixes

- geyser: fix filter with followed ping ([#532](https://github.com/rpcpool/yellowstone-grpc/pull/532))

### Breaking

- proto: add enum `SlotStatus` ([#529](https://github.com/rpcpool/yellowstone-grpc/pull/529))

## 2025-02-05

- yellowstone-grpc-client-simple-4.4.1
- yellowstone-grpc-client-4.2.1
- yellowstone-grpc-geyser-4.3.1
- yellowstone-grpc-proto-4.2.1

### Fixes

- geyser: runtime error on invalid commitment ([#528](https://github.com/rpcpool/yellowstone-grpc/pull/528))

## 2025-02-05

- @triton-one/yellowstone-grpc@2.1.0
- yellowstone-grpc-client-simple-4.4.0
- yellowstone-grpc-client-4.2.0
- yellowstone-grpc-geyser-4.3.0
- yellowstone-grpc-proto-4.2.0

### Features

- proto: add field `interslot_updates` ([#527](https://github.com/rpcpool/yellowstone-grpc/pull/527))

## 2025-01-13

- yellowstone-grpc-client-4.1.1

### Fixes

- client: re-export `ClientTlsConfig` ([#512](https://github.com/rpcpool/yellowstone-grpc/pull/512))

## 2025-01-07

- @triton-one/yellowstone-grpc@2.0.0

### Fixes

- nodejs: fix port for https ([#502](https://github.com/rpcpool/yellowstone-grpc/pull/502))

### Breaking

- nodejs: support ESM environment ([#509](https://github.com/rpcpool/yellowstone-grpc/pull/509))

## 2024-12-16

- yellowstone-grpc-client-simple-4.3.0
- yellowstone-grpc-geyser-4.2.2
- yellowstone-grpc-proto-4.1.1

### Fixes

- geyser: fix `lamports` filter ([#498](https://github.com/rpcpool/yellowstone-grpc/pull/498))

### Features

- example: add `ca_certificate` option ([#497](https://github.com/rpcpool/yellowstone-grpc/pull/497))

## 2024-12-15

- yellowstone-grpc-geyser-4.2.1

### Fixes

- geyser: fix `replay_stored_slots` ([#496](https://github.com/rpcpool/yellowstone-grpc/pull/496))

## 2024-12-13

- yellowstone-grpc-client-simple-4.2.0
- yellowstone-grpc-client-4.1.0
- yellowstone-grpc-geyser-4.2.0
- yellowstone-grpc-proto-4.1.0

### Fixes

- nodejs: fix connector for custom port ([#488](https://github.com/rpcpool/yellowstone-grpc/pull/488))
- nodejs: fix connector for host/hostname ([#491](https://github.com/rpcpool/yellowstone-grpc/pull/491))

### Features

- proto: add tonic feature ([#474](https://github.com/rpcpool/yellowstone-grpc/pull/474))
- proto: add `from_slot` ([#477](https://github.com/rpcpool/yellowstone-grpc/pull/477))
- proto: add field `created_at` to update message ([#479](https://github.com/rpcpool/yellowstone-grpc/pull/479))
- nodejs: add parse err function ([#483](https://github.com/rpcpool/yellowstone-grpc/pull/483))
- geyser: add gRPC server options to config ([#493](https://github.com/rpcpool/yellowstone-grpc/pull/493))

## 2024-12-01

- yellowstone-grpc-client-simple-4.1.0
- yellowstone-grpc-geyser-4.1.0

### Features

- nodejs: add parse tx function ([#471](https://github.com/rpcpool/yellowstone-grpc/pull/471))
- geyser: use default compression as gzip and zstd ([#475](https://github.com/rpcpool/yellowstone-grpc/pull/475))
- example: add connection options to Rust client ([#478](https://github.com/rpcpool/yellowstone-grpc/pull/478))
- geyser: add worker_threads and affinity ([#481](https://github.com/rpcpool/yellowstone-grpc/pull/481))

## 2024-11-28

- yellowstone-grpc-geyser-4.0.1

### Fixes

- geyser: raise default filter name length limit ([#473](https://github.com/rpcpool/yellowstone-grpc/pull/473))

## 2024-11-21

- yellowstone-grpc-client-simple-4.0.0
- yellowstone-grpc-client-4.0.0
- yellowstone-grpc-geyser-4.0.0
- yellowstone-grpc-proto-4.0.0

### Features

- solana: upgrade to v2.1.1 ([#468](https://github.com/rpcpool/yellowstone-grpc/pull/468))

## 2024-11-20

- yellowstone-grpc-client-simple-3.0.0
- yellowstone-grpc-client-3.0.0
- yellowstone-grpc-geyser-3.0.0
- yellowstone-grpc-proto-3.0.0

### Fixes

- examples: fix commitment in TypeScript example ([#440](https://github.com/rpcpool/yellowstone-grpc/pull/440))
- geyser: fix missed status messages ([#444](https://github.com/rpcpool/yellowstone-grpc/pull/444))

### Features

- proto: use `gzip`/`zstd` features by default ([#436](https://github.com/rpcpool/yellowstone-grpc/pull/436))
- geyser: optimize consuming of new filters ([#439](https://github.com/rpcpool/yellowstone-grpc/pull/439))
- proto: add filter by lamports ([#369](https://github.com/rpcpool/yellowstone-grpc/pull/369))
- geyser: use Arc wrapped messages in block message ([#446](https://github.com/rpcpool/yellowstone-grpc/pull/446))
- node: remove generated grpc files ([#447](https://github.com/rpcpool/yellowstone-grpc/pull/447))
- proto: add txn_signature filter ([#445](https://github.com/rpcpool/yellowstone-grpc/pull/445))
- examples: add progress bar to client tool ([#456](https://github.com/rpcpool/yellowstone-grpc/pull/456))
- proto: add mod `plugin` with `FilterNames` cache ([#458](https://github.com/rpcpool/yellowstone-grpc/pull/458))
- proto: move enum Message from geyser crate ([#459](https://github.com/rpcpool/yellowstone-grpc/pull/459))
- proto: move `Filter` from geyser crate ([#466](https://github.com/rpcpool/yellowstone-grpc/pull/466))
- geyser: serialize from custom message istead of generated ([#467](https://github.com/rpcpool/yellowstone-grpc/pull/467))
- proto: implement encoding instead of clone to generated ([#465](https://github.com/rpcpool/yellowstone-grpc/pull/465))

### Breaking

- geyser: limit length of filter name ([#448](https://github.com/rpcpool/yellowstone-grpc/pull/448))
- proto: change error type in mod `convert_from` ([#457](https://github.com/rpcpool/yellowstone-grpc/pull/457))
- geyser: remove option `block_fail_action` ([#463](https://github.com/rpcpool/yellowstone-grpc/pull/463))

## 2024-10-04

- yellowstone-grpc-client-simple-2.0.0
- yellowstone-grpc-client-2.0.0
- yellowstone-grpc-geyser-2.0.0
- yellowstone-grpc-proto-2.0.0

### Features

- solana: relax dependencies ([#430](https://github.com/rpcpool/yellowstone-grpc/pull/430))
- tools: remove ([#431](https://github.com/rpcpool/yellowstone-grpc/pull/431))

## 2024-09-12

- yellowstone-grpc-client-1.16.2+solana.2.0.10
- yellowstone-grpc-geyser-1.16.3+solana.2.0.10
- yellowstone-grpc-proto-1.15.0+solana.2.0.10
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.10

### Features

- solana: update to 2.0.10 ([#427](https://github.com/rpcpool/yellowstone-grpc/pull/427))

## 2024-09-12

- yellowstone-grpc-client-1.16.2+solana.2.0.9
- yellowstone-grpc-geyser-1.16.3+solana.2.0.9
- yellowstone-grpc-proto-1.15.0+solana.2.0.9
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.9

### Features

- solana: update to 2.0.9 ([#425](https://github.com/rpcpool/yellowstone-grpc/pull/425))

## 2024-09-03

- yellowstone-grpc-client-1.16.2+solana.2.0.8
- yellowstone-grpc-geyser-1.16.3+solana.2.0.8
- yellowstone-grpc-proto-1.15.0+solana.2.0.8
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.8

### Features

- solana: update to 2.0.8 ([#419](https://github.com/rpcpool/yellowstone-grpc/pull/419))

## 2024-09-02

- yellowstone-grpc-geyser-1.16.3+solana.2.0.7

### Features

- geyser: wrap message into `Box` in snapshot channel ([#418](https://github.com/rpcpool/yellowstone-grpc/pull/418))

## 2024-08-26

- yellowstone-grpc-client-1.16.2+solana.2.0.7
- yellowstone-grpc-geyser-1.16.2+solana.2.0.7
- yellowstone-grpc-proto-1.15.0+solana.2.0.7
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.7

### Features

- solana: update to 2.0.7 ([#415](https://github.com/rpcpool/yellowstone-grpc/pull/415))

## 2024-08-23

- yellowstone-grpc-client-1.16.2+solana.2.0.5
- yellowstone-grpc-geyser-1.16.2+solana.2.0.5
- yellowstone-grpc-proto-1.15.0+solana.2.0.5
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.5

### Fixes

- geyser: fix `x-request-snapshot` handler ([#413](https://github.com/rpcpool/yellowstone-grpc/pull/413))

## 2024-08-22

- yellowstone-grpc-client-1.16.1+solana.2.0.5
- yellowstone-grpc-geyser-1.16.1+solana.2.0.5
- yellowstone-grpc-proto-1.15.0+solana.2.0.5
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.5

### Fixes

- example: fix tls root issue in rust example ([#404](https://github.com/rpcpool/yellowstone-grpc/pull/404))
- geyser: fix filter update loop on snapshot ([#410](https://github.com/rpcpool/yellowstone-grpc/pull/410))

### Features

- geyser: handle `x-request-snapshot` on client request ([#411](https://github.com/rpcpool/yellowstone-grpc/pull/411))

## 2024-08-09

- yellowstone-grpc-client-1.16.0+solana.2.0.5
- yellowstone-grpc-geyser-1.16.0+solana.2.0.5
- yellowstone-grpc-proto-1.15.0+solana.2.0.5
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.5

### Features

- solana: update to 2.0.5 ([#395](https://github.com/rpcpool/yellowstone-grpc/pull/395))

## 2024-08-07

- yellowstone-grpc-client-1.16.0+solana.2.0.4
- yellowstone-grpc-geyser-1.16.0+solana.2.0.4
- yellowstone-grpc-proto-1.15.0+solana.2.0.4
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.4

### Features

- solana: update to 2.0.4 ([#390](https://github.com/rpcpool/yellowstone-grpc/pull/390))
- geyser: add `zstd` support ([#391](https://github.com/rpcpool/yellowstone-grpc/pull/391))
- deps: update `hyper` to 1 ([#392](https://github.com/rpcpool/yellowstone-grpc/pull/392))

## 2024-07-12

- yellowstone-grpc-client-1.16.0+solana.2.0.2
- yellowstone-grpc-geyser-1.16.0+solana.2.0.2
- yellowstone-grpc-proto-1.15.0+solana.2.0.2
- yellowstone-grpc-tools-1.0.0-rc.12+solana.2.0.2

### Features

- solana: update to 2.0.2 ([#377](https://github.com/rpcpool/yellowstone-grpc/pull/377))

## 2024-07-12

- yellowstone-grpc-client-1.15.0+solana.1.18.18
- yellowstone-grpc-geyser-1.15.0+solana.1.18.18
- yellowstone-grpc-proto-1.14.0+solana.1.18.18
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.18

### Features

- solana: update to 1.18.18 ([#374](https://github.com/rpcpool/yellowstone-grpc/pull/374))

## 2024-06-26

- yellowstone-grpc-client-1.15.0+solana.1.18.17
- yellowstone-grpc-geyser-1.15.0+solana.1.18.17
- yellowstone-grpc-proto-1.14.0+solana.1.18.17
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.17

### Features

- solana: update to 1.18.17 ([#367](https://github.com/rpcpool/yellowstone-grpc/pull/367))

## 2024-06-12

- yellowstone-grpc-client-1.15.0+solana.1.18.16
- yellowstone-grpc-geyser-1.15.0+solana.1.18.16
- yellowstone-grpc-proto-1.14.0+solana.1.18.16
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.16

### Features

- solana: update to 1.18.16 ([#361](https://github.com/rpcpool/yellowstone-grpc/pull/361))

## 2024-06-07

- yellowstone-grpc-geyser-1.15.0+solana.1.18.15

### Features

- geyser: add compression option to config ([#356](https://github.com/rpcpool/yellowstone-grpc/pull/356))
- geyser: add `x-endpoint` to metric `subscriptions_total` ([#358](https://github.com/rpcpool/yellowstone-grpc/pull/358))
- geyser: check `x-token` for health service too ([#359](https://github.com/rpcpool/yellowstone-grpc/pull/359))

## 2024-06-05

- yellowstone-grpc-geyser-1.14.4+solana.1.18.15

### Features

- geyser: add metric `subscriptions_total` ([#355](https://github.com/rpcpool/yellowstone-grpc/pull/355))

## 2024-06-02

- yellowstone-grpc-client-1.15.0+solana.1.18.15
- yellowstone-grpc-geyser-1.14.3+solana.1.18.15
- yellowstone-grpc-proto-1.14.0+solana.1.18.15
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.15

### Fixes

- geyser: fix getLatestBlockhash unary method ([#349](https://github.com/rpcpool/yellowstone-grpc/pull/349))

### Features

- geyser: add optional x_token check in grpc server ([#345](https://github.com/rpcpool/yellowstone-grpc/pull/345))
- solana: update to 1.18.15 ([#354](https://github.com/rpcpool/yellowstone-grpc/pull/354))

## 2024-05-21

- yellowstone-grpc-client-1.15.0+solana.1.18.14
- yellowstone-grpc-geyser-1.14.2+solana.1.18.14
- yellowstone-grpc-proto-1.14.0+solana.1.18.14
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.14

### Features

- solana: update to 1.18.14 ([#343](https://github.com/rpcpool/yellowstone-grpc/pull/343))

## 2024-05-15

- yellowstone-grpc-client-1.15.0+solana.1.18.13
- yellowstone-grpc-geyser-1.14.2+solana.1.18.13
- yellowstone-grpc-proto-1.14.0+solana.1.18.13
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.13

### Features

- geyser: use runtime instaed of unconstrained ([#332](https://github.com/rpcpool/yellowstone-grpc/pull/332))
- solana: update to 1.18.13 ([#339](https://github.com/rpcpool/yellowstone-grpc/pull/339))

## 2024-04-30

- yellowstone-grpc-client-1.15.0+solana.1.18.12
- yellowstone-grpc-geyser-1.14.1+solana.1.18.12
- yellowstone-grpc-proto-1.14.0+solana.1.18.12
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.12

### Features

- solana: update to 1.18.12 ([#330](https://github.com/rpcpool/yellowstone-grpc/pull/330))

## 2024-04-12

- yellowstone-grpc-client-1.15.0+solana.1.18.11
- yellowstone-grpc-geyser-1.14.1+solana.1.18.11
- yellowstone-grpc-proto-1.14.0+solana.1.18.11
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.11

### Features

- solana: update to 1.18.11 ([#321](https://github.com/rpcpool/yellowstone-grpc/pull/321))

## 2024-04-04

- yellowstone-grpc-geyser-1.14.1+solana.1.18.9

### Features

- geyser: allow to skip fields in config for `grpc.filters` ([#319](https://github.com/rpcpool/yellowstone-grpc/pull/319))

## 2024-04-04

- yellowstone-grpc-client-1.15.0+solana.1.18.9
- yellowstone-grpc-geyser-1.14.0+solana.1.18.9
- yellowstone-grpc-proto-1.14.0+solana.1.18.9
- yellowstone-grpc-tools-1.0.0-rc.11+solana.1.18.9

### Fixes

- deps: update `h2` crate (`RUSTSEC-2024-0332`) ([#316](https://github.com/rpcpool/yellowstone-grpc/pull/316))

### Features

- client: add gRPC channel options to Node.js ([#306](https://github.com/rpcpool/yellowstone-grpc/pull/306))
- geyser: add `transactions_status` filter ([#310](https://github.com/rpcpool/yellowstone-grpc/pull/310))
- geyser: add metric `slot_status_plugin` ([#312](https://github.com/rpcpool/yellowstone-grpc/pull/312))
- geyser: wrap `geyser_loop` with `unconstrained` ([#313](https://github.com/rpcpool/yellowstone-grpc/pull/313))
- geyser: handle `/debug_clients` on prometheus endpoint ([#314](https://github.com/rpcpool/yellowstone-grpc/pull/314))
- geyser: wrap messages to `Arc` ([#315](https://github.com/rpcpool/yellowstone-grpc/pull/315))

### Breaking

- client: add `GeyserGrpcBuilder` ([#309](https://github.com/rpcpool/yellowstone-grpc/pull/309))

## 2024-03-20

- yellowstone-grpc-client-1.14.0+solana.1.18.7
- yellowstone-grpc-geyser-1.13.0+solana.1.18.7
- yellowstone-grpc-proto-1.13.0+solana.1.18.7
- yellowstone-grpc-tools-1.0.0-rc.10+solana.1.18.7

### Features

- solana: update to 1.18.7 ([#302](https://github.com/rpcpool/yellowstone-grpc/pull/302))

## 2024-03-04

- yellowstone-grpc-client-1.14.0+solana.1.18.4
- yellowstone-grpc-geyser-1.13.0+solana.1.18.4
- yellowstone-grpc-proto-1.13.0+solana.1.18.4
- yellowstone-grpc-tools-1.0.0-rc.10+solana.1.18.4

### Features

- solana: update to 1.18.4 ([#293](https://github.com/rpcpool/yellowstone-grpc/pull/293))

## 2024-03-04

- yellowstone-grpc-client-1.14.0+solana.1.17.22
- yellowstone-grpc-geyser-1.13.0+solana.1.17.22
- yellowstone-grpc-proto-1.13.0+solana.1.17.22
- yellowstone-grpc-tools-1.0.0-rc.10+solana.1.17.22

### Fixes

- deps: make cargo-deny happy about openssl, unsafe-libyaml, h2, ahash ([#278](https://github.com/rpcpool/yellowstone-grpc/pull/278))
- geyser: allow to set custom filter size in the config ([#288](https://github.com/rpcpool/yellowstone-grpc/pull/288))

### Features

- proto: add `entries_count` to block meta message ([#283](https://github.com/rpcpool/yellowstone-grpc/pull/283))
- geyser: use `Vec::binary_search` instead of `HashSet::contains` in the filters ([#284](https://github.com/rpcpool/yellowstone-grpc/pull/284))
- proto: add `starting_transaction_index` to entry message ([#289](https://github.com/rpcpool/yellowstone-grpc/pull/289))
- geyser: add `hostname` to version response ([#291](https://github.com/rpcpool/yellowstone-grpc/pull/291))
- solana: update to 1.17.22 ([#292](https://github.com/rpcpool/yellowstone-grpc/pull/292))

### Breaking

- tools: add metrics, new config for google-pubsub ([#280](https://github.com/rpcpool/yellowstone-grpc/pull/280))

## 2024-02-27

- yellowstone-grpc-client-1.13.0+solana.1.18.3
- yellowstone-grpc-geyser-1.12.0+solana.1.18.3
- yellowstone-grpc-proto-1.12.0+solana.1.18.3
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.18.3

### Features

- solana: update to 1.18.3

## 2024-02-06

- yellowstone-grpc-client-1.13.0+solana.1.17.20
- yellowstone-grpc-geyser-1.12.0+solana.1.17.20
- yellowstone-grpc-proto-1.12.0+solana.1.17.20
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.20

### Features

- solana: update to 1.17.20

## 2024-01-26

- yellowstone-grpc-client-1.13.0+solana.1.17.18
- yellowstone-grpc-geyser-1.12.0+solana.1.17.18
- yellowstone-grpc-proto-1.12.0+solana.1.17.18
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.18

### Features

- solana: update to 1.17.18

## 2024-01-26

- yellowstone-grpc-client-1.13.0+solana.1.17.17
- yellowstone-grpc-geyser-1.12.0+solana.1.17.17
- yellowstone-grpc-proto-1.12.0+solana.1.17.17
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.17

### Features

- solana: update to 1.17.17

## 2024-01-15

- yellowstone-grpc-client-1.13.0+solana.1.17.16
- yellowstone-grpc-geyser-1.12.0+solana.1.17.16
- yellowstone-grpc-proto-1.12.0+solana.1.17.16
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.16

### Features

- solana: update to 1.17.16 ([#274](https://github.com/rpcpool/yellowstone-grpc/pull/274))

## 2024-01-08

- yellowstone-grpc-client-1.13.0+solana.1.17.15
- yellowstone-grpc-geyser-1.12.0+solana.1.17.15
- yellowstone-grpc-proto-1.12.0+solana.1.17.15
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.15

### Features

- proto: add more convert functions ([#264](https://github.com/rpcpool/yellowstone-grpc/pull/264))
- geyser: set plugin name to `{name}-{version}` ([#270](https://github.com/rpcpool/yellowstone-grpc/pull/270))

## 2023-12-22

- yellowstone-grpc-client-1.12.0+solana.1.17.12
- yellowstone-grpc-geyser-1.11.2+solana.1.17.12
- yellowstone-grpc-proto-1.11.1+solana.1.17.12
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.12

### Features

- geyser: add name to tokio threads ([#267](https://github.com/rpcpool/yellowstone-grpc/pull/267))

## 2023-12-19

- yellowstone-grpc-client-1.12.0+solana.1.17.12
- yellowstone-grpc-geyser-1.11.1+solana.1.17.12
- yellowstone-grpc-proto-1.11.1+solana.1.17.12
- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.12

### Features

- solana: update to 1.17.12 ([#266](https://github.com/rpcpool/yellowstone-grpc/pull/266))

## 2023-12-08

- yellowstone-grpc-tools-1.0.0-rc.9+solana.1.17.6

### Fixes

- tools: fix panic on Ping/Pong messages in google pubsub ([#261](https://github.com/rpcpool/yellowstone-grpc/pull/261))

## 2023-12-06

- yellowstone-grpc-tools-1.0.0-rc.8+solana.1.17.6

### Fixes

- client: include request in initial subscribe to gRPC endpoint to fix LB connection delay ([#252](https://github.com/rpcpool/yellowstone-grpc/pull/252))
- tools: remove `ordering_key` from `PubsubMessage` ([#257](https://github.com/rpcpool/yellowstone-grpc/pull/257))

## 2023-11-24

- yellowstone-grpc-geyser-1.11.1+solana.1.17.6

### Fixes

- geyser: reconstruct blocks with zero entries ([#245](https://github.com/rpcpool/yellowstone-grpc/pull/245))

## 2023-11-21

- yellowstone-grpc-client-1.12.0+solana.1.17.6
- yellowstone-grpc-geyser-1.11.0+solana.1.17.6
- yellowstone-grpc-proto-1.11.0+solana.1.17.6
- yellowstone-grpc-tools-1.0.0-rc.7+solana.1.17.6

### Fixes

- tools: fixes openssl link problem (macos+aarch64) ([#236](https://github.com/rpcpool/yellowstone-grpc/pull/236))

### Features

- use workspace for dependencies ([#240](https://github.com/rpcpool/yellowstone-grpc/pull/240))
- solana: update to 1.17.6 ([#244](https://github.com/rpcpool/yellowstone-grpc/pull/244))

## 2023-11-14

- yellowstone-grpc-client-1.12.0+solana.1.17.5
- yellowstone-grpc-geyser-1.11.0+solana.1.17.5
- yellowstone-grpc-proto-1.11.0+solana.1.17.5
- yellowstone-grpc-tools-1.0.0-rc.6+solana.1.17.5

### Features

- solana: update to 1.17.5 ([#235](https://github.com/rpcpool/yellowstone-grpc/pull/235))

## 2023-11-13

- yellowstone-grpc-client-1.12.0+solana.1.17.4
- yellowstone-grpc-geyser-1.11.0+solana.1.17.4
- yellowstone-grpc-proto-1.11.0+solana.1.17.4
- yellowstone-grpc-tools-1.0.0-rc.6+solana.1.17.4

### Features

- solana: update to 1.17.4 ([#234](https://github.com/rpcpool/yellowstone-grpc/pull/234))

## 2023-11-01

- @triton-one/yellowstone-grpc:0.3.0
- yellowstone-grpc-client-1.12.0+solana.1.17.1
- yellowstone-grpc-geyser-1.11.0+solana.1.17.1
- yellowstone-grpc-proto-1.11.0+solana.1.17.1
- yellowstone-grpc-tools-1.0.0-rc.6+solana.1.17.1

### Fixes

- geyser: trigger end of startup when parent slot 0 seen in `update_slot_status` notification because `notify_end_of_startup` is not triggered when cluster started from genesis ([#207](https://github.com/rpcpool/yellowstone-grpc/pull/207))
- tools: correctly handle SIGINT in kafka ([#219](https://github.com/rpcpool/yellowstone-grpc/pull/219))
- geyser: use Ordering::Relaxed instead of SeqCst ([#221](https://github.com/rpcpool/yellowstone-grpc/pull/221))
- proto: add optional field `ping` to `SubscribeRequest` ([#227](https://github.com/rpcpool/yellowstone-grpc/pull/227))
- geyser: remove startup_status (allow reload plugin)  ([#230](https://github.com/rpcpool/yellowstone-grpc/pull/230))

### Features

- proto: add optional field `filter_by_commitment` to Slots filter ([#223](https://github.com/rpcpool/yellowstone-grpc/pull/223))

## 2023-10-19

- yellowstone-grpc-tools-1.0.0-rc.5+solana.1.17.1

### Features

- tools: add Google Pub/Sub ([#211](https://github.com/rpcpool/yellowstone-grpc/pull/211))

### Breaking

- kafka: rename to tools ([#203](https://github.com/rpcpool/yellowstone-grpc/pull/203))

## 2023-10-12

- yellowstone-grpc-geyser-1.10.0+solana.1.16.16

### Features

- geyser: support snapshot data ([#182](https://github.com/rpcpool/yellowstone-grpc/pull/182))

## 2023-10-10

- yellowstone-grpc-client-1.11.1+solana.1.16.16
- yellowstone-grpc-geyser-1.9.1+solana.1.16.16
- yellowstone-grpc-kafka-1.0.0-rc.3+solana.1.16.16
- yellowstone-grpc-proto-1.10.0+solana.1.16.16

### Fixes

- geyser: use `entry_count` from `ReplicaBlockInfoV3` ([#186](https://github.com/rpcpool/yellowstone-grpc/pull/186))

### Features

- client: add `GeyserGrpcClient::subscribe_once2` ([#195](https://github.com/rpcpool/yellowstone-grpc/pull/195))

## 2023-10-09

- yellowstone-grpc-kafka-1.0.0-rc.3+solana.1.16.15

### Features

- kafka: add metrics (stats, sent, recv) ([#196](https://github.com/rpcpool/yellowstone-grpc/pull/196))
- kafka: support YAML config ([#197](https://github.com/rpcpool/yellowstone-grpc/pull/197))
- kafka: support prometheus address in config ([#198](https://github.com/rpcpool/yellowstone-grpc/pull/198))

## 2023-10-06

- yellowstone-grpc-kafka-1.0.0-rc.2+solana.1.16.15

### Fixes

- kafka: fix message size for gRPC client ([#195](https://github.com/rpcpool/yellowstone-grpc/pull/195))

## 2023-10-05

- yellowstone-grpc-client-1.11.0+solana.1.16.15
- yellowstone-grpc-geyser-1.9.0+solana.1.16.15
- yellowstone-grpc-kafka-1.0.0-rc.1+solana.1.16.15
- yellowstone-grpc-proto-1.10.0+solana.1.16.15

### Features

- kafka: support strings for queue size ([#191](https://github.com/rpcpool/yellowstone-grpc/pull/191))
- solana: update to 1.16.15 ([#193](https://github.com/rpcpool/yellowstone-grpc/pull/193))

## 2023-10-03

- yellowstone-grpc-client-1.11.0+solana.1.16.14
- yellowstone-grpc-geyser-1.9.0+solana.1.16.14
- yellowstone-grpc-proto-1.10.0+solana.1.16.14

### Features

- proto: add mod `convert_to`, `convert_from` ([#190](https://github.com/rpcpool/yellowstone-grpc/pull/190))
- client: add tx pretty print to rust ([#189](https://github.com/rpcpool/yellowstone-grpc/pull/189))
- geyser: update deps, tokio=1.32.0 ([#191](https://github.com/rpcpool/yellowstone-grpc/pull/191))

## 2023-10-02

- yellowstone-grpc-client-1.10.0+solana.1.16.14
- yellowstone-grpc-geyser-1.8.0+solana.1.16.14
- yellowstone-grpc-kafka-1.0.0-rc.0+solana.1.16.14
- yellowstone-grpc-proto-1.9.0+solana.1.16.14

### Features

- geyser: add optional TLS to gRPC server config ([#183](https://github.com/rpcpool/yellowstone-grpc/pull/183))
- client: add timeout options to rust ([#187](https://github.com/rpcpool/yellowstone-grpc/pull/187))
- geyser: update solana =1.16.14 ([#188](https://github.com/rpcpool/yellowstone-grpc/pull/188))

### Fixes

- geyser: add `fs` feature to `tokio` dependencies in the plugin ([#184](https://github.com/rpcpool/yellowstone-grpc/pull/184))

## 2023-08-28

- yellowstone-grpc-kafka-1.0.0-rc.0+solana.1.16.1

### Features

- kafka: init ([#170](https://github.com/rpcpool/yellowstone-grpc/pull/170))

## 2023-08-21

- yellowstone-grpc-geyser-1.7.1+solana.1.16.1

### Features

- geyser: add package name to version info ([#173](https://github.com/rpcpool/yellowstone-grpc/pull/173))

### Fixes

- geyser: fix overflow for small slot number ([#171](https://github.com/rpcpool/yellowstone-grpc/pull/171))
- geyser: use Notify instead of AtomicBool in send loop ([#176](https://github.com/rpcpool/yellowstone-grpc/pull/176))
- geyser: update block reconstruction code ([#177](https://github.com/rpcpool/yellowstone-grpc/pull/177))

## 2023-08-10

- @triton-one/yellowstone-grpc:0.2.1
- yellowstone-grpc-client-1.9.0+solana.1.16.1
- yellowstone-grpc-geyser-1.7.0+solana.1.16.1
- yellowstone-grpc-proto-1.9.0+solana.1.16.1

### Features

- geyser: include entries to block message ([#169](https://github.com/rpcpool/yellowstone-grpc/pull/169))

## 2023-07-26

- yellowstone-grpc-geyser-1.6.1+solana.1.16.1

### Fixes

- geyser: fix config example ([#168](https://github.com/rpcpool/yellowstone-grpc/pull/168))

## 2023-07-22

- yellowstone-grpc-geyser-1.6.0+solana.1.16.1

### Features

- geyser: add panic config option on failed block reconstruction ([#165](https://github.com/rpcpool/yellowstone-grpc/pull/165))
- geyser: allow to disable unary methods ([#166](https://github.com/rpcpool/yellowstone-grpc/pull/166))

## 2023-07-20

- @triton-one/yellowstone-grpc:0.2.0
- yellowstone-grpc-client-1.8.0+solana.1.16.1
- yellowstone-grpc-geyser-1.5.0+solana.1.16.1
- yellowstone-grpc-proto-1.8.0+solana.1.16.1

### Features

- geyser: add `Entry` message ([#163](https://github.com/rpcpool/yellowstone-grpc/pull/163))

## 2023-07-18

- yellowstone-grpc-geyser-1.4.0+solana.1.16.1

### Features

- geyser: reduce the amount of locks ([#161](https://github.com/rpcpool/yellowstone-grpc/pull/161))

## 2023-07-17

- @triton-one/yellowstone-grpc:0.1.5
- yellowstone-grpc-client-1.7.0+solana.1.16.1
- yellowstone-grpc-geyser-1.3.0+solana.1.16.1
- yellowstone-grpc-proto-1.7.0+solana.1.16.1

### Features

- geyser: add `accounts` to Block message ([#160](https://github.com/rpcpool/yellowstone-grpc/pull/160))

## 2023-07-07

- @triton-one/yellowstone-grpc:0.1.4
- yellowstone-grpc-client-1.6.0+solana.1.16.1
- yellowstone-grpc-geyser-1.2.0+solana.1.16.1
- yellowstone-grpc-proto-1.6.0+solana.1.16.1

### Features

- geyser: add `account_include` to Blocks filter ([#155](https://github.com/rpcpool/yellowstone-grpc/pull/155))

## 2023-06-29

- @triton-one/yellowstone-grpc:0.1.3
- yellowstone-grpc-client-1.5.0+solana.1.16.1
- yellowstone-grpc-geyser-1.1.0+solana.1.16.1
- yellowstone-grpc-proto-1.5.0+solana.1.16.1

### Features

- geyser: support TokenAccountState in accounts filter ([#154](https://github.com/rpcpool/yellowstone-grpc/pull/154))

## 2023-06-29

- @triton-one/yellowstone-grpc:0.1.2
- yellowstone-grpc-client-1.4.0+solana.1.16.1
- yellowstone-grpc-geyser-1.0.0+solana.1.16.1
- yellowstone-grpc-proto-1.4.0+solana.1.16.1

### Features

- geyser: support data_slice for accounts ([#150](https://github.com/rpcpool/yellowstone-grpc/pull/150))
- client: add TypeScript client ([#142](https://github.com/rpcpool/yellowstone-grpc/pull/142))

### Fixes

- client: set max message size for decode ([#151](https://github.com/rpcpool/yellowstone-grpc/pull/151))
- geyser: remove duplicated account updates for confirmed/finalized ([#152](https://github.com/rpcpool/yellowstone-grpc/pull/152))

## 2023-06-16

- yellowstone-grpc-client-1.3.0+solana.1.16.1
- yellowstone-grpc-geyser-0.8.2+solana.1.16.1
- yellowstone-grpc-proto-1.3.0+solana.1.16.1

### Features

- geyser: update solana =1.16.1 ([#146](https://github.com/rpcpool/yellowstone-grpc/pull/146))

## 2023-06-15

- yellowstone-grpc-client-1.3.0+solana.1.14.18
- yellowstone-grpc-client-1.3.0+solana.1.15.2
- yellowstone-grpc-geyser-0.8.2+solana.1.14.18
- yellowstone-grpc-geyser-0.8.2+solana.1.15.2
- yellowstone-grpc-proto-1.3.0+solana.1.14.18
- yellowstone-grpc-proto-1.3.0+solana.1.15.2

### Features

- geyser: Update `tonic`, `0.8.2` => `0.9.2` ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145))
- geyser: Add methods `health_check` and `health_watch` ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145))
- geyser: Add prometheus metric `message_queue_size` ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145))
- geyser: Send task per connection ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145))
- geyser: Send processed immediately without `Slot` message ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145))
