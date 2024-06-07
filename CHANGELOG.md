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
