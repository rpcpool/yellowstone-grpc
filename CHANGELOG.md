# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Features

### Fixes

### Breaking

## 2023-07-20

- @triton-one/yellowstone-grpc:0.2.0
- yellowstone-grpc-client-1.8.0+solana.1.16.1
- yellowstone-grpc-geyser-1.5.0+solana.1.16.1
- yellowstone-grpc-proto-1.8.0+solana.1.16.1

### Features

- geyser: add `Entry` message ([#163](https://github.com/rpcpool/yellowstone-grpc/pull/163)).

## 2023-07-18

- yellowstone-grpc-geyser-1.4.0+solana.1.16.1

### Features

- geyser: reduce the amount of locks ([#161](https://github.com/rpcpool/yellowstone-grpc/pull/161)).

## 2023-07-17

- @triton-one/yellowstone-grpc:0.1.5
- yellowstone-grpc-client-1.7.0+solana.1.16.1
- yellowstone-grpc-geyser-1.3.0+solana.1.16.1
- yellowstone-grpc-proto-1.7.0+solana.1.16.1

### Features

- geyser: add `accounts` to Block message ([#160](https://github.com/rpcpool/yellowstone-grpc/pull/160)).

## 2023-07-07

- @triton-one/yellowstone-grpc:0.1.4
- yellowstone-grpc-client-1.6.0+solana.1.16.1
- yellowstone-grpc-geyser-1.2.0+solana.1.16.1
- yellowstone-grpc-proto-1.6.0+solana.1.16.1

### Features

- geyser: add `account_include` to Blocks filter ([#155](https://github.com/rpcpool/yellowstone-grpc/pull/155)).

## 2023-06-29

- @triton-one/yellowstone-grpc:0.1.3
- yellowstone-grpc-client-1.5.0+solana.1.16.1
- yellowstone-grpc-geyser-1.1.0+solana.1.16.1
- yellowstone-grpc-proto-1.5.0+solana.1.16.1

### Features

- geyser: support TokenAccountState in accounts filter ([#154](https://github.com/rpcpool/yellowstone-grpc/pull/154)).

## 2023-06-29

- @triton-one/yellowstone-grpc:0.1.2
- yellowstone-grpc-client-1.4.0+solana.1.16.1
- yellowstone-grpc-geyser-1.0.0+solana.1.16.1
- yellowstone-grpc-proto-1.4.0+solana.1.16.1

### Features

- geyser: support data_slice for accounts ([#150](https://github.com/rpcpool/yellowstone-grpc/pull/150)).
- client: add TypeScript client ([#142](https://github.com/rpcpool/yellowstone-grpc/pull/142)).

### Fixes

- client: set max message size for decode ([#151](https://github.com/rpcpool/yellowstone-grpc/pull/151)).
- geyser: remove duplicated account updates for confirmed/finalized ([#152](https://github.com/rpcpool/yellowstone-grpc/pull/152)).

## 2023-06-16

- yellowstone-grpc-client-1.3.0+solana.1.16.1
- yellowstone-grpc-geyser-0.8.2+solana.1.16.1
- yellowstone-grpc-proto-1.3.0+solana.1.16.1

### Features

- geyser: update solana =1.16.1 ([#146](https://github.com/rpcpool/yellowstone-grpc/pull/146)).

## 2023-06-15

- yellowstone-grpc-client-1.3.0+solana.1.14.18
- yellowstone-grpc-client-1.3.0+solana.1.15.2
- yellowstone-grpc-geyser-0.8.2+solana.1.14.18
- yellowstone-grpc-geyser-0.8.2+solana.1.15.2
- yellowstone-grpc-proto-1.3.0+solana.1.14.18
- yellowstone-grpc-proto-1.3.0+solana.1.15.2

### Features

- geyser: Update `tonic`, `0.8.2` => `0.9.2` ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145)).
- geyser: Add methods `health_check` and `health_watch` ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145)).
- geyser: Add prometheus metric `message_queue_size` ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145)).
- geyser: Send task per connection ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145)).
- geyser: Send processed immediately without `Slot` message ([#145](https://github.com/rpcpool/yellowstone-grpc/pull/145)).
