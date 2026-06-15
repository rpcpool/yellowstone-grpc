#!/usr/bin/env bash

# Source:
# https://github.com/solana-labs/solana-accountsdb-plugin-postgres/blob/master/ci/cargo-build-test.sh

set -e
cd "$(dirname "$0")/.."

source ./ci/rust-version.sh stable

export RUSTBACKTRACE=1

set -x

# Build/test all host crates
RUSTFLAGS="-D warnings" cargo +"$rust_stable" build
cargo +"$rust_stable" test -- --nocapture

exit 0
