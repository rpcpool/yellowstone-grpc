#!/usr/bin/env bash

# Prints the Solana version.

set -e

cd "$(dirname "$0")/.."

grep solana-geyser-plugin-interface crates/plugin/Cargo.toml | cut -d ' ' -f3 | sed  's/\"/ /g;s/=//;s/ //'
