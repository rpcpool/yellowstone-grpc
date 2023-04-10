#!/usr/bin/env bash

# Prints the Solana version.

set -e
cargo metadata --format-version 1 | jq -r '.packages[] | select(.name=="solana-geyser-plugin-interface") | .version'
