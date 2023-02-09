#!/usr/bin/env bash

# Prints the Solana version.

set -e
grep solana-geyser-plugin-interface Cargo.toml | head -n1 | awk '{print $3}' | tr -d \" | tr -d =
