#!/usr/bin/env bash

# Prints the Solana version.

set -e

grep solana-program Cargo.lock | head -n1 | awk '{print $2}' | tr -d v
