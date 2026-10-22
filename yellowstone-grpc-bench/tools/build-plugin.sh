#!/usr/bin/env bash
# Builds the plugin library and ygb in one cargo invocation, so both sides of dlopen share dependency builds.
# Usage: build-plugin.sh public|public-fork [extra cargo args]. Writes plugin-abi.txt next to the library.
set -euo pipefail

flavour=${1:?usage: build-plugin.sh public|public-fork [cargo args]}
shift
# The tree this script sits in, never a checkout above it.
root=$(cd "$(dirname "$0")/../.." && pwd -P)
[[ -f "$root/Cargo.toml" && -d "$root/yellowstone-grpc-geyser" ]] || { echo "$root is not a yellowstone-grpc tree" >&2; exit 1; }
cd "$root"
# Counted before cargo runs: the public-fork patch rewrites Cargo.lock for the build.
changed=$(git status --porcelain | wc -l | tr -d ' ')

args=(build --release -p yellowstone-grpc-geyser -p yellowstone-grpc-bench)
# TRACE=1 builds the plugin with latency-trace checkpoints into a separate target dir.
suffix=""
if [[ "${TRACE:-0}" == 1 ]]; then
  args+=(--features yellowstone-grpc-geyser/latency-trace)
  suffix="-trace"
fi
case "$flavour" in
  public)
    target_dir=target${suffix:+/bench$suffix}
    ;;
  public-fork)
    # The rpcpool tonic fork that production builds with, patched in for this build only.
    target_dir=target/bench-fork$suffix
    for crate in tonic tonic-build tonic-prost tonic-prost-build tonic-health; do
      args+=(--config "patch.crates-io.${crate}.git=\"https://github.com/rpcpool/grpc-rust\""
        --config "patch.crates-io.${crate}.tag=\"0.1.0+triton.grpcrust.0.14.6\"")
    done
    # The patch rewrites Cargo.lock; restore it so the tree stays clean.
    saved_lock=$(mktemp "${TMPDIR:-/tmp}/ygb-cargo-lock.XXXXXX")
    cp Cargo.lock "$saved_lock"
    trap 'cp "$saved_lock" Cargo.lock; rm -f "$saved_lock"' EXIT
    ;;
  *)
    echo "unknown flavour: $flavour" >&2
    exit 2
    ;;
esac

cargo "${args[@]}" --target-dir "$target_dir" "$@"

lock_version() {
  awk -v name="$1" '$0 == "name = \"" name "\"" { getline; gsub(/version = |"/, ""); print; exit }' Cargo.lock
}
{
  echo "flavour: $flavour"
  echo "commit: $(git rev-parse HEAD), $changed changed files"
  rustc -vV
  for crate in agave-geyser-plugin-interface solana-transaction solana-transaction-status solana-account solana-entry solana-message tokio tonic; do
    echo "$crate: $(lock_version "$crate")"
  done
} > "$target_dir/release/plugin-abi.txt"
echo "built the plugin library and ygb in $target_dir/release" >&2
# The last stdout line is the variant directory, for bench.sh.
echo "$root/$target_dir/release"
