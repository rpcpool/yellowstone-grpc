#!/usr/bin/env bash
# Runs one replay per load-subscriber count with the same profile and layout, and reports each run.
# Usage: sweep.sh <name> "<counts>" [ygb run args...]. Env: YGB, FIXTURE, PLUGIN, PROFILE, RUNS.
set -euo pipefail

name=${1:?usage: sweep.sh <name> "<counts>" [ygb run args...]}
counts=${2:?missing counts}
shift 2
: "${YGB:?}" "${FIXTURE:?}" "${PLUGIN:?}" "${PROFILE:?}" "${RUNS:?}"
mkdir -p "$RUNS"

# Codespace layout: plugin on CCD1 (32 MB L3, like a 9575F CCD); everything else on CCD0.
layout=(
  --plugin-cpus 8-13,24-29 --driver-cpus 2-3,18-19 --decoder-cpus 4-5,20-21
  --probe-cpus 0-1,16-17 --load-cpus 6-7,22-23,14-15,30-31 --probe-threads 2 --load-threads 6
)

for count in $counts; do
  out="$RUNS/$name-n$count"
  if [[ -e "$out/driver.json" ]]; then
    echo "skip $out (exists)"
    continue
  fi
  echo "$(date -u +%FT%TZ) run $out"
  "$YGB" run --fixture "$FIXTURE" --plugin "$PLUGIN" --out "$out" --fleet-profile "$PROFILE" \
    --max-load "$count" "${layout[@]}" "$@" > "$out.log" 2>&1
  "$YGB" report --run "$out" > "$out.report.txt"
  grep -E '^streamlab +(none|zstd) +(account|transaction|entry) ' "$out.report.txt" || true
done
