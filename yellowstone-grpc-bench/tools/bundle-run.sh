#!/usr/bin/env bash
# Runs the latency harness from a bundle made by `bench.sh --bundle`: prebuilt binaries, scenarios and
# a fixture, so the target needs no repository, Rust or git. Usage: ./run.sh help
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd -P)
results=$here/results

usage() {
  cat <<'USAGE'
Usage: ./run.sh COMMAND [options]

  start    run a bench in the background; the log goes to results/<scenario>-<UTC time>.log
  run      run a bench in the foreground
  status   say whether a bench runs, its progress, and where its summary is
  stop     stop the running bench (a later `resume` finishes it)
  resume   start the last bench again with the same options; finished runs are skipped
  setcap   grant every ygb CAP_SYS_NICE with sudo, for --set plugin.fifo_priority=N (Linux)
  layout   print the CPU layout a bench would pick here
  help     show this text

Options for start and run:
  --scenario NAME|FILE   bundled scenario (quick, prod-like, ab, network, compression, stress) or a file
  --fixture DIR          fixture directory (default: the one in fixture/)
  --layout NAME|FILE     bundled layout (see layouts/) or a file (default: detected)
  --out DIR              results directory (default: results/<scenario>-<UTC time>)
  --variants A,B         run only these bundled variants, in this order (default: all, see bin/variants)
  Any other option goes to `ygb bench`, e.g. --set KEY=VALUE, --only TEXT, --max-foreign SHARE,
  --max-timer-lag-us US, --retries N, --grpc-port PORT, --prometheus-port PORT, --dry-run.
  The bundle's own --set options (bundle.args) come first, so yours override them.
USAGE
}

variants=()
while IFS= read -r name; do
  [[ -z "$name" ]] || variants+=("$name")
done <"$here/bin/variants"
ygb=$here/bin/${variants[0]}/ygb

pid_alive() {
  [[ -f "$results/current.pid" ]] && kill -0 "$(cat "$results/current.pid")" 2>/dev/null
}

check_glibc() {
  local need have
  need=$(sed -n 's/^glibc: //p' "$here/MANIFEST.txt")
  [[ -n "$need" && "$(uname -s)" == Linux ]] || return 0
  have=$(ldd --version 2>/dev/null | head -1 | grep -o '[0-9][0-9.]*$' || true)
  [[ -z "$have" ]] && return 0
  if [[ "$(printf '%s\n%s\n' "$need" "$have" | sort -V | head -1)" != "$need" ]]; then
    echo "this bundle needs glibc $need, this system has $have; build the bundle on an older system" >&2
    exit 1
  fi
}

# Turns start/run options into the `ygb bench` command line in `bench_args`, and sets `out_dir` and `log`.
prepare() {
  local scenario=quick fixture="" layout="" out="" chosen="" extra=()
  while (($#)); do
    case "$1" in
      --scenario) scenario=$2; shift 2 ;;
      --variants) chosen=$2; shift 2 ;;
      --fixture) fixture=$2; shift 2 ;;
      --layout) layout=$2; shift 2 ;;
      --out) out=$2; shift 2 ;;
      *) extra+=("$1"); shift ;;
    esac
  done
  local scenario_file=$scenario
  [[ -f "$here/scenarios/$scenario.toml" ]] && scenario_file=$here/scenarios/$scenario.toml
  [[ -f "$scenario_file" ]] || { echo "no scenario $scenario" >&2; exit 2; }
  if [[ -z "$fixture" ]]; then
    local found=()
    for dir in "$here"/fixture/*/; do
      [[ -f "$dir/manifest.json" ]] && found+=("${dir%/}")
    done
    ((${#found[@]} == 1)) || { echo "pass --fixture DIR: fixture/ holds ${#found[@]} fixtures" >&2; exit 2; }
    fixture=${found[0]}
  fi
  out_dir=${out:-$results/$(basename "$scenario_file" .toml)-$(date -u +%Y%m%dT%H%M%SZ)}
  bench_args=(bench --scenario "$scenario_file" --fixture "$fixture" --out "$out_dir")
  local name names=("${variants[@]}")
  if [[ -n "$chosen" ]]; then
    IFS=, read -r -a names <<<"$chosen"
  fi
  for name in "${names[@]}"; do
    [[ -x "$here/bin/$name/ygb" ]] || { echo "no variant $name in this bundle (see bin/variants)" >&2; exit 2; }
    bench_args+=(--variant "$name=$here/bin/$name")
  done
  if [[ -n "$layout" ]]; then
    [[ -f "$here/layouts/$layout.toml" ]] && layout=$here/layouts/$layout.toml
    bench_args+=(--layout "$layout")
  fi
  local line
  while IFS= read -r line; do
    [[ -z "$line" ]] || bench_args+=("$line")
  done <"$here/bundle.args"
  bench_args+=(${extra[@]+"${extra[@]}"})
  log=$out_dir.log
}

launcher=()
[[ "$(uname -s)" != Darwin ]] || launcher=(caffeinate -i)

start() {
  if pid_alive; then
    echo "a bench already runs (pid $(cat "$results/current.pid")); ./run.sh status or ./run.sh stop" >&2
    exit 1
  fi
  check_glibc
  mkdir -p "$results"
  prepare "$@"
  : >"$results/last.args"
  (($# == 0)) || printf '%s\n' "$@" >"$results/last.args"
  echo "$out_dir" >"$results/last.out"
  # Job control gives the bench its own process group, so stop reaches ygb run and the fleet too.
  set -m
  RUST_LOG=${RUST_LOG:-info} nohup ${launcher[@]+"${launcher[@]}"} "$ygb" "${bench_args[@]}" >"$log" 2>&1 &
  echo $! >"$results/current.pid"
  set +m
  echo "$log" >"$results/current.log"
  echo "started (pid $!): log $log"
  echo "follow with: tail -f $log    progress: ./run.sh status    stop: ./run.sh stop"
}

case "${1:-help}" in
  start)
    shift
    start "$@"
    ;;
  resume)
    [[ -f "$results/last.args" ]] || { echo "nothing to resume" >&2; exit 1; }
    args=()
    while IFS= read -r line; do
      [[ -z "$line" ]] || args+=("$line")
    done <"$results/last.args"
    # The same results directory, so finished runs are skipped.
    start ${args[@]+"${args[@]}"} --out "$(cat "$results/last.out")"
    ;;
  run)
    shift
    check_glibc
    prepare "$@"
    mkdir -p "$results"
    RUST_LOG=${RUST_LOG:-info} exec ${launcher[@]+"${launcher[@]}"} "$ygb" "${bench_args[@]}"
    ;;
  status)
    if pid_alive; then
      echo "running (pid $(cat "$results/current.pid")), log $(cat "$results/current.log")"
    elif [[ -f "$results/current.log" ]]; then
      echo "not running; last log $(cat "$results/current.log")"
    else
      echo "no bench started from this bundle"
      exit 0
    fi
    log=$(cat "$results/current.log")
    { grep -E 'plan:|\] \[[0-9]+/[0-9]+\]|: probe p99|invalid|Error' "$log" || true; } | tail -6 | sed 's/^\[[^]]*\] *//'
    summary=$(cat "$results/last.out")/summary.md
    if [[ -f "$summary" ]]; then echo "summary: $summary"; fi
    ;;
  stop)
    pid_alive || { echo "no bench is running"; exit 0; }
    pid=$(cat "$results/current.pid")
    kill -INT -- "-$pid" 2>/dev/null || kill -INT "$pid"
    for _ in $(seq 60); do
      kill -0 -- "-$pid" 2>/dev/null || { echo "stopped; ./run.sh resume finishes it"; exit 0; }
      sleep 1
    done
    kill -KILL -- "-$pid" 2>/dev/null || true
    echo "killed after 60 s; ./run.sh resume finishes it"
    ;;
  setcap)
    [[ "$(uname -s)" == Linux ]] || { echo "SCHED_FIFO and setcap work only on Linux" >&2; exit 2; }
    for name in "${variants[@]}"; do
      sudo setcap cap_sys_nice+ep "$here/bin/$name/ygb"
    done
    echo "granted CAP_SYS_NICE to ${#variants[@]} ygb binaries"
    ;;
  layout)
    exec "$ygb" layout
    ;;
  help | -h | --help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
