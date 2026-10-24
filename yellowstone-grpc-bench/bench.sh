#!/usr/bin/env bash
# One command per benchmark: builds every variant with this harness, records a fixture when asked,
# then runs `ygb bench`. Runs on Linux and macOS (bash 3.2). Options: ./bench.sh --help, README.md.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)

usage() {
  cat <<'USAGE'
Usage: yellowstone-grpc-bench/bench.sh [options]

  --scenario NAME|FILE   scenario from scenarios/ (quick, prod-like, ab, network, compression,
                         stress) or a TOML file (default: quick)
  --fixture DIR          fixture directory (default: $YGB_FIXTURE)
  --record SLOTS         record SLOTS slots into --fixture first when it has no manifest
  --endpoint URL         gRPC endpoint of the node to record from, with --record
  --variant NAME=REF     build git REF as variant NAME; `.` is this checkout as it is. Repeat for
                         an A/B; the first variant is the baseline (default: base=.)
  --flavour FLAVOUR      public (upstream tonic) or patched (tonic from YGB_TONIC_GIT at
                         YGB_TONIC_TAG); patched is the default when YGB_TONIC_GIT is set
  --layout FILE          CPU layout (default: detected; see layouts/)
  --out DIR              results (default: ../ygb-results/<scenario>-<UTC time> next to the repo)
  --set KEY=VALUE        override a scenario key, e.g. --set plugin.fifo_priority=40
  --only TEXT            run only cases whose name contains TEXT
  --plugin-cores N       plugin cores in a detected layout (default 6)
  --max-foreign SHARE    other tenants' share of a plugin or probe CPU that invalidates a run
                         (default 0.02)
  --max-timer-lag-us US  driver timer lag p99 that invalidates a run (default 200; macOS may need 2000)
  --retries N            run again a run that failed only an environment check (default 1)
  --grpc-port PORT       loopback gRPC port of the plugin (default 10000)
  --prometheus-port PORT loopback Prometheus port of the plugin (default 9123)
  --setcap               grant each built ygb CAP_SYS_NICE with sudo setcap, for
                         plugin.fifo_priority; the build and the results stay yours
  --dry-run              build, then print the plan without running it
  --bundle DIR           build, then write a bundle to DIR instead of running: every variant's
                         binaries, scenarios, layouts, the --fixture (optional) and run.sh. Copy
                         DIR to a server and run ./run.sh there; --set options become its defaults

Environment: TRACE=1 builds plugins with latency-trace checkpoints; YGB_FIXTURE sets --fixture.
USAGE
}

scenario=quick
fixture=${YGB_FIXTURE:-}
record_slots=0
endpoint=""
flavour=public
[[ -z "${YGB_TONIC_GIT:-}" ]] || flavour=patched
out=""
layout=""
bundle=""
setcap=0
variants=()
passthrough=()
while (($#)); do
  case "$1" in
    --scenario) scenario=$2; shift 2 ;;
    --fixture) fixture=$2; shift 2 ;;
    --record) record_slots=$2; shift 2 ;;
    --endpoint) endpoint=$2; shift 2 ;;
    --variant) variants+=("$2"); shift 2 ;;
    --flavour) flavour=$2; shift 2 ;;
    --layout) layout=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --bundle) bundle=$2; shift 2 ;;
    --set | --only | --plugin-cores | --max-foreign | --max-timer-lag-us | --retries | --grpc-port | --prometheus-port)
      passthrough+=("$1" "$2")
      shift 2
      ;;
    --dry-run) passthrough+=("$1"); shift ;;
    --setcap) setcap=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for tool in cargo git; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done
[[ -n "$fixture" || -n "$bundle" ]] || { echo "set --fixture DIR or YGB_FIXTURE" >&2; exit 2; }
if [[ -n "$bundle" && -e "$bundle" ]]; then
  echo "$bundle exists; pick a new --bundle directory" >&2
  exit 2
fi
if [[ -f "$here/scenarios/$scenario.toml" ]]; then
  scenario_file=$here/scenarios/$scenario.toml
elif [[ -f "$scenario" ]]; then
  scenario_file=$(cd "$(dirname "$scenario")" && pwd)/$(basename "$scenario")
else
  echo "no scenario $scenario" >&2
  exit 2
fi
((${#variants[@]})) || variants=("base=.")

# Every variant gets this checkout's harness, so the variants differ only in the plugin.
overlay() {
  local tree=$1
  rm -rf "$tree/yellowstone-grpc-bench"
  mkdir -p "$tree/yellowstone-grpc-bench"
  local file
  (cd "$here" && git ls-files -z --cached --others --exclude-standard . |
    while IFS= read -r -d '' file; do
      [[ -e "$file" ]] || continue
      mkdir -p "$tree/yellowstone-grpc-bench/$(dirname "$file")"
      cp "$file" "$tree/yellowstone-grpc-bench/$file"
    done)
  # perl -pi edits in place the same way with GNU and BSD tools.
  grep -q '"yellowstone-grpc-bench"' "$tree/Cargo.toml" ||
    perl -pi -e 's/^members = \[/members = [\n    "yellowstone-grpc-bench",/' "$tree/Cargo.toml"
  local dep line
  sed -n 's/^\([a-z0-9_-]*\) = { workspace = true.*/\1/p' "$here/Cargo.toml" | while read -r dep; do
    if ! grep -q "^$dep = " "$tree/Cargo.toml"; then
      line=$(grep "^$dep = " "$root/Cargo.toml")
      LINE=$line perl -pi -e '$_ .= "$ENV{LINE}\n" if /^\[workspace\.dependencies\]/' "$tree/Cargo.toml"
    fi
  done
}

build_variant() {
  # $(build_variant ...) turns errexit off; without it a failed checkout would build this checkout instead.
  set -e
  local name=$1 ref=$2 tree
  if [[ "$ref" == "." ]]; then
    tree=$root
  else
    tree=$root/target/ygb-variants/$name
    local commit
    commit=$(git -C "$root" rev-parse --verify --quiet "$ref^{commit}") || {
      echo "variant $name: ref $ref does not exist in this clone (a --single-branch clone has no origin/master)" >&2
      exit 2
    }
    if [[ -e "$tree" ]]; then
      # Without its own .git, git -C would walk up to this checkout and reset it.
      if [[ ! -e "$tree/.git" || "$(git -C "$tree" rev-parse --show-toplevel)" != "$tree" ]]; then
        echo "$tree exists but is not a worktree; remove it or run git worktree prune" >&2
        exit 1
      fi
      git -C "$tree" checkout --quiet --force --detach "$commit"
      git -C "$tree" clean -fdq
    else
      git -C "$root" worktree add --quiet --detach "$tree" "$commit"
    fi
    overlay "$tree"
  fi
  if [[ "${TRACE:-0}" == 1 ]] && ! grep -q 'latency-trace' "$tree/yellowstone-grpc-geyser/Cargo.toml"; then
    echo "variant $name ($ref) has no latency-trace feature; build it without TRACE=1" >&2
    exit 1
  fi
  echo "building $name ($ref) in $tree" >&2
  local dir
  dir=$("$tree/yellowstone-grpc-bench/tools/build-plugin.sh" "$flavour" | tail -n1)
  [[ "$dir" == "$tree/"* ]] || { echo "variant $name built into $dir, outside $tree" >&2; exit 1; }
  echo "$dir"
}

variant_args=()
variant_names=()
variant_dirs=()
first_dir=""
for spec in "${variants[@]}"; do
  name=${spec%%=*}
  ref=${spec#*=}
  [[ "$name" != "$spec" && -n "$ref" ]] || { echo "variant must be NAME=REF: $spec" >&2; exit 2; }
  [[ "$name" =~ ^[A-Za-z0-9_.]+$ ]] || { echo "variant name $name: use letters, digits, _ and ." >&2; exit 2; }
  dir=$(build_variant "$name" "$ref")
  [[ -x "$dir/ygb" ]] || { echo "build of $name did not produce $dir/ygb" >&2; exit 1; }
  if ((setcap)); then
    [[ "$(uname -s)" == Linux ]] || { echo "--setcap and SCHED_FIFO work only on Linux" >&2; exit 2; }
    sudo setcap cap_sys_nice+ep "$dir/ygb"
  fi
  variant_args+=(--variant "$name=$dir")
  variant_names+=("$name")
  variant_dirs+=("$dir")
  first_dir=${first_dir:-$dir}
done
ygb=$first_dir/ygb

# A bundle holds what a run needs, so the target needs no repository or toolchain.
make_bundle() {
  local dest=$1 i name dir lib
  mkdir -p "$dest/bin" "$dest/scenarios" "$dest/layouts" "$dest/fixture" "$dest/results"
  : >"$dest/bin/variants"
  for i in "${!variant_names[@]}"; do
    name=${variant_names[$i]}
    dir=${variant_dirs[$i]}
    mkdir -p "$dest/bin/$name"
    for lib in ygb libyellowstone_grpc_geyser.so libyellowstone_grpc_geyser.dylib plugin-abi.txt; do
      [[ ! -e "$dir/$lib" ]] || cp "$dir/$lib" "$dest/bin/$name/"
    done
    echo "$name" >>"$dest/bin/variants"
  done
  cp "$here"/scenarios/*.toml "$dest/scenarios/"
  cp "$here"/layouts/*.toml "$dest/layouts/"
  cp "$here/tools/bundle-run.sh" "$dest/run.sh"
  chmod +x "$dest/run.sh"
  # The bundle's --set options; run.sh passes them before the caller's own options.
  : >"$dest/bundle.args"
  i=0
  while ((i < ${#passthrough[@]})); do
    if [[ "${passthrough[$i]}" == --set ]]; then
      printf '%s
%s
' --set "${passthrough[$((i + 1))]}" >>"$dest/bundle.args"
      i=$((i + 2))
    else
      i=$((i + 1))
    fi
  done
  if [[ -n "$fixture" ]]; then
    local src file target
    src=$(cd "$fixture" && pwd -P)
    target=$dest/fixture/$(basename "$src")
    # Hard links when the bundle shares a file system with the fixture, copies otherwise.
    (cd "$src" && find . -type f -print0) | while IFS= read -r -d '' file; do
      mkdir -p "$target/$(dirname "$file")"
      ln "$src/$file" "$target/$file" 2>/dev/null || cp "$src/$file" "$target/$file"
    done
  fi
  local glibc=""
  if command -v objdump >/dev/null && [[ "$(uname -s)" == Linux ]]; then
    glibc=$(for dir in "$dest"/bin/*/; do objdump -T "$dir"ygb "$dir"libyellowstone_grpc_geyser.so 2>/dev/null; done |
      grep -o 'GLIBC_[0-9][0-9.]*' | sed 's/GLIBC_//' | sort -V | tail -1 || true)
  fi
  {
    echo "built: $(date -u +%Y-%m-%dT%H:%M:%SZ) on $(uname -sm)"
    echo "harness: $(git -C "$root" rev-parse --short HEAD) on $(git -C "$root" branch --show-current)"
    for i in "${!variant_names[@]}"; do
      echo "variant ${variant_names[$i]}: $(sed -n 's/^commit: //p' "${variant_dirs[$i]}/plugin-abi.txt")"
    done
    echo "fixture: ${fixture:+$(basename "$fixture")}"
    [[ -z "$glibc" ]] || echo "glibc: $glibc"
  } >"$dest/MANIFEST.txt"
  "$dest/run.sh" help >"$dest/README.txt"
  echo "bundle: $dest ($(du -sh "$dest" | cut -f1))" >&2
  echo "copy it with: rsync -a --info=progress2 $dest/ SERVER:$(basename "$dest")/" >&2
  echo "then on the server: cd $(basename "$dest") && ./run.sh start --scenario prod-like" >&2
}

if [[ -n "$bundle" && -z "$fixture" ]]; then
  make_bundle "$bundle"
  exit 0
fi

if [[ ! -f "$fixture/manifest.json" ]]; then
  if ((record_slots == 0)) || [[ -z "$endpoint" ]]; then
    echo "no fixture at $fixture; add --record SLOTS --endpoint URL to record one" >&2
    exit 1
  fi
  echo "recording $record_slots slots from $endpoint into $fixture (about 2.6 GB per 1000 slots)" >&2
  "$ygb" record --endpoint "$endpoint" --out "$fixture" --slots "$record_slots" --tail-secs 40 --split-accounts
  "$ygb" inspect --fixture "$fixture" | tee "$fixture.inspect.txt"
fi

if [[ -n "$bundle" ]]; then
  make_bundle "$bundle"
  exit 0
fi

out=${out:-$(dirname "$root")/ygb-results/$(basename "$scenario_file" .toml)-$(date -u +%Y%m%dT%H%M%SZ)}
args=(bench --scenario "$scenario_file" --fixture "$fixture" --out "$out" "${variant_args[@]}")
[[ -z "$layout" ]] || args+=(--layout "$layout")
echo "results: $out" >&2
# macOS sleeps when the user is idle, which would stall the replay; caffeinate holds it awake.
launcher=()
[[ "$(uname -s)" != Darwin ]] || launcher=(caffeinate -i)
# bash 3.2, as on macOS, treats an empty "${array[@]}" as unbound under set -u.
RUST_LOG=${RUST_LOG:-info} exec ${launcher[@]+"${launcher[@]}"} "$ygb" "${args[@]}" ${passthrough[@]+"${passthrough[@]}"}
