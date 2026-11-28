#!/usr/bin/env bash
# Usage: ci_build_tools.sh <build dir> <cache dir> <s2 prefix>
#
# The nightly's tools for this source: the binaries in <cache dir> when its
# SHA256SUMS still holds (the workflow restores it keyed on the builder's
# source), else a fresh build that it saves there. Then the unit tests, which
# gate everything. Tests that fail on cached binaries (say the runner image's
# libraries moved) get one fresh build before they count.
#
# The unit tests run as 16 shards at once (GEOCODER_TEST_SHARD=i/16, see
# tests/test_main.cpp), or a shard a CPU when the CPU budget is smaller
# (cpu_budget.sh: the CPUs this process may run on, or fewer under its
# cgroup's CPU quota; it sizes the compile too): on one core the suite took
# ~430 s of the runner's time, 16 shards 25 s on the test box. Every shard
# must pass and run at least one test, and the shards' counts must add up to
# the registry's size (geocoder-tests --count): a shard index past the count,
# or a geocoder-tests that ignored its shard, would otherwise pass on fewer
# tests. The total printed ("N/N tests passed") is the registry's. Each
# phase starts with a "[tools HH:MM:SS]" line.
set -euo pipefail

build=$(realpath -m "$1")
cache=$(realpath -m "$2")
s2_prefix=$3
tools=(build-index geocoder-diff geocoder-patch pbf-apply heap-peak-shim heap-peak-probe namecensus geocoder-tests)
source_dir=$(cd "$(dirname "$0")/.." && pwd)
cpus=$("$source_dir/tools/cpu_budget.sh")
shards=$((cpus < 16 ? cpus : 16))

phase() { echo "[tools $(date -u +%H:%M:%S)] $*"; }

compile() {
  phase "compiling"
  local start=$SECONDS
  cmake -S "$source_dir" -B "$build" -DCMAKE_BUILD_TYPE=Release -DGEOCODER_S2_PREFIX="$s2_prefix"
  make -C "$build" -j"$cpus" "${tools[@]}"
  rm -rf "$cache"
  mkdir -p "$cache/bin"
  (cd "$build" && cp "${tools[@]}" "$cache/bin/")
  (cd "$cache" && sha256sum bin/* > SHA256SUMS)
  phase "compiled in $((SECONDS - start))s"
}

# The unit tests in the build dir, as $shards shards at once; fails, saying
# why, unless every shard passed having run a test and the shards ran the
# registry's tests between them. Without a dir for the shards' logs it runs
# none and ends the script: a fresh build would fail alike.
unit_tests() {
  local total logs i status line shard_passed shard_ran ran=0 passed=0 failed=0 start=$SECONDS
  local -a pids=()
  total=$(./geocoder-tests --count)
  phase "unit tests: $total in $shards shards"
  logs=$(mktemp -d) || { echo "Unit tests: no dir for the shards' logs; none ran"; exit 1; }
  for ((i = 0; i < shards; i++)); do
    GEOCODER_TEST_SHARD="$i/$shards" ./geocoder-tests > "$logs/$i.log" 2>&1 &
    pids+=($!)
  done
  for ((i = 0; i < shards; i++)); do
    status=0
    wait "${pids[$i]}" || status=$?
    line=$(grep -E '^[0-9]+/[0-9]+ tests passed' "$logs/$i.log" | tail -1 || true)
    if [ "$status" != 0 ] || [ -z "$line" ]; then
      echo "Unit tests: shard $i/$shards failed (exit status $status):"
      grep -E '^FAILED|^    FAIL' "$logs/$i.log" || tail -20 "$logs/$i.log"
      failed=$((failed + 1))
      continue
    fi
    shard_passed=${line%%/*}
    shard_ran=${line#*/}
    shard_ran=${shard_ran%% *}
    if [ "$shard_ran" = 0 ]; then
      echo "Unit tests: shard $i/$shards ran no test"
      failed=$((failed + 1))
      continue
    fi
    passed=$((passed + shard_passed))
    ran=$((ran + shard_ran))
  done
  rm -rf "$logs"
  phase "unit tests done in $((SECONDS - start))s"
  if [ "$failed" != 0 ]; then return 1; fi
  if [ "$ran" != "$total" ]; then
    echo "Unit tests: the shards ran $ran tests; the registry holds $total"
    return 1
  fi
  echo "$passed/$total tests passed"
}

mkdir -p "$build"
if (cd "$cache" 2> /dev/null && sha256sum -c --quiet SHA256SUMS); then
  phase "Tools from the cache"
  cp "$cache"/bin/* "$build/"
  cd "$build"
  if unit_tests; then exit 0; fi
  echo "Unit tests failed on the cached tools; building them afresh"
fi
compile
cd "$build"
unit_tests
