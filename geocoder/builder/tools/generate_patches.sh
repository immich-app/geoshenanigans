#!/usr/bin/env bash
# Usage: generate_patches.sh <old-root> <new-root> <results-dir> <log-dir>
#
# For every variant dir under <new-root> holding .bin files: diff the same
# variant under <old-root> against it into <new-root>/<variant>/patch.gcpatch,
# apply that patch to an isolated copy of the old variant dir in scratch (its
# client files only, link_client_files.sh, so a read outside the dir fails),
# and verify the result is the new client file set byte for byte
# (verify_patch.sh). Writes one result file per
# variant to <results-dir>/<variant with / as _>:
#   SKIP | PASS <files> <patch KiB> | FAIL <problems> <files> <patch KiB>
# plus <name>.mismatches on failure. Diff and patch logs go to <log-dir>.
#
# Jobs start largest first and only while their estimated peak memory fits:
# a diff peaks at about 2 × its old + new non-string .bin bytes plus 14 × the
# string tiers it loads, its own or borrowed from ../full. Measured vs
# estimated GiB: planet/full 114.8 vs 122.5, planet/poi/all 24.6 vs 25.1,
# planet/quality/q2.5 5.4 vs 9.7. The budget is 3/4 of the memory available at
# start, or of the job's cgroup limit if lower. A variant holds its estimate
# only while its diff runs: the patch + verify after it stream in about 10 MiB,
# so they run alongside later diffs. At most nproc jobs run at once.
#
# Env: GEOCODER_DIFF, GEOCODER_PATCH (default: on PATH).
set -uo pipefail

# The memory-aware scheduler needs `wait -n -p` (bash 5.1).
if (( BASH_VERSINFO[0] < 5 || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] < 1) )); then
  echo "generate_patches.sh: bash >= 5.1 required (wait -n -p), have $BASH_VERSION" >&2
  exit 1
fi

old_root=$1
new_root=$2
results_dir=$3
log_dir=$4
diff_bin=${GEOCODER_DIFF:-geocoder-diff}
patch_bin=${GEOCODER_PATCH:-geocoder-patch}
tools=$(dirname "$(readlink -f "$0")")
verify=$tools/verify_patch.sh
link=$tools/link_client_files.sh
mkdir -p "$results_dir" "$log_dir"

bin_count() { find "$1" -maxdepth 1 -name '*.bin' 2>/dev/null | wc -l; }
size_kib() { [ -f "$1" ] && echo $(( $(stat -c%s "$1") / 1024 )) || echo 0; }

# Estimated peak diff memory (KiB) for one side of a variant: its non-string
# .bin files, plus every string tier as the diff resolves it (dir, ../full,
# ../../full).
side_weight_kib() {
  local dir=$1 kib tier f strings=0
  kib=$(find "$dir" -maxdepth 1 -name '*.bin' ! -name 'strings_*' -printf '%s\n' 2>/dev/null \
    | awk '{s += $1} END {printf "%d", s / 1024}')
  for tier in core street addr postcode poi; do
    for f in "$dir" "$dir/../full" "$dir/../../full"; do
      if [ -s "$f/strings_$tier.bin" ]; then strings=$((strings + $(size_kib "$f/strings_$tier.bin"))); break; fi
    done
  done
  echo $(( kib * 2 + strings * 14 ))
}

# The tightest memory limit on this process's cgroup path (v2 memory.max or
# v1 memory.limit_in_bytes, ancestors included), in KiB; empty if none.
cgroup_limit_kib() {
  local best="" path root file v
  while IFS=: read -r _ controllers path; do
    if [ -z "$controllers" ]; then root=/sys/fs/cgroup; file=memory.max
    elif [[ ",$controllers," == *,memory,* ]]; then root=/sys/fs/cgroup/memory; file=memory.limit_in_bytes
    else continue
    fi
    while :; do
      if [ -r "$root$path/$file" ]; then
        v=$(cat "$root$path/$file")
        if [ "$v" != max ] && [ "$v" -lt 4611686018427387904 ]; then
          v=$((v / 1024))
          if [ -z "$best" ] || [ "$v" -lt "$best" ]; then best=$v; fi
        fi
      fi
      [ -z "$path" ] || [ "$path" = / ] && break
      path=${path%/*}
    done
  done < /proc/self/cgroup
  echo "$best"
}

budget_kib=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
cgroup_kib=$(cgroup_limit_kib)
if [ -n "$cgroup_kib" ] && [ "$cgroup_kib" -lt "$budget_kib" ]; then budget_kib=$cgroup_kib; fi
budget_kib=$(( budget_kib * 3 / 4 ))
max_jobs=$(nproc)

# Diff stage: the memory-heavy half. Writes the result only when the variant
# is done (SKIP or a diff failure); otherwise verify_variant takes over.
diff_variant() {
  local variant=$1
  local new_dir="$new_root/$variant" old_dir="$old_root/$variant"
  local safe=${variant//\//_}
  local result="$results_dir/$safe" log="$log_dir/$safe.log"
  # Only a fresh build (no previous output at all) skips. A file-count change
  # with a previous build present is a real diff the tools must reproduce.
  if [ "$(bin_count "$new_dir")" -lt 2 ] || [ "$(bin_count "$old_dir")" -eq 0 ]; then
    echo "SKIP" > "$result"
    return
  fi
  if ! "$diff_bin" "$old_dir" "$new_dir" -o "$new_dir/patch.gcpatch" 2> "$log"; then
    echo "FAIL 1 0 0" > "$result"
    echo "geocoder-diff failed: $(tail -1 "$log")" > "$result.mismatches"
  fi
}

# Verify stage: apply the patch to an isolated copy and compare. The patcher
# streams (about 10 MiB), so this half holds no memory budget.
verify_variant() {
  local variant=$1
  local new_dir="$new_root/$variant" old_dir="$old_root/$variant"
  local safe=${variant//\//_}
  local result="$results_dir/$safe" log="$log_dir/$safe.log"
  [ -e "$result" ] && return
  local patch_file="$new_dir/patch.gcpatch" work
  work=$(mktemp -d "${TMPDIR:-/tmp}/verify-$safe-XXXXXX")
  # <work>/old/variant has no siblings: ../full and ../../full don't exist.
  local iso_dir="$work/old/variant" verify_dir="$work/new"
  mkdir -p "$work/old" "$verify_dir"
  "$link" "$old_dir" "$iso_dir"
  local kib=$(( $(stat -c%s "$patch_file") / 1024 ))
  if ! "$patch_bin" "$iso_dir" "$patch_file" -o "$verify_dir/" 2>> "$log"; then
    echo "FAIL 1 0 $kib" > "$result"
    echo "geocoder-patch failed: $(tail -1 "$log")" > "$result.mismatches"
    rm -rf "$work"
    return
  fi
  local report
  report=$("$verify" "$new_dir" "$verify_dir")
  local verdict
  verdict=$(echo "$report" | tail -1)
  case "$verdict" in
    PASS*) echo "PASS $(echo "$verdict" | awk '{print $2}') $kib" > "$result" ;;
    *)
      echo "FAIL $(echo "$verdict" | awk '{print $2}') $(echo "$verdict" | awk '{print $4}') $kib" > "$result"
      echo "$report" | sed '$d' | tr '\n' ';' > "$result.mismatches"
      ;;
  esac
  rm -rf "$work"
}

# Largest first, so the planet-scale diffs never pile up at the end.
declare -A weight=()
for variant in $(cd "$new_root" && find . -name '*.bin' -printf '%h\n' | sed 's|^\./||' | sort -u); do
  weight[$variant]=$(( $(side_weight_kib "$old_root/$variant") + $(side_weight_kib "$new_root/$variant") ))
done

# running: pid -> memory weight held (verifies hold none); stage/name: what it
# runs. A diff that ends queues its variant's verify.
pending=($(for v in "${!weight[@]}"; do echo "${weight[$v]} $v"; done | sort -rn | awk '{print $2}'))
next=0
verifies=()
declare -A running=() stage=() name=()
running_kib=0
while [ $next -lt ${#pending[@]} ] || [ ${#verifies[@]} -gt 0 ] || [ ${#running[@]} -gt 0 ]; do
  while [ ${#verifies[@]} -gt 0 ] && [ ${#running[@]} -lt "$max_jobs" ]; do
    verify_variant "${verifies[0]}" &
    running[$!]=0; stage[$!]=verify; name[$!]=${verifies[0]}
    verifies=("${verifies[@]:1}")
  done
  while [ $next -lt ${#pending[@]} ] && [ ${#running[@]} -lt "$max_jobs" ]; do
    variant=${pending[$next]}
    w=${weight[$variant]}
    # A diff bigger than the whole budget still runs, alone.
    [ ${#running[@]} -gt 0 ] && [ $((running_kib + w)) -gt "$budget_kib" ] && break
    diff_variant "$variant" &
    running[$!]=$w; stage[$!]=diff; name[$!]=$variant
    running_kib=$((running_kib + w))
    next=$((next + 1))
  done
  # Only tracked jobs: bash 5.3's bare wait -n also returns process substitutions.
  finished=""
  wait -n -p finished "${!running[@]}" || true
  if [ -z "${finished:-}" ] || [ -z "${running[$finished]+x}" ]; then
    # Cannot tell which job ended: drain them all rather than launch past
    # the memory budget.
    wait "${!running[@]}"
    for pid in "${!running[@]}"; do
      [ "${stage[$pid]}" = diff ] && verifies+=("${name[$pid]}")
    done
    running=(); stage=(); name=()
    running_kib=0
    continue
  fi
  running_kib=$((running_kib - running[$finished]))
  [ "${stage[$finished]}" = diff ] && verifies+=("${name[$finished]}")
  unset "running[$finished]" "stage[$finished]" "name[$finished]"
done
