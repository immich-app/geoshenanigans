#!/usr/bin/env bash
# Usage: generate_patches.sh <old-root> <new-root> <results-dir> <log-dir>
#
# For every variant dir under <new-root> holding .bin files: diff the same
# variant under <old-root> against it into <new-root>/<variant>/patch.gcpatch,
# apply that patch to the old dir in scratch, and verify the result is the new
# client file set byte for byte (verify_patch.sh). Writes one result file per
# variant to <results-dir>/<variant with / as _>:
#   SKIP | PASS <files> <patch KiB> | FAIL <problems> <files> <patch KiB>
# plus <name>.mismatches on failure. Diff and patch logs go to <log-dir>.
#
# Jobs start largest first and only while their estimated peak memory fits:
# a diff peaks at about 2 × its old + new non-string .bin bytes plus 14 × the
# string tiers it loads, its own or borrowed from ../full. Measured vs
# estimated GiB: planet/full 114.8 vs 122.5, planet/poi/all 24.6 vs 25.1,
# planet/quality/q2.5 5.4 vs 9.7. The budget is 3/4 of the memory available at
# start, or of the job's cgroup limit if lower. At most nproc jobs run at once.
#
# Env: GEOCODER_DIFF, GEOCODER_PATCH (default: on PATH).
set -uo pipefail

old_root=$1
new_root=$2
results_dir=$3
log_dir=$4
diff_bin=${GEOCODER_DIFF:-geocoder-diff}
patch_bin=${GEOCODER_PATCH:-geocoder-patch}
verify=$(dirname "$(readlink -f "$0")")/verify_patch.sh
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

run_variant() {
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
  local patch_file="$new_dir/patch.gcpatch" verify_dir
  verify_dir=$(mktemp -d "${TMPDIR:-/tmp}/verify-$safe-XXXXXX")
  if ! "$diff_bin" "$old_dir" "$new_dir" -o "$patch_file" 2> "$log"; then
    echo "FAIL 1 0 0" > "$result"
    echo "geocoder-diff failed: $(tail -1 "$log")" > "$result.mismatches"
    rm -rf "$verify_dir"
    return
  fi
  local kib=$(( $(stat -c%s "$patch_file") / 1024 ))
  if ! "$patch_bin" "$old_dir" "$patch_file" -o "$verify_dir/" 2>> "$log"; then
    echo "FAIL 1 0 $kib" > "$result"
    echo "geocoder-patch failed: $(tail -1 "$log")" > "$result.mismatches"
    rm -rf "$verify_dir"
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
  rm -rf "$verify_dir"
}

# Largest first, so the planet-scale diffs never pile up at the end.
declare -A weight=()
for variant in $(cd "$new_root" && find . -name '*.bin' -printf '%h\n' | sed 's|^\./||' | sort -u); do
  weight[$variant]=$(( $(side_weight_kib "$old_root/$variant") + $(side_weight_kib "$new_root/$variant") ))
done

declare -A running=()
running_kib=0
for variant in $(for v in "${!weight[@]}"; do echo "${weight[$v]} $v"; done | sort -rn | awk '{print $2}'); do
  w=${weight[$variant]}
  while [ ${#running[@]} -gt 0 ] && { [ $((running_kib + w)) -gt "$budget_kib" ] || [ ${#running[@]} -ge "$max_jobs" ]; }; do
    # Only tracked jobs: bash 5.3's bare wait -n also returns process substitutions.
    finished=""
    wait -n -p finished "${!running[@]}" || true
    if [ -z "${finished:-}" ] || [ -z "${running[$finished]+x}" ]; then break; fi
    running_kib=$((running_kib - running[$finished]))
    unset "running[$finished]"
  done
  run_variant "$variant" &
  running[$!]=$w
  running_kib=$((running_kib + w))
done
wait
