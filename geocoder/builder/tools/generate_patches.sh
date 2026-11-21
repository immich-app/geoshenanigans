#!/usr/bin/env bash
# Usage: generate_patches.sh <old-root> <new-root> <results-dir> <log-dir>
#
# For every variant dir under <new-root> holding .bin files: diff the same
# variant under <old-root> against it into <new-root>/<variant>/patch.gcpatch,
# matching street / interp / addr records by slot (geocoder-diff
# --identity-slots: <new-root> was built with --prev-output <old-root>, so a
# record's slot is its entity, and the diff takes each day's keys from its
# records). Then apply that patch to an isolated copy of the old variant dir
# in scratch (its client files only, link_client_files.sh, so a read outside
# the dir fails), and verify the result is the new client file set byte for
# byte (verify_patch.sh). Writes one result file per variant to
# <results-dir>/<variant with / as _>:
#   SKIP | PASS <files> <patch KiB> | FAIL <problems> <files> <patch KiB>
# plus <name>.mismatches on failure. Diff and patch logs go to <log-dir>,
# each variant's estimates, a line a variant, to stdout, and each job's start
# (with what it holds and what the running jobs hold) and end, timestamped,
# to stderr. The estimates are made for every variant at once, as many at a
# time as there are CPUs in the budget (cpu_budget.sh: those this process
# may run on, or fewer under its cgroup's CPU quota): two --logical-size
# reads a variant, ~20 s one after another on a planet night.
#
# Jobs start largest first and only while their estimated peak memory fits:
# 9/8 × the bytes a diff maps for its old + new record files, plus 4 × the
# string tiers it loads, its own or borrowed from ../full, plus 64 MiB a day.
# The mapped bytes are the first number of geocoder-diff --logical-size: a
# dir's non-string .bin files, those the builder writes encoded at their
# logical size, which is what the diff maps of them. Measured peak (max RSS)
# vs estimated GiB on two planet days: planet/full 24.9 vs 43.1,
# planet/no-addresses 11.9 vs 16.5, europe/full 12.6 vs 17.8, planet/poi/all
# 4.4 vs 6.6, planet/poi/major 1.25 vs 1.75, planet/admin 1.22 vs 1.69,
# planet/quality/exact 1.32 vs 2.65 (its admin geometry merged by slot),
# planet/quality/simplified 1.22 vs 1.66, antarctica/full 0.08 vs 0.13: each
# estimate 1.36 × its peak or more. The budget is 3/4 of the memory available
# at start, or of the job's cgroup limit if lower. A variant holds its
# estimate only while its diff runs: the patch + verify after it stream in
# about 10 MiB, so they run alongside later diffs. A diff bigger than the
# whole budget still runs, alone. At most a job a CPU of the budget runs at
# once.
#
# Jobs also start only while what they write to scratch fits $TMPDIR's volume.
# A diff holds, from its start to its end, the second number of geocoder-diff
# --logical-size for both days: the logical files and the string pools it
# decodes there, all gone before it exits. A verify holds the new variant's
# client files, which the patcher writes there, the decompressed patch (its
# size is in the diff's log) and 1 GiB for the patcher's spills. A job starts
# while the needs of the running jobs plus its own are within the volume's
# free space, read at that moment, less the reserve: what the caller says
# other writers may still add to the volume. What a running job has written
# is in its need and has left the free space too, so the rule errs to the
# safe side. A job that needs more than the free space less the reserve with
# no job running never starts: its variant fails, the reason in
# <name>.mismatches, in place of a volume filled under its other writers.
#
# Env: GEOCODER_DIFF, GEOCODER_PATCH (default: on PATH).
#   GEOCODER_PATCH_MEM_BUDGET_KIB: the memory budget, in place of the 3/4 of
#     what is available.
#   GEOCODER_PATCH_MEM_RESERVE_KIB: memory the caller's other jobs hold
#     beside these, taken off the budget (default 0).
#   GEOCODER_PATCH_SCRATCH_FREE_KIB: the scratch volume's free space, in
#     place of reading it: the jobs' needs are then counted against this
#     figure alone.
#   GEOCODER_PATCH_SCRATCH_RESERVE_KIB: the reserve (default 0).
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
scratch_root=${TMPDIR:-/tmp}
reserve_kib=${GEOCODER_PATCH_SCRATCH_RESERVE_KIB:-0}
# What a verify holds for the patcher's spills. The decompressed patch, which
# the patcher writes to scratch too, is added when the variant's diff ends
# (queue_verify).
patcher_scratch_kib=$((1024 * 1024))
mkdir -p "$results_dir" "$log_dir"

bin_count() { find "$1" -maxdepth 1 -name '*.bin' 2>/dev/null | wc -l; }
size_kib() { [ -f "$1" ] && echo $(( $(stat -c%s "$1") / 1024 )) || echo 0; }
# Bytes as whole KiB, rounded up.
kib_of() { echo $(( ($1 + 1023) / 1024 )); }

# geocoder-diff --logical-size of a variant dir: the bytes a diff maps for its
# record files, then the bytes it writes to scratch for it. Fails, the reason
# on stderr, when the diff cannot say.
logical_sizes() {
  local dir=$1 sizes
  sizes=$("$diff_bin" --logical-size "$dir") || return 1
  [[ "$sizes" =~ ^[0-9]+\ [0-9]+$ ]] || { echo "geocoder-diff --logical-size $dir printed: $sizes" >&2; return 1; }
  echo "$sizes"
}

# Estimated peak diff memory (KiB) for one side of a variant: 9/8 of the
# bytes the diff maps for its record files (logical_sizes' first number), 4 ×
# every string tier as the diff resolves it (dir, ../full, ../../full), and
# 64 MiB.
side_weight_kib() {
  local dir=$1 mapped=$2 tier f strings=0
  for tier in core street addr postcode poi; do
    for f in "$dir" "$dir/../full" "$dir/../../full"; do
      if [ -s "$f/strings_$tier.bin" ]; then strings=$((strings + $(size_kib "$f/strings_$tier.bin"))); break; fi
    done
  done
  echo $(( mapped / 1024 * 9 / 8 + strings * 4 + 64 * 1024 ))
}

# The bytes of a variant dir's client files: the set link_client_files.sh
# links and verify_patch.sh compares. Printed as %.0f: an awk whose %d is 32
# bits wide would cap the sum at 2 GiB.
client_bytes() {
  find "$1" -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' -printf '%s\n' \
    | awk '{s += $1} END {printf "%.0f", s}'
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

if [ -n "${GEOCODER_PATCH_MEM_BUDGET_KIB:-}" ]; then
  budget_kib=$GEOCODER_PATCH_MEM_BUDGET_KIB
else
  budget_kib=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
  cgroup_kib=$(cgroup_limit_kib)
  if [ -n "$cgroup_kib" ] && [ "$cgroup_kib" -lt "$budget_kib" ]; then budget_kib=$cgroup_kib; fi
  budget_kib=$(( budget_kib * 3 / 4 ))
fi
budget_kib=$(( budget_kib - ${GEOCODER_PATCH_MEM_RESERVE_KIB:-0} ))
max_jobs=$("$tools/cpu_budget.sh")

# The scratch volume's free space at this moment, in KiB.
scratch_free_kib() {
  if [ -n "${GEOCODER_PATCH_SCRATCH_FREE_KIB:-}" ]; then echo "$GEOCODER_PATCH_SCRATCH_FREE_KIB"
  else df --output=avail -k "$scratch_root" | tail -1 | tr -d ' '
  fi
}

# FAIL for a variant a job of which did not run: one problem, no file
# compared, a patch of $2 KiB, and the reason.
fail_variant() {
  local result="$results_dir/${1//\//_}"
  echo "FAIL 1 0 $2" > "$result"
  echo "$3" > "$result.mismatches"
}

# Diff stage: the memory-heavy half. Writes the result only when the diff
# fails; otherwise verify_variant takes over.
diff_variant() {
  local variant=$1
  local new_dir="$new_root/$variant" old_dir="$old_root/$variant"
  local safe=${variant//\//_}
  local log="$log_dir/$safe.log"
  if ! "$diff_bin" "$old_dir" "$new_dir" -o "$new_dir/patch.gcpatch" --identity-slots 2> "$log"; then
    fail_variant "$variant" 0 "geocoder-diff failed: $(tail -1 "$log")"
  fi
}

# Verify stage: apply the patch to an isolated copy and compare. The patcher
# streams (about 10 MiB), so this half holds no memory budget.
verify_variant() {
  local variant=$1
  local new_dir="$new_root/$variant" old_dir="$old_root/$variant"
  local safe=${variant//\//_}
  local result="$results_dir/$safe" log="$log_dir/$safe.log"
  local patch_file="$new_dir/patch.gcpatch" work
  work=$(mktemp -d "$scratch_root/verify-$safe-XXXXXX")
  # <work>/old/variant has no siblings: ../full and ../../full don't exist.
  local iso_dir="$work/old/variant" verify_dir="$work/new"
  mkdir -p "$work/old" "$verify_dir"
  "$link" "$old_dir" "$iso_dir"
  local kib=$(( $(stat -c%s "$patch_file") / 1024 ))
  if ! "$patch_bin" "$iso_dir" "$patch_file" -o "$verify_dir/" 2>> "$log"; then
    fail_variant "$variant" "$kib" "geocoder-patch failed: $(tail -1 "$log")"
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

# A variant's estimates, in KiB, written to $2 as "weight diff_scratch
# verify_scratch": weight, its diff's peak memory; diff_scratch and
# verify_scratch, what its diff and its verify write to scratch. Nothing for a
# variant that skips or can't be sized, whose result it writes.
estimate_variant() {
  local variant=$1 to=$2 old_sizes new_sizes old_mapped old_scratch new_mapped new_scratch
  local new_dir="$new_root/$variant" old_dir="$old_root/$variant"
  local log="$log_dir/${variant//\//_}.log"
  # Only a fresh build (no previous output at all) skips. A file-count change
  # with a previous build present is a real diff the tools must reproduce.
  if [ "$(bin_count "$new_dir")" -lt 2 ] || [ "$(bin_count "$old_dir")" -eq 0 ]; then
    echo "SKIP" > "$results_dir/${variant//\//_}"
    return
  fi
  if ! old_sizes=$(logical_sizes "$old_dir" 2> "$log") || ! new_sizes=$(logical_sizes "$new_dir" 2>> "$log"); then
    fail_variant "$variant" 0 "geocoder-diff --logical-size failed: $(tail -1 "$log")"
    return
  fi
  read -r old_mapped old_scratch <<< "$old_sizes"
  read -r new_mapped new_scratch <<< "$new_sizes"
  echo "$(( $(side_weight_kib "$old_dir" "$old_mapped") + $(side_weight_kib "$new_dir" "$new_mapped") ))" \
    "$(( $(kib_of "$old_scratch") + $(kib_of "$new_scratch") ))" \
    "$(( $(kib_of "$(client_bytes "$new_dir")") + patcher_scratch_kib ))" > "$to"
}

# Every variant sized at once, max_jobs at a time, then read in in order.
# Without a dir for the estimates none could be sized: the script fails
# (mktemp says why) rather than leave every variant without a result.
declare -A weight=() diff_scratch=() verify_scratch=()
variants=($(cd "$new_root" && find . -name '*.bin' -printf '%h\n' | sed 's|^\./||' | sort -u))
estimates=$(mktemp -d "$scratch_root/estimates-XXXXXX") || exit 1
sizing=0
for variant in "${variants[@]}"; do
  if [ "$sizing" -ge "$max_jobs" ]; then
    wait -n
    sizing=$((sizing - 1))
  fi
  estimate_variant "$variant" "$estimates/${variant//\//_}" &
  sizing=$((sizing + 1))
done
wait
for variant in "${variants[@]}"; do
  [ -s "$estimates/${variant//\//_}" ] || continue
  read -r "weight[$variant]" "diff_scratch[$variant]" "verify_scratch[$variant]" < "$estimates/${variant//\//_}"
  echo "$variant: diff ${weight[$variant]} KiB of memory and ${diff_scratch[$variant]} KiB of scratch," \
    "verify ${verify_scratch[$variant]} KiB of scratch"
done
rm -rf "$estimates"

# A job's start or end, timestamped, on stderr.
log_job() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

# Queues the verify of a variant whose diff ended, its scratch need raised by
# the decompressed patch: the "Uncompressed patch: N bytes" line of the diff's
# log (none for a diff that failed, whose result is written).
queue_verify() {
  local variant=$1 raw
  raw=$(sed -n 's/^Uncompressed patch: \([0-9][0-9]*\) bytes.*/\1/p' "$log_dir/${variant//\//_}.log" 2> /dev/null | tail -1) || true
  verify_scratch[$variant]=$(( verify_scratch[$variant] + $(kib_of "${raw:-0}") ))
  verifies+=("$variant")
}

# running: pid -> memory weight held (verifies hold none); held: pid ->
# scratch held; stage/name: what it runs; began: when. A diff that ends
# queues its variant's verify.
declare -A running=() held=() stage=() name=() began=()
running_kib=0
held_kib=0

# Whether a job that writes $1 KiB to scratch may start. 1: not beside the
# running jobs. 2: not even alone; no_room then says why.
scratch_fits() {
  local need=$1 free
  free=$(scratch_free_kib)
  [ $((held_kib + need)) -le $((free - reserve_kib)) ] && return 0
  [ ${#running[@]} -gt 0 ] && return 1
  no_room="needs $need KiB of scratch, more than the $free KiB free less the reserve of $reserve_kib KiB"
  return 2
}

# Largest first, so the planet-scale diffs never pile up at the end.
pending=($(for v in "${!weight[@]}"; do echo "${weight[$v]} $v"; done | sort -rn | awk '{print $2}'))
next=0
verifies=()
while [ $next -lt ${#pending[@]} ] || [ ${#verifies[@]} -gt 0 ] || [ ${#running[@]} -gt 0 ]; do
  while [ ${#verifies[@]} -gt 0 ] && [ ${#running[@]} -lt "$max_jobs" ]; do
    variant=${verifies[0]}
    # Its diff failed: the result is written.
    if [ -e "$results_dir/${variant//\//_}" ]; then verifies=("${verifies[@]:1}"); continue; fi
    scratch_fits "${verify_scratch[$variant]}"
    case $? in
      1) break ;;
      2)
        fail_variant "$variant" $(( $(stat -c%s "$new_root/$variant/patch.gcpatch") / 1024 )) "the verify $no_room"
        verifies=("${verifies[@]:1}")
        continue
        ;;
    esac
    log_job "start verify $variant: ${verify_scratch[$variant]} KiB of scratch" \
      "(running: ${#running[@]} jobs, $running_kib KiB of memory, $held_kib KiB of scratch)"
    verify_variant "$variant" &
    running[$!]=0; held[$!]=${verify_scratch[$variant]}; stage[$!]=verify; name[$!]=$variant; began[$!]=$SECONDS
    held_kib=$((held_kib + verify_scratch[$variant]))
    verifies=("${verifies[@]:1}")
  done
  while [ $next -lt ${#pending[@]} ] && [ ${#running[@]} -lt "$max_jobs" ]; do
    variant=${pending[$next]}
    w=${weight[$variant]}
    # A diff bigger than the whole budget still runs, alone.
    [ ${#running[@]} -gt 0 ] && [ $((running_kib + w)) -gt "$budget_kib" ] && break
    scratch_fits "${diff_scratch[$variant]}"
    case $? in
      1) break ;;
      2)
        fail_variant "$variant" 0 "the diff $no_room"
        next=$((next + 1))
        continue
        ;;
    esac
    log_job "start diff $variant: $w KiB of memory and ${diff_scratch[$variant]} KiB of scratch" \
      "(running: ${#running[@]} jobs, $running_kib KiB of memory, $held_kib KiB of scratch)"
    diff_variant "$variant" &
    running[$!]=$w; held[$!]=${diff_scratch[$variant]}; stage[$!]=diff; name[$!]=$variant; began[$!]=$SECONDS
    running_kib=$((running_kib + w))
    held_kib=$((held_kib + diff_scratch[$variant]))
    next=$((next + 1))
  done
  # Every job left to start failed for its scratch.
  [ ${#running[@]} -eq 0 ] && continue
  # Only tracked jobs: bash 5.3's bare wait -n also returns process substitutions.
  finished=""
  wait -n -p finished "${!running[@]}" || true
  if [ -z "${finished:-}" ] || [ -z "${running[$finished]+x}" ]; then
    # Cannot tell which job ended: drain them all rather than launch past
    # the budgets.
    wait "${!running[@]}"
    for pid in "${!running[@]}"; do
      log_job "end ${stage[$pid]} ${name[$pid]} after $((SECONDS - began[$pid]))s"
      [ "${stage[$pid]}" = diff ] && queue_verify "${name[$pid]}"
    done
    running=(); held=(); stage=(); name=(); began=()
    running_kib=0
    held_kib=0
    continue
  fi
  log_job "end ${stage[$finished]} ${name[$finished]} after $((SECONDS - began[$finished]))s"
  running_kib=$((running_kib - running[$finished]))
  held_kib=$((held_kib - held[$finished]))
  [ "${stage[$finished]}" = diff ] && queue_verify "${name[$finished]}"
  unset "running[$finished]" "held[$finished]" "stage[$finished]" "name[$finished]" "began[$finished]"
done
