# Shared helpers for the end-to-end step (sourced). Box only.
E=/ssd/shrink/sortpatch/e2e
SORT=/ssd/shrink/sortpatch/sort
ENC=$E/enc   # server's encoded days enc/<date>/planet/...: 10-12, 10-13 link to the codecs step's, 10-11 encoded here
PATCHES=/ssd/shrink/sortpatch/patch/out/v8     # v8 sorted-day patches: <new>/planet_{full,quality_uncapped,poi_all}/patch.gcpatch
GC=/ssd/shrink/sortpatch/codecs/bin/gc
PATCH8=/ssd/shrink/sortpatch/patch/bin/geocoder-patch8
PATCH7=/ssd/shrink/sortpatch/baseline/bin/geocoder-patch
DIFF8=/ssd/shrink/sortpatch/patch/bin/geocoder-diff8
DIFF7=/ssd/shrink/sortpatch/baseline/bin/geocoder-diff
CANON=$SORT/bin/canon_sort
TOOLS=/ssd/shrink/repo/geocoder/builder/tools
LOCK=/ssd/shrink/sortpatch/TIMING.lock
VARIANTS="planet/quality/uncapped planet/poi/all planet/full"
export PATH=/ssd/shrink/zstd157/bin:$PATH   # geocoder-patch shells out to zstd (CI puts 1.5.7 first)
export TMPDIR=/ssd/shrink/tmp
MY_TAG="e2e-$$"

ts() { date +%T; }
safe() { echo "${1//\//_}"; }

lock_acquire() {  # description
  local waited=0
  until (set -o noclobber; echo "$MY_TAG $(date +%s) $1" > "$LOCK") 2> /dev/null; do
    if [ $((waited % 300)) -eq 0 ]; then echo "  [$(ts)] waiting for TIMING.lock: $(cat "$LOCK" 2> /dev/null)"; fi
    sleep 5; waited=$((waited + 5))
  done
}
lock_release() {
  if [ -f "$LOCK" ] && [ "$(awk '{print $1}' "$LOCK")" = "$MY_TAG" ]; then rm -f "$LOCK"; fi
}
trap lock_release EXIT

drop_caches() { sync; echo 3 > /proc/sys/vm/drop_caches; }

sib_snapshot() { awk '/^cpu(2[4-9]|3[01]) / {busy += $2 + $3 + $4 + $7 + $8; tot += $2 + $3 + $4 + $5 + $6 + $7 + $8} END {print busy, tot}' /proc/stat; }
sib_busy_pct() {
  read b0 t0 <<< "$1"; read b1 t1 <<< "$2"
  awk -v b=$((b1 - b0)) -v t=$((t1 - t0)) 'BEGIN {printf "%.0f", (t > 0 ? 100 * b / t : 0)}'
}
# Waits (up to 2 h) until the SMT siblings of the timing cores (24-31) are under 10% busy.
wait_quiet_siblings() {
  local waited=0 a b pct
  while [ $waited -lt 7200 ]; do
    a=$(sib_snapshot); sleep 3; b=$(sib_snapshot)
    pct=$(sib_busy_pct "$a" "$b")
    [ "$pct" -lt 10 ] && return 0
    if [ $((waited % 300)) -eq 0 ]; then echo "  [$(ts)] waiting for quiet siblings 24-31 (${pct}% busy)"; fi
    sleep 27; waited=$((waited + 30))
  done
  echo "  [$(ts)] siblings still busy after 2 h, timing anyway"
}

need_free_gb() {  # GB: waits until /ssd has GB + 40 GB free
  local need_kb=$(( ($1 + 40) * 1000000 ))
  until [ "$(df -k --output=avail /ssd | tail -1)" -ge "$need_kb" ]; do
    echo "  [$(ts)] waiting for disk: need $(( $1 + 40 )) GB free"; sleep 60
  done
}

# Families gc encodes per variant (from the encoded day's gc.manifest).
families_of() { awk '$1 == "gc.families" {print $2}' "$1/gc.manifest"; }
