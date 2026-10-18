# Shared helpers for the baseline step (sourced). Box only.
B=/ssd/shrink/sortpatch/baseline
DAYS=/ssd/shrink/days
LOCK=/ssd/shrink/sortpatch/TIMING.lock
TOOLS=/ssd/shrink/repo/geocoder/builder/tools
export PATH=/ssd/shrink/zstd157/bin:$PATH   # CI puts zstd 1.5.7 first on PATH
export TMPDIR=/ssd/shrink/tmp
MY_TAG="baseline-$$"

ts() { date +%T; }

# Atomic create; waits while another step holds it.
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

# Busy jiffies / total jiffies of the SMT siblings of the timing cores (24-31),
# so a timing run can say whether its physical cores were shared.
sib_snapshot() { awk '/^cpu(2[4-9]|3[01]) / {busy += $2 + $3 + $4 + $7 + $8; tot += $2 + $3 + $4 + $5 + $6 + $7 + $8} END {print busy, tot}' /proc/stat; }
sib_busy_pct() {  # "b0 t0" "b1 t1"
  read b0 t0 <<< "$1"; read b1 t1 <<< "$2"
  awk -v b=$((b1 - b0)) -v t=$((t1 - t0)) 'BEGIN {printf "%.0f", (t > 0 ? 100 * b / t : 0)}'
}

# Pulls key fields out of a /usr/bin/time -v file as key=value.
time_fields() {
  awk -F': ' '
    /Elapsed \(wall clock\)/ {n = split($2, p, ":"); w = 0; for (i = 1; i <= n; i++) w = w * 60 + p[i]; printf "wall=%.2f ", w}
    /User time/ {u = $2} /System time/ {s = $2}
    /Maximum resident set size/ {printf "maxrss_mib=%.0f ", $2 / 1024}
    /File system inputs/ {printf "fs_in_gb=%.2f ", $2 * 512 / 1e9}
    /File system outputs/ {printf "fs_out_gb=%.2f ", $2 * 512 / 1e9}
    /Exit status/ {printf "exit=%s ", $2}
    END {printf "user=%.2f sys=%.2f cpu=%.2f", u, s, u + s}' "$1"
}
