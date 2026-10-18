# Shared helpers for the sorted-patch step (sourced). Box only.
# Copy of /ssd/shrink/sortpatch/baseline/common.sh with this step's paths.
B=/ssd/shrink/sortpatch/patch
SORT=/ssd/shrink/sortpatch/sort
DAYS=$SORT/days          # canonical sorted days, published layout
TREE=$SORT/tree          # same days + planet/quality/q2.5 (diff borrows admin from it)
META=$SORT/meta          # identity sidecars: <date>/{street,interp,addr}.slot.u32
LOCK=/ssd/shrink/sortpatch/TIMING.lock
TOOLS=/ssd/shrink/repo/geocoder/builder/tools
export PATH=/ssd/shrink/zstd157/bin:$PATH   # CI puts zstd 1.5.7 first on PATH
export TMPDIR=/ssd/shrink/tmp
MY_TAG="sortpatch-$$"

ts() { date +%T; }

# Tool sets: v7 = today's tools (baseline step's build of main),
# v8 = this step's identity/derived-remap tools, v8c = v8 tools without the
# identity sidecar (content matching only).
# v8s = v8 tools on the PUBLISHED stable-slot days (baseline step's tree),
# identity = slot (meta-pub, gen_pub_identity.py).
diff_bin() { case $1 in v7) echo /ssd/shrink/sortpatch/baseline/bin/geocoder-diff ;; v8|v8c|v8s) echo $B/bin/geocoder-diff8 ;; esac; }
diff_sec_bin() { case $1 in v7) echo /ssd/shrink/sortpatch/baseline/bin/geocoder-diff-sec ;; v8|v8c|v8s) echo $B/bin/geocoder-diff8-sec ;; esac; }
patch_bin() { case $1 in v7) echo /ssd/shrink/sortpatch/baseline/bin/geocoder-patch ;; v8|v8c|v8s) echo $B/bin/geocoder-patch8 ;; esac; }
diff_extra() {  # tool old new
  case $1 in v8) echo "--identity $META/$2 $META/$3" ;; v8s) echo "--identity $B/meta-pub/$2 $B/meta-pub/$3" ;; *) echo "" ;; esac
}
tree_of() { case $1 in v8s) echo /ssd/shrink/sortpatch/baseline/tree ;; *) echo $TREE ;; esac; }
days_of() { case $1 in v8s) echo /ssd/shrink/days ;; *) echo $DAYS ;; esac; }

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

# Waits (up to 2 h) until the SMT siblings of the timing cores (24-31) are
# under 10% busy over 3 s: another step's untimed work there (no lock) slows
# CPU-bound timing runs on 56-63. Called before lock_acquire and again after.
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

sib_snapshot() { awk '/^cpu(2[4-9]|3[01]) / {busy += $2 + $3 + $4 + $7 + $8; tot += $2 + $3 + $4 + $5 + $6 + $7 + $8} END {print busy, tot}' /proc/stat; }
sib_busy_pct() {  # "b0 t0" "b1 t1"
  read b0 t0 <<< "$1"; read b1 t1 <<< "$2"
  awk -v b=$((b1 - b0)) -v t=$((t1 - t0)) 'BEGIN {printf "%.0f", (t > 0 ? 100 * b / t : 0)}'
}

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
