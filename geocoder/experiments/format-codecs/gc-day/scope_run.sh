#!/bin/bash
# scope_run.sh RES CORES OUT CMD... (run inside a systemd scope; adapted from the baseline step's
# scope_run.sh / /ssd/review/scratch/in_scope_io.sh). Runs CMD pinned to CORES under /usr/bin/time -v;
# prints one key=value line: rc, wall, user+sys, max RSS, sampled peak anonymous memory of any process
# in the scope, the scope's peak memory (page cache included), OOM kills, md4 bytes read / written by
# the scope at exit and after fsync'ing every output file, and the fsync time.
set -u
res=$1 cores=$2 out=$3; shift 3
cg=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
md=$(cat /sys/block/md4/dev)
iostat() { awk -v d=$md '$1 == d {for (i = 2; i <= NF; i++) {split($i, kv, "="); v[kv[1]] = kv[2]}} END {print v["rbytes"] + 0, v["wbytes"] + 0}' $cg/io.stat 2> /dev/null || echo "0 0"; }
read r0 w0 < <(iostat)
/usr/bin/time -v -o "$res.time" taskset -c "$cores" "$@" > "$res.log" 2>&1 &
tpid=$!
anon=0
while kill -0 $tpid 2> /dev/null; do
  for p in $(cat $cg/cgroup.procs 2> /dev/null); do
    a=$(awk '/^RssAnon/ {print $2}' /proc/$p/status 2> /dev/null)
    [ -n "$a" ] && [ "$a" -gt "$anon" ] && anon=$a
  done
  sleep 0.25
done
wait $tpid; rc=$?
read r1 w1 < <(iostat)
t0=$(date +%s.%N)
find "$out" -type f -print0 | xargs -0 -r sync
t1=$(date +%s.%N)
read r2 w2 < <(iostat)
wall=$(awk -F': ' '/Elapsed \(wall clock\)/ {n = split($2, p, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + p[i]; print s}' "$res.time")
user=$(awk -F': ' '/User time/ {print $2}' "$res.time"); sys=$(awk -F': ' '/System time/ {print $2}' "$res.time")
maxrss=$(awk -F': ' '/Maximum resident/ {print $2}' "$res.time")
printf "rc=%d wall_s=%s cpu_s=%.2f user_s=%s sys_s=%s maxrss_mib=%d anon_peak_mib=%d cgroup_peak_mib=%d oom_kills=%s read_gb=%.2f written_gb_at_exit=%.2f written_gb_after_fsync=%.2f fsync_s=%.1f\n" \
  $rc "$wall" "$(awk -v u=$user -v s=$sys 'BEGIN {print u + s}')" "$user" "$sys" $((maxrss / 1024)) $((anon / 1024)) \
  $(( $(cat $cg/memory.peak 2> /dev/null || echo 0) / 1048576 )) "$(awk '/^oom_kill / {print $2}' $cg/memory.events 2> /dev/null)" \
  "$(awk -v b=$((r1 - r0)) 'BEGIN {print b / 1e9}')" "$(awk -v b=$((w1 - w0)) 'BEGIN {print b / 1e9}')" \
  "$(awk -v b=$((w2 - w0)) 'BEGIN {print b / 1e9}')" "$(awk -v a=$t0 -v b=$t1 'BEGIN {print b - a}')"
