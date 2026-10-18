#!/bin/bash
# Usage (inside a systemd scope): scope_run.sh RES_DIR CORES OUT_DIR TMP_DIR CMD...
# Adapted from /ssd/review/scratch/in_scope_io.sh. Runs CMD pinned to CORES
# under /usr/bin/time -v and prints one key=value line: wall/CPU/max RSS,
# sampled peak anonymous memory, the scope's peak memory (page cache
# included), OOM kills, the scope's md4 bytes read/written (io.stat) when CMD
# exits and again after fsync'ing every output file, the fsync time, and the
# sampled peak disk used by OUT_DIR + TMP_DIR (du, allocated blocks).
set -u
res=$1 cores=$2 out=$3 tmp=$4; shift 4
cg=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
md=$(cat /sys/block/md4/dev)
iostat() { awk -v d=$md '$1 == d {for (i = 2; i <= NF; i++) {split($i, kv, "="); v[kv[1]] = kv[2]}} END {print v["rbytes"] + 0, v["wbytes"] + 0}' $cg/io.stat 2> /dev/null || echo "0 0"; }
read r0 w0 < <(iostat)
/usr/bin/time -v -o "$res.time" taskset -c "$cores" "$@" > /dev/null 2> "$res.log" &
tpid=$!
anon=0 disk=0 n=0
while kill -0 $tpid 2> /dev/null; do
  for p in $(pgrep -P $tpid); do
    a=$(awk '/^RssAnon/ {print $2}' /proc/$p/status 2> /dev/null)
    [ -n "$a" ] && [ "$a" -gt "$anon" ] && anon=$a
  done
  if [ $((n % 2)) -eq 0 ]; then
    d=$(du -skc "$out" "$tmp" 2> /dev/null | tail -1 | awk '{print $1}')
    [ -n "$d" ] && [ "$d" -gt "$disk" ] && disk=$d
  fi
  n=$((n + 1))
  sleep 0.25
done
wait $tpid; rc=$?
read r1 w1 < <(iostat)
d=$(du -skc "$out" "$tmp" 2> /dev/null | tail -1 | awk '{print $1}')
[ -n "$d" ] && [ "$d" -gt "$disk" ] && disk=$d
t0=$(date +%s.%N)
sync "$out"/* 2> /dev/null
t1=$(date +%s.%N)
read r2 w2 < <(iostat)
printf "rc=%d anon_peak_mib=%d cgroup_peak_mib=%d oom_kills=%s read_gb=%.2f written_gb_at_exit=%.2f written_gb_after_fsync=%.2f fsync_s=%.1f peak_disk_gb=%.2f out_gb=%.2f\n" \
  $rc $((anon / 1024)) $(( $(cat $cg/memory.peak 2> /dev/null || echo 0) / 1048576 )) \
  "$(awk '/^oom_kill / {print $2}' $cg/memory.events 2> /dev/null)" \
  "$(awk -v b=$((r1 - r0)) 'BEGIN {print b / 1e9}')" "$(awk -v b=$((w1 - w0)) 'BEGIN {print b / 1e9}')" \
  "$(awk -v b=$((w2 - w0)) 'BEGIN {print b / 1e9}')" "$(awk -v a=$t0 -v b=$t1 'BEGIN {print b - a}')" \
  "$(awk -v k=$disk 'BEGIN {print k * 1024 / 1e9}')" "$(du -sk --apparent-size "$out" | awk '{print $1 * 1024 / 1e9}')"
