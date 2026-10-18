#!/bin/bash
# timeq.sh : single-thread query latency (plain reader built here) under /ssd/shrink/sortpatch/TIMING.lock
# (waits up to 40 min, else gives up). Pools: fresh photo10k, fresh border10k. Pinned to core 7,
# one warm untimed pass, best of 3 timed passes; best of 3 one-point runs (start + load) subtracted.
V=/ssd/tiers2-verify; GQ=$V/target/release/examples/grid_query; L=/ssd/shrink/sortpatch/TIMING.lock
awk "{print \$1, \$2}" $V/pts/border10k.meta > $V/pts/border10k.txt; head -1 $V/pts/photo10k.txt > $V/pts/one.txt
for i in $(seq 240); do (set -o noclobber; echo "tiers2-verify timing $(date -Is)" > $L) 2> /dev/null && break; sleep 10; done
grep -q tiers2-verify $L 2>/dev/null || { echo "LOCK_TIMEOUT"; exit 1; }
trap "rm -f $L" EXIT
echo "lock taken $(date -Is) load $(cat /proc/loadavg)"
best3() { local b=999 s e; for r in 1 2 3; do s=$(date +%s.%N); taskset -c 7 $GQ "$1" < "$2" > /dev/null; e=$(date +%s.%N); b=$(awk -v a=$b -v s=$s -v e=$e "BEGIN{d=e-s; print (d<a)?d:a}"); done; echo $b; }
for st in smallest recommended; do for s in q25 cap10 rec small; do
  d=$V/sets/$s/$st; load=$(best3 $d $V/pts/one.txt)
  for p in photo10k border10k; do taskset -c 7 $GQ $d < $V/pts/$p.txt > /dev/null; b=$(best3 $d $V/pts/$p.txt); n=$(wc -l < $V/pts/$p.txt)
    printf "%s\t%s\t%s\t%s\t%.3f\t%.3f\t%.1f\n" $st $s $p $n $b $load $(awk -v b=$b -v l=$load -v n=$n "BEGIN{print (b-l)*1e6/n}"); done
done; done > $V/cmp/time.tsv
echo "TIMEQ_DONE $(date -Is) load $(cat /proc/loadavg)"
