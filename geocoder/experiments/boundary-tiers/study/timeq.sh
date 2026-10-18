#!/bin/bash
# timeq.sh <set: smallest|recommended> <name> ... : single-thread query latency of each candidate set
# under /ssd/shrink/sortpatch/TIMING.lock (waits for it). Pools: the first 50k points of p200k
# (photo-like) and all 20k of L2 (±200 m of country borders). Pinned to core 5, page cache warm
# (one untimed pass first), 3 timed passes, the fastest kept; the fastest of 3 one-point runs
# (process start + index load) is subtracted. CHAIN=1 behaviour for labels ending in g.
# Output: cmp/time_<set>.tsv (name, pool, points, best s, load s, µs per query).
set -u
st=$1; shift
T=/ssd/tiers2
GQ=$T/target/release/examples/grid_query
L=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "tiers2 timing $(date -Is)" > $L) 2> /dev/null; do sleep 10; done
trap 'rm -f $L' EXIT
head -50000 $T/pts/p200k.txt > $T/tmp/p50k.txt
cp $T/pts/L2.txt $T/tmp/L2t.txt
head -1 $T/pts/p200k.txt > $T/tmp/one.txt
out=$T/cmp/time_$st.tsv
best_of3() {  # best_of3 <set dir> <pool file>
  local best=999999 s e
  for r in 1 2 3; do
    s=$(date +%s.%N); taskset -c 5 $GQ "$1" < "$2" > /dev/null; e=$(date +%s.%N)
    best=$(awk -v a=$best -v s=$s -v e=$e 'BEGIN{b=e-s; print (b<a)?b:a}')
  done
  echo $best
}
for name in "$@"; do
  g=$name; chain=0
  if [[ $name == *g && -d $T/sets/${name%g} ]]; then g=${name%g}; chain=1; fi
  export GC_CHAIN_ADMIN=$chain
  d=$T/sets/$g/$st
  load=$(best_of3 $d $T/tmp/one.txt)
  for pool in p50k L2t; do
    taskset -c 5 $GQ $d < $T/tmp/$pool.txt > /dev/null
    best=$(best_of3 $d $T/tmp/$pool.txt)
    n=$(wc -l < $T/tmp/$pool.txt)
    printf '%s\t%s\t%s\t%.3f\t%.3f\t%.1f\n' "$name" "$pool" "$n" "$best" "$load" "$(awk -v b=$best -v l=$load -v n=$n 'BEGIN{print (b-l)*1e6/n}')" >> $out
  done
done
echo TIMEQ_DONE
