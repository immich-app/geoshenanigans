#!/bin/bash
# timeq.sh <set: smallest|recommended> <label>=<reader>:<set name> ... : single-thread query latency
# under /ssd/shrink/sortpatch/TIMING.lock (waits for it, removes it after).
#   readers: r0 = fix/admin-nesting (today's reader, /ssd/arcs/target)
#            r1 = r0 + search-prod's four admin reader commits (/ssd/arcs/target-r, per-polygon files)
#            r2 = the same binary on the arc format (set name ending in "a": sets/<name>a)
# warm: pools p50k (first 50k of p200k, photo-like) and L2 (20k, +-200 m of country borders), pinned
#       to core 5, one untimed pass, best of 3 timed passes, minus the best of 3 one-point runs
#       (process start + index load).
# cold: echo 3 > drop_caches, then one timed pass over p10k (first 10k of p200k), minus a cold
#       one-point run (also after drop_caches).
# Output: cmp/time_<set>.tsv (label, pool, points, best s, load s, us per query).
set -u
st=$1; shift
A=/ssd/arcs
L=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "arcs timing $(date -Is)" > $L) 2> /dev/null; do sleep 10; done
trap 'rm -f $L' EXIT
mkdir -p $A/tmp/t
head -50000 $A/pts/p200k.txt > $A/tmp/t/p50k.txt
head -10000 $A/pts/p200k.txt > $A/tmp/t/p10k.txt
cp $A/pts/L2.txt $A/tmp/t/L2.txt
head -1 $A/pts/p200k.txt > $A/tmp/t/one.txt
out=$A/cmp/time_$st.tsv
best_of3() {  # best_of3 <bin> <set dir> <pool file>
  local best=999999 s e
  for r in 1 2 3; do
    s=$(date +%s.%N); taskset -c 5 $1 "$2" < "$3" > /dev/null; e=$(date +%s.%N)
    best=$(awk -v a=$best -v s=$s -v e=$e 'BEGIN{b=e-s; print (b<a)?b:a}')
  done
  echo $best
}
once() {  # once <bin> <set dir> <pool file> : wall seconds of one run
  local s e
  s=$(date +%s.%N); taskset -c 5 $1 "$2" < "$3" > /dev/null; e=$(date +%s.%N)
  awk -v s=$s -v e=$e 'BEGIN{print e-s}'
}
for spec in "$@"; do
  label=${spec%%=*}; rest=${spec#*=}; reader=${rest%%:*}; name=${rest#*:}
  case $reader in r0) GQ=$A/target/release/examples/grid_query ;; r3) GQ=$A/target-r3/release/examples/grid_query ;; r5) GQ=$A/target-r5/release/examples/grid_query ;; *) GQ=$A/target-r/release/examples/grid_query ;; esac
  d=$A/sets/$name/$st
  [ -d "$d" ] || { echo "no set $d"; continue; }
  load=$(best_of3 $GQ $d $A/tmp/t/one.txt)
  for pool in p50k L2; do
    taskset -c 5 $GQ $d < $A/tmp/t/$pool.txt > /dev/null
    best=$(best_of3 $GQ $d $A/tmp/t/$pool.txt)
    n=$(wc -l < $A/tmp/t/$pool.txt)
    printf '%s\t%s\t%s\t%.3f\t%.3f\t%.1f\n' "$label" "$pool" "$n" "$best" "$load" "$(awk -v b=$best -v l=$load -v n=$n 'BEGIN{print (b-l)*1e6/n}')" >> $out
  done
  sync; echo 3 > /proc/sys/vm/drop_caches
  cload=$(once $GQ $d $A/tmp/t/one.txt)
  sync; echo 3 > /proc/sys/vm/drop_caches
  cold=$(once $GQ $d $A/tmp/t/p10k.txt)
  printf '%s\t%s\t%s\t%.3f\t%.3f\t%.1f\n' "$label" "p10k_cold" 10000 "$cold" "$cload" "$(awk -v b=$cold -v l=$cload 'BEGIN{print (b-l)*1e6/10000}')" >> $out
done
echo TIMEQ_DONE
