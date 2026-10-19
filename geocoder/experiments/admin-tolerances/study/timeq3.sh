#!/bin/bash
# timeq3.sh <out tsv> <label=set dir>... : single-thread reverse-geocode latency per query under
# /ssd/shrink/sortpatch/TIMING.lock (waits for it), for each reader in READERS (default: main psp pspr =
# /ssd/tiers3/bin/grid_query.<r>) and each set. Pinned to core 7.
#   warm: page cache warm (one untimed pass), best of 3 timed passes over the pool, minus the best
#         of 3 one-point runs (process start + index load): p50k = first 50k of tiers2 p200k
#         (photo-like), L2 = tiers2 L2 (20k points within 200 m of country borders)
#   cold: caches dropped, 2,000 photo-like points (p200k 100001..102000), minus a cold one-point run
#         (caches dropped again before it): first-queries cost with every page read from disk
# Rows: reader label pool points best_s load_s us_per_query
set -u
out=$1; shift
T=/ssd/tiers3
L=/ssd/shrink/sortpatch/TIMING.lock
if [ -z "${HOLDS_LOCK:-}" ]; then  # HOLDS_LOCK=1: the caller already holds it
  until (set -o noclobber; echo "tiers3 timing $(date -Is)" > $L) 2> /dev/null; do sleep 15; done
  trap 'rm -f $L' EXIT
fi
head -50000 /ssd/tiers2/pts/p200k.txt > $T/tmp/p50k.txt
cp /ssd/tiers2/pts/L2.txt $T/tmp/L2.txt
sed -n 100001,102000p /ssd/tiers2/pts/p200k.txt > $T/tmp/cold2k.txt
head -1 /ssd/tiers2/pts/p200k.txt > $T/tmp/one.txt
run() { local s e; s=$(date +%s.%N); taskset -c 7 "$1" "$2" < "$3" > /dev/null 2>&1; e=$(date +%s.%N); awk -v s=$s -v e=$e 'BEGIN{printf "%.4f", e-s}'; }
best3() { local b=999999 x; for r in 1 2 3; do x=$(run "$1" "$2" "$3"); b=$(awk -v a=$b -v x=$x 'BEGIN{print (x<a)?x:a}'); done; echo $b; }
for r in ${READERS:-main psp pspr}; do
  GQ=$T/bin/grid_query.$r
  for item in "$@"; do
    label=${item%%=*}; d=${item#*=}
    load=$(best3 $GQ $d $T/tmp/one.txt)
    for pool in p50k L2; do
      run $GQ $d $T/tmp/$pool.txt > /dev/null
      b=$(best3 $GQ $d $T/tmp/$pool.txt)
      n=$(wc -l < $T/tmp/$pool.txt)
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%.1f\n' $r $label $pool $n $b $load $(awk -v b=$b -v l=$load -v n=$n 'BEGIN{print (b-l)*1e6/n}') >> $out
    done
    sync; echo 3 > /proc/sys/vm/drop_caches; cl=$(run $GQ $d $T/tmp/one.txt)
    sync; echo 3 > /proc/sys/vm/drop_caches; cb=$(run $GQ $d $T/tmp/cold2k.txt)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%.1f\n' $r $label cold2k 2000 $cb $cl $(awk -v b=$cb -v l=$cl 'BEGIN{print (b-l)*1e6/2000}') >> $out
  done
done
echo TIMEQ_DONE
