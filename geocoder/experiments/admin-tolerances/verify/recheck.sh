#!/bin/bash
# recheck.sh : interleaved re-timing of the headline cells (D1 admin-minimal, q2.5+cap vs r1, readers
# main / pspr / nest3) under TIMING.lock, so both schemes see the same background load: per pool, one
# untimed pass of each (reader, set), then 5 rounds alternating every (reader, set); fastest round per
# cell minus the fastest of 5 one-point runs, per query. Core 11. Load average logged per round.
set -u
V=/ssd/verify5
cd $V
L=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "verify5 recheck $(date -Is)" > $L) 2> /dev/null; do sleep 1; done
trap 'grep -q "^verify5" $L && rm -f $L' EXIT
echo "LOCKED $(date -Is)"
out=cmp/time_recheck.tsv; : > $out
run() { local s e; s=$(date +%s.%N); taskset -c 11 "$1" "$2" < "$3" > /dev/null 2>&1; e=$(date +%s.%N); awk -v s=$s -v e=$e 'BEGIN{printf "%.4f", e-s}'; }
head -1 pts/time/D1_p50k.txt > tmp/one.txt
cells=()
for r in main pspr nest3; do for s in qcapB r1B; do cells+=("$r:$s"); done; done
declare -A load best
for c in "${cells[@]}"; do
  r=${c%%:*}; s=${c#*:}; b=999
  for k in 1 2 3 4 5; do x=$(run bin/grid_query.$r sets/$s/smallest tmp/one.txt); b=$(awk -v a=$b -v x=$x 'BEGIN{print (x<a)?x:a}'); done
  load[$c]=$b
done
for pool in pts/time/D1_p50k.txt pts/time/D1_L2.txt; do
  for c in "${cells[@]}"; do r=${c%%:*}; s=${c#*:}; run bin/grid_query.$r sets/$s/smallest $pool > /dev/null; best[$c]=999; done
  for k in 1 2 3 4 5; do
    echo "round $k $(basename $pool) load $(cut -d' ' -f1 /proc/loadavg)"
    for c in "${cells[@]}"; do
      r=${c%%:*}; s=${c#*:}
      x=$(run bin/grid_query.$r sets/$s/smallest $pool)
      best[$c]=$(awk -v a=${best[$c]} -v x=$x 'BEGIN{print (x<a)?x:a}')
    done
  done
  n=$(wc -l < $pool)
  for c in "${cells[@]}"; do
    printf '%s\t%s\t%s\t%s\t%.1f\n' $c $(basename $pool .txt) ${best[$c]} ${load[$c]} $(awk -v b=${best[$c]} -v l=${load[$c]} -v n=$n 'BEGIN{print (b-l)*1e6/n}') >> $out
  done
done
cat $out
echo "RECHECK_DONE $(date -Is)"
