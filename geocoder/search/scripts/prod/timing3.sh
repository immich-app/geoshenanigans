#!/usr/bin/env bash
# Timing runs v3 (hold TIMING.lock): queries with the final reverse-geocoder changes
# (specialised encoded PIP, query-cell-only find_admin). Same protocol as v2.
set -uo pipefail
R=/ssd/search-prod
B=$R/target/release/place-search
W="/ssd/shrink/venv/bin/python $R/scripts/memwatch.py"
O=$R/results/timing3
N="nice -n -20"
mkdir -p $O
cd $R
drop() { sync; echo 3 > /proc/sys/vm/drop_caches; sleep 2; }
query() { # tag set idx cgroup-limit warm|cold queries [extra args]
  local tag=$1 set=$2 idx=$3 lim=$4 mode=$5 q=$6
  shift 6
  local wrap=()
  [ "$lim" != "none" ] && wrap=(systemd-run --scope -q -p MemoryMax=$lim -p MemorySwapMax=0)
  local args=(--iso extras/$set/iso_codes.tsv --bench 1 --lat-out $O/q-$tag.lat)
  [ "$mode" = cold ] && args+=(--no-warmup)
  args+=("$@")
  drop
  $W $O/q-$tag.mem.json -- "${wrap[@]}" $N taskset -c 0 $B query sets/$set idx/$idx "${args[@]}" < $q > $O/q-$tag.json 2> $O/q-$tag.log
}
for s in min:minimal:min-1c rec:recommended:rec-1c max:max:max-1c maxn:max:maxn-1c; do
  IFS=: read t set idx <<< "$s"
  query $t-warm $set $idx none warm tmp/q10k.txt
  query $t-cold $set $idx none cold tmp/q10k.txt
done
query recx-warm recommended rec-x-1c none warm tmp/q10k.txt --extras extras/recommended/extra_names.bin
query rec5-warm recommended rec-1c none warm tmp/q10k.txt --limit 5
for lim in 32M 64M 128M; do
  query rec-cg$lim-cold recommended rec-1c $lim cold tmp/q1k.txt
  query min-cg$lim-cold minimal min-1c $lim cold tmp/q1k.txt
done
query max-cg128M-cold max max-1c 128M cold tmp/q1k.txt
query rec5-cg64M-cold recommended rec-1c 64M cold tmp/q1k.txt --limit 5
for s in recommended max; do
  for v in pre lazy pip2 final; do
    tmp/grid_query.$v sets/$s < tmp/pts_grid.txt > /dev/null
    /usr/bin/time -f "$s $v %e s %M kB" $N taskset -c 0 tmp/grid_query.$v sets/$s < tmp/pts_grid.txt > /dev/null 2>> $O/grid_times.txt
  done
done
echo done > $O/DONE
