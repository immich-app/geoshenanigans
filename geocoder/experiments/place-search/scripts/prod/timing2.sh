#!/usr/bin/env bash
# Timing runs v2 (hold TIMING.lock): after the prefix table streams to disk, fst list values
# carry their best importance, and per-query memory is bounded. nice -20 against the other
# tracks' unpinned jobs.
#  1. builds: 1 core, run budgets 64 / 32 / 16 MiB; 8 cores; 1 core in 128 / 64 MiB cgroups
#  2. queries: 10k warm and cold (dropped caches) unconstrained; 1k cold in 32 / 64 / 128 MiB
set -uo pipefail
R=/ssd/search-prod
B=$R/target/release/place-search
W="/ssd/shrink/venv/bin/python $R/scripts/memwatch.py"
O=$R/results/timing2
N="nice -n -20"
mkdir -p $O $R/tmp/build-t
cd $R
head -1000 tmp/q10k.txt > tmp/q1k.txt
drop() { sync; echo 3 > /proc/sys/vm/drop_caches; sleep 2; }

build() { # tag set cores mem extra...
  local tag=$1 set=$2 cores=$3 mem=$4; shift 4
  rm -rf idx/$tag
  drop
  $W $O/build-$tag.json -- $N taskset -c 0-$((cores - 1)) $B build sets/$set idx/$tag --threads $cores --mem-mb $mem --tmp tmp/build-t "$@" 2> $O/build-$tag.log
}
cgbuild() { # tag set limit mem extra...
  local tag=$1 set=$2 lim=$3 mem=$4; shift 4
  rm -rf idx/$tag
  drop
  $W $O/build-$tag.json -- systemd-run --scope -q -p MemoryMax=$lim -p MemorySwapMax=0 \
    $N taskset -c 0 $B build sets/$set idx/$tag --threads 1 --mem-mb $mem --tmp tmp/build-t "$@" 2> $O/build-$tag.log
}

build min-1c minimal 1 32 --imp extras/minimal
build rec-1c-m64 recommended 1 64 --imp extras/recommended
build rec-1c recommended 1 32 --imp extras/recommended
build rec-1c-m16 recommended 1 16 --imp extras/recommended
build rec-8c recommended 8 64 --imp extras/recommended
build rec-x-1c recommended 1 32 --imp extras/recommended --extras extras/recommended/extra_names.bin
build max-1c max 1 32 --imp extras/max
build max-8c max 8 64 --imp extras/max
build maxn-1c max 1 32 --imp extras/max --poi-max-tier 2
cgbuild rec-cg128M recommended 128M 32 --imp extras/recommended
cgbuild rec-cg64M recommended 64M 16 --imp extras/recommended
cgbuild min-cg64M minimal 64M 16 --imp extras/minimal

query() { # tag set idx cgroup-limit warm|cold queries
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
query rec5-warm recommended rec-1c none warm tmp/q10k.txt --limit 5
for lim in 32M 64M 128M; do
  query rec-cg$lim-cold recommended rec-1c $lim cold tmp/q1k.txt
  query min-cg$lim-cold minimal min-1c $lim cold tmp/q1k.txt
done
query max-cg128M-cold max max-1c 128M cold tmp/q1k.txt
# reverse geocoding (grid_query, full query_json) before / after the find_admin changes, warm
for s in recommended max; do
  for v in pre lazy pip; do
    tmp/grid_query.$v sets/$s < tmp/pts_grid.txt > /dev/null
    /usr/bin/time -f "$s $v %e s %M kB" $N taskset -c 0 tmp/grid_query.$v sets/$s < tmp/pts_grid.txt > /dev/null 2>> $O/grid_times.txt
  done
done
echo done > $O/DONE
