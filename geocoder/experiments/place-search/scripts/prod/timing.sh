#!/usr/bin/env bash
# Timing runs (hold /ssd/shrink/sortpatch/TIMING.lock while this runs).
#  1. index builds: 1 core (taskset) and 8 cores; peak anonymous memory by memwatch; 1 core
#     inside 256 MiB and 128 MiB cgroups (recommended places)
#  2. queries: 10k queries warm and from dropped caches, unconstrained and in 32/64 MiB cgroups
set -uo pipefail
R=/ssd/search-prod
B=$R/target/release/place-search
W="/ssd/shrink/venv/bin/python $R/scripts/memwatch.py"
O=$R/results/timing
mkdir -p $O $R/tmp/build-t
cd $R
drop() { sync; echo 3 > /proc/sys/vm/drop_caches; sleep 2; }

build() { # tag set cores mem extra...
  local tag=$1 set=$2 cores=$3 mem=$4; shift 4
  rm -rf idx/$tag
  drop
  $W $O/build-$tag.json -- taskset -c 0-$((cores - 1)) $B build sets/$set idx/$tag --threads $cores --mem-mb $mem --tmp tmp/build-t "$@" 2> $O/build-$tag.log
}

# ---- builds (final variant: builder flags E1-E4 + importance byte)
build min-1c minimal 1 64 --imp extras/minimal
build min-8c minimal 8 64 --imp extras/minimal
build rec-1c recommended 1 64 --imp extras/recommended
build rec-8c recommended 8 64 --imp extras/recommended
build rec-x-1c recommended 1 64 --imp extras/recommended --extras extras/recommended/extra_names.bin
build max-1c max 1 64 --imp extras/max
build max-8c max 8 64 --imp extras/max
build maxn-1c max 1 64 --imp extras/max --poi-max-tier 2
for lim in 256M 128M; do
  rm -rf idx/rec-cg$lim
  drop
  $W $O/build-rec-cg$lim.json -- systemd-run --scope -q -p MemoryMax=$lim -p MemorySwapMax=0 \
    taskset -c 0 $B build sets/recommended idx/rec-cg$lim --sub places --threads 1 --mem-mb 32 --tmp tmp/build-t --imp extras/recommended \
    2> $O/build-rec-cg$lim.log
done

# ---- queries: 10k, limit 20
query() { # tag set idx cgroup-limit warm?(warm|cold)
  local tag=$1 set=$2 idx=$3 lim=$4 mode=$5
  local wrap=()
  [ "$lim" != "none" ] && wrap=(systemd-run --scope -q -p MemoryMax=$lim -p MemorySwapMax=0)
  local args=(--iso extras/$set/iso_codes.tsv --bench 1 --lat-out $O/q-$tag.lat)
  [ "$mode" = cold ] && args+=(--no-warmup)
  drop
  $W $O/q-$tag.mem.json -- "${wrap[@]}" taskset -c 0 $B query sets/$set idx/$idx "${args[@]}" < tmp/q10k.txt > $O/q-$tag.json 2> $O/q-$tag.log
}
for s in min:minimal:min-1c rec:recommended:rec-1c max:max:max-1c; do
  IFS=: read t set idx <<< "$s"
  query $t-warm $set $idx none warm
  query $t-cold $set $idx none cold
done
query rec-cg32-cold recommended rec-1c 32M cold
query rec-cg32-warm recommended rec-1c 32M warm
query rec-cg64-cold recommended rec-1c 64M cold
query min-cg32-cold minimal min-1c 32M cold
query max-cg32-cold max max-1c 32M cold
echo done > $O/DONE
