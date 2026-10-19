#!/bin/bash
# rd_time.sh : reverse-geocode latency of main f48ddc4, 66a5ad0 and the reader tip 9b16065
# (verify6's own rg_bench builds) under TIMING.lock. rg_bench, single thread, core 2, nice -20.
# warm: one untimed pass + 3 timed passes, fastest per point (two rounds, readers interleaved)
# cold: the set's files evicted (posix_fadvise DONTNEED) before one timed pass.
# Modes: admin (Index::find_admin) and query (MultiIndex::query_json).
# Pools from rd30k: photo6k (lines 1-6000), border9k (21001-30000); b-max: photo3k (1-3000).
set -u
V=/ssd/verify6
L=/ssd/shrink/sortpatch/TIMING.lock
O=$V/rd/timing
mkdir -p $O
sed -n 1,6000p $V/pts/rd30k.txt > $V/pts/photo6k.txt
sed -n 21001,30000p $V/pts/rd30k.txt > $V/pts/border9k.txt
sed -n 1,3000p $V/pts/rd30k.txt > $V/pts/photo3k.txt
until ( set -o noclobber; echo "verify6 reader timing $(date -Is)" > $L ) 2>/dev/null; do sleep 10; done
echo "LOCKED $(date -Is)"
evict() { /ssd/shrink/venv/bin/python -I -c '
import os, sys
d = sys.argv[1]
for f in sorted(os.listdir(d)):
    p = os.path.realpath(os.path.join(d, f))
    fd = os.open(p, os.O_RDONLY); os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED); os.close(fd)
' "$1"; }
RD="f48ddc4 66a5ad0 9b16065"
run() {  # set pool
  local set=$1 pool=$2 dir=/ssd/bugs2/sets/base/$1
  for mode in admin query; do
    for round in 1 2; do
      for r in $RD; do
        nice -n -20 taskset -c 2 $V/bin/rg_bench.$r $dir $mode 3 < $V/pts/$pool.txt > $O/$set.$pool.$mode.warm$round.$r.json 2> $O/$set.$pool.$mode.warm$round.$r.err
      done
    done
    for r in $RD; do
      sync; evict $dir; sleep 1
      nice -n -20 taskset -c 2 $V/bin/rg_bench.$r $dir $mode 0 < $V/pts/$pool.txt > $O/$set.$pool.$mode.cold.$r.json 2> $O/$set.$pool.$mode.cold.$r.err
    done
    echo "$(date -Is) $set $pool $mode"
  done
}
run smallest photo6k
run recommended photo6k
run recommended border9k
run max photo3k
rm -f $L
echo "UNLOCKED $(date -Is)"
/ssd/shrink/venv/bin/python -I - << 'EOF'
import glob, json, os
O = "/ssd/verify6/rd/timing/"
rows = {}
for f in sorted(glob.glob(O + "*.json")):
    set_, pool, mode, kind, rev = os.path.basename(f)[:-5].split(".")
    try:
        d = json.load(open(f))
    except Exception:
        continue
    rows.setdefault((set_, pool, mode, kind), {})[rev] = d
print("| set | pool | call | cache | main f48ddc4 | 66a5ad0 | tip 9b16065 | tip / 66a5ad0 | tip / main |")
print("|---|---|---|---|---|---|---|---:|---:|")
for (s, p, m, k), r in rows.items():
    c = lambda v: f"{r[v]['mean_us']:.1f} / {r[v]['p99_us']:.0f}" if v in r else "-"
    ratio = lambda a, b: f"{r[a]['mean_us'] / r[b]['mean_us']:.2f}" if a in r and b in r else "-"
    print(f"| {s} | {p} | {'find_admin' if m == 'admin' else 'query_json'} | {k} | {c('f48ddc4')} | {c('66a5ad0')} | {c('9b16065')} | {ratio('9b16065', '66a5ad0')} | {ratio('9b16065', 'f48ddc4')} |")
EOF
echo RD_TIME_DONE
