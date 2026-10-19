#!/bin/bash
# batch3.sh <name> ... : gaps and overlaps between same-level polygons (cover) on the border points
# (L2 + L4 + L8 + the verifier's 10k border points, 70k), admin-minimal sets, truth = unc.
set -u
cd /ssd/arcs
[ -f pts/borders70k.txt ] || { cat pts/L2.txt pts/L4.txt pts/L8.txt; head -10000 pts/fresh20k.txt; } > pts/borders70k.txt
for n in "$@"; do
  OMP_NUM_THREADS=32 ./cover sets/unc/smallest sets/$n/smallest pts/borders70k.txt > cmp/$n.cover.json
  echo "COVER $n $(cat cmp/$n.cover.json)"
done
echo BATCH3_DONE
