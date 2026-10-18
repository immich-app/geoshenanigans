#!/bin/bash
# dirs/reo_both = idx/max with both the spatially reordered street files and interp files
set -e
cd /ssd/shrink/max/streets
D=/ssd/shrink/max/streets/dirs/reo_both
mkdir -p $D
for f in /ssd/shrink/idx/max/*; do ln -f "$f" "$D/"; done
for R in reo/street reo/interp; do
  for f in $R/*.bin; do b=$(basename "$f"); if [ -e "/ssd/shrink/idx/max/$b" ]; then ln -f "$f" "$D/$b"; fi; done
done
ls -la $D | grep -E "interp_|street_|way_|addr_points"
./runq.sh $D reo_both
cd q && for s in 22k 35k addr interp street; do
  if cmp -s base.$s.jsonl reo_both.$s.jsonl; then echo "$s identical"; else echo "$s DIFF $(diff base.$s.jsonl reo_both.$s.jsonl | grep -c '^<')"; fi
done
