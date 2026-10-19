#!/bin/bash
# queue2.sh: after the private set copies, run matrix2 on them (binaries built from the commits:
# s7 = 957ef7f, s7f = 6196c84 with the vendored fst; s5 = 09ef864, before the lazy country code).
cd /ssd/searchbuild
until grep -q "PSETS_DONE" logs/psets.log; do sleep 20; done
export SETS=/ssd/searchbuild/psets RUNS=/ssd/searchbuild/runs2
./matrix.sh logs/matrix2.log \
  base:min:cg32:16 s5:min:cg32:16 s7:min:cg32:16 s7:min:cg32:8 \
  base:rec:cg32:16 s7:rec:cg32:16 s7:rec:cg32:8 s7f:rec:cg32:16 \
  base:max:cg32:16 s7:max:cg32:16 s7:max:cg32:8 s7f:max:cg32:16 s7f:max:cg32:8 \
  s7:recx:cg32:16 s7:maxn:cg32:16 \
  base:min:free:16 s7:min:free:16 base:rec:free:16 s7:rec:free:16 base:max:free:16 s7:max:free:16 \
  s7:min:free:64 s7:rec:free:64 s7:max:free:64 s7:recx:free:16 s7:maxn:free:16
