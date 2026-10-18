#!/bin/bash
# runq.sh <dir> <tag> : grid_query every q/pts_*.txt -> q/<tag>.<set>.jsonl (parallel over sets)
Q=/ssd/shrink/target/release/examples/grid_query
cd /ssd/shrink/max/streets
for p in q/pts_*.txt; do s=$(basename $p .txt); s=${s#pts_}; ( $Q $1 < $p > q/$2.$s.jsonl 2> q/$2.$s.err ) & done
wait
wc -l q/$2.*.jsonl
