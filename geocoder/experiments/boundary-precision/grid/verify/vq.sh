#!/bin/bash
# grid_query on the fresh sample for each name=dir, JOBS at a time -> q/<name>.fresh.jsonl
cd /ssd/shrink/grid/vfy
export GQ=/ssd/shrink/target/release/examples/grid_query
printf '%s\n' "$@" | xargs -P ${JOBS:-2} -L 1 bash -c 'n=${0%%=*}; d=${0#*=}; s=$(date +%s); "$GQ" "$d" < pts/fresh.txt > q/$n.fresh.jsonl 2> q/$n.fresh.err; echo "$n $d $(wc -l < q/$n.fresh.jsonl) answers $(($(date +%s)-s)) s stderr $(wc -c < q/$n.fresh.err) B" >> logs/vq.log'
