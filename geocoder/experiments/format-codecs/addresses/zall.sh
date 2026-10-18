#!/bin/bash
# zall.sh <dir> : zstd 1.5.7 -18 -T1 of every <dir>/*.bin in parallel (JOBS, default 8) -> <dir>/sizes.tsv
D=$1
job() { printf "%s\t%s\t%s\n" "$(basename $1 .bin)" "$(stat -c %s $1)" "$(/ssd/shrink/zstd157/bin/zstd -18 -T1 -q -c $1 | wc -c)"; }
export -f job
ls $D/*.bin | xargs -P ${JOBS:-8} -I{} bash -c "job {}" | sort > $D/sizes.tsv
