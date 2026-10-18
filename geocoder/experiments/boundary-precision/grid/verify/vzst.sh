#!/bin/bash
# zstd 1.5.7 -18 -T1 byte counts, 8 files in parallel. usage: vzst.sh label=file ... -> logs/zst.tsv
export TMPDIR=/ssd/shrink/tmp
V=/ssd/shrink/grid/vfy
Z=/ssd/shrink/zstd157/bin/zstd
printf '%s\n' "$@" | xargs -P ${JOBS:-8} -L 1 bash -c 'l=${0%%=*}; f=${0#*=}; n=$('"$Z"' -18 -T1 -q -c "$f" | wc -c); printf "%s\t%s\t%s\t%s\n" "$l" "$(stat -c %s "$f")" "$n" "$f" >> '"$V"'/logs/zst.tsv'
