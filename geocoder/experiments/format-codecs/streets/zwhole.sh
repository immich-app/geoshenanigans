#!/bin/bash
# zwhole.sh <out.tsv> files... : zstd 1.5.7 -18 -T${T:-16} whole-file compressed sizes, sequential
Z=/ssd/shrink/zstd157/bin/zstd
out=$1; shift
for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -c %s "$f") $($Z -18 -T${T:-16} -q -c "$f" | wc -c) >> $out; done
