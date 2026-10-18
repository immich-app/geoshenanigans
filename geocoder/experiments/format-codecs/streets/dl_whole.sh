#!/bin/bash
# dl_whole.sh <src_dir> <tag> <stride 12|24> <opts> : whole-file download-only codec
# (nodedl streams for ways+nodes), inverse verified, zstd -18 size appended to dl/whole.tsv
set -e
cd /ssd/shrink/max/streets
SRC=$1; TAG=$2; S=$3; OPTS=$4
if [ $S = 12 ]; then WF=street_ways.bin; NF=street_nodes.bin; else WF=interp_ways.bin; NF=interp_nodes.bin; fi
mkdir -p dl/whole
OUT=dl/whole/$TAG.bin
./nodedl $S $SRC/$WF $SRC/$NF $OUT $OPTS > dl/whole/$TAG.log
cat dl/whole/$TAG.log
printf "%s\t%s\t%s\t%s\n" $TAG "$OPTS" $(stat -c %s $OUT) $(/ssd/shrink/zstd157/bin/zstd -18 -T${ZT:-12} -q -c $OUT | wc -c) >> dl/whole.tsv
rm -f /ssd/shrink/max/streets/dl/whole/$TAG.bin
