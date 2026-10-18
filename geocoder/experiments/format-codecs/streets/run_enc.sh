#!/bin/bash
# encode + verify + sample each variant (no full outputs unless FULL=1); one process per variant
#   SRC: dir with the source ways/nodes (default idx/max); OUT: output dir (default enc/<kind>)
cd /ssd/shrink/max/streets
kind=$1; shift
SRC=${SRC:-/ssd/shrink/idx/max}
if [ $kind = street ]; then S=12; WF=street_ways.bin; NF=street_nodes.bin; else S=24; WF=interp_ways.bin; NF=interp_nodes.bin; fi
OUT=${OUT:-enc/$kind}
mkdir -p $OUT
NOFULL_ENV="NOFULL=1"; [ -n "$FULL" ] && NOFULL_ENV=""
for v in "$@"; do echo $v; done | xargs -P ${JOBS:-8} -I{} bash -c "$NOFULL_ENV ./nodeenc enc $S $SRC/$WF $SRC/$NF {} $OUT 8 0.06 > $OUT/{}.log 2>&1"
for v in "$@"; do cat $OUT/$v.log; done
