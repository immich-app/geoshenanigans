#!/bin/bash
# run entenc for street (col 0) and interp (col 2) entries; args: variants...; REMAP_S/REMAP_I optional remap tables; OUTD dir
cd /ssd/shrink/max/streets
OUTD=${OUTD:-ent}; mkdir -p $OUTD
G=/ssd/shrink/idx/max/geo_cells.bin
for v in "$@"; do
  ./entenc $G 0 ${SE:-/ssd/shrink/idx/max/street_entries.bin} $v $OUTD/street_$v.bin $REMAP_S > $OUTD/street_$v.log 2>&1 &
  ./entenc $G 2 ${IE:-/ssd/shrink/idx/max/interp_entries.bin} $v $OUTD/interp_$v.bin $REMAP_I > $OUTD/interp_$v.log 2>&1 &
done
wait
cat $OUTD/*.log
