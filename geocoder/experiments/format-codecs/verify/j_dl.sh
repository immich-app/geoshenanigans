#!/bin/bash
# Streets download-only codec (nodedl, today's way order) re-run: inverse check + whole-file zstd -18 -T16.
set -uo pipefail
V=/ssd/shrink/max/verify
D=$V/dl
I=/ssd/shrink/idx/max
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
mkdir -p $D && cd $D
cp /ssd/shrink/max/streets/nodedl.cpp . && g++ -O2 -o nodedl nodedl.cpp || exit 1
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
./nodedl 12 $I/street_ways.bin $I/street_nodes.bin s_today.bin d2,hyb,planes,fd; echo "street nodedl rc=$?"
zs s_today.bin; rm -f s_today.bin
./nodedl 24 $I/interp_ways.bin $I/interp_nodes.bin i_today.bin d2,hyb,planes,fd; echo "interp nodedl rc=$?"
zs i_today.bin; rm -f i_today.bin
echo DONE
