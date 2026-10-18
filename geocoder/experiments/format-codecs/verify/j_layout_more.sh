#!/bin/bash
# admin_vertices: per-polygon divisor (Bg, on-disk) and whole-file planes (An / A, download-only) on the
# planet presets, each round-tripped byte-for-byte to today's file and zstd'd whole-file.
# Three chains in parallel (max lossless, withaddr lossless, withaddr 11 m grid), zstd -T5 each.
set -uo pipefail
V=/ssd/shrink/max/verify
L=$V/lay
P=/ssd/shrink/venv/bin/python
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
mkdir -p $L
cd $V
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T5 -q -c "$f" | wc -c) >> $L/zst.tsv; done; }
rt() {  # rt <tag> <fmt> <polys_dir> <today admin_vertices>
  local tag=$1 fmt=$2 d=$3 src=$4
  $P -u scripts/layout_more.py encode $fmt $d $src $L/$tag.$fmt || return 1
  $P -u scripts/layout_more.py decode $fmt $d $L/$tag.$fmt $L/$tag.$fmt.dec || return 1
  if cmp $L/$tag.$fmt.dec $src; then echo "$tag $fmt: round trip byte-identical to today's file"; else echo "$tag $fmt: ROUND TRIP MISMATCH"; fi
  rm -f $L/$tag.$fmt.dec
  zs $L/$tag.$fmt
}
(
  for f in Bg An; do rt max $f /ssd/shrink/idx/max /ssd/shrink/idx/max/admin_vertices.bin; done
) > $L/max.log 2>&1 &
(
  for f in Bg An; do rt wa $f /ssd/shrink/idx/withaddr /ssd/shrink/idx/withaddr/admin_vertices.bin; done
) > $L/wa.log 2>&1 &
(
  G=$L/grid_wa
  mkdir -p $G
  /ssd/shrink/max/four-changes/work/variant /ssd/shrink/idx/withaddr $G snap 0.0001
  for f in B Bg A; do rt grid $f $G $G/admin_vertices.bin; done
  zs $G/admin_vertices.bin
) > $L/grid.log 2>&1 &
wait
echo DONE >> $L/max.log
