#!/bin/bash
# Admin outlines, layout B: independent C encoder vs the track's Python encoder (same bytes?),
# independent C decoder back to today's file (cmp), whole-file zstd -18 -T16 of today's and B.
set -uo pipefail
cd /ssd/shrink/max/verify
export TMPDIR=/ssd/shrink/tmp
Z=/ssd/shrink/zstd157/bin/zstd
mkdir -p work/admin
for p in max withaddr; do
  d=/ssd/shrink/idx/$p
  work/layoutb enc $d/admin_polygons.bin $d/admin_vertices.bin work/admin/B_$p.mine
  ( cd /ssd/shrink/max/four-changes && /ssd/shrink/venv/bin/python -u layoutB4.py encode $d $d/admin_vertices.bin /ssd/shrink/max/verify/work/admin/B_$p.theirs )
  if cmp -s work/admin/B_$p.mine work/admin/B_$p.theirs; then echo "$p: my encoder == track encoder (byte-identical)"; else echo "$p: ENCODERS DIFFER"; fi
  work/layoutb dec $d/admin_polygons.bin work/admin/B_$p.mine work/admin/dec_$p.bin
  if cmp $d/admin_vertices.bin work/admin/dec_$p.bin; then echo "$p: my decoder -> today's admin_vertices.bin BYTE-IDENTICAL ($(sha256sum < work/admin/dec_$p.bin | cut -c1-16))"; else echo "$p: DECODE MISMATCH"; fi
  rm -f work/admin/dec_$p.bin work/admin/B_$p.theirs
  for f in $d/admin_vertices.bin work/admin/B_$p.mine; do
    printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) | tee -a work/zst_T16.tsv
  done
done
echo DONE
