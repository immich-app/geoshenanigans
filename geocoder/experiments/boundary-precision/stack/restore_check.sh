#!/bin/bash
# End-to-end check of the shipped set: final/ -> (decode layout B, rebuild place index with
# the server's s2 crate) -> must equal todays-format/ (the set grid_query answered from)
# byte for byte, file by file. Writes final/SHA256SUMS. Restored copy is deleted afterwards.
set -euo pipefail
cd /ssd/shrink/stack
R=work/restored
rm -rf $R && mkdir -p $R
cp final/admin_cells.bin final/admin_entries.bin final/admin_polygons.bin final/place_nodes.bin \
   final/strings_core.bin final/strings_layout.json $R/
/ssd/shrink/venv/bin/python layouts.py decode B final final/admin_vertices.bin $R/admin_vertices.bin
rbench/target/release/rbench $R live $R
ok=1
for f in $(ls todays-format); do
  if cmp -s todays-format/$f $R/$f; then echo "IDENTICAL $f"; else echo "DIFFERENT $f"; ok=0; fi
done
[ "$(ls $R | wc -l)" = "$(ls todays-format | wc -l)" ] || { echo "file count differs"; ok=0; }
echo "restore check: $([ $ok = 1 ] && echo PASS || echo FAIL)"
rm -rf $R
(cd final && sha256sum $(ls | grep -v SHA256SUMS) > SHA256SUMS)
