#!/bin/bash
# extras: (a) intermediate stages from real files, (b) regenerate A/An/Bg with the stack layouts.py, round trip + zstd
set -uo pipefail
export TMPDIR=/ssd/shrink/tmp
cd /ssd/shrink/stack/verify
Z=/ssd/shrink/zstd157/bin/zstd; P=/ssd/shrink/venv/bin/python; T=/ssd/shrink/stack/todays-format; B=/ssd/shrink/idx/smallest
mkdir -p layouts sets
rt() {
  f=$1
  $P ../layouts.py encode $f $T layouts/$f.bin
  $P ../layouts.py decode $f $T layouts/$f.bin layouts/$f.dec
  cmp -s $T/admin_vertices.bin layouts/$f.dec && r=IDENTICAL || r=DIFFERENT
  echo -e "$f\t$r\t$(stat -c %s layouts/$f.bin)\t$($Z -18 -T1 -q -c layouts/$f.bin | wc -c)"
  rm -f layouts/$f.dec
}
export -f rt; export Z P T
printf "%s\n" A An Bg | xargs -P 3 -I{} bash -c "rt {}" > layouts.tsv 2> layouts.err &
echo -e "v11_polys\t$(stat -c %s v11_polys.bin)\t$($Z -18 -T1 -q -c v11_polys.bin | wc -c)" > v11.tsv
# symlink farms for layers
mk() { d=sets/$1; rm -rf $d; mkdir -p $d; }
mk v11; for f in $(ls $T); do ln -s $T/$f sets/v11/$f; done; rm sets/v11/admin_polygons.bin sets/v11/admin_vertices.bin
ln -s $PWD/v11_polys.bin sets/v11/admin_polygons.bin; ln -s $B/admin_vertices.bin sets/v11/admin_vertices.bin
mk s12; for f in $(ls sets/v11 | grep -v place_cells | grep -v place_entries); do ln -s $(readlink -f sets/v11/$f) sets/s12/$f; done
mk s123; for f in $(ls $T | grep -v place_cells | grep -v place_entries); do ln -s $T/$f sets/s123/$f; done
for s in v11 s12 s123; do
  echo -e "$s\t$(ls sets/$s | tr "\n" ,)\t$(tar -ch --sort=name --owner=0 --group=0 --numeric-owner --mtime=@0 -C sets/$s -f - . | gzip -6 -n | wc -c)" >> v11.tsv &
done
wait
echo done > extras.done
