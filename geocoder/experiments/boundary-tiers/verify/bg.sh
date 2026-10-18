#!/bin/bash
# bg.sh : planned layout Bg (/ssd/shrink/grid/bgcodec, tiers2 sizes.py method) of admin_vertices per scheme/part:
# encode, decode + cmp round trip, zstd -18 -T1 of the Bg file -> cmp/bg.tsv
V=/ssd/tiers2-verify; BG=/ssd/shrink/grid/bgcodec; Z=/ssd/shrink/zstd157/bin/zstd
one() { s=$1; p=$2; d=$V/var/$s/$p; o=$V/tmp/$s.$p.bg
  $BG encode $d $d/admin_vertices.bin $o > /dev/null 2>&1 && $BG decode $d $o $o.dec > /dev/null 2>&1
  cmp -s $d/admin_vertices.bin $o.dec && rt=IDENTICAL || rt=DIFFERENT
  printf "%s\t%s\t%s\t%s\t%s\n" $s $p $(stat -c%s $o) $($Z -18 -T1 -q -c $o | wc -c) $rt; rm -f $o $o.dec; }
export -f one; export V BG Z
for s in q25 cap10 rec small; do for p in min q; do echo "$s $p"; done; done | xargs -P 8 -L 1 bash -c "one \$0 \$1" > $V/cmp/bg.tsv
echo BG_DONE
