#!/bin/bash
# bg3.sh <name>... : planned layout Bg (/ssd/shrink/grid/bgcodec, the tiers2 method) of each scheme's
# admin_vertices (var6/<name>/{min,q}): encode, decode + cmp round trip, zstd 1.5.7 -18 -T1 of the Bg file
# -> cmp/bg_<name>.tsv (part, Bg raw bytes, Bg zstd bytes, round trip).
T=/ssd/tiers3; BG=/ssd/shrink/grid/bgcodec; Z=/ssd/shrink/zstd157/bin/zstd
one() { s=$1; p=$2; d=$T/var6/$s/$p; o=$T/tmp/$s.$p.bg
  $BG encode $d $d/admin_vertices.bin $o > /dev/null 2>&1 && $BG decode $d $o $o.dec > /dev/null 2>&1
  cmp -s $d/admin_vertices.bin $o.dec && rt=IDENTICAL || rt=DIFFERENT
  printf "%s\t%s\t%s\t%s\n" $p $(stat -c%s $o) $($Z -18 -T1 -q -c $o | wc -c) $rt > $T/cmp/bg_$s.$p.tsv; rm -f $o $o.dec; }
export -f one; export T BG Z
for s in "$@"; do for p in min q; do echo "$s $p"; done; done | xargs -P 8 -L 1 bash -c 'one $0 $1'
for s in "$@"; do cat $T/cmp/bg_$s.min.tsv $T/cmp/bg_$s.q.tsv > $T/cmp/bg_$s.tsv; rm -f $T/cmp/bg_$s.min.tsv $T/cmp/bg_$s.q.tsv; done
echo BG_DONE
