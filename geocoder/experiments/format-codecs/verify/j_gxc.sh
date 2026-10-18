#!/bin/bash
# Combined geo index (gxc.py) variants: encode, independent full decode vs today's files, zstd -18 -T16.
set -uo pipefail
V=/ssd/shrink/max/verify
G=$V/gxc
I=/ssd/shrink/idx/max
P=/ssd/shrink/venv/bin/python
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
mkdir -p $G
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
RS=/ssd/shrink/max/streets/reo/street/remap_street.u32
RI=/ssd/shrink/max/streets/reo/interp/remap_interp.u32
sed -i -e "s#/ssd/shrink/max/verify/st/reo/street/remap_street.u32#$RS#" -e "s#/ssd/shrink/max/verify/st/reo/interp/remap_interp.u32#$RI#" $V/scripts/gxc.py
for v in "128 delta inline" "128 abs inline" "128 delta sep" "256 delta inline"; do
  set -- $v
  d=$G/b$1_$2_$3
  mkdir -p $d
  ln -sf $G/lists_s.npz $d/lists_s.npz 2>/dev/null; ln -sf $G/lists_i.npz $d/lists_i.npz; ln -sf $G/lists_a.npz $d/lists_a.npz
  $P -u $V/scripts/gxc.py $d $1 $2 $3
  # first run created the caches inside $d: move them to the shared place
  for k in s i a; do [ -f $d/lists_$k.npz ] && [ ! -L $d/lists_$k.npz ] && mv $d/lists_$k.npz $G/ && ln -sf $G/lists_$k.npz $d/lists_$k.npz; done
  $V/work/gxcdec $d $1 $2 $3 $I $RS $RI && echo "gxc $v: independent full decode matches today's geo index"
  zs $d/gxc_rows.bin $d/gxc_top.bin $d/gxc_street.bin $d/gxc_interp.bin
  [ -f $d/gxc_acnt.bin ] && zs $d/gxc_acnt.bin
done
echo DONE
