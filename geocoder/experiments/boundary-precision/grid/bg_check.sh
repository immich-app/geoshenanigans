#!/bin/bash
# Proofs behind sizes.py: (1) bgcodec encode == layouts.py encode Bg, and both decoders invert it, on the q2.5 L2-8
# filter (work/chk/minfilter) and on the stack 11 m snapped minimal set (work/chk/bg); zstd of the latter must equal the
# stack report (80,059,640 B). (2) gridvar filter of q2.5 L2-8 == the bundled admin-minimal ring multiset.
# (3) gridvar legacy snap reproduces /ssd/shrink/stack/todays-format/admin_vertices.bin.
cd /ssd/shrink/grid
PY=/ssd/shrink/venv/bin/python; L=/ssd/shrink/stack/layouts.py
for d in work/chk/minfilter work/chk/bg; do
  $PY $L encode Bg $d work/chk/py_Bg.bin > /dev/null
  ./bgcodec encode $d $d/admin_vertices.bin work/chk/c_Bg.bin > /dev/null
  cmp -s work/chk/py_Bg.bin work/chk/c_Bg.bin && echo "$d: bgcodec encode == layouts.py encode Bg: IDENTICAL" || echo "$d: encode DIFFERENT"
  ./bgcodec decode $d work/chk/c_Bg.bin work/chk/c_dec.bin > /dev/null
  cmp -s work/chk/c_dec.bin $d/admin_vertices.bin && echo "$d: bgcodec decode round trip: IDENTICAL" || echo "$d: bgcodec decode DIFFERENT"
  $PY $L decode Bg $d work/chk/c_Bg.bin work/chk/py_dec.bin > /dev/null
  cmp -s work/chk/py_dec.bin $d/admin_vertices.bin && echo "$d: layouts.py decode of bgcodec output: IDENTICAL" || echo "$d: layouts.py decode DIFFERENT"
  echo "$d: Bg zstd -18 -T1 = $(/ssd/shrink/zstd157/bin/zstd -18 -T1 -q -c work/chk/c_Bg.bin | wc -c) B"
  rm -f work/chk/py_Bg.bin work/chk/c_Bg.bin work/chk/c_dec.bin work/chk/py_dec.bin
done
$PY work/chk/blocks_equal.py work/chk/minfilter /ssd/shrink/idx/smallest
./gridvar var /ssd/shrink/idx/smallest work/chk/legacy 0 1e-4 legacy > /dev/null
cmp -s work/chk/legacy/admin_vertices.bin /ssd/shrink/stack/todays-format/admin_vertices.bin && echo "gridvar legacy snap == stack todays-format admin_vertices.bin: IDENTICAL" || echo "legacy DIFFERENT"
