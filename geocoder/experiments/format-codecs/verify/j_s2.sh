#!/bin/bash
# Experiment: ways ordered along the S2 curve (the geo index's row order) instead of lat/lng Hilbert.
# Measures the combined geo index streams and the packed street / interp ways under that order.
set -uo pipefail
V=/ssd/shrink/max/verify
X=$V/s2
I=/ssd/shrink/idx/max
P=/ssd/shrink/venv/bin/python
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
mkdir -p $X/street $X/interp $X/gxc $X/pk
cd $X
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
$P -u $V/scripts/reorder_s2.py street street & $P -u $V/scripts/reorder_s2.py interp interp & wait
GXC_REMAP_S=$X/street/remap_street.u32 GXC_REMAP_I=$X/interp/remap_interp.u32 $P -u $V/scripts/gxc.py gxc 128 delta inline
$V/work/gxcdec gxc 128 delta inline $I $X/street/remap_street.u32 $X/interp/remap_interp.u32 && echo "s2 gxc: independent full decode matches today's geo index"
zs gxc/gxc_rows.bin gxc/gxc_top.bin gxc/gxc_street.bin gxc/gxc_interp.bin
rm -f gxc/lists_*.npz
W=$V/st/waypack
$W enc 12 street/street_ways.bin street/street_nodes.bin 16 pk/s_g16
$W enc 24 interp/interp_ways.bin interp/interp_nodes.bin 16 pk/i_g16
$V/work/wdec street 16 pk/s_g16 pk/s_dec.ways pk/s_dec.nodes && cmp pk/s_dec.nodes street/street_nodes.bin && echo "s2 street: decode byte-identical"
$V/work/wdec interp 16 pk/i_g16 pk/i_dec.ways pk/i_dec.nodes && cmp pk/i_dec.nodes interp/interp_nodes.bin && echo "s2 interp: decode byte-identical"
rm -f pk/*_dec.*
$P -u $V/scripts/split_cols.py street pk/s_g16.ways2 pk/s_cols
$P -u $V/scripts/split_cols.py interp pk/i_g16.ways2 pk/i_cols
zs pk/s_g16.nodes2 pk/s_g16.groups pk/s_cols.cnt pk/s_cols.code pk/s_cols.name \
   pk/i_g16.nodes2 pk/i_g16.groups pk/i_cols.cntcode pk/i_cols.street pk/i_cols.start pk/i_cols.span pk/i_cols.interp
# sidecars in this order (u24 / u16 dictionary forms)
$P - <<'EOF'
import numpy as np
X = "/ssd/shrink/max/verify/s2"
I = "/ssd/shrink/idx/max"
ND = np.uint32(0xFFFFFFFF)
for kind, files in (("street", ("way_parents.bin", "way_postcodes.bin")), ("interp", ("interp_postcodes.bin",))):
    rm = np.fromfile(f"{X}/{kind}/remap_{kind}.u32", "<u4")
    perm = np.empty_like(rm); perm[rm] = np.arange(len(rm), dtype=np.uint32)
    for f in files:
        v = np.fromfile(f"{I}/{f}", "<u4")[perm]
        live = v != ND
        if f == "way_parents.bin":
            w = np.where(live, v, np.uint32(0xFFFFFF)).astype("<u4").view(np.uint8).reshape(-1, 4)[:, :3].copy()
            w.tofile(f"{X}/pk/{f}.u24")
        else:
            d = np.unique(v[live])
            np.where(live, np.searchsorted(d, v), 0xFFFF).astype("<u2").tofile(f"{X}/pk/{f}.u16dict")
print("sidecars written")
EOF
zs pk/way_parents.bin.u24 pk/way_postcodes.bin.u16dict pk/interp_postcodes.bin.u16dict
# addr parent_way_id under this order is measured by the addresses re-run only for the Hilbert order
rm -rf street interp pk/*.nodes2
echo DONE
