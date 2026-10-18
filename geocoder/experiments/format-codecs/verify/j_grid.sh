#!/bin/bash
# With-addresses 11 m grid (LOSSY, q2.5 outlines only): snap today's q2.5 admin outlines to a global
# 1e-4 deg grid with the four-changes track's variant tool, layout B (independent C codec, round trip),
# whole-file zstd, and fresh-20k answer differences vs today's with-addresses preset.
set -uo pipefail
V=/ssd/shrink/max/verify
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
WA=/ssd/shrink/idx/withaddr
cd $V
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
G=work/grid_wa
mkdir -p $G
/ssd/shrink/max/four-changes/work/variant $WA $G snap 0.0001
for f in $WA/*; do b=$(basename $f); [ -e $G/$b ] || ln -f $f $G/$b; done
ls -la $G/admin_polygons.bin $G/admin_vertices.bin
work/layoutb enc $G/admin_polygons.bin $G/admin_vertices.bin $G.B
work/layoutb dec $G/admin_polygons.bin $G.B $G.dec && cmp $G.dec $G/admin_vertices.bin && echo "grid: layout B round trip byte-identical"
rm -f $G.dec
zs $G/admin_vertices.bin $G.B $G/admin_polygons.bin
scripts/runq_v.sh grid_wa $V/$G base_wa
# field-level breakdown of the differences (Immich city/state/country + road/house number/postcode)
/ssd/shrink/venv/bin/python - <<'EOF'
import json
a = [l.split(" ", 2) for l in open("/ssd/shrink/max/verify/q/base_wa.jsonl")]
b = [l.split(" ", 2) for l in open("/ssd/shrink/max/verify/q/grid_wa.jsonl")]
def imm(j):  # same mapping as the four-changes track's compare4.py
    ad = j.get("address", {})
    co = lambda *ks: next((ad[k] for k in ks if ad.get(k) is not None), None)
    return (co("city", "town", "village", "hamlet", "municipality"), co("state", "province"), ad.get("country_code"))
full = imm_n = road = hn = pc = 0
for (la, lo, ja), (lb, lob, jb) in zip(a, b):
    assert (la, lo) == (lb, lob)
    if ja == jb:
        continue
    full += 1
    x, y = json.loads(ja), json.loads(jb)
    imm_n += imm(x) != imm(y)
    ax, ay = x.get("address", {}), y.get("address", {})
    road += ax.get("road") != ay.get("road")
    hn += ax.get("house_number") != ay.get("house_number")
    pc += ax.get("postcode") != ay.get("postcode")
print(f"grid vs today (with-addresses, fresh 20k): full JSON {full} ({100*full/len(a):.3f}%), city/state/country {imm_n} ({100*imm_n/len(a):.3f}%), road {road}, house_number {hn}, postcode {pc}")
EOF
rm -rf $G $G.B
echo DONE
