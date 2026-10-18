"""Per-family size table of the encoded 10-13 day: raw and zstd -18 -T16 (measured here, logs/
zst_enc_1013.tsv) against the earlier tracks' whole-file measurements of the same formats
(max/verify/work/zst_T16.tsv) and today's published files (four-changes/work/published.json, preset max).
usage: sizes_table.py"""
import json

C = "/ssd/shrink/sortpatch/codecs"
MiB = 1 << 20
mine, full = {}, {}
for fn in (f"{C}/logs/zst_enc_1013.tsv", f"{C}/logs/zst_pax.tsv"):
    for line in open(fn):
        p, r, z = line.rstrip("\n").split("\t")
        mine[p.split("/")[-1]] = (int(r), int(z))
        if "zst_enc" in fn:
            full[p] = (int(r), int(z))
old = {}
for line in open("/ssd/shrink/max/verify/work/zst_T16.tsv"):
    p, r, z = line.rstrip("\n").split("\t")
    old[p] = (int(r), int(z))
pub = json.load(open("/ssd/shrink/max/four-changes/work/published.json"))["max"]

ROWS = [
    ("street ways + nodes", ["street.nodes2", "street.groups", "street.cnt", "street.code", "street.name"],
     ["pk/s_g16.nodes2", "pk/s_g16.groups", "pk/s_cols.cnt", "pk/s_cols.code", "pk/s_cols.name"], ["street_nodes.bin", "street_ways.bin"]),
    ("interp ways + nodes", ["interp.nodes2", "interp.groups", "interp.cntcode", "interp.street", "interp.start", "interp.span", "interp.interp"],
     ["pk/i_g16.nodes2", "pk/i_g16.groups", "pk/i_cols.cntcode", "pk/i_cols.street", "pk/i_cols.start", "pk/i_cols.span", "pk/i_cols.interp"],
     ["interp_nodes.bin", "interp_ways.bin"]),
    ("way_parents (u24)", ["way_parents.u24"], ["side/way_parents.bin.u24"], ["way_parents.bin"]),
    ("way_postcodes (u16 + table)", ["way_postcodes.u16", "way_postcodes.dict"], ["side/way_postcodes.bin.u16dict", "side/way_postcodes.bin.dict"],
     ["way_postcodes.bin"]),
    ("interp_postcodes (u16 + table)", ["interp_postcodes.u16", "interp_postcodes.dict"],
     ["side/interp_postcodes.bin.u16dict", "side/interp_postcodes.bin.dict"], ["interp_postcodes.bin"]),
    ("geo index (gxc)", ["gxc_rows.bin", "gxc_top.bin", "gxc_street.bin", "gxc_interp.bin"],
     ["/ssd/shrink/max/verify/gxc/b128_delta_inline/gxc_rows.bin", "/ssd/shrink/max/verify/gxc/b128_delta_inline/gxc_top.bin",
      "/ssd/shrink/max/verify/gxc/b128_delta_inline/gxc_street.bin", "/ssd/shrink/max/verify/gxc/b128_delta_inline/gxc_interp.bin"],
     ["geo_cells.bin", "street_entries.bin", "interp_entries.bin", "addr_entries.bin"]),
    ("addr_points (col)", ["addr_points.col"], ["final_pw/col.bin"], ["addr_points.bin"]),
    ("addr_points (pax170, alternative)", ["addr_points.pax170"], ["final_pw/pax170.bin"], ["addr_points.bin"]),
    ("addr_vertices (abs2)", ["addr_vertices.abs2"], ["tb/abs2/addr_vertices.bin"], ["addr_vertices.bin"]),
    ("addr_postcodes (u32, sorted)", ["addr_postcodes.bin"], ["v_sorted/addr_postcodes.bin"], ["addr_postcodes.bin"]),
    ("admin_vertices (layout B)", ["admin_vertices.B"], ["work/admin/B_max.mine"], ["admin_vertices.bin"]),
    ("poi_records (R3, lossless)", ["poi_records.r3"], ["work/poi_rec/R3_aos32.bin"], ["poi_records.bin"]),
    ("poi_vertices (B-open)", ["poi_vertices.bopen"], ["work/poi_vert/Bopen.bin"], ["poi_vertices.bin"]),
]


def tot(d, keys):
    return sum(d[k][0] for k in keys), sum(d[k][1] for k in keys)


def f(x):
    return f"{x / MiB:,.1f}"


print("| family | today published zst | earlier measurement zst | gc zst | gc - earlier | gc raw | today raw |")
print("|---|---:|---:|---:|---:|---:|---:|")
T = [0, 0, 0, 0, 0]
for name, m, o, t in ROWS:
    mr, mz = tot(mine, m)
    orr, oz = tot(old, o)
    tz = sum(pub[x]["zst"] for x in t)
    tr = sum(pub[x]["raw"] for x in t)
    print(f"| {name} | {f(tz)} | {f(oz)} | {f(mz)} | {(mz - oz) / oz * 100:+.2f}% | {mr / 1e6:,.0f} MB | {tr / 1e6:,.0f} MB |")
    if "alternative" not in name:
        for k, v in enumerate((tz, oz, mz, mr, tr)):
            T[k] += v
print(f"| **encoded families** | **{f(T[0])}** | **{f(T[1])}** | **{f(T[2])}** | {(T[2] - T[1]) / T[1] * 100:+.2f}% | {T[3] / 1e6:,.0f} MB | {T[4] / 1e6:,.0f} MB |")
enc_files = {x for _, m, _, _ in ROWS for x in m}
rest = {k: v for k, v in full.items() if k.split("/")[-1] not in enc_files}
rr = sum(v[0] for v in rest.values())
rz = sum(v[1] for v in rest.values())
print(f"\nunchanged files (hardlinked, incl. manifests and configurations.json): raw {rr / 1e6:,.0f} MB, zst {f(rz)} MiB")
allz = sum(v[1] for v in full.values())
allr = sum(v[0] for v in full.values())
print(f"whole encoded day: raw {allr / 1e6:,.0f} MB ({allr / MiB:,.1f} MiB), zst {f(allz)} MiB")
