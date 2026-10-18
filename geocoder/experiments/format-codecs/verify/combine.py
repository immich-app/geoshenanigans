"""Combined per-file tables for the maximum-quality and with-street-addresses presets.
today = published size_zst/size_raw (four-changes work/published.json); four changes = the four-changes
track's measured stage '1+2+3 + postal dedup P1' (sizes4.json, its change-1 files re-measured here);
best on-disk / download-only = the verifier's whole-file zstd 1.5.7 -18 -T16 measurements
(work/zst_T16.tsv) where marked 'm', otherwise the track's whole-file measurement ('t').
Prints markdown tables (MiB) and totals."""
import json
import sys

V = "/ssd/shrink/max/verify"
MiB = 1 << 20
pub = json.load(open("/ssd/shrink/max/four-changes/work/published.json"))
s4 = json.load(open("/ssd/shrink/max/four-changes/work/sizes4.json"))
STAGE = "1+2+3 + postal dedup P1 (drop postal_*)"
tsv = {}
for fn in (f"{V}/work/zst_T16.tsv", f"{V}/lay/zst.tsv"):
    for line in open(fn):
        p, raw, z = line.rstrip("\n").split("\t")
        tsv[p.replace(f"{V}/", "")] = (int(raw), int(z))


def m(*paths):
    """measured (raw, zst) summed over files (verifier tsv)."""
    r = z = 0
    for p in paths:
        a, b = tsv[p]
        r += a
        z += b
    return r, z


def t(raw, zst):
    return raw, zst


def today(preset, *files):
    return sum(pub[preset][f]["raw"] for f in files), sum(pub[preset][f]["zst"] for f in files)


def four(preset, *files):
    fs = s4[preset][STAGE]["files"]
    r = z = 0
    for f in files:
        if f in fs:
            r += fs[f][0]
            z += fs[f][1]
    return r, z


GX = "gxc/b128_delta_inline"
IX = "work"


def rows(preset):
    av = "B_max.mine" if preset == "max" else "B_withaddr.mine"
    R = []
    R.append(("street_nodes + street_ways", ("street_nodes.bin", "street_ways.bin"),
              m("pk/s_g16.nodes2", "pk/s_g16.groups", "pk/s_cols.cnt", "pk/s_cols.code", "pk/s_cols.name"), "m",
              m("s_today.bin"), "m", "per way (group table + <=15 header hops)"))
    R.append(("interp_nodes + interp_ways", ("interp_nodes.bin", "interp_ways.bin"),
              m("pk/i_g16.nodes2", "pk/i_g16.groups", "pk/i_cols.cntcode", "pk/i_cols.street", "pk/i_cols.start",
                "pk/i_cols.span", "pk/i_cols.interp"), "m", m("i_today.bin"), "m", "per way"))
    R.append(("way_parents", ("way_parents.bin",), m("side/way_parents.bin.u24"), "m", None, "", "per way (u24)"))
    R.append(("way_postcodes", ("way_postcodes.bin",), m("side/way_postcodes.bin.u16dict", "side/way_postcodes.bin.dict"),
              "m", None, "", "per way (u16 + table)"))
    R.append(("interp_postcodes", ("interp_postcodes.bin",),
              m("side/interp_postcodes.bin.u16dict", "side/interp_postcodes.bin.dict"), "m", None, "", "per way (u16 + table)"))
    gx = m(f"{GX}/gxc_rows.bin", f"{GX}/gxc_top.bin", f"{GX}/gxc_street.bin", f"{GX}/gxc_interp.bin")
    gxdl = m(f"{GX}/gxc_rows.bin", f"{GX}/gxc_street.bin", f"{GX}/gxc_interp.bin")
    R.append(("geo index: geo_cells + street/interp/addr entries", ("geo_cells.bin", "street_entries.bin",
              "interp_entries.bin", "addr_entries.bin"), gx, "m", gxdl, "m", "per cell (block of 128 rows)"))
    R.append(("addr_points", ("addr_points.bin",), m("final_pw/col.bin"), "m", m("final_pw/dlpts.bin"), "m",
              "per record (byte planes)"))
    R.append(("addr_vertices", ("addr_vertices.bin",), m("tb/abs2/addr_vertices.bin"), "m", m("final/dlverts.bin"), "m",
              "per outline"))
    R.append(("addr_postcodes", ("addr_postcodes.bin",), m("v_sorted/addr_postcodes.bin"), "m", None, "", "per record"))
    an = "lay/max.An" if preset == "max" else "lay/wa.An"
    R.append(("admin_vertices", ("admin_vertices.bin",), m(f"work/admin/{av}"), "m", m(an) if an in tsv else None, "m",
              "per polygon (same block)"))
    R.append(("admin_polygons", ("admin_polygons.bin",), four(preset, "admin_polygons.bin"), "m", None, "", "per record"))
    adm = m("work/l10/admin_bitmap.bin", "work/l10/admin_rank.bin", "work/l10/admin_offsets_dg.bin",
            "work/v_admin_dg/admin_entries.bin")
    R.append(("admin_cells + admin_entries", ("admin_cells.bin", "admin_entries.bin"), adm, "m", None, "",
              "per cell (bitmap + rank)"))
    R.append(("admin_parents", ("admin_parents.bin",), today(preset, "admin_parents.bin"), "-", None, "", "unchanged"))
    R.append(("postal_vertices + postal_polygons", ("postal_vertices.bin", "postal_polygons.bin"), (0, 0), "m", None, "",
              "dropped (never read)"))
    R.append(("place_nodes", ("place_nodes.bin",), m("fc/sets/max1/place_nodes.bin"), "m", None, "", "per record"))
    R.append(("place_cells + place_entries", ("place_cells.bin", "place_entries.bin"), (0, 0), "t", None, "",
              "rebuilt at load (+0.5-0.8 s)"))
    R.append(("strings_core", ("strings_core.bin",), m("fc/sets/max1/strings_core.bin"), "m", None, "", "by offset"))
    R.append(("strings_street", ("strings_street.bin",), m("v_pstreet/strings_street.bin"), "m", None, "", "by offset"))
    R.append(("strings_addr", ("strings_addr.bin",), today(preset, "strings_addr.bin"), "-", t(8033138, 2361111), "t",
              "by offset"))
    R.append(("strings_postcode", ("strings_postcode.bin",), today(preset, "strings_postcode.bin"), "-", None, "", ""))
    R.append(("strings_poi", ("strings_poi.bin",), today(preset, "strings_poi.bin"), "-", None, "", ""))
    R.append(("poi_records", ("poi_records.bin",), m(f"{IX}/poi_rec/R3_aos32.bin"), "m", m(f"{IX}/poi_rec/R3_dl.bin"), "m",
              "per record (32 B)"))
    R.append(("poi_vertices", ("poi_vertices.bin",), m(f"{IX}/poi_vert/Bopen.bin"), "m", m(f"{IX}/poi_vert/Anopen.bin"),
              "m", "per polygon"))
    poi = m("work/l10/poi_bitmap.bin", "work/l10/poi_rank.bin", "work/l10/poi_offsets_dg.bin",
            f"{IX}/v_poi_dg/poi_entries.bin")
    R.append(("poi_cells + poi_entries", ("poi_cells.bin", "poi_entries.bin"), poi, "m", None, "", "per cell (bitmap + rank)"))
    R.append(("postcode_centroids + cells + entries", ("postcode_centroids.bin", "postcode_centroid_cells.bin",
              "postcode_centroid_entries.bin"), today(preset, "postcode_centroids.bin", "postcode_centroid_cells.bin",
              "postcode_centroid_entries.bin"), "-", None, "", "unchanged"))
    R.append(("poi_meta.json + strings_layout.json", ("poi_meta.json", "strings_layout.json"),
              today(preset, "poi_meta.json", "strings_layout.json"), "-", None, "", "unchanged"))
    return R


def fmt(x):
    return f"{x / MiB:,.1f}"


for preset in ("max", "withaddr"):
    R = rows(preset)
    covered = set()
    print(f"\n### {preset}\n")
    print("| file | today | four changes | best lossless on-disk | best download-only | raw on disk today -> best (MB) | random access |")
    print("|---|---:|---:|---:|---:|---:|---|")
    T = [0, 0, 0, 0, 0, 0]
    for name, files, best, src, dl, dsrc, ra in R:
        covered |= set(files)
        td = today(preset, *files)
        fo = four(preset, *files)
        bl = dl if dl is not None and dl[1] < best[1] else best
        print(f"| {name} | {fmt(td[1])} | {fmt(fo[1])} | {fmt(best[1])} | {fmt(bl[1])} | {td[0] / 1e6:,.0f} -> {best[0] / 1e6:,.0f} | {ra} |")
        for k, v in enumerate((td[1], fo[1], best[1], bl[1], td[0], best[0])):
            T[k] += v
    missing = set(pub[preset]) - covered
    print(f"| **total** | **{fmt(T[0])}** | **{fmt(T[1])}** | **{fmt(T[2])}** | **{fmt(T[3])}** | {T[4] / 1e6:,.0f} -> {T[5] / 1e6:,.0f} | |")
    print("files not covered:", sorted(missing), file=sys.stderr)
    print(f"four-changes stage total check: {s4[preset][STAGE]['zst'] / MiB:,.1f}", file=sys.stderr)
    if preset == "withaddr":
        # LOSSY line: 11 m grid on the q2.5 outlines only. admin_vertices: layout B with divisor (Bg) of the
        # snapped file (verifier, whole-file); admin_polygons: the four-changes track's change 1 + 4 file.
        lossy = "1+2+3+4 + postal dedup P1 (LOSSY)"
        gv = tsv["lay/grid.Bg"][1]                                   # snapped outlines, layout B + divisor
        gp = s4[preset][lossy]["files"]["admin_polygons.bin"][1]      # change 1 + 4 polygons (track)
        bv = tsv["work/admin/B_withaddr.mine"][1]                     # lossless on-disk admin_vertices
        bvd = min(bv, tsv["lay/wa.An"][1]) if "lay/wa.An" in tsv else bv  # lossless download-only
        bp = four(preset, "admin_polygons.bin")[1]
        print(f"| **+ 11 m grid on q2.5 outlines (LOSSY)** | | **{fmt(s4[preset][lossy]['zst'])}** "
              f"| **{fmt(T[2] - bv - bp + gv + gp)}** | **{fmt(T[3] - bvd - bp + gv + gp)}** | | "
              f"admin_vertices {fmt(gv)} (B + divisor), admin_polygons {fmt(gp)} |")
