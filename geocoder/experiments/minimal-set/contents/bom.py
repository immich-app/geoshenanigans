#!/usr/bin/env python3
"""Bill of materials for admin-minimal: content -> file -> raw -> zstd -> share of the download.
zstd per content = that content's slice compressed alone at -18 (zstd_shares.json), scaled so a
file's slices sum to the file's real compressed size (manifest size_zst).

Usage: bom.py <inventory.json> <zstd_shares.json> <manifest json>
"""
import json
import sys

inv = json.load(open(sys.argv[1]))
zs = json.load(open(sys.argv[2]))
man = json.load(open(sys.argv[3]))["files"]
F = {k.split("/")[-1]: v for k, v in man.items() if k.startswith("planet/admin-minimal/")}
TOTAL = sum(v["size_zst"] for v in F.values())
TOTAL_RAW = sum(v["size_raw"] for v in F.values())
MiB = 2 ** 20
rows = []


def split(file, parts):
    """parts: [(label, raw, zst_slice)] -> scaled to the file's real size_zst."""
    s = sum(z for _, _, z in parts)
    for label, raw, z in parts:
        rows.append((label, file, raw, z * F[file]["size_zst"] / s))


lv = inv["admin_levels"]
split("admin_vertices.bin", [(f"Admin rings L{L} geometry", lv[str(L)]["bytes"], zs[f"vert_L{L}"]["zst"])
                             for L in range(2, 9)])
split("admin_polygons.bin", [(f"Admin ring records L{L} (24 B each)", zs[f"polyrec_L{L}"]["raw"],
                              zs[f"polyrec_L{L}"]["zst"]) for L in range(2, 9)])
rows.append(("Admin S2 L10 cell table (cell id + offset)", "admin_cells.bin", F["admin_cells.bin"]["size_raw"],
             F["admin_cells.bin"]["size_zst"]))
ent = inv["admin_entries_by_level"]
cnt_bytes = inv["admin_entries_count_bytes"]
split("admin_entries.bin", [(f"Admin cell entries -> L{L} rings", 4 * ent[str(L)] + cnt_bytes * ent[str(L)] /
                             inv["admin_entries_total"], zs[f"aent_L{L}"]["zst"]) for L in range(2, 9)])
pbt = inv["place_by_type"]
split("place_nodes.bin", [(f"Place nodes {t} (20 B each)", 20 * pbt[t]["records"], zs[f"pn_type_{t}"]["zst"])
                          for t in ("city", "town", "village", "hamlet")])
rows.append(("Place S2 L10 cell table", "place_cells.bin", F["place_cells.bin"]["size_raw"],
             F["place_cells.bin"]["size_zst"]))
pe = inv["place_entries_by_type"]
pc = 2 * inv["place_cells_rows"]
split("place_entries.bin", [(f"Place cell entries -> {t}", 4 * pe[t] + pc * pe[t] / inv["place_entries_total"],
                             zs[f"pent_{t}"]["zst"]) for t in ("city", "town", "village", "hamlet")])
sa = inv["strings_attr"]
used_z, unused_z = zs["str_used_min"]["zst"], zs["str_unused_min"]["zst"]
scale = F["strings_core.bin"]["size_zst"] / (used_z + unused_z)
for label, key, zkey in [("Names: admin rings only", "  admin polygon names only", "str_admin_only"),
                         ("Names: place nodes only", "  place node names only", "str_place_only"),
                         ("Names: both", "  both", "str_both")]:
    rows.append((label, "strings_core.bin", sa[key]["bytes"], zs[zkey]["zst"] * scale *
                 used_z / (zs["str_admin_only"]["zst"] + zs["str_place_only"]["zst"] + zs["str_both"]["zst"])))
un_raw = sa["not used by admin-minimal"]["bytes"]
for label, key in [("Unused here: L9/L10/L11/L15 names", "  admin L9/L10/L15 names (admin mode only)"),
                   ("Unused here: suburb/neighbourhood/quarter names", "  suburb/neighbourhood/quarter names"),
                   ("Unused here: POI-major/postcode strings", "  POI-major/postcode refs"),
                   ("Unused here: other POI names + orphans", "  none of the above (other tiers' shared strings / orphans)")]:
    rows.append((label, "strings_core.bin", sa[key]["bytes"], unused_z * scale * sa[key]["bytes"] / un_raw))
rows.append(("String tier layout", "strings_layout.json", 460, F["strings_layout.json"]["size_zst"]))

print(f"total raw {TOTAL_RAW:,} ({TOTAL_RAW / MiB:.1f} MiB), zst {TOTAL:,} ({TOTAL / MiB:.2f} MiB)")
print("| Content | File | Raw MiB | zstd MiB | % of download |")
print("|---|---|---:|---:|---:|")
for label, f, raw, z in rows:
    print(f"| {label} | {f} | {raw / MiB:.2f} | {z / MiB:.2f} | {z / TOTAL * 100:.1f}% |")
print(f"sum zst {sum(r[3] for r in rows) / MiB:.2f} MiB")
