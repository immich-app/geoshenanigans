"""Planet max per group (combined_tables.md rows): published vs transport runs vs our codecs.

groups.py <manifest.json> <label=results.json> ...
Our codec columns are copied from /ssd/shrink/max/verify/work/combined_tables.md (max section).
"""
import json, os, sys

GROUPS = [
    ("street_nodes + street_ways", ["street_nodes.bin", "street_ways.bin"], 2424.6, 1508.6, 1411.4),
    ("interp_nodes + interp_ways", ["interp_nodes.bin", "interp_ways.bin"], 824.5, 576.9, 535.7),
    ("way_parents", ["way_parents.bin"], 75.2, 7.6, 7.6),
    ("way_postcodes", ["way_postcodes.bin"], 16.9, 0.9, 0.9),
    ("interp_postcodes", ["interp_postcodes.bin"], 28.9, 2.0, 2.0),
    ("geo index: geo_cells + street/interp/addr entries",
     ["geo_cells.bin", "street_entries.bin", "interp_entries.bin", "addr_entries.bin"], 2328.8, 517.8, 480.1),
    ("addr_points", ["addr_points.bin"], 1522.6, 874.9, 755.3),
    ("addr_vertices", ["addr_vertices.bin"], 1731.7, 1014.9, 870.2),
    ("addr_postcodes", ["addr_postcodes.bin"], 72.8, 16.6, 16.6),
    ("admin_vertices", ["admin_vertices.bin"], 725.8, 725.8, 684.7),
    ("admin_polygons", ["admin_polygons.bin"], 11.8, 11.8, 11.8),
    ("admin_cells + admin_entries", ["admin_cells.bin", "admin_entries.bin"], 10.2, 7.3, 7.3),
    ("admin_parents", ["admin_parents.bin"], 2.1, 2.1, 2.1),
    ("postal_vertices + postal_polygons", ["postal_vertices.bin", "postal_polygons.bin"], 0.0, 0.0, 0.0),
    ("place_nodes", ["place_nodes.bin"], 30.5, 30.5, 30.5),
    ("place_cells + place_entries", ["place_cells.bin", "place_entries.bin"], 0.0, 0.0, 0.0),
    ("strings_core", ["strings_core.bin"], 11.2, 11.2, 11.2),
    ("strings_street", ["strings_street.bin"], 46.9, 45.8, 45.8),
    ("strings_addr", ["strings_addr.bin"], 3.5, 3.5, 2.3),
    ("strings_postcode", ["strings_postcode.bin"], 3.0, 3.0, 3.0),
    ("strings_poi", ["strings_poi.bin"], 82.5, 82.5, 82.5),
    ("poi_records", ["poi_records.bin"], 387.5, 315.7, 267.1),
    ("poi_vertices", ["poi_vertices.bin"], 523.8, 433.1, 395.0),
    ("poi_cells + poi_entries", ["poi_cells.bin", "poi_entries.bin"], 74.8, 72.3, 72.3),
    ("postcode_centroids + cells + entries",
     ["postcode_centroids.bin", "postcode_centroid_cells.bin", "postcode_centroid_entries.bin"], 22.2, 22.2, 22.2),
    ("poi_meta.json + strings_layout.json", ["poi_meta.json", "strings_layout.json"], 0.0, 0.0, 0.0),
]
M = 1048576
man = sys.argv[1]
F = json.load(open(man))["files"]
pub = {}
for p in ["planet/full/", "planet/quality/uncapped/", "planet/poi/all/"]:
    for k, v in F.items():
        if k.startswith(p) and "/" not in k[len(p):]:
            pub[os.path.basename(k)] = pub.get(os.path.basename(k), 0) + v["size_zst"]
runs = []
for arg in sys.argv[2:]:
    label, path = arg.split("=", 1)
    j = json.load(open(path))
    rr = {}
    for r in j["results"]:
        rr[r["file"]] = pub[r["file"]] if (r["kind"] == "r" and j["cfg"] != "raw") else r["zst"]
    runs.append((label, rr))
print("| group | published | " + " | ".join(l for l, _ in runs) + " | four changes | best on-disk | best download-only |")
print("|---|---:|" + "---:|" * len(runs) + "---:|---:|---:|")
T = [0.0] * (1 + len(runs) + 3)
for name, files, four, disk, dl in GROUPS:
    p = sum(pub[f] for f in files) / M
    vals = [p] + [sum(r.get(f, pub[f]) for f in files) / M for _, r in runs] + [four, disk, dl]
    T = [a + b for a, b in zip(T, vals)]
    best_ours = min(disk, dl)
    print(f"| {name} | " + " | ".join(f"{v:,.1f}" for v in vals) + " |")
print("| **total** | " + " | ".join(f"**{v:,.1f}**" for v in T) + " |")
