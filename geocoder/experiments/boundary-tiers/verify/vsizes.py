#!/usr/bin/env python3
"""vsizes.py : per-scheme raw (file size) and zstd 1.5.7 -18 -T1 sizes from cmp/zst_real.tsv.
admin4 = the 4 admin files the report counts (admin-minimal: admin_vertices/polygons/cells/entries;
quality dir: admin_vertices/polygons + postal_vertices/polygons); set = every file of the flat set.
MB = 1e6 bytes. -> cmp/vsizes.json + table."""
import json
import os

V = "/ssd/tiers2-verify"
z = {}
for line in open(f"{V}/cmp/zst_real.tsv"):
    p, raw, zs = line.rstrip("\n").split("\t")
    z[p] = (int(raw), int(zs))
FILES = {"smallest": ["admin_vertices.bin", "admin_polygons.bin", "admin_cells.bin", "admin_entries.bin"],
         "recommended": ["admin_vertices.bin", "admin_polygons.bin", "postal_vertices.bin", "postal_polygons.bin"]}
out = {}
for st in ("smallest", "recommended"):
    print(f"== {st}: scheme | admin4 raw MB (d vs q25) | admin4 zst MB (d) | admin_vertices raw / zst | whole set raw MB (d) | whole set zst MB (d)")
    base = None
    for s in ("q25", "cap10", "rec", "small", "unc"):
        d = f"{V}/sets/{s}/{st}"
        files = sorted(os.listdir(d))
        real = {f: os.path.realpath(f"{d}/{f}") for f in files}
        a_raw = sum(z[real[f]][0] for f in FILES[st])
        a_zst = sum(z[real[f]][1] for f in FILES[st])
        s_raw = sum(z[real[f]][0] for f in files)
        s_zst = sum(z[real[f]][1] for f in files)
        v = z[real["admin_vertices.bin"]]
        out[f"{s}_{st}"] = {"admin4_raw": a_raw, "admin4_zst": a_zst, "set_raw": s_raw, "set_zst": s_zst,
                            "verts_raw": v[0], "verts_zst": v[1], "per_file": {f: z[real[f]] for f in FILES[st]}}
        if base is None:
            base = (a_raw, a_zst, s_raw, s_zst)

        def dl(x, b):
            return f"{(x / b - 1) * 100:+.1f}%"
        print(f"{s} | {a_raw / 1e6:.1f} ({dl(a_raw, base[0])}) | {a_zst / 1e6:.1f} ({dl(a_zst, base[1])}) | "
              f"{v[0] / 1e6:.1f} / {v[1] / 1e6:.1f} | {s_raw / 1e6:.1f} ({dl(s_raw, base[2])}) | {s_zst / 1e6:.1f} ({dl(s_zst, base[3])})")
json.dump(out, open(f"{V}/cmp/vsizes.json", "w"), indent=1)
