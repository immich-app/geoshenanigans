#!/usr/bin/env python3
"""sizes.py <name> ... : raw (file size) and zstd 1.5.7 -18 -T1 sizes of a candidate's admin files.

Per-polygon candidates (var/<n>/{min,q} from tiervar, today's format):
  min: admin_vertices, admin_polygons, admin_cells, admin_entries ("4 admin files")
  q:   admin_vertices, admin_polygons, postal_vertices, postal_polygons
  planned = the same with every *_vertices file in layout Bg (/ssd/shrink/grid/bgcodec: same raw
            size, per-polygon zigzag deltas as byte planes; decode + cmp round trip checked)
Arc candidates (var/<n>/{min,q}/arc + dec):
  min: arc_vertices, arc_index, poly_arcs (u32 refs) | poly_arcs_v (varint refs), admin_polygons (arc
       records), and the unchanged admin_cells / admin_entries (dec/)
  q:   arc_vertices, arc_index, poly_arcs | poly_arcs_v, admin_polygons, postal_polygons
zstd results cached by sha256 (cmp/zcache.json). MB = 1e6 bytes.
-> cmp/<name>.sizes.json and one line per name.
"""
import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

A = "/ssd/arcs"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
BG = "/ssd/shrink/grid/bgcodec"
CACHE = f"{A}/cmp/zcache.json"
os.makedirs(f"{A}/cmp", exist_ok=True)
os.makedirs(f"{A}/tmp/bg", exist_ok=True)
cache = json.load(open(CACHE)) if os.path.exists(CACHE) else {}


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(1 << 24):
            h.update(b)
    return h.hexdigest()


def zst(path):
    k = sha(path)
    if k in cache:
        return cache[k]
    p = subprocess.Popen([ZSTD, "-18", "-T1", "-q", "-c", path], stdout=subprocess.PIPE)
    n = 0
    while b := p.stdout.read(1 << 20):
        n += len(b)
    assert p.wait() == 0
    cache[k] = n
    return n


def bg(polys, verts, tag):
    """layout Bg of a vertex file whose polygon records are `polys`: (raw, zst, round trip ok)."""
    d = f"{A}/tmp/bg/{tag}"
    os.makedirs(d, exist_ok=True)
    link = f"{d}/admin_polygons.bin"
    if os.path.lexists(link):
        os.remove(link)
    os.symlink(polys, link)
    out, dec = f"{d}/v.bg", f"{d}/v.dec"
    subprocess.run([BG, "encode", d, verts, out], check=True, capture_output=True)
    subprocess.run([BG, "decode", d, out, dec], check=True, capture_output=True)
    rt = subprocess.run(["cmp", "-s", verts, dec]).returncode == 0
    raw, z = os.path.getsize(out), zst(out)
    for f in (out, dec, link):
        os.remove(f)
    os.rmdir(d)
    return raw, z, rt


def sizes(name):
    V = f"{A}/var/{name}"
    r = {}
    if os.path.isdir(f"{V}/min/arc"):
        r["kind"] = "arc"
        for part in ("min", "q"):
            a, d = f"{V}/{part}/arc", f"{V}/{part}/dec"
            files = {f: f"{a}/{f}" for f in ("arc_vertices.bin", "arc_index.bin", "poly_arcs.bin", "poly_arcs_v.bin", "admin_polygons.bin")}
            if part == "min":
                files.update({f: f"{d}/{f}" for f in ("admin_cells.bin", "admin_entries.bin")})
            elif os.path.exists(f"{a}/postal_polygons.bin"):
                files["postal_polygons.bin"] = f"{a}/postal_polygons.bin"
            r[part] = {f: {"raw": os.path.getsize(p), "zst": zst(p)} for f, p in files.items()}
            u32 = sum(v["raw"] for f, v in r[part].items() if f != "poly_arcs_v.bin")
            u32z = sum(v["zst"] for f, v in r[part].items() if f != "poly_arcs_v.bin")
            var = sum(v["raw"] for f, v in r[part].items() if f != "poly_arcs.bin")
            varz = sum(v["zst"] for f, v in r[part].items() if f != "poly_arcs.bin")
            r[part]["total"] = {"raw": u32, "zst": u32z, "raw_varint": var, "zst_varint": varz}
    else:
        r["kind"] = "pp"
        for part in ("min", "q"):
            d = f"{V}/{part}"
            names = ["admin_vertices.bin", "admin_polygons.bin"] + (
                ["admin_cells.bin", "admin_entries.bin"] if part == "min" else ["postal_vertices.bin", "postal_polygons.bin"])
            r[part] = {f: {"raw": os.path.getsize(f"{d}/{f}"), "zst": zst(f"{d}/{f}")} for f in names if os.path.exists(f"{d}/{f}")}
            b = bg(f"{d}/admin_polygons.bin", f"{d}/admin_vertices.bin", f"{name}.{part}.a")
            r[part]["admin_vertices.bin"]["bg"] = {"raw": b[0], "zst": b[1], "rt": b[2]}
            if part == "q" and os.path.exists(f"{d}/postal_vertices.bin"):
                b = bg(f"{d}/postal_polygons.bin", f"{d}/postal_vertices.bin", f"{name}.{part}.p")
                r[part]["postal_vertices.bin"]["bg"] = {"raw": b[0], "zst": b[1], "rt": b[2]}
            raw = sum(v["raw"] for v in r[part].values())
            z = sum(v["zst"] for v in r[part].values())
            planned = sum(v["bg"]["zst"] if "bg" in v else v["zst"] for v in r[part].values())
            r[part]["total"] = {"raw": raw, "zst": z, "planned_zst": planned}
    return r


if __name__ == "__main__":
    names = sys.argv[1:]
    with ThreadPoolExecutor(int(os.environ.get("JOBS", "8"))) as ex:
        res = dict(zip(names, ex.map(sizes, names)))
    json.dump(cache, open(CACHE, "w"))
    for n, r in res.items():
        json.dump(r, open(f"{A}/cmp/{n}.sizes.json", "w"), indent=1)
        line = f"SIZES {n} ({r['kind']})"
        for part in ("min", "q"):
            t = r[part]["total"]
            if r["kind"] == "arc":
                line += (f" | {part}: raw {t['raw'] / 1e6:.1f} zst {t['zst'] / 1e6:.1f} (varint refs: raw {t['raw_varint'] / 1e6:.1f}"
                         f" zst {t['zst_varint'] / 1e6:.1f})")
            else:
                line += f" | {part}: raw {t['raw'] / 1e6:.1f} zst {t['zst'] / 1e6:.1f} planned {t['planned_zst'] / 1e6:.1f}"
        print(line, flush=True)
