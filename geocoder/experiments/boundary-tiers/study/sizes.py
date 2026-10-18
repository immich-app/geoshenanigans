#!/usr/bin/env python3
"""Raw and zstd sizes of a candidate's files, today's format and layout Bg.

For var/<name>/min (admin-minimal: admin_vertices, admin_polygons, admin_cells, admin_entries,
admin_parents) and var/<name>/q (quality dir: admin_vertices, admin_polygons, postal_vertices,
postal_polygons):
  raw  = file size (on disk, today's format)
  zst  = len(zstd 1.5.7 -18 -T1 -c file)
  Bg   = admin_vertices in layout Bg (layout B + per-polygon divisor, /ssd/shrink/grid/bgcodec,
         same block sizes so raw is unchanged); its zst, and a decode + cmp round trip (rt).
zstd results are cached by sha256 (cmp/zcache.json): byte-identical files are not recompressed.
usage: sizes.py <name> ...   -> cmp/<name>.sizes.json, one line per name
"""
import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

T = "/ssd/tiers2"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
BG = "/ssd/shrink/grid/bgcodec"
CACHE = f"{T}/cmp/zcache.json"
os.makedirs(f"{T}/cmp", exist_ok=True)
os.makedirs(f"{T}/tmp/bg", exist_ok=True)
FILES = {"min": ["admin_vertices.bin", "admin_polygons.bin", "admin_cells.bin", "admin_entries.bin", "admin_parents.bin"],
         "q": ["admin_vertices.bin", "admin_polygons.bin", "postal_vertices.bin", "postal_polygons.bin"]}
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


def bg(name, part):
    d = f"{T}/var/{name}/{part}"
    out = f"{T}/tmp/bg/{name}.{part}.bg"
    dec = out + ".dec"
    subprocess.run([BG, "encode", d, f"{d}/admin_vertices.bin", out], check=True, capture_output=True)
    subprocess.run([BG, "decode", d, out, dec], check=True, capture_output=True)
    rt = subprocess.run(["cmp", "-s", f"{d}/admin_vertices.bin", dec]).returncode == 0
    os.remove(dec)
    return out, rt


res = {}
with ThreadPoolExecutor(int(os.environ.get("JOBS", "12"))) as ex:
    jobs = {}
    for name in sys.argv[1:]:
        for part, files in FILES.items():
            d = f"{T}/var/{name}/{part}"
            if not os.path.exists(f"{d}/admin_vertices.bin"):
                continue
            for f in files:
                if os.path.exists(f"{d}/{f}"):
                    jobs[(name, part, f)] = (os.path.getsize(f"{d}/{f}"), ex.submit(zst, f"{d}/{f}"))
            jobs[(name, part, "bg")] = ex.submit(bg, name, part)
    bgz = {}
    for key, v in jobs.items():
        if key[2] == "bg":
            path, rt = v.result()
            bgz[key] = (os.path.getsize(path), rt, ex.submit(zst, path), path)
    for (name, part, f), v in jobs.items():
        r = res.setdefault(name, {}).setdefault(part, {})
        if f == "bg":
            raw, rt, fut, path = bgz[(name, part, f)]
            r["bg"] = {"raw": raw, "zst": fut.result(), "rt": "IDENTICAL" if rt else "DIFFERENT"}
            os.remove(path)
        else:
            raw, fut = v
            r[f] = {"raw": raw, "zst": fut.result()}
json.dump(cache, open(CACHE, "w"))
for name, r in res.items():
    json.dump(r, open(f"{T}/cmp/{name}.sizes.json", "w"), indent=1)
    line = name
    for part in ("min", "q"):
        if part not in r:
            continue
        v, b = r[part]["admin_vertices.bin"], r[part]["bg"]
        line += f" | {part} verts raw {v['raw']:,} zst {v['zst']:,} Bg zst {b['zst']:,} rt {b['rt']}"
    print(line, flush=True)
