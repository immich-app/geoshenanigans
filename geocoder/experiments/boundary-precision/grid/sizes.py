#!/usr/bin/env python3
"""Vertex counts and admin_vertices.bin sizes per configuration, full levels and L2-8.

today raw  = file size of admin_vertices.bin in today's format
today zst  = len(zstd 1.5.7 -18 -T1 -c file)  (publish setting)
Bg   zst   = the same for layout Bg (layout B + per-polygon divisor, /ssd/shrink/stack/layouts.py),
             encoded with bgcodec (byte-identical to layouts.py, see logs/bg_check.log); every Bg file
             is decoded back with bgcodec decode (polygons file + Bg file only) and cmp'd against
             today's file: "rt" = IDENTICAL / DIFFERENT.
L2-8       = gridvar filter <dir> l28/<name> 2 8 (rings of admin_level 2..8, bytes verbatim, back to back);
             for q2.5 this is the bundled admin-minimal set's ring multiset (logs/minimal_check.log).
usage: sizes.py name=dir ...   -> work/sizes/<name>.json"""
import json, os, subprocess, sys, hashlib
from concurrent.futures import ThreadPoolExecutor

G = "/ssd/shrink/grid"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
os.makedirs(f"{G}/work/sizes", exist_ok=True)
os.makedirs(f"{G}/work/bg", exist_ok=True)
os.makedirs(f"{G}/work/l28", exist_ok=True)


def zst(path):
    p = subprocess.Popen([ZSTD, "-18", "-T1", "-q", "-c", path], stdout=subprocess.PIPE)
    n = 0
    while True:
        b = p.stdout.read(1 << 20)
        if not b:
            break
        n += len(b)
    assert p.wait() == 0
    return n


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 24)
            if not b:
                break
            h.update(b)
    return h.hexdigest()[:16]


def live_vertices(d):
    import numpy as np
    PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("rest", "V16")])
    p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
    live = (p["cnt"] > 0) & (p["off"] != 0xFFFFFFFF)
    return int(live.sum()), int(p["cnt"][live].astype(np.int64).sum())


def prep(name, d):
    """filter, Bg encode + decode + cmp; returns the files to zstd."""
    l28 = f"{G}/work/l28/{name}"
    os.makedirs(l28, exist_ok=True)
    flt = json.loads(subprocess.run([f"{G}/gridvar", "filter", d, l28, "2", "8"], capture_output=True, text=True,
                                    check=True).stdout)
    out = {"dir": d, "filter": flt}
    for scope, sd in (("full", d), ("l28", l28)):
        av = os.path.realpath(f"{sd}/admin_vertices.bin")
        bg = f"{G}/work/bg/{name}.{scope}.bin"
        dec = f"{G}/work/bg/{name}.{scope}.decoded"
        e = subprocess.run([f"{G}/bgcodec", "encode", sd, av, bg], capture_output=True, text=True, check=True).stdout.strip()
        subprocess.run([f"{G}/bgcodec", "decode", sd, bg, dec], capture_output=True, text=True, check=True)
        same = subprocess.run(["cmp", "-s", av, dec]).returncode == 0
        rings, verts = live_vertices(sd)
        out[scope] = {"rings": rings, "vertices": verts, "today_raw": os.path.getsize(av), "bg_raw": os.path.getsize(bg),
                      "rt": "IDENTICAL" if same else "DIFFERENT", "sha_today": sha(av), "sha_decoded": sha(dec),
                      "bg_encode": e, "today_path": av, "bg_path": bg}
        os.remove(dec)
    return out


args = [a.split("=", 1) for a in sys.argv[1:]]
with ThreadPoolExecutor(int(os.environ.get("JOBS", "6"))) as ex:
    preps = {n: ex.submit(prep, n, d) for n, d in args}
    res = {n: f.result() for n, f in preps.items()}
    zf = {}
    for n, r in res.items():
        for scope in ("full", "l28"):
            zf[(n, scope, "today_zst")] = ex.submit(zst, r[scope]["today_path"])
            zf[(n, scope, "bg_zst")] = ex.submit(zst, r[scope]["bg_path"])
    for (n, scope, k), f in zf.items():
        res[n][scope][k] = f.result()
for n, r in res.items():
    json.dump(r, open(f"{G}/work/sizes/{n}.json", "w"), indent=1)
    for scope in ("full", "l28"):
        s = r[scope]
        print(f"{n:12s} {scope:4s} rings {s['rings']:>9,} verts {s['vertices']:>12,} today raw {s['today_raw']:>14,} "
              f"zst {s['today_zst']:>14,} | Bg zst {s['bg_zst']:>14,} rt {s['rt']}", flush=True)
