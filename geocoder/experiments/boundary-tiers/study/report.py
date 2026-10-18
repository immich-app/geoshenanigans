#!/usr/bin/env python3
"""Pareto tables of the tier candidates (markdown on stdout, all numbers in cmp/report.json).

Inputs (all measured on the box): cmp/<name>.sizes.json (sizes.py), cmp/<label>_<set>.score.json
(score.py), cmp/<label>_<set>.grids.json (grids.py), l2c/<label>/eval.json (l2c.sh), var/<name>/*.json
(tiervar stats). A label ending in "g" is the chain reader (candidate g) on the geometry of <name>
(the label without the "g"); it ships admin_parents.bin besides the 4 admin files.
usage: report.py <label> ...
"""
import hashlib
import json
import os
import subprocess
import sys

T = "/ssd/tiers2"
MB = 1e6
GRIDS = ["vatican", "campione", "busingen", "baarle", "llivia", "jungholz", "san_marino", "lesotho"]
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
cache = json.load(open(f"{T}/cmp/zcache.json"))


def zst(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(1 << 24):
            h.update(b)
    k = h.hexdigest()
    if k not in cache:
        out = subprocess.run([ZSTD, "-18", "-T1", "-q", "-c", path], capture_output=True, check=True).stdout
        cache[k] = len(out)
    return cache[k]


def geom_of(label):
    return label[:-1] if label.endswith("g") and os.path.exists(f"{T}/var/{label[:-1]}") else label


rows = {}
for label in sys.argv[1:]:
    g = geom_of(label)
    chain = g != label
    sz = json.load(open(f"{T}/cmp/{g}.sizes.json"))
    st = {p: json.load(open(f"{T}/var/{g}/{p}.json")) for p in ("min", "q") if os.path.exists(f"{T}/var/{g}/{p}.json")}
    r = {"label": label, "geometry": g, "chain": chain, "scheme": st["min"]["scheme"]}
    for s, part in (("smallest", "min"), ("recommended", "q")):
        z = sz[part]
        files = ["admin_vertices.bin", "admin_polygons.bin"] + (["admin_cells.bin", "admin_entries.bin"] if part == "min" else
                                                                ["postal_vertices.bin", "postal_polygons.bin"])
        raw = sum(z[f]["raw"] for f in files)
        zz = sum(z[f]["zst"] for f in files)
        zb = zz - z["admin_vertices.bin"]["zst"] + z["bg"]["zst"]
        par_raw = par_zst = 0
        if chain:
            pf = f"{T}/var/{g}/min/admin_parents.bin" if part == "min" else os.path.realpath(f"{T}/sets/{g}/recommended/admin_parents.bin")
            par_raw, par_zst = os.path.getsize(pf), zst(pf)
        verts = sum(v[1] for v in st[part]["levels"].values())
        if part == "min":
            lv = st[part]["levels"]
        sc = json.load(open(f"{T}/cmp/{label}_{s}.score.json"))
        gr = json.load(open(f"{T}/cmp/{label}_{s}.grids.json"))
        l2c = json.load(open(f"{T}/l2c/{label}/eval.json"))["summary"][s] if os.path.exists(f"{T}/l2c/{label}/eval.json") else None
        pct = lambda p, k: 100 * sc[p][k] / sc[p]["n"]
        r[s] = {
            "vertices": verts, "verts_raw": z["admin_vertices.bin"]["raw"], "verts_zst": z["admin_vertices.bin"]["zst"],
            "verts_bg_zst": z["bg"]["zst"], "files": files, "raw": raw + par_raw, "zst": zz + par_zst, "zst_bg": zb + par_zst,
            "parents_raw": par_raw, "parents_zst": par_zst,
            "acc": {p: {k: pct(p, k) for k in ("immich", "city", "state", "country", "cc_wrong", "cc_none", "subcity", "postcode")} for p in sc},
            "enc_ok": sum(gr[x]["enc_cc"] for x in GRIDS), "enc_n": sum(gr[x]["n_enc"] for x in GRIDS),
            "leaks": sum(gr[x]["leak"] for x in GRIDS), "grid_cc": sum(gr[x]["cc"] for x in GRIDS),
            "grid_n": sum(gr[x]["n_cc"] for x in GRIDS),
            "enc_per_grid": {x: f"{gr[x]['enc_cc']}/{gr[x]['n_enc']}" for x in GRIDS},
            "l2c": l2c["all_pass"] if l2c else None,
        }
    rows[label] = r
json.dump(cache, open(f"{T}/cmp/zcache.json", "w"))
json.dump(rows, open(f"{T}/cmp/report.json", "w"), indent=1)

for s in ("smallest", "recommended"):
    base = rows.get("q25", next(iter(rows.values())))[s]
    print(f"\n### {s}: sizes (MB = 10^6 B)\n")
    print("| candidate | scheme | vertices M | admin_vertices raw | zst today | zst Bg | parents raw / zst | files raw (Δ vs q2.5) | files zst today (Δ) | files zst planned Bg (Δ) |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for label, r in rows.items():
        x = r[s]
        d = lambda k: f"{x[k] / MB:.1f} ({100 * (x[k] - base[k]) / base[k]:+.1f}%)"
        par = f"{x['parents_raw'] / MB:.2f} / {x['parents_zst'] / MB:.2f}" if r["chain"] else "-"
        name = label + (" (chain)" if r["chain"] else "")
        print(f"| {name} | `{r['scheme']}` | {x['vertices'] / 1e6:.2f} | {x['verts_raw'] / MB:.1f} | {x['verts_zst'] / MB:.1f} | "
              f"{x['verts_bg_zst'] / MB:.1f} | {par} | {d('raw')} | {d('zst')} | {d('zst_bg')} |")
    print(f"\n### {s}: accuracy (% of points whose answer differs from the uncapped-with-holes truth)\n")
    print("| candidate | photo p200k Immich (city/state/country) | places 20k | L2 ±200 m (wrong country / country↔none) | L4 ±200 m (state) | L8 ±200 m (city) | uniform 100k | sub-city p200k | enclave pts right | leaks | l2c pass |")
    print("|---|---|---:|---|---|---|---:|---:|---:|---:|---:|")
    for label, r in rows.items():
        x = r[s]
        a = x["acc"]
        name = label + (" (chain)" if r["chain"] else "")
        p = a["p200k"]
        print(f"| {name} | {p['immich']:.2f} ({p['city']:.2f}/{p['state']:.2f}/{p['country']:.2f}) | {a['places']['immich']:.2f} | "
              f"{a['L2']['immich']:.1f} ({a['L2']['cc_wrong']:.1f} / {a['L2']['cc_none']:.1f}) | {a['L4']['immich']:.1f} ({a['L4']['state']:.1f}) | "
              f"{a['L8']['immich']:.2f} ({a['L8']['city']:.2f}) | {a['uniform100k']['immich']:.3f} | {p['subcity']:.2f} | "
              f"{x['enc_ok']}/{x['enc_n']} | {x['leaks']} | {x['l2c']} |")
