#!/usr/bin/env python3
"""Shipped size of each set (planet-261006, shipped record files) and what search adds to it, at
the nightly's compression (zstd 1.5.7 -18, compress_index.py) and raw:
  set files          sets-orig/<set>/* (each distinct file once)
  index              idx/<config>/* except search_meta.json
  importance bytes   extras/<set>/imp_*.u8 (dense, one byte per record slot)
  iso codes          extras/<set>/iso_codes.tsv
  builder change     sets/<set> record files minus the shipped ones (hidden flag + names kept,
                     boundary primary-ring bit, POI secondary-ring flag)
Output: rg/sizes.json + a markdown table on stdout."""
import concurrent.futures as cf
import json
import os
import subprocess

Z = "/ssd/shrink/zstd157/bin/zstd"
SP = "/ssd/search-prod/"
SETS = {"minimal": "min-1c", "recommended": "rec-1c", "max": "max-1c"}
EXTRA_IDX = {"recommended": ["rec-x-1c"], "max": ["maxn-1c"]}


def zsize(path):
    out = subprocess.run([Z, "-18", "-T8", "-c", "-q", path], capture_output=True, check=True).stdout
    return len(out)


def files(d, skip=()):
    return sorted(os.path.join(d, f) for f in os.listdir(d) if f not in skip)


jobs = set()
plan = {}
for s, cfg in SETS.items():
    plan[s] = {
        "set": files(SP + f"rg/sets-orig/{s}"),
        "index": {c: files(SP + f"idx/{c}", ("search_meta.json",)) for c in [cfg] + EXTRA_IDX.get(s, [])},
        "imp": [p for p in files(SP + f"extras/{s}") if os.path.basename(p).startswith("imp_")],
        "iso": [SP + f"extras/{s}/iso_codes.tsv"],
        "emu": [(SP + f"rg/sets-orig/{s}/{f}", SP + f"sets/{s}/{f}") for f in ("admin_polygons.bin", "place_nodes.bin", "poi_records.bin")
                if os.path.exists(SP + f"sets/{s}/{f}")],
    }
    for k in ("set", "imp", "iso"):
        jobs.update(os.path.realpath(p) for p in plan[s][k])
    for ps in plan[s]["index"].values():
        jobs.update(ps)
    for a, b in plan[s]["emu"]:
        jobs.update((os.path.realpath(a), b))
jobs = sorted(jobs, key=os.path.getsize, reverse=True)
with cf.ThreadPoolExecutor(6) as ex:
    z = dict(zip(jobs, ex.map(zsize, jobs)))


def tot(paths):
    rp = sorted({os.path.realpath(p) for p in paths})
    return sum(os.path.getsize(p) for p in rp), sum(z[p] for p in rp)


res = {}
for s, pl in plan.items():
    r = {"set": tot(pl["set"]), "imp": tot(pl["imp"]), "iso": tot(pl["iso"])}
    r["index"] = {c: tot(ps) for c, ps in pl["index"].items()}
    r["index_files"] = {c: {os.path.basename(p): [os.path.getsize(p), z[p]] for p in ps} for c, ps in pl["index"].items()}
    r["emu"] = {os.path.basename(b): [os.path.getsize(b) - os.path.getsize(a), z[b] - z[os.path.realpath(a)]] for a, b in pl["emu"]}
    res[s] = r
json.dump(res, open(SP + "rg/sizes.json", "w"), indent=1)
MiB = 2 ** 20
print("| set | set raw / zst MiB | index config | index raw / zst MiB | imp bytes zst MiB | iso zst KiB | builder change zst MiB | search total zst MiB | % of set (zst) |")
print("|---|---:|---|---:|---:|---:|---:|---:|---:|")
for s, r in res.items():
    emu = sum(v[1] for v in r["emu"].values())
    for c, (ir, iz) in r["index"].items():
        total = iz + r["imp"][1] + r["iso"][1] + emu
        print(f"| {s} | {r['set'][0] / MiB:.1f} / {r['set'][1] / MiB:.1f} | {c} | {ir / MiB:.1f} / {iz / MiB:.1f} | {r['imp'][1] / MiB:.2f} | "
              f"{r['iso'][1] / 1024:.1f} | {emu / MiB:+.2f} | {total / MiB:.1f} | {100 * total / r['set'][1]:.1f} |")
