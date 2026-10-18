#!/usr/bin/env python3
"""One markdown table: configuration -> vertices -> admin_vertices.bin sizes (today's format raw / zstd -18,
layout Bg zstd -18; full levels and L2-8) -> Immich-field and admin-field flip rates per pool vs truth.
Inputs: work/sizes/<name>.json (sizes.py), work/compare_T_*.json (compare.py T ... --json).
usage: report.py name ...  (in table order)"""
import glob, json, sys

G = "/ssd/shrink/grid"
MB = 1e6
POOLS = ["places", "random", "p200k", "L2", "L4", "L8"]
names = sys.argv[1:]
cmp = {}
for f in glob.glob(f"{G}/work/compare_T_*.json"):
    j = json.load(open(f))
    assert j["base"] == "T"
    cmp.update(j["res"])
print("| config | vertices full | full: today raw / zst MB | full: Bg zst MB | vertices L2-8 | L2-8: today raw / zst MB | L2-8: Bg zst MB | "
      + " | ".join(f"{p} Immich / admin %" for p in POOLS) + " |")
print("|---|---:|---:|---:|---:|---:|---:|" + "---:|" * len(POOLS))
for n in names:
    s = json.load(open(f"{G}/work/sizes/{n}.json"))
    f, l = s["full"], s["l28"]
    assert f["rt"] == "IDENTICAL" and l["rt"] == "IDENTICAL", n
    cells = []
    for p in POOLS:
        if n == "T":
            cells.append("truth")
            continue
        r = cmp[n][p]
        cells.append(f"{100 * r['immich'] / r['n']:.2f} / {100 * r['admin'] / r['n']:.2f}")
    print(f"| {n} | {f['vertices'] / 1e6:.1f} M | {f['today_raw'] / MB:,.0f} / {f['today_zst'] / MB:,.0f} | {f['bg_zst'] / MB:,.0f} | "
          f"{l['vertices'] / 1e6:.1f} M | {l['today_raw'] / MB:,.0f} / {l['today_zst'] / MB:,.0f} | {l['bg_zst'] / MB:,.0f} | " + " | ".join(cells) + " |")
