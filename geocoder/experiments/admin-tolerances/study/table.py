#!/usr/bin/env python3
"""table.py [--set smallest|recommended] [--ref cap10] <label>... : candidate table from
cmp/size_<label>.tsv (raw + zstd -18 -T1 of the 4 admin files per set) and cmp/score_<label>.json.
Sizes: admin-minimal = min/admin_{vertices,polygons,cells,entries}; recommended quality dir =
q/admin_{vertices,polygons} + q/postal_{vertices,polygons}. MB = 1e6 bytes. Errors in % of points."""
import argparse
import json
import os

ap = argparse.ArgumentParser()
ap.add_argument("--set", default="smallest")
ap.add_argument("--ref", default="cap10")
ap.add_argument("--md", action="store_true")
ap.add_argument("labels", nargs="+")
a = ap.parse_args()
T = "/ssd/tiers3"
FILES = {"smallest": "min/", "recommended": "q/"}


def sizes(label):
    p = f"{T}/cmp/size_{label}.tsv"
    if not os.path.exists(p):
        return None
    raw = zst = 0
    for line in open(p):
        f, r, z = line.split()
        if f.startswith(FILES[a.set]):
            raw += int(r)
            zst += int(z)
    return raw, zst


def pct(acc, pool, fld):
    r = acc.get(pool)
    return 100 * r.get(fld, 0) / r["n"] if r and r.get("n") else float("nan")


ref = sizes(a.ref)
cols = ["label", "raw MB", "Δraw", "zstd MB", "Δzstd", "photo any", "photo city", "B2 cc", "B4 st", "B3 any", "B5 any",
        "B6 any", "B6 city", "B7 city", "B8 city", "B9 any", "B10 any", "brd2-8 any", "vL7 city", "vL8 city", "vphoto", "enc", "leak"]
rows = []
for lab in a.labels:
    s = sizes(lab)
    sc = json.load(open(f"{T}/cmp/score_{lab}.json")).get(a.set)
    if not sc:
        continue
    acc, g = sc["acc"], (sc["grids"] or {}).get("sum8", {})
    brd = sum(pct(acc, f"B{k}", "any") for k in range(2, 9)) / 7
    r = [lab]
    if s:
        r += [f"{s[0] / 1e6:.1f}", f"{(s[0] / ref[0] - 1) * 100:+.1f}%" if ref else "", f"{s[1] / 1e6:.1f}",
              f"{(s[1] / ref[1] - 1) * 100:+.1f}%" if ref else ""]
    else:
        r += ["", "", "", ""]
    r += [f"{pct(acc, 'photo', 'any'):.3f}", f"{pct(acc, 'photo', 'city'):.3f}", f"{pct(acc, 'B2', 'country'):.2f}",
          f"{pct(acc, 'B4', 'state'):.2f}", f"{pct(acc, 'B3', 'any'):.2f}", f"{pct(acc, 'B5', 'any'):.2f}",
          f"{pct(acc, 'B6', 'any'):.2f}", f"{pct(acc, 'B6', 'city'):.2f}", f"{pct(acc, 'B7', 'city'):.2f}",
          f"{pct(acc, 'B8', 'city'):.2f}", f"{pct(acc, 'B9', 'any'):.2f}", f"{pct(acc, 'B10', 'any'):.2f}", f"{brd:.2f}",
          f"{pct(acc, 'vborder_L7', 'city'):.2f}", f"{pct(acc, 'vborder_L8', 'city'):.2f}", f"{pct(acc, 'vphoto', 'any'):.2f}",
          f"{g.get('enc_right', 0)}", f"{g.get('leak', 0)}"]
    rows.append(r)
if a.md:
    print("| " + " | ".join(cols) + " |")
    print("|" + "---|" * len(cols))
    for r in rows:
        print("| " + " | ".join(r) + " |")
else:
    w = [max(len(c), *(len(r[i]) for r in rows)) for i, c in enumerate(cols)]
    print("  ".join(c.rjust(w[i]) for i, c in enumerate(cols)))
    for r in rows:
        print("  ".join(x.rjust(w[i]) for i, x in enumerate(r)))
