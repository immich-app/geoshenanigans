#!/usr/bin/env python3
"""Verifier side checks:
 1. share of truth (idx/max) L2-8 vertices stored on the 1e-4 grid (enc 1), by storage encoding
 2. vertices per admin level for q2.5 (withaddr), truth, and the uniform-tolerance variants
 3. truth answers of the measured L2 pool without a country_code
 4. l28 of q2.5 vs idx/smallest: multiset of (level, cc, area bits, block bytes) per live ring"""
import collections, hashlib, json
import numpy as np

NO = 0xFFFFFFFF
H = 10
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
G = "/ssd/shrink/grid"


def load(d):
    p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
    live = (p["cnt"] > 0) & (p["off"] != NO)
    return p, live


p, live = load("/ssd/shrink/idx/max")
vb = np.memmap("/ssd/shrink/idx/max/admin_vertices.bin", np.uint8, "r")
sel = live & (p["lvl"] >= 2) & (p["lvl"] <= 8)
enc = np.asarray(vb[p["off"][sel].astype(np.int64)])
cnt = p["cnt"][sel].astype(np.int64)
tot = cnt.sum()
print("1. truth L2-8 vertices by storage enc (0=1e-5,1=1e-4,2=1e-6,3=1e-7):",
      {int(e): f"{cnt[enc == e].sum():,} ({100 * cnt[enc == e].sum() / tot:.1f}%)" for e in range(4)})

out = {}
for n, d in (("T", "/ssd/shrink/idx/max"), ("q25", "/ssd/shrink/idx/withaddr"), ("unc_dp37_g4", f"{G}/var/unc_dp37_g4"),
             ("unc_dp22_g4", f"{G}/var/unc_dp22_g4"), ("unc_dp11_g4", f"{G}/var/unc_dp11_g4")):
    q, lv = load(d)
    out[n] = {int(L): int(q["cnt"][lv & (q["lvl"] == L)].astype(np.int64).sum()) for L in np.unique(q["lvl"][lv])}
print("2. vertices per level:")
for n, v in out.items():
    print("  ", n, {k: f"{x:,}" for k, x in v.items() if k <= 8}, "L2-8 total", f"{sum(x for k, x in v.items() if 2 <= k <= 8):,}")

nocc = tot_l2 = 0
with open(f"{G}/q/T.L2.jsonl") as f:
    for line in f:
        js = json.loads(line.split(" ", 2)[2])
        tot_l2 += 1
        nocc += js.get("address", {}).get("country_code") is None
print(f"3. measured L2 pool, truth answers without country_code: {nocc} / {tot_l2} = {100 * nocc / tot_l2:.2f}%")


def multiset(d):
    q, lv = load(d)
    b = np.fromfile(f"{d}/admin_vertices.bin", np.uint8)
    c = collections.Counter()
    for i in np.nonzero(lv)[0]:
        o, n = int(q["off"][i]), int(q["cnt"][i])
        size = H + n * (8 if b[o] == 3 else 4)
        c[(int(q["lvl"][i]), int(q["cc"][i]), q["area"][i].tobytes(), hashlib.blake2b(b[o:o + size].tobytes(), digest_size=16).digest())] += 1
    return c


a, s = multiset(f"{G}/vfy/l28/q25"), multiset("/ssd/shrink/idx/smallest")
print(f"4. q2.5 L2-8 filter vs idx/smallest ring multiset: rings {sum(a.values()):,} vs {sum(s.values()):,}; equal: {a == s}; "
      f"only-in-filter {sum((a - s).values())}, only-in-smallest {sum((s - a).values())}")
