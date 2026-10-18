#!/usr/bin/env python3
"""Are admin-minimal rings byte-identical to the same rings in quality/q2.5 (admin mode)?
Matches rings on (name_id, admin_level, vertex_count, area bits) and compares packed vertex bytes.
Usage: q25_check.py <minimal dir> <recommended dir>"""
import sys
import numpy as np
from inventory import load_polys
m, r = sys.argv[1:3]
pm, vm, lm, hm, em, nm = load_polys(m)
pr, vr, lr, hr, er, nr = load_polys(r)
def keys(p, has):
    a = p["area"].view(np.uint32).astype(np.uint64)
    d = {}
    for i in np.flatnonzero(has):
        d.setdefault((int(p["name"][i]), int(p["level"][i]), int(p["vcnt"][i]), int(a[i])), []).append(i)
    return d
kr = keys(pr, hr)
same = diff = miss = 0
for i in np.flatnonzero(hm):
    k = (int(pm["name"][i]), int(pm["level"][i]), int(pm["vcnt"][i]), int(pm["area"].view(np.uint32)[i]))
    js = kr.get(k)
    if js is None:
        miss += 1; continue
    a = vm[pm["voff"][i]:pm["voff"][i] + nm[i]]
    if any(nm[i] == nr[j] and (a == vr[pr["voff"][j]:pr["voff"][j] + nr[j]]).all() for j in js): same += 1
    else: diff += 1
print("minimal rings", int(hm.sum()), "identical to a q2.5 ring", same, "differ", diff, "no match", miss)
lv = pr["level"][lr]
print("admin-mode q2.5 rings by level:", {int(L): int((lv == L).sum()) for L in np.unique(lv)})
