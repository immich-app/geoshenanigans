#!/usr/bin/env python3
"""Facts about the dump's admin polygons: country_code fill per level, parent-chain coverage, and
how many minimal-set polygons a chain start could be (levels 3..8)."""
import numpy as np

D = "/ssd/tiers2/dump"
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])
p = np.fromfile(f"{D}/admin_polygons.raw", PDT)
par = np.fromfile(f"{D}/admin_parent_ids.raw", "<u4")
NO = 0xFFFFFFFF
print("polygons", len(p), "parents", len(par))
for l in sorted(set(p["lvl"].tolist())):
    s = p["lvl"] == l
    live = s & (p["cnt"] >= 3)
    hp = live & (par != NO)
    plv = p["lvl"][par[hp]] if hp.any() else []
    print(f"L{l}: {live.sum():,} live, cc set {np.count_nonzero(p['cc'][live]):,}, with parent {hp.sum():,}, "
          f"parent levels {np.bincount(plv, minlength=12)[:12].tolist() if len(plv) else []}, holes {int(p['holes'][live].sum()):,}")
