#!/usr/bin/env python3
"""Zero-raw-byte importance: the shipped importance byte written into record padding instead of
imp_places.u8 / imp_polys.u8 (PlaceNode._pad2 low byte, AdminPolygon._pad3), compressed size vs the
emulated record file plus the separate dense byte file (zstd 1.5.7 -18, the nightly's level).
POIs already carry a builder importance byte (PoiRecord.imp); imp_poi.u8 is left as a file.
Output: rg/imp_in_pad.txt"""
import subprocess

import numpy as np

Z = "/ssd/shrink/zstd157/bin/zstd"
SP = "/ssd/search-prod/"
OUT = SP + "rg/tmp_pad/"
pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])


def z(path):
    return len(subprocess.run([Z, "-18", "-T8", "-c", "-q", path], capture_output=True, check=True).stdout)


import os
os.makedirs(OUT, exist_ok=True)
lines = []
for s in ("minimal", "recommended"):
    for rec, dt, field, imp in (("place_nodes.bin", pn_dt, "p2", "imp_places.u8"), ("admin_polygons.bin", ap_dt, "p3", "imp_polys.u8")):
        a = np.fromfile(SP + f"sets/{s}/{rec}", dtype=dt)
        b = np.fromfile(SP + f"extras/{s}/{imp}", dtype=np.uint8)
        assert len(a) == len(b), (s, rec, len(a), len(b))
        assert (a[field] == 0).all(), (s, rec, field)
        a[field] = b
        p = OUT + f"{s}.{rec}"
        a.tofile(p)
        base, sep, pad = z(SP + f"sets/{s}/{rec}"), z(SP + f"extras/{s}/{imp}"), z(p)
        lines.append(f"{s} {rec} {field}: emulated record file {base} + {imp} {sep} = {base + sep}; importance in padding {pad} ({pad - base - sep:+d})")
open(SP + "rg/imp_in_pad.txt", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
