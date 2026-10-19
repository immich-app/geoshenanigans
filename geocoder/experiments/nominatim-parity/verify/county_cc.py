"""county_cc.py : countries of the place=county / place=district areas (admin level 15, place type 10 / 11)
in verify6's tip recommended set: vertex mean of each ring, country from grid_query.64183ef at that
point. Nominatim ranks place=county 0 in ca and de (address-levels.json), 12 elsewhere."""
import subprocess
from collections import Counter
import json

import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}
D = "/ssd/verify6/sets/tip/recommended/"
p = np.fromfile(D + "admin_polygons.bin", PDT)
v = np.memmap(D + "admin_vertices.bin", np.uint8, "r")
out = {}
for pto in (10, 11):
    pts = []
    for i in np.flatnonzero((p["lvl"] == 15) & (p["pto"] == pto)):
        off, n = int(p["off"][i]), int(p["cnt"][i])
        if n < 3:
            continue
        enc = int(v[off])
        blat, blng = np.frombuffer(v[off + 2:off + 10].tobytes(), "<f4").astype(np.float64)
        body = v[off + 10:]
        if enc == 4:
            a = np.frombuffer(body[:8 * n].tobytes(), "<f4").reshape(-1, 2).astype(np.float64)
            lat, lng = a[:, 0], a[:, 1]
        else:
            a = np.frombuffer(body[:(8 if enc == 3 else 4) * n].tobytes(), "<u4" if enc == 3 else "<u2").reshape(-1, 2).astype(np.float64)
            lat, lng = blat + a[:, 0] * SCALE[enc], blng + a[:, 1] * SCALE[enc]
        pts.append(f"{lat.mean():.6f} {lng.mean():.6f}\n")
    res = subprocess.run(["/ssd/verify6/bin/grid_query.64183ef", D], input="".join(pts), capture_output=True, text=True).stdout.splitlines()
    out[pto] = Counter((json.loads(l.split(" ", 2)[2]).get("address") or {}).get("country_code", "-") for l in res)
print("place=county areas by country:", out[10].most_common())
print("place=district areas by country:", out[11].most_common(12))
