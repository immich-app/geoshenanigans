"""region_cc.py : the base set's place areas of place type 9 (state / province / region, all dropped by
f7096b0) located in Saudi Arabia or Spain, where Nominatim's address-levels.json ranks place=province
12 (sa) and 10 (es), not 0. Vertex mean of each ring -> country (grid_query.bc2e758 on the base set);
for SA/ES rings, admin_probe at that point names the place areas there, and their OSM tags come from
/ssd/verify6/osm/place_r0.opl (every place=state/province/region relation of planet-261018b)."""
import json
import re
import subprocess
from collections import Counter

import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}
V = "/ssd/verify6/"
D = V + "sets/base/recommended/"
p = np.fromfile(D + "admin_polygons.bin", PDT)
v = np.memmap(D + "admin_vertices.bin", np.uint8, "r")
pts = []
for i in np.flatnonzero((p["lvl"] == 15) & (p["pto"] == 9)):
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
    pts.append(f"{lat.mean():.6f} {lng.mean():.6f}")
res = subprocess.run([V + "bin/grid_query.bc2e758", D], input="\n".join(pts) + "\n", capture_output=True, text=True).stdout.splitlines()
cc = [(json.loads(l.split(" ", 2)[2]).get("address") or {}).get("country_code", "-") for l in res]
print("rings by country:", Counter(cc).most_common(12))
sel = [pts[k] for k in range(len(pts)) if cc[k] in ("SA", "ES")]
probe = subprocess.run([V + "bin/admin_probe.64183ef", D, V + "build/out-base/planet/quality/q2.5/admin_polygons.osm_ids"],
                       input="\n".join(sel) + "\n", capture_output=True, text=True).stdout.splitlines()
tags = {}
for line in open(V + "osm/place_r0.opl", encoding="utf-8"):
    f = line.split(" ")
    t = next((x[1:] for x in f if x.startswith("T")), "")
    tags[f[0]] = dict(kv.split("=", 1) for kv in t.strip().split(",") if "=" in kv)
seen = Counter()
for l in probe:
    for q in json.loads(l)["pip"]:
        if q.get("level") == 15 and q.get("pto") == 9:
            m = re.match(r"r(\d+)/", q.get("osm", ""))
            t = tags.get("r" + m.group(1), {}) if m else {}
            seen[(q.get("osm"), q.get("name"), t.get("place"), t.get("type"), t.get("boundary"))] += 1
for k, n in seen.most_common():
    print(n, k)
