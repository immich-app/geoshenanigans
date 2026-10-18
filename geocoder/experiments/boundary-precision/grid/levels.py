"""Vertex counts per admin level and reader-skipped (area <= 0) rings for each configuration dir.
usage: levels.py name=dir ...  -> work/levels.json"""
import json, sys, numpy as np
PDT = np.dtype([("off","<u4"),("cnt","<u4"),("name","<u4"),("lvl","u1"),("pto","u1"),("p2","u1"),("p3","u1"),("area","<f4"),("cc","<u2"),("p4","<u2")])
out = {}
for a in sys.argv[1:]:
    n, d = a.split("=", 1)
    p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
    live = (p["cnt"] > 0) & (p["off"] != 0xFFFFFFFF)
    lv = {int(l): int(p["cnt"][live & (p["lvl"] == l)].astype(np.int64).sum()) for l in np.unique(p["lvl"][live])}
    out[n] = {"slots": len(p), "live": int(live.sum()), "area_le0_live": int((live & ~(p["area"] > 0)).sum()), "vertices_by_level": lv,
              "vertices": int(p["cnt"][live].astype(np.int64).sum())}
    print(n, out[n], flush=True)
json.dump(out, open("work/levels.json", "w"), indent=1)
