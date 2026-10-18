"""Can rings of one boundary be grouped from shipped fields alone? (planet-261006, admin-minimal + q2.5)"""
import collections

import numpy as np

P = "/ssd/precision/build/out-nocap/planet/"
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
slot_dt = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])

for q in ("admin-minimal", "quality/q2.5"):
    ap = np.fromfile(P + q + "/admin_polygons.bin", dtype=ap_dt)
    raw = np.fromfile(P + q + "/admin_polygons.osm_ids", dtype=np.uint8)
    sl = np.frombuffer(raw[12:].tobytes(), dtype=slot_dt)
    rid = np.where(sl["type"] == 3, sl["id"] >> np.uint64(16), sl["id"])
    k = (sl["type"].astype(np.uint64) << np.uint64(56)) | rid
    # rings sharing an osm key: same area value?
    order = np.argsort(k, kind="stable")
    ks = k[order]
    starts = np.r_[0, np.nonzero(ks[1:] != ks[:-1])[0] + 1]
    ends = np.r_[starts[1:], len(ks)]
    multi = [(s, e) for s, e in zip(starts, ends) if e - s > 1]
    same_area = sum(1 for s, e in multi if len(set(ap["area"][order[s:e]].tolist())) == 1)
    same_key = sum(1 for s, e in multi if len({(int(ap["name_id"][i]), int(ap["level"][i]), int(ap["ovr"][i]), int(ap["cc"][i])) for i in order[s:e]}) == 1)
    contiguous = sum(1 for s, e in multi if (np.diff(np.sort(order[s:e])) == 1).all())
    print(q, "boundaries with >1 ring", len(multi), "same area on all rings", same_area, "same name/level/ovr/cc", same_key, "contiguous", contiguous)
    # adjacent different boundaries with same name/level/ovr/cc: same area?
    g = np.stack([ap["name_id"].astype(np.int64), ap["level"].astype(np.int64), ap["ovr"].astype(np.int64), ap["cc"].astype(np.int64)], 1)
    adj_same = (g[1:] == g[:-1]).all(1)
    diff_b = k[1:] != k[:-1]
    both = np.nonzero(adj_same & diff_b)[0]
    eq_area = int((ap["area"][both] == ap["area"][both + 1]).sum())
    print("  adjacent distinct boundaries with same name/level/ovr/cc:", len(both), "of which equal area", eq_area)
    same_b = np.nonzero(adj_same & ~diff_b)[0]
    print("  adjacent rings of the same boundary with same key:", len(same_b), "equal area", int((ap["area"][same_b] == ap["area"][same_b + 1]).sum()))
    # sample: what are the adjacent distinct same-named ones
    print("  levels of adjacent distinct same-named:", sorted(collections.Counter(ap["level"][both].tolist()).items()))
    print("  sample osm keys:", [(int(sl["type"][i]), int(rid[i]), int(sl["type"][i + 1]), int(rid[i + 1])) for i in both[:8]])
