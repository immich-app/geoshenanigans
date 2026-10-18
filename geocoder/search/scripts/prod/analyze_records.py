"""Survey of the shipped records the search index would reference (planet-261006 instrumented build)."""
import collections
import hashlib

import numpy as np

P = "/ssd/precision/build/out-nocap/planet/"
NO = 0xFFFFFFFF
pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
slot_dt = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])
rec_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("voff", "<u4"), ("vcnt", "<u4"), ("name", "<u4"), ("cat", "u1"), ("tier", "u1"), ("flags", "u1"), ("imp", "u1"), ("ps", "<u4"), ("pp", "<u4"), ("ppoly", "<u4")])


def md5(p):
    return hashlib.md5(open(p, "rb").read()).hexdigest()


for mode in ("admin-minimal", "admin", "full"):
    pn = np.fromfile(P + mode + "/place_nodes.bin", dtype=pn_dt)
    vis = pn["name_id"] != NO
    print(mode, "place_nodes", len(pn), "visible", int(vis.sum()), "hidden", int((~vis).sum()))
    print("  by type visible", dict(collections.Counter(pn["pt"][vis].tolist())), "hidden", dict(collections.Counter(pn["pt"][~vis].tolist())))
print("admin vs full place_nodes identical:", md5(P + "admin/place_nodes.bin") == md5(P + "full/place_nodes.bin"))
print("strings_core identical minimal/admin/full:", len({md5(P + m + "/strings_core.bin") for m in ("admin-minimal", "admin", "full")}) == 1)

for q in ("admin-minimal", "quality/q2.5", "quality/uncapped"):
    ap = np.fromfile(P + q + "/admin_polygons.bin", dtype=ap_dt)
    named = ap["name_id"] != NO
    print(q, "polygons", len(ap), "named", int(named.sum()), "area>0", int((ap["area"] > 0).sum()))
    print("  levels named", sorted(collections.Counter(ap["level"][named].tolist()).items()))
    print("  overrides named", sorted(collections.Counter(ap["ovr"][named].tolist()).items()))
a = np.fromfile(P + "quality/q2.5/admin_polygons.bin", dtype=ap_dt)
b = np.fromfile(P + "quality/uncapped/admin_polygons.bin", dtype=ap_dt)
same = all((a[f] == b[f]).all() for f in ("name_id", "level", "ovr", "cc"))
print("q2.5 vs uncapped same name/level/ovr/cc per record:", same, "area equal:", bool((a["area"] == b["area"]).all()))

for q in ("admin-minimal", "quality/q2.5"):
    ap = np.fromfile(P + q + "/admin_polygons.bin", dtype=ap_dt)
    raw = np.fromfile(P + q + "/admin_polygons.osm_ids", dtype=np.uint8)
    sl = np.frombuffer(raw[12:].tobytes(), dtype=slot_dt)
    assert len(sl) == len(ap), (len(sl), len(ap))
    # relation slots carry (id << 16 | ring); way slots carry the way id
    rid = np.where(sl["type"] == 3, sl["id"] >> np.uint64(16), sl["id"])
    k = (sl["type"].astype(np.uint64) << np.uint64(56)) | rid
    distinct = len(np.unique(k))
    kb = np.r_[True, k[1:] != k[:-1]]
    g = np.stack([ap["name_id"].astype(np.int64), ap["level"].astype(np.int64), ap["cc"].astype(np.int64)], 1)
    hb = np.r_[True, (g[1:] != g[:-1]).any(1)]
    print(q, "slots", len(sl), "types", dict(collections.Counter(sl["type"].tolist())), "distinct boundaries", distinct,
          "osm runs", int(kb.sum()), "heuristic runs", int(hb.sum()),
          "key-break-not-heur", int((kb & ~hb).sum()), "heur-break-not-key", int((hb & ~kb).sum()))

pn = np.fromfile(P + "admin-minimal/place_nodes.bin", dtype=pn_dt)
ap = np.fromfile(P + "admin-minimal/admin_polygons.bin", dtype=ap_dt)
hid = pn[pn["name_id"] == NO]
par = hid["parent"]
ok = par != NO
print("minimal hidden", len(hid), "with parent", int(ok.sum()), "parent has override", int((ap["ovr"][par[ok]] != 0).sum()))

for t in ("major", "notable", "all"):
    r = np.fromfile(P + "poi/" + t + "/poi_records.bin", dtype=rec_dt)
    named = r["name"] != NO
    print("poi", t, len(r), "named", int(named.sum()), "tiers", dict(collections.Counter(r["tier"].tolist())))
