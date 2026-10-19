#!/usr/bin/env python3
"""patchcmp.py <arc var old> <arc var new> <pp var old> <pp var new> <dump old> <dump new>

Day-to-day change of the admin geometry in the arc format and in today's per-polygon format at
the same scheme (two cold builds of consecutive days). Payload = the bytes a patch has to carry
at least: blocks that are new or whose bytes changed, matched by identity (no slot shifts; a
real patcher adds its own framing). zstd 1.5.7 -18 of the concatenated payload.

arc format, both parts (min = admin-minimal, q = quality dir):
  arcs: identity = the hash of the raw (unsimplified) vertex sequence; same raw + same encoded
        bytes / same raw but other bytes (its tolerance moved: a ring using it changed level or
        area) / new raw. Way identity: the set of member ways an arc runs along.
  polygons: identity = osm key; ref list (arc content hashes in ring order) same / changed / new.
  payload = new + rebytes arc blocks + changed + new ref lists + their polygon records (24 B).
per-polygon format:
  polygons by osm key: vertex block same / changed / new; payload = changed + new blocks + records.
Polygon records are counted as 24 bytes wherever the geometry changed in either format (name ids
and offsets shift daily in both formats alike and are left out).
-> one JSON object on stdout.
"""
import json
import os
import subprocess
import sys
import tempfile

import numpy as np

ZSTD = "/ssd/shrink/zstd157/bin/zstd"
POLY = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                 ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])
STRIDE = {0: 4, 1: 4, 2: 4, 3: 8, 4: 8}


def zst_len(chunks):
    with tempfile.NamedTemporaryFile(dir="/ssd/arcs/tmp", delete=False) as f:
        for c in chunks:
            f.write(c)
        name = f.name
    out = subprocess.run([ZSTD, "-18", "-T1", "-q", "-c", name], capture_output=True, check=True).stdout
    os.remove(name)
    return len(out)


def arc_side(d):
    keys = np.fromfile(f"{d}/arc_keys.bin", dtype="<u8").reshape(-1, 3)
    off = np.fromfile(f"{d}/arc_index.bin", dtype="<u4").astype(np.int64)
    av = open(f"{d}/arc_vertices.bin", "rb").read()
    pk = np.fromfile(f"{d}/poly_keys.bin", dtype="<u8").reshape(-1, 2)
    polys = np.fromfile(f"{d}/admin_polygons.bin", dtype=POLY)
    pa = open(f"{d}/poly_arcs.bin", "rb").read()
    live = polys[(polys["off"] != 0xFFFFFFFF) & (polys["cnt"] > 0)]
    # poly_keys follow the selected polygons in slot order, as the live records
    assert len(live) == len(pk), (len(live), len(pk))
    refblk = {}
    for (osm, h), p in zip(pk.tolist(), live):
        n = 4 * (int(p["holes"]) + 1 + int(p["cnt"]))
        refblk[osm] = (h, pa[int(p["off"]):int(p["off"]) + n])
    return keys, off, av, refblk


def compare_arcs(a, b):
    ka, offa, ava, ra = arc_side(a)
    kb, offb, avb, rb = arc_side(b)
    raw_a = {int(h): i for i, h in enumerate(ka[:, 0])}
    block_a = set(int(h) for h in ka[:, 1])
    ways_a = set(int(h) for h in ka[:, 2])
    raw_b = set(int(h) for h in kb[:, 0])
    same, rebytes, new, new_wayknown = 0, 0, 0, 0
    payload = []
    pay_arc = 0
    for i, (hr, hb, hw) in enumerate(kb.tolist()):
        blk = avb[offb[i]:offb[i + 1]]
        if hr in raw_a:
            if hb in block_a:
                same += 1
                continue
            rebytes += 1
        else:
            new += 1
            new_wayknown += hw in ways_a
        payload.append(blk)
        pay_arc += len(blk)
    removed = sum(1 for h in raw_a if h not in raw_b)
    pc = {"same": 0, "changed": 0, "new": 0}
    pay_ref = 0
    for osm, (h, blk) in rb.items():
        if osm in ra:
            if ra[osm][0] == h:
                pc["same"] += 1
                continue
            pc["changed"] += 1
        else:
            pc["new"] += 1
        payload.append(blk)
        payload.append(b"\0" * 24)
        pay_ref += len(blk) + 24
    pc["removed"] = sum(1 for o in ra if o not in rb)
    return {"arcs_new_day": len(kb), "arcs_same": same, "arcs_rebytes": rebytes, "arcs_new": new,
            "arcs_new_with_known_way_set": new_wayknown, "arcs_removed": removed, "polygons": pc,
            "payload_raw": pay_arc + pay_ref, "payload_arc_blocks": pay_arc, "payload_refs_records": pay_ref,
            "payload_zst": zst_len(payload) if payload else 0}


def pp_side(d, dump, part):
    polys = np.fromfile(f"{d}/admin_polygons.bin", dtype=POLY)
    verts = open(f"{d}/admin_vertices.bin", "rb").read()
    dpol = np.fromfile(f"{dump}/admin_polygons.raw", dtype=POLY)
    osm = np.fromfile(f"{dump}/admin_osm_ids.raw", dtype="<u8")
    if part == "min":
        kept = np.nonzero((dpol["lvl"] >= 2) & (dpol["lvl"] <= 8) & (dpol["cnt"] >= 3))[0]
        if len(kept) != len(polys):
            raise SystemExit(f"{d}: {len(polys)} slots, {len(kept)} kept polygons: no identity map")
        ids = osm[kept]
    else:
        ids = osm
    out = {}
    for s, p in enumerate(polys):
        if p["cnt"] == 0 or p["off"] == 0xFFFFFFFF:
            continue
        o = int(p["off"])
        n = 10 + int(p["cnt"]) * STRIDE[verts[o]]
        out[int(ids[s])] = (verts[o:o + n], b"\0" * 24)
    return out


def compare_pp(a, b, da, db, part):
    pa, pb = pp_side(a, da, part), pp_side(b, db, part)
    c = {"same": 0, "changed": 0, "new": 0}
    payload, raw = [], 0
    for k, (blk, rec) in pb.items():
        if k in pa:
            if pa[k][0] == blk and pa[k][1] == rec:
                c["same"] += 1
                continue
            c["changed"] += 1
        else:
            c["new"] += 1
        payload += [blk, rec]
        raw += len(blk) + len(rec)
    c["removed"] = sum(1 for k in pa if k not in pb)
    return {"polygons": c, "payload_raw": raw, "payload_zst": zst_len(payload) if payload else 0}


if __name__ == "__main__":
    arc_a, arc_b, pp_a, pp_b, dump_a, dump_b = sys.argv[1:7]
    res = {}
    for part in ("min", "q"):
        res[part] = {"arc": compare_arcs(f"{arc_a}/{part}/arc", f"{arc_b}/{part}/arc"),
                     "pp": compare_pp(f"{pp_a}/{part}", f"{pp_b}/{part}", dump_a, dump_b, part)}
    print(json.dumps(res))
