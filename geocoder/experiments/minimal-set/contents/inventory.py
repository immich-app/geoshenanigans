#!/usr/bin/env python3
"""Inventory of the admin-minimal geocoder set (read-only on the inputs).

Formats (geocoder/builder/src/types.h, cell_index_io.h, vertex_pack.h):
  admin_polygons.bin  24 B/slot: u32 vertex_offset, u32 vertex_count, u32 name_id,
                      u8 admin_level, u8 place_type_override, u16 pad, f32 area,
                      u16 country_code, u16 pad. Tombstone: name_id == NO_DATA.
  admin_vertices.bin  per polygon: u8 enc, u8 pad, f32 bbox_min_lat, f32 bbox_min_lng,
                      then vertex_count * stride (enc 0/1/2: u16+u16 = 4 B, enc 3: u32+u32 = 8 B).
  place_nodes.bin     20 B: f32 lat, f32 lng, u32 name_id, u8 place_type, u8 pad, u16 pad,
                      u32 parent_poly_id.
  *_cells.bin         12 B rows: u64 S2 cell id, u32 byte offset into *_entries.bin.
  *_entries.bin       per cell: u16 count, count * u32 id (admin ids carry 0x80000000 interior flag).
  strings_core.bin    NUL-terminated strings, offsets global (core base = 0).

Usage: inventory.py <minimal dir> <recommended dir> <out json>
"""
import json
import sys
from collections import Counter, defaultdict

import numpy as np

NO_DATA = 0xFFFFFFFF
STRIDE = {0: 4, 1: 4, 2: 4, 3: 8}
ENC_NAME = {0: "u16@1e-5 (1.1 m)", 1: "u16@1e-4 (11 m)", 2: "u16@1e-6 (11 cm)", 3: "u32@1e-7 (1 cm)"}
PLACE_TYPE = {0: "city", 1: "town", 2: "village", 3: "suburb", 4: "hamlet", 5: "neighbourhood", 6: "quarter"}
OVERRIDE = {0: "none", 1: "city", 2: "town", 3: "village", 4: "suburb", 5: "neighbourhood", 6: "quarter",
            7: "state", 8: "province", 9: "region", 10: "county", 11: "district", 12: "borough",
            13: "hamlet", 14: "municipality"}

POLY_DT = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name", "<u4"), ("level", "u1"), ("pto", "u1"),
                    ("pad", "<u2"), ("area", "<f4"), ("cc", "<u2"), ("pad2", "<u2")])
PLACE_DT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name", "<u4"), ("pt", "u1"), ("pad1", "u1"),
                     ("pad2", "<u2"), ("parent", "<u4")])
ROW_DT = np.dtype([("cell", "<u8"), ("off", "<u4")])


def load_polys(d):
    p = np.fromfile(f"{d}/admin_polygons.bin", dtype=POLY_DT)
    v = np.fromfile(f"{d}/admin_vertices.bin", dtype=np.uint8)
    live = p["name"] != NO_DATA
    enc = np.full(len(p), 255, dtype=np.int16)
    has = live & (p["vcnt"] > 0) & (p["voff"] != NO_DATA)
    enc[has] = v[p["voff"][has].astype(np.int64)]
    stride = np.zeros(len(p), dtype=np.int64)
    for e, s in STRIDE.items():
        stride[enc == e] = s
    nbytes = np.where(has, 10 + p["vcnt"].astype(np.int64) * stride, 0)
    return p, v, live, has, enc, nbytes


def entries_per_cell(d, prefix):
    rows = np.fromfile(f"{d}/{prefix}_cells.bin", dtype=ROW_DT)
    ent = np.fromfile(f"{d}/{prefix}_entries.bin", dtype=np.uint8)
    offs = rows["off"].astype(np.int64)
    counts = ent[offs].astype(np.int64) | (ent[offs + 1].astype(np.int64) << 8)
    # all ids, in order (entries are packed back to back: u16 count then u32s)
    ids = np.empty(int(counts.sum()), dtype=np.uint32)
    at = 0
    # vectorised gather: build index array of every id's byte start
    starts = offs + 2
    rep = np.repeat(starts, counts)
    within = np.arange(len(rep)) - np.repeat(np.cumsum(counts) - counts, counts)
    pos = rep + within * 4
    ids = (ent[pos].astype(np.uint32) | (ent[pos + 1].astype(np.uint32) << 8) |
           (ent[pos + 2].astype(np.uint32) << 16) | (ent[pos + 3].astype(np.uint32) << 24))
    return rows, counts, ids


def s2_level(cell_ids):
    # level = 30 - (trailing zero bits - 1) / 2  (lsb marker bit)
    lv = []
    for c in cell_ids[:1000]:
        c = int(c)
        tz = (c & -c).bit_length() - 1
        lv.append(30 - tz // 2)
    return Counter(lv)


def load_strings(path):
    b = np.fromfile(path, dtype=np.uint8)
    nul = np.flatnonzero(b == 0)
    starts = np.concatenate(([0], nul[:-1] + 1))
    lens = nul - starts  # without terminator
    return b, starts, lens


def main():
    mdir, rdir, out = sys.argv[1], sys.argv[2], sys.argv[3]
    R = {}

    # ---------------- admin polygons ----------------
    p, v, live, has, enc, nbytes = load_polys(mdir)
    assert int(nbytes.sum()) == len(v), (int(nbytes.sum()), len(v))
    R["admin_slots"] = int(len(p))
    R["admin_live"] = int(live.sum())
    R["admin_tombstones"] = int((~live).sum())
    lv = {}
    for L in sorted(set(p["level"][live].tolist())):
        m = live & (p["level"] == L)
        names = p["name"][m]
        lv[int(L)] = {
            "polys": int(m.sum()),
            "distinct_names": int(len(np.unique(names))),
            "vertices": int(p["vcnt"][m].sum()),
            "bytes": int(nbytes[m].sum()),
            "max_vertices": int(p["vcnt"][m].max()),
            "median_vertices": float(np.median(p["vcnt"][m])),
            "enc": {ENC_NAME[e]: {"polys": int((m & (enc == e)).sum()),
                                  "vertices": int(p["vcnt"][m & (enc == e)].sum()),
                                  "bytes": int(nbytes[m & (enc == e)].sum())}
                    for e in range(4) if (m & (enc == e)).any()},
            "override": {OVERRIDE.get(int(k), str(k)): int(c)
                         for k, c in Counter(p["pto"][m].tolist()).most_common()},
            "cc_zero": int((m & (p["cc"] == 0)).sum()),
            "cc_zero_bytes": int(nbytes[m & (p["cc"] == 0)].sum()),
        }
    R["admin_levels"] = lv
    R["admin_enc_total"] = {ENC_NAME[e]: {"polys": int((live & (enc == e)).sum()),
                                          "vertices": int(p["vcnt"][live & (enc == e)].sum()),
                                          "bytes": int(nbytes[live & (enc == e)].sum())}
                            for e in range(4)}
    R["admin_header_bytes"] = int(10 * has.sum())

    # strings
    sb, sstarts, slens = load_strings(f"{mdir}/strings_core.bin")
    off_index = {int(o): i for i, o in enumerate(sstarts.tolist())}

    def name_of(off):
        if off == NO_DATA:
            return None
        e = sb[off:off + 200].tobytes().split(b"\0", 1)[0]
        return e.decode("utf-8", "replace")

    # largest polygons by bytes
    order = np.argsort(-nbytes)[:20]
    R["largest_polys"] = [{"slot": int(i), "name": name_of(int(p["name"][i])), "level": int(p["level"][i]),
                           "cc": (chr(p["cc"][i] >> 8) + chr(p["cc"][i] & 0xFF)) if p["cc"][i] else "",
                           "vertices": int(p["vcnt"][i]), "bytes": int(nbytes[i]), "enc": ENC_NAME[int(enc[i])]}
                          for i in order]
    # multi-ring estimate: rings sharing (name, level, override)
    key = defaultdict(int)
    for nm, L, o in zip(p["name"][live].tolist(), p["level"][live].tolist(), p["pto"][live].tolist()):
        key[(nm, L, o)] += 1
    per_level_groups = Counter()
    per_level_multi = Counter()
    for (nm, L, o), c in key.items():
        per_level_groups[L] += 1
        if c > 1:
            per_level_multi[L] += c
    R["ring_groups_est"] = {int(L): {"groups": per_level_groups[L], "rings_in_multi_groups": per_level_multi[L]}
                            for L in per_level_groups}

    # L2 cc==0 examples
    m = live & (p["level"] == 2) & (p["cc"] == 0)
    idx = np.flatnonzero(m)
    idx = idx[np.argsort(-nbytes[idx])][:10]
    R["l2_no_cc_examples"] = [{"name": name_of(int(p["name"][i])), "vertices": int(p["vcnt"][i]),
                               "bytes": int(nbytes[i])} for i in idx]
    # distinct cc values at L2
    R["l2_distinct_cc"] = int(len(np.unique(p["cc"][live & (p["level"] == 2) & (p["cc"] != 0)])))

    # ---------------- admin cell index ----------------
    rows, counts, ids = entries_per_cell(mdir, "admin")
    R["admin_cells_rows"] = int(len(rows))
    R["admin_cells_s2_levels_sample"] = dict(s2_level(rows["cell"]))
    R["admin_entries_total"] = int(len(ids))
    R["admin_entries_interior"] = int(((ids & 0x80000000) != 0).sum())
    pid = (ids & 0x7FFFFFFF).astype(np.int64)
    ent_level = p["level"][pid]
    R["admin_entries_by_level"] = {int(L): int(c) for L, c in Counter(ent_level.tolist()).items()}
    R["admin_entries_count_bytes"] = int(2 * len(rows))
    R["admin_entries_max_per_cell"] = int(counts.max())
    R["admin_entries_mean_per_cell"] = float(counts.mean())
    # cells where every entry is interior & one per level (could be implicit)
    # how many cells would exist with only levels {2,4} or {2,4,8}?
    cell_rep = np.repeat(np.arange(len(rows)), counts)
    for name, keep in {"L2": {2}, "L2+L4": {2, 4}, "L2+L4+L8": {2, 4, 8}, "L2..L6": {2, 3, 4, 5, 6},
                       "L2+L3+L4+L7+L8": {2, 3, 4, 7, 8}}.items():
        km = np.isin(ent_level, list(keep))
        R.setdefault("admin_index_if_only", {})[name] = {
            "cells": int(len(np.unique(cell_rep[km]))), "entries": int(km.sum())}

    # ---------------- place nodes ----------------
    pn = np.fromfile(f"{mdir}/place_nodes.bin", dtype=PLACE_DT)
    R["place_records"] = int(len(pn))
    tomb = (pn["name"] == NO_DATA) & (pn["lat"] == 0) & (pn["lng"] == 0)
    hidden = (pn["name"] == NO_DATA) & ~tomb
    named = pn["name"] != NO_DATA
    R["place_tombstones"] = int(tomb.sum())
    R["place_hidden_linked"] = int(hidden.sum())
    R["place_named"] = int(named.sum())
    R["place_by_type"] = {PLACE_TYPE.get(int(t), str(t)): {
        "records": int((pn["pt"] == t).sum()),
        "named": int((named & (pn["pt"] == t)).sum()),
        "hidden": int((hidden & (pn["pt"] == t)).sum()),
        "tombstone": int((tomb & (pn["pt"] == t)).sum())} for t in sorted(set(pn["pt"].tolist()))}
    R["place_pad_nonzero"] = int(((pn["pad1"] != 0) | (pn["pad2"] != 0)).sum())
    R["place_parent_nodata"] = int((pn["parent"] == NO_DATA).sum())
    prow, pcounts, pids = entries_per_cell(mdir, "place")
    R["place_cells_rows"] = int(len(prow))
    R["place_cells_s2_levels_sample"] = dict(s2_level(prow["cell"]))
    R["place_entries_total"] = int(len(pids))
    R["place_entries_to_hidden"] = int((~named[pids]).sum())
    R["place_entries_max_per_cell"] = int(pcounts.max())
    R["place_entries_by_type"] = {PLACE_TYPE[int(t)]: int(c) for t, c in Counter(pn["pt"][pids].tolist()).items()}

    # ---------------- strings_core attribution ----------------
    ns = len(sstarts)
    R["strings_count"] = int(ns)
    R["strings_bytes"] = int(len(sb))
    R["strings_avg_len"] = float(slens.mean())
    cls = np.zeros(ns, dtype=np.uint16)  # bitmask
    BIT = {"min_admin": 1, "min_place": 2, "rec_admin_other": 4, "rec_place_other": 8, "postal": 16,
           "poi_major": 32, "postcode_centroid": 64}

    def mark(offs, bit):
        offs = offs[(offs != NO_DATA) & (offs < len(sb))]
        if len(offs) == 0:
            return 0
        i = np.searchsorted(sstarts, offs)
        ok = (i < ns) & (sstarts[np.minimum(i, ns - 1)] == offs)
        np.bitwise_or.at(cls, i[ok], bit)
        return int((~ok).sum())

    mark(p["name"][live], BIT["min_admin"])
    mark(pn["name"][named], BIT["min_place"])
    # recommended (admin mode, q2.5) — every admin level incl. 9/10/15 and postal (L11 in postal_polygons)
    rp = np.fromfile(f"{rdir}/admin_polygons.bin", dtype=POLY_DT)
    rlive = rp["name"] != NO_DATA
    R["rec_admin_levels"] = {int(L): int(c) for L, c in Counter(rp["level"][rlive].tolist()).items()}
    min_keep = np.isin(rp["level"], [2, 3, 4, 5, 6, 7, 8])
    mark(rp["name"][rlive & ~min_keep], BIT["rec_admin_other"])
    for L, bit in [(9, 128), (10, 256), (11, 512), (15, 1024)]:
        mark(rp["name"][rlive & (rp["level"] == L)], bit)
    pp = np.fromfile(f"{rdir}/postal_polygons.bin", dtype=POLY_DT)
    mark(pp["name"][pp["name"] != NO_DATA], BIT["postal"])
    rpn = np.fromfile(f"{rdir}/place_nodes.bin", dtype=PLACE_DT)
    rnamed = rpn["name"] != NO_DATA
    R["rec_place_by_type_named"] = {PLACE_TYPE[int(t)]: int((rnamed & (rpn["pt"] == t)).sum())
                                    for t in range(7)}
    mark(rpn["name"][rnamed & np.isin(rpn["pt"], [3, 5, 6])], BIT["rec_place_other"])
    poi_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("voff", "<u4"), ("vcnt", "<u4"), ("name", "<u4"),
                       ("cat", "u1"), ("tier", "u1"), ("flags", "u1"), ("imp", "u1"), ("street", "<u4"),
                       ("pc", "<u4"), ("parent", "<u4")])
    poi = np.fromfile(f"{rdir}/poi_records.bin", dtype=poi_dt)
    mark(poi["name"], BIT["poi_major"])
    mark(poi["street"], BIT["poi_major"])
    mark(poi["pc"], BIT["poi_major"])
    pcd = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("pc", "<u4"), ("cc", "<u2"), ("pad", "<u2")])
    pcs = np.fromfile(f"{rdir}/postcode_centroids.bin", dtype=pcd)
    mark(pcs["pc"], BIT["postcode_centroid"])

    nbytes_s = slens + 1
    groups = {
        "used by admin-minimal (admin and/or place)": (cls & 3) != 0,
        "  admin polygon names only": (cls & 3) == 1,
        "  place node names only": (cls & 3) == 2,
        "  both": (cls & 3) == 3,
        "not used by admin-minimal": (cls & 3) == 0,
        "  admin L9/L10/L15 names (admin mode only)": ((cls & 3) == 0) & ((cls & 4) != 0),
        "    of which L9 (first match L9>L10>L11>L15)": ((cls & 3) == 0) & ((cls & 128) != 0),
        "    of which L10": ((cls & 3) == 0) & ((cls & 128) == 0) & ((cls & 256) != 0),
        "    of which L11 postal": ((cls & 3) == 0) & ((cls & (128 | 256)) == 0) & ((cls & 512) != 0),
        "    of which L15 place areas": ((cls & 3) == 0) & ((cls & (128 | 256 | 512)) == 0) & ((cls & 1024) != 0),
        "  postal boundary codes (L11)": ((cls & 3) == 0) & ((cls & 4) == 0) & ((cls & 16) != 0),
        "  suburb/neighbourhood/quarter names": ((cls & 3) == 0) & ((cls & (4 | 16)) == 0) & ((cls & 8) != 0),
        "  POI-major/postcode refs": ((cls & 3) == 0) & ((cls & (4 | 8 | 16)) == 0) & ((cls & (32 | 64)) != 0),
        "  none of the above (other tiers' shared strings / orphans)": (cls & ~np.uint16(128 | 256 | 512 | 1024)) == 0,
    }
    R["strings_attr"] = {k: {"strings": int(m.sum()), "bytes": int(nbytes_s[m].sum())} for k, m in groups.items()}
    # samples of unattributed strings
    rng = np.random.default_rng(1)
    un = np.flatnonzero(cls == 0)
    sample = rng.choice(un, size=min(60, len(un)), replace=False) if len(un) else []
    R["strings_unattributed_sample"] = [name_of(int(sstarts[i])) for i in sorted(sample)]
    # how many unattributed look like postcodes (digits) or have digits
    un_lens = slens[un]
    R["strings_unattributed_len_mean"] = float(un_lens.mean()) if len(un) else 0
    # save masks for zstd experiments
    np.save(f"{out}.strcls.npy", cls)

    json.dump(R, open(out, "w"), indent=1, ensure_ascii=False)
    print(json.dumps({k: R[k] for k in R if k not in ("largest_polys",)}, indent=1, ensure_ascii=False)[:20000])


if __name__ == "__main__":
    main()
