#!/usr/bin/env python3
"""Search-corpus churn between two published days (planet max, stable record slots): per record
file, slots whose searchable content changed (name string, point, type), added, removed."""
import json
import sys

import numpy as np

A, B = sys.argv[1].rstrip("/") + "/", sys.argv[2].rstrip("/") + "/"
NO = 0xFFFFFFFF
pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
rec_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("cat", "u1"), ("tier", "u1"), ("flags", "u1"), ("imp", "u1"), ("ps", "<u4"), ("pp", "<u4"), ("ppoly", "<u4")])


class Strings:
    def __init__(self, d):
        lay = json.load(open(d + "strings_layout.json"))["tiers"]
        self.t = []
        for t in lay:
            try:
                self.t.append((t["start"], t["end"], np.memmap(d + t["file"], dtype=np.uint8, mode="r")))
            except FileNotFoundError:
                pass

    def get(self, off):
        if off == NO:
            return b""
        for s, e, m in self.t:
            if s <= off < e:
                o = off - s
                end = o
                while m[end] != 0:
                    end += 1
                return bytes(m[o:end])
        return b""


sa, sb = Strings(A), Strings(B)
out = {}
for f, dt, kind_field in (("place_nodes.bin", pn_dt, "pt"), ("admin_polygons.bin", ap_dt, "level"), ("poi_records.bin", rec_dt, "cat")):
    a = np.fromfile(A + f, dtype=dt)
    b = np.fromfile(B + f, dtype=dt)
    n = min(len(a), len(b))
    la, lb = a[:n], b[:n]
    named_a, named_b = la["name_id"] != NO, lb["name_id"] != NO
    added = int((~named_a & named_b).sum()) + int((b["name_id"][n:] != NO).sum())
    removed = int((named_a & ~named_b).sum()) + int((a["name_id"][n:] != NO).sum())
    both = named_a & named_b
    if "lat" in dt.names:
        moved = both & ((la["lat"] != lb["lat"]) | (la["lng"] != lb["lng"]))
    else:  # polygons: geometry changed (area)
        moved = both & (la["area"] != lb["area"])
    typ = both & (la[kind_field] != lb[kind_field])
    offdiff = np.nonzero(both & (la["name_id"] != lb["name_id"]))[0]
    renamed = 0
    for i in offdiff.tolist():
        if sa.get(int(la["name_id"][i])) != sb.get(int(lb["name_id"][i])):
            renamed += 1
    out[f] = dict(slots_a=len(a), slots_b=len(b), named_b=int((b["name_id"] != NO).sum()), added=added, removed=removed,
                  renamed=renamed, name_offset_changed=len(offdiff), moved=int(moved.sum()), type_changed=int(typ.sum()))
    print(f, out[f], flush=True)
json.dump(out, open("/ssd/search-prod/results/churn/corpus_churn.json", "w"), indent=1)
