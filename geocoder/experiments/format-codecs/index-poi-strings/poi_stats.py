"""poi_records / poi_vertices / poi_cells / poi_entries: what the reader can ever return."""
import json
import numpy as np
from lists import *

POI_DT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("vertex_offset", "<u4"), ("vertex_count", "<u4"),
                   ("name_id", "<u4"), ("category", "u1"), ("tier", "u1"), ("flags", "u1"), ("importance", "u1"),
                   ("parent_street_id", "<u4"), ("parent_postcode_id", "<u4"), ("parent_poly_id", "<u4")])
p = np.fromfile(f"{D}/poi_records.bin", POI_DT)
n = len(p)
print("records", n, "size ok", POI_DT.itemsize == 40)
tomb = (p["name_id"] == NO) & (p["vertex_offset"] == NO)
named = p["name_id"] != NO
poly = (p["vertex_count"] > 0) & (p["vertex_offset"] != NO)
print(f"tombstones (name NO_DATA & vertex_offset NO_DATA) {int(tomb.sum()):,}")
print(f"named {int(named.sum()):,}  unnamed {int((~named).sum()):,}  polygon {int(poly.sum()):,}  unnamed polygons {int((~named & poly).sum()):,}")
print(f"has parent_street {int((p['parent_street_id']!=NO).sum()):,}; named & parent_street {int((named & (p['parent_street_id']!=NO)).sum()):,}")
meta = json.load(open(f"{D}/poi_meta.json"))
cats, cc = np.unique(p["category"], return_counts=True)
print("categories present", len(cats), "UNNAMED_RANK30(170) count", int((p["category"] == 170).sum()),
      "named among 170:", int(((p["category"] == 170) & named).sum()))
# empty-string names
s = {}
lay = json.load(open(f"{D}/strings_layout.json"))
print("strings layout", lay)
# vertex file layout
vb = np.fromfile(f"{D}/poi_vertices.bin", np.uint8)
off = p["vertex_offset"][poly].astype(np.int64)
cnt = p["vertex_count"][poly].astype(np.int64)
o = np.argsort(off, kind="stable")
offs, cnts = off[o], cnt[o]
enc = vb[offs]
stride = np.where(enc >= 3, 8, 4)
end = offs + 10 + cnts * stride
print("poi_vertices bytes", len(vb), "polygons", len(offs), "distinct offsets", len(np.unique(offs)))
print("encodings", dict(zip(*[x.tolist() for x in np.unique(enc, return_counts=True)])))
print("back to back:", bool(offs[0] == 0 and np.array_equal(end[:-1], offs[1:]) and end[-1] == len(vb)),
      "offsets ascending in record order:", bool(np.all(np.diff(off) > 0)))
print("header pad byte all zero:", bool(np.all(vb[offs + 1] == 0)))
print("vertex_count pctl", np.percentile(cnts, [50, 90, 99, 99.9]).tolist(), "max", int(cnts.max()), "> 65535:", int((cnts > 65535).sum()))
unn_bytes = int((10 + cnt * np.where(vb[off] >= 3, 8, 4))[~named[poly]].sum())
print(f"vertex bytes of unnamed polygon POIs {unn_bytes:,}")
# polygons: is the ring closed (first == last q)?
# poi index
pc = np.fromfile(f"{D}/poi_cells.bin", CELL_DT)
pe = np.fromfile(f"{D}/poi_entries.bin", np.uint8)
c2, ids, st = load_lists(pe, pc["off"])
print("poi cells", len(pc), "entries ids", len(ids), "interior-flag share", float(np.mean(ids >> 31)))
iid = ids & 0x7FFFFFFF
print("entries pointing at unnamed POIs", int((~named[iid]).sum()), "at tombstones", int(tomb[iid].sum()))
print("poi records never referenced by any cell:", int(n - len(np.unique(iid))))
exp = np.concatenate([[0], np.cumsum(2 + 4 * c2)[:-1]])
print("poi offsets == prefix sum:", bool(np.array_equal(exp, pc["off"].astype(np.int64))), "end==size", int(exp[-1] + 2 + 4 * c2[-1]) == len(pe))
lid = np.repeat(np.arange(len(c2)), c2)
inner = lid[1:] == lid[:-1]
print("poi entry ids ascending within list (flag masked):", bool(np.all(iid[1:][inner] > iid[:-1][inner])))
# per category meta for unreachable analysis
md = {int(k): v for k, v in meta.items()}
maxd = np.array([md.get(c, {}).get("max_distance", 500.0) for c in range(256)])
print("categories with max_distance 0:", [c for c in range(256) if c in md and md[c].get("max_distance", 0) == 0][:50])
