"""poi_records.bin (36 B/record) variants + poi_cells/poi_entries list dedup.
What the reader reads (server/src/lib.rs): poi_record() returns None when name_id == NO_DATA and
vertex_offset == NO_DATA; lat/lng only on the point branch (vertex_count > 0 && vertex_offset !=
NO_DATA takes the polygon branch: find_nearest_poi_with_parent ~2420, find_pois ~2522).
Writes work/poi_rec/*.bin and work/v_poi_*/ (today's-format dirs' changed files).
usage: poi_rec.py"""
import os
import numpy as np
from lists import *
from make_dedup_lib import write_lists

POI_DT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("vertex_offset", "<u4"), ("vertex_count", "<u4"),
                   ("name_id", "<u4"), ("category", "u1"), ("tier", "u1"), ("flags", "u1"), ("importance", "u1"),
                   ("parent_street_id", "<u4"), ("parent_postcode_id", "<u4"), ("parent_poly_id", "<u4")])
p = np.fromfile(f"{D}/poi_records.bin", POI_DT)
n = len(p)
R = f"{W}/poi_rec"
os.makedirs(R, exist_ok=True)
poly = (p["vertex_count"] > 0) & (p["vertex_offset"] != NO)
point = ~poly
dead = (p["name_id"] == NO) & (p["vertex_offset"] == NO)
print(f"records {n:,}; polygon {int(poly.sum()):,}; dead (reader returns None) {int(dead.sum()):,}")
print("vertex_count == 0 <=> vertex_offset == NO_DATA:",
      bool(np.array_equal(p["vertex_count"] == 0, p["vertex_offset"] == NO)))
print("dead records: fields other than name/vertex_offset non-zero:",
      {f: int((p[f][dead] != 0).sum()) for f in ["lat", "lng", "vertex_count", "category", "tier", "flags",
                                                  "importance", "parent_street_id", "parent_postcode_id", "parent_poly_id"]})

# R1: polygon POIs' lat/lng zeroed (never read) + dead records' unread fields zeroed (today's format)
r1 = p.copy()
r1["lat"][poly] = 0
r1["lng"][poly] = 0
for f in ["lat", "lng", "vertex_count", "category", "tier", "flags", "importance", "parent_street_id",
          "parent_postcode_id", "parent_poly_id"]:
    r1[f][dead] = 0
r1.tofile(f"{R}/R1.bin")
os.makedirs(f"{W}/v_poi_r1", exist_ok=True)
r1.tofile(f"{W}/v_poi_r1/poi_records.bin")


def soa(a, fields):
    return np.concatenate([np.ascontiguousarray(a[f]).view(np.uint8) for f in fields])


F_ALL = list(POI_DT.names)
soa(p, F_ALL).tofile(f"{R}/R0_soa.bin")
soa(r1, F_ALL).tofile(f"{R}/R1_soa.bin")
# R3: vertex_count dropped; vertex_offset made monotone (a point record holds the offset the next
# polygon starts at), so count = (offset[i+1] - offset[i] - 10) / stride(enc at offset[i]) and a
# point is offset[i+1] == offset[i]. Lossless iff polygons are back to back in record order.
vb_len = os.path.getsize(f"{D}/poi_vertices.bin")
vo = p["vertex_offset"].astype(np.int64)
mono = np.where(poly, vo, -1)
nxt = np.full(n + 1, vb_len, np.int64)
# next polygon start at or after i (suffix min over polygon offsets)
tmp = np.where(poly, vo, vb_len)
nxt[:n] = np.minimum.accumulate(tmp[::-1])[::-1]
mono = np.where(poly, vo, nxt[:n]).astype(np.uint32)
# decode check: counts and NO_DATA restored exactly
vbh = np.memmap(f"{D}/poi_vertices.bin", np.uint8, "r")
mo = np.concatenate([mono.astype(np.int64), [vb_len]])
is_poly = mo[1:] != mo[:-1]
enc = np.zeros(n, np.int64)
enc[is_poly] = vbh[mo[:-1][is_poly]]
stride = np.where(enc >= 3, 8, 4)
cnt_dec = np.where(is_poly, (mo[1:] - mo[:-1] - 10) // stride, 0)
vo_dec = np.where(is_poly, mo[:-1], NO)
ok = np.array_equal(cnt_dec, p["vertex_count"].astype(np.int64)) and np.array_equal(vo_dec, vo)
print("R3 monotone offsets decode back to (vertex_offset, vertex_count) exactly:", ok)
assert ok
r3 = np.zeros(n, np.dtype([("lat", "<f4"), ("lng", "<f4"), ("vertex_offset", "<u4"), ("name_id", "<u4"),
                           ("category", "u1"), ("tier", "u1"), ("flags", "u1"), ("importance", "u1"),
                           ("parent_street_id", "<u4"), ("parent_postcode_id", "<u4"), ("parent_poly_id", "<u4")]))
for f in r3.dtype.names:
    r3[f] = mono if f == "vertex_offset" else r1[f]
r3.tofile(f"{R}/R3_aos32.bin")
soa(r3, list(r3.dtype.names)).tofile(f"{R}/R3_soa.bin")
# download-only: SoA with name_id delta (records sort by category, tier, name_id) and offset delta
d = r3.copy()
nm = d["name_id"].astype(np.int64)
dn = np.diff(nm, prepend=0)
vod = np.diff(mono.astype(np.int64), prepend=0)
cols = [np.ascontiguousarray(d[f]).view(np.uint8) for f in ["lat", "lng"]]
cols += [varint(zz(dn))[0], varint(vod.astype(np.uint64))[0]]
cols += [np.ascontiguousarray(d[f]).view(np.uint8) for f in ["category", "tier", "flags", "importance",
                                                           "parent_street_id", "parent_postcode_id", "parent_poly_id"]]
np.concatenate(cols).tofile(f"{R}/R3_dl.bin")
for fn in ["R1.bin", "R0_soa.bin", "R1_soa.bin", "R3_aos32.bin", "R3_soa.bin", "R3_dl.bin"]:
    print(f"{fn:14s} {os.path.getsize(f'{R}/{fn}'):>12,d} B")

# poi_cells / poi_entries: identical lists stored once (today's format), interior flags kept
pc = np.fromfile(f"{D}/poi_cells.bin", CELL_DT)
pe = np.fromfile(f"{D}/poi_entries.bin", np.uint8)
cnt, ids, starts = load_lists(pe, pc["off"])
inv, first, nd = list_keys(cnt, ids, starts)
print(f"poi lists {len(cnt):,}, distinct {nd:,}")
_, first_occ = np.unique(inv, return_index=True)
order = np.sort(first_occ)
rank = np.empty(len(first_occ), np.int64)
rank[np.argsort(first_occ)] = np.arange(len(first_occ))
os.makedirs(f"{W}/v_poi_dg", exist_ok=True)
offs = write_lists(f"{W}/v_poi_dg/poi_entries.bin", cnt, ids, starts, order)
pc2 = pc.copy()
pc2["off"] = offs[rank[inv]].astype(np.uint32)
pc2.tofile(f"{W}/v_poi_dg/poi_cells.bin")
print(f"poi_entries dedup {os.path.getsize(f'{W}/v_poi_dg/poi_entries.bin'):,} B")
# download-only: cell ids as varint deltas (offsets are prefix sums of list sizes)
cd = np.diff(pc["cell"].astype(np.uint64) >> np.uint64(41), prepend=np.uint64(0)).astype(np.uint64)
varint(cd)[0].tofile(f"{R}/poi_cells_dl.bin")
print("poi_cells download-only (L10 cell index deltas, varint):", os.path.getsize(f"{R}/poi_cells_dl.bin"))
