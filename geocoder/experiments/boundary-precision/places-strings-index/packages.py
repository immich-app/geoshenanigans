"""Measure three shrink packages for the six files of this track
(place_nodes, strings_core, admin_cells, admin_entries, place_cells, place_entries).

A  builder-only, reader unchanged: drop hidden place nodes, sort nodes by L10 cell,
   purge strings_core to referenced strings, clear INTERIOR_FLAG. (= variants V3 + V6, answers identical)
B  small reader format change: PlaceNode 20 -> 16 B (drop unused parent_poly_id),
   place_entries dropped (a cell's nodes are one contiguous run: place_cells row = (cell, first node)),
   admin_cells loses its offset column (prefix-summed at load).
C  transfer encoding (decoded to the B/A runtime layout at install): columnar + delta/varint,
   1e-5 deg coords, presence bitmaps, deduplicated admin cell lists, names inline in node order.
All sizes: zstd -18 (compress_index.py LEVEL), bytes.
"""
import gzip, json
from common import *
from place_encode_lib import varint, zigzag

V3 = f"{WORK}/variants/V3"
V6 = f"{WORK}/variants/V6"
base = {f: zst_file(f"{SM}/{f}") for f in ["place_nodes.bin", "strings_core.bin", "admin_cells.bin",
                                            "admin_entries.bin", "place_cells.bin", "place_entries.bin"]}
print("baseline:", {k: v for k, v in base.items()}, "total", sum(base.values()), mib(sum(base.values())))

# ---------- A
A = {"place_nodes.bin": zst_file(f"{V3}/place_nodes.bin"),
     "strings_core.bin": zst_file(f"{V3}/strings_core.bin"),
     "admin_cells.bin": base["admin_cells.bin"],
     "admin_entries.bin": zst_file(f"{V6}/admin_entries.bin"),
     "place_cells.bin": zst_file(f"{V3}/place_cells.bin"),
     "place_entries.bin": zst_file(f"{V3}/place_entries.bin")}
print("A:", A, "total", sum(A.values()), mib(sum(A.values())))
ap_delta = zst_file(f"{V3}/admin_polygons.bin") - zst_file(f"{SM}/admin_polygons.bin")
print("A: admin_polygons.bin zst change from name_id remap:", ap_delta)

# ---------- B
P = load("place_nodes.bin", PLACE_DT, V3)          # live, sorted by (cell, lat, lng), purged name ids
pc = load("place_cells.bin", CELL_DT, V3)
pe = open(f"{V3}/place_entries.bin", 'rb').read()
counts = np.array([int.from_bytes(pe[o:o + 2], 'little') for o in pc['off']], np.int64)
first = np.concatenate([[0], np.cumsum(counts)[:-1]])
# check contiguity claim: entries of each cell are first..first+count-1
lists = decode_entries(pe, pc['off'])
assert all(np.array_equal(l, np.arange(f, f + len(l))) for l, f in zip(lists, first))
P16 = np.zeros(len(P), np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name_id', '<u4'), ('t', 'u1'), ('p1', 'u1'), ('p2', '<u2')]))
P16['lat'], P16['lng'], P16['name_id'], P16['t'] = P['lat'], P['lng'], P['name_id'], P['place_type']
pcB = np.zeros(len(pc), CELL_DT); pcB['cell'] = pc['cell']; pcB['off'] = first
ac = load("admin_cells.bin", CELL_DT)
B = {"place_nodes.bin (16 B)": zst(P16),
     "strings_core.bin": A["strings_core.bin"],
     "admin_cells.bin (u64 cell only)": zst(ac['cell'].copy()),
     "admin_entries.bin": A["admin_entries.bin"],
     "place_cells.bin (cell, first)": zst(pcB),
     "place_entries.bin": 0}
print("B:", B, "total", sum(B.values()), mib(sum(B.values())))
print("B raw bytes on disk:", {"place_nodes": P16.nbytes, "place_cells": pcB.nbytes, "admin_cells": ac['cell'].nbytes})

# ---------- C
NC = 6 * 4 ** 10
SH = np.uint64(41)


def bitmap(cells):
    bm = np.zeros(NC, bool)
    bm[(cells >> SH).astype(np.int64)] = True
    return np.packbits(bm)


core = open(f"{V3}/strings_core.bin", 'rb').read()


def get(off):
    return core[off:core.index(b"\0", off) + 1]


ql = np.round(P['lat'].astype(np.float64) * 1e5).astype(np.int64)
qg = np.round(P['lng'].astype(np.float64) * 1e5).astype(np.int64)
ql6 = np.round(P['lat'].astype(np.float64) * 1e6).astype(np.int64)
qg6 = np.round(P['lng'].astype(np.float64) * 1e6).astype(np.int64)
inline = b"".join(get(int(o)) for o in P['name_id'])
ap = load("admin_polygons.bin", ADMIN_DT)          # name_ids index the ORIGINAL pool
orig = open(f"{SM}/strings_core.bin", 'rb').read()
au = np.unique(ap['name_id'][ap['name_id'] != NO_DATA])
admin_pool = b"".join(orig[int(o):orig.index(b"\0", int(o)) + 1] for o in au)
# admin index: dedup lists without INTERIOR_FLAG (answers identical, variant V6)
acells = load("admin_cells.bin", CELL_DT)
aeb = open(f"{SM}/admin_entries.bin", 'rb').read()
alists = decode_entries(aeb, acells['off'])
uniq, lid = {}, np.empty(len(alists), np.int64)
for i, l in enumerate(alists):
    lid[i] = uniq.setdefault(np.sort(l & np.uint32(0x7FFFFFFF)).tobytes(), len(uniq))
ul = [np.frombuffer(k, '<u4') for k in uniq]
tbl = np.concatenate([np.array([len(x) for x in ul], '<u2').view(np.uint8), np.concatenate(ul).view(np.uint8)])
C = {"place coords 1e-5 delta varint": zst(np.concatenate([varint(zigzag(np.diff(ql, prepend=0))), varint(zigzag(np.diff(qg, prepend=0)))])),
     "place type": zst(P['place_type'].copy()),
     "place names inline (node order)": zst(inline),
     "place cell bitmap": zst(bitmap(pc['cell'])),
     "place per-cell counts varint": zst(varint(counts)),
     "admin names pool (alphabetical)": zst(admin_pool),
     "admin cell bitmap": zst(bitmap(acells['cell'])),
     "admin list table (dedup, no flag)": zst(tbl),
     "admin per-cell list id (delta varint)": zst(varint(zigzag(np.diff(lid, prepend=0)))),
     }
for k, v in C.items():
    print(f"  C {k:45s} {v:>11,d}")
print("C total", sum(C.values()), mib(sum(C.values())))
c6 = zst(np.concatenate([varint(zigzag(np.diff(ql6, prepend=0))), varint(zigzag(np.diff(qg6, prepend=0)))]))
print("C variant: coords at 1e-6 instead of 1e-5:", c6, "delta", c6 - C["place coords 1e-5 delta varint"])
# C': names as ordinals into the purged alphabetical pool instead of inline
starts = np.concatenate([[0], np.nonzero(np.frombuffer(core, np.uint8) == 0)[0][:-1] + 1])
o4 = np.searchsorted(starts, P['name_id']).astype('<u4').view(np.uint8).reshape(-1, 4)
ordz = zst(np.concatenate([o4[:, 0], o4[:, 1], o4[:, 2]]))
print("C' names as ordinal byte-planes + purged pool instead of inline + admin pool:", ordz + A["strings_core.bin"],
      "vs", C["place names inline (node order)"] + C["admin names pool (alphabetical)"])
json.dump({"base": base, "A": A, "B": B, "C": C}, open(f"{WORK}/packages.json", "w"), indent=1)

# gzip -6 (docker layer default) of the shipped raw files vs package A raw files
gz = lambda p: len(gzip.compress(open(p, 'rb').read(), 6))
print("gzip-6 shipped:", sum(gz(f"{SM}/{f}") for f in base), " gzip-6 package A:",
      gz(f"{V3}/place_nodes.bin") + gz(f"{V3}/strings_core.bin") + gz(f"{SM}/admin_cells.bin") + gz(f"{V6}/admin_entries.bin") + gz(f"{V3}/place_cells.bin") + gz(f"{V3}/place_entries.bin"))
