"""Postal boundary de-duplication variants, today's on-disk format, on top of a change-1 (or 1+4) set.
The builder writes every boundary=postal_code polygon (admin_level 11) twice: in admin_polygons /
admin_vertices / the admin cell index AND in postal_polygons / postal_vertices
(builder/src/cell_index.cpp:1177-1203). Queries never read postal_* (lib.rs:2843-2847; only the
main.rs:307 debug polygon dump does) and skip level 11 in every admin walk (lib.rs:1782, 2696;
1629-1634 keep it in its own best_by_level slot; 2029/2069 filter 8..10 / 7..8).
usage: postal.py p1 <src_set> <out_set>   drop postal_polygons.bin + postal_vertices.bin
       postal.py p2 <src_set> <out_set>   drop the level-11 copy from the admin files instead:
           admin_polygons L11 slots -> tombstones (slots stay id-aligned for admin_parents,
           way_parents, POI parent_poly_id), admin_vertices L11 blocks dropped and offsets
           rewritten, L11 ids dropped from the admin cell lists (lists re-deduplicated, cells left
           empty dropped); postal_* kept."""
import shutil, sys
from common4 import *

mode, SRC, OUT = sys.argv[1:4]
shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(OUT)
skip = {"p1": {"postal_polygons.bin", "postal_vertices.bin"},
        "p2": {"admin_polygons.bin", "admin_vertices.bin", "admin_cells.bin", "admin_entries.bin"}}[mode]
for f in os.listdir(SRC):
    if f not in skip:
        os.link(os.path.realpath(f"{SRC}/{f}"), f"{OUT}/{f}")
if mode == "p1":
    print("p1: linked", len(os.listdir(OUT)), "files; dropped", sorted(skip))
    sys.exit()

ap = np.fromfile(f"{SRC}/admin_polygons.bin", ADMIN_DT)
vb = np.fromfile(f"{SRC}/admin_vertices.bin", np.uint8)
live = (ap["vertex_count"] > 0) & (ap["vertex_offset"] != NO_DATA)
is11 = live & (ap["admin_level"] == 11)
order = np.flatnonzero(live)
order = order[np.argsort(ap["vertex_offset"][order], kind="stable")]
offs = ap["vertex_offset"][order].astype(np.int64)
ends = np.append(offs[1:], len(vb))
keep = ~is11[order]
new = ap.copy()
pos = 0
parts = []
for i, o, e, k in zip(order, offs, ends, keep):
    if not k:
        continue
    new["vertex_offset"][i] = pos
    parts.append(vb[o:e])
    pos += e - o
nv = np.concatenate(parts)
tomb = np.zeros(1, ADMIN_DT)
tomb["vertex_offset"] = NO_DATA
tomb["name_id"] = NO_DATA
new[is11] = tomb
new.tofile(f"{OUT}/admin_polygons.bin")
nv.tofile(f"{OUT}/admin_vertices.bin")
print(f"p2: L11 polygons {int(is11.sum()):,} of {int(live.sum()):,} live; admin_vertices {len(vb):,} -> {len(nv):,} B "
      f"(L11 blocks {len(vb) - len(nv):,} B)")

ac = np.fromfile(f"{SRC}/admin_cells.bin", CELL_DT)
eb = open(f"{SRC}/admin_entries.bin", "rb").read()
table, where, cells = bytearray(), {}, []
dropped_ids = 0
for c, l in zip(ac["cell"], decode_entries(eb, ac["off"])):
    m = ~is11[l & 0x7FFFFFFF]
    dropped_ids += int((~m).sum())
    l = l[m]
    if len(l) == 0:
        continue
    k = l.astype("<u4").tobytes()
    if k not in where:
        where[k] = len(table)
        table += (len(l)).to_bytes(2, "little") + k
    cells.append((c, where[k]))
np.array(cells, dtype=CELL_DT).tofile(f"{OUT}/admin_cells.bin")
open(f"{OUT}/admin_entries.bin", "wb").write(table)
print(f"p2: admin cells {len(ac):,} -> {len(cells):,}, L11 ids dropped {dropped_ids:,}, stored lists {len(where):,}, "
      f"entries {len(eb):,} -> {len(table):,} B")

# references into L11 slots from files that point at admin polygon slots
for f, stride, o in (("admin_parents.bin", 4, 0), ("way_parents.bin", 4, 0), ("poi_records.bin", 36, 32),
                     ("place_nodes.bin", 20, 16)):
    if os.path.exists(f"{SRC}/{f}"):
        v = np.asarray(u32col(f"{SRC}/{f}", stride, o))
        v = v[v != NO_DATA]
        v = v[v < len(ap)]
        print(f"   {f}: {int(is11[v].sum()):,} references to L11 slots (of {len(v):,})")
