"""Change (1), the dead-weight package, on a full-mode preset, written in TODAY's on-disk format
(reader unchanged) so grid_query can answer from it.
usage: build1.py <max|withaddr>   -> sets/<preset>1/
- strings_core purged to the strings this preset's files reference (census.py core_keep_<preset>.npy:
  admin + postal polygons, place nodes, street ways, addr points (street + housenumber), interp ways,
  POI name/parent street/parent postcode, postcode centroids, way/addr/interp postcode sidecars),
  pool order kept, every core reference in every file remapped, strings_layout core end = new length
  (other tiers keep their start/end, so their ids are untouched)
- place_nodes: parent_poly_id = NO_DATA; hidden slots (name_id NO_DATA) lat = lng = place_type = 0
- place_cells/place_entries: hidden slots left out of every list, cells left empty dropped
- admin_entries: per-cell lists with INTERIOR_FLAG cleared, ids sorted, identical lists stored once
Files nothing changes are hard links of the preset's files. Remapped files that are the same inode in
both presets and get the same keep mask are hard-linked from the other preset's set when present."""
import hashlib, shutil, sys, time
from common4 import *

preset = sys.argv[1]
SRC = {"max": MAX, "withaddr": WA}[preset]
OTHER = {"max": "withaddr", "withaddr": "max"}[preset]
OTHER_SRC = {"max": MAX, "withaddr": WA}[OTHER]
OUT = f"{W}/sets/{preset}1"
shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(OUT)
t0 = time.time()
log = lambda *a: print(f"[{time.time() - t0:6.1f}s]", *a, flush=True)

core = np.fromfile(f"{SRC}/strings_core.bin", np.uint8)
CL = len(core)
starts = np.load(f"{W}/work/core_starts.npy")
keep = np.load(f"{W}/work/core_keep_{preset}.npy")
other_keep = f"{W}/work/core_keep_{OTHER}.npy"
same_keep = os.path.exists(other_keep) and np.array_equal(keep, np.load(other_keep))
nul = np.flatnonzero(core == 0)
lens = nul - starts + 1
ki = np.flatnonzero(keep)
new_off = np.full(len(starts), NO_DATA, np.uint32)
new_off[ki] = np.concatenate([[0], np.cumsum(lens[ki])[:-1]]).astype(np.uint32)
idx_of = np.full(CL, -1, np.int32)
idx_of[starts] = np.arange(len(starts), dtype=np.int32)
# kept bytes, vectorised: mark [start, start+len) of every kept string
mark = np.zeros(CL + 1, np.int32)
np.add.at(mark, starts[ki], 1)
np.add.at(mark, starts[ki] + lens[ki], -1)
take = np.cumsum(mark[:-1]) > 0
blob = core[take]
assert len(blob) == int(lens[ki].sum())
blob.tofile(f"{OUT}/strings_core.bin")
lay = layout(SRC)
assert lay["tiers"][0]["end"] == CL
lay["tiers"][0]["end"] = len(blob)
open(f"{OUT}/strings_layout.json", "w").write(json.dumps(lay))
log(f"strings_core {CL:,} -> {len(blob):,} B, {len(starts):,} -> {len(ki):,} strings; same keep as {OTHER}: {same_keep}")


def remap_inplace(col):
    v = np.asarray(col)
    m = (v != NO_DATA) & (v < CL)
    si = idx_of[v[m].astype(np.int64)]
    assert (si >= 0).all(), "core reference not at a string start"
    nv = new_off[si]
    assert (nv != NO_DATA).all(), "core reference to a purged string"
    out = v.copy()
    out[m] = nv
    col[:] = out
    return int(m.sum())


changed = set()
for f, stride, fields in STRING_REFS:
    p = f"{SRC}/{f}"
    if not os.path.exists(p):
        continue
    if f in ("place_nodes.bin",):
        continue  # handled below
    n_core = sum(int(((np.asarray(u32col(p, stride, o)) < CL)).sum()) for _, o in fields)
    if n_core == 0:
        log(f"{f}: no core references, hard link")
        continue
    o_path = f"{W}/sets/{OTHER}1/{f}"
    if same_keep and os.path.exists(o_path) and os.stat(p).st_ino == os.stat(f"{OTHER_SRC}/{f}").st_ino:
        os.link(o_path, f"{OUT}/{f}")
        changed.add(f)
        log(f"{f}: {n_core:,} core refs, linked from {OTHER}1 (same source inode, same keep mask)")
        continue
    shutil.copyfile(p, f"{OUT}/{f}")
    tot = 0
    mm = np.memmap(f"{OUT}/{f}", dtype=np.uint8, mode="r+")
    for name, o in fields:
        col = np.ndarray((len(mm) // stride,), dtype='<u4', buffer=mm, offset=o, strides=(stride,))
        tot += remap_inplace(col)
    mm.flush()
    del mm, col
    changed.add(f)
    log(f"{f}: remapped {tot:,} core refs")

# place nodes
pn = np.fromfile(f"{SRC}/place_nodes.bin", PLACE_DT).copy()
live = pn["name_id"] != NO_DATA
remap_inplace(pn["name_id"])
pn["parent_poly_id"] = NO_DATA
pn["lat"][~live] = 0
pn["lng"][~live] = 0
pn["place_type"][~live] = 0
pn.tofile(f"{OUT}/place_nodes.bin")
changed.add("place_nodes.bin")
log(f"place_nodes: {len(pn):,} slots, {int(live.sum()):,} live, {int((~live).sum()):,} hidden zeroed")

pc = np.fromfile(f"{SRC}/place_cells.bin", CELL_DT)
pe = open(f"{SRC}/place_entries.bin", "rb").read()
cells, ent = [], bytearray()
for c, l in zip(pc["cell"], decode_entries(pe, pc["off"])):
    l = l[live[l]]
    if len(l) == 0:
        continue
    cells.append((c, len(ent)))
    ent += len(l).to_bytes(2, "little") + l.astype("<u4").tobytes()
np.array(cells, dtype=CELL_DT).tofile(f"{OUT}/place_cells.bin")
open(f"{OUT}/place_entries.bin", "wb").write(ent)
changed |= {"place_cells.bin", "place_entries.bin"}
log(f"place index: cells {len(pc):,} -> {len(cells):,}, entries {len(pe):,} -> {len(ent):,} B")

ac = np.fromfile(f"{SRC}/admin_cells.bin", CELL_DT)
eb = open(f"{SRC}/admin_entries.bin", "rb").read()
table, where = bytearray(), {}
newc = ac.copy()
for i, l in enumerate(decode_entries(eb, ac["off"])):
    k = np.sort(l & np.uint32(0x7FFFFFFF)).astype("<u4").tobytes()
    if k not in where:
        where[k] = len(table)
        table += (len(k) // 4).to_bytes(2, "little") + k
    newc["off"][i] = where[k]
newc.tofile(f"{OUT}/admin_cells.bin")
open(f"{OUT}/admin_entries.bin", "wb").write(table)
changed |= {"admin_cells.bin", "admin_entries.bin", "strings_core.bin", "strings_layout.json"}
log(f"admin index: {len(ac):,} cells, {len(set(ac['off'].tolist())):,} distinct lists -> {len(where):,} stored; entries {len(eb):,} -> {len(table):,} B")

for f in sorted(os.listdir(SRC)):
    if f not in changed and not os.path.exists(f"{OUT}/{f}"):
        os.link(f"{SRC}/{f}", f"{OUT}/{f}")
log("changed files:", sorted(changed))
