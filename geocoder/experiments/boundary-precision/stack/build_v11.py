"""Change (1): "dead weight" package V11 on planet/admin-minimal, written as real files in
today's on-disk format (reader unchanged) into work/v11. Consolidates the steps of
places-strings-index make_variants.py (V3 strings purge), admin_dedup_inplace.py (V10) and
slot_stable.py (V11), whose variant dirs no longer exist:
- strings_core purged to the strings admin_polygons + live place_nodes reference
  (work/core_m_min.npy from strings_stats.py), order kept, name ids remapped in
  admin_polygons and place_nodes, strings_layout core tier end = new length
- place_nodes: parent_poly_id = NO_DATA, hidden slots (name_id NO_DATA) zeroed (lat=lng=type=0)
- place_cells/place_entries: hidden slots left out of every list, empty cells dropped
- admin_entries: identical per-cell lists (INTERIOR_FLAG cleared, ids sorted) stored once
- admin_vertices: untouched (symlink to the baseline file)
Checks the zstd sizes against the V11 numbers places-strings-index recorded (work/slot_stable.txt)."""
import json, os, shutil, sys
sys.path.insert(0, "/ssd/shrink/places-strings-index")
from common import SM, WORK, NO_DATA, PLACE_DT, ADMIN_DT, CELL_DT, load, decode_entries, np
import subprocess

OUT = "/ssd/shrink/stack/work/v11"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(OUT)

# ---- strings purge (V3)
core = open(f"{SM}/strings_core.bin", "rb").read()
starts = np.load(f"{WORK}/core_starts.npy")
keep = np.load(f"{WORK}/core_m_min.npy")
nul = np.nonzero(np.frombuffer(core, np.uint8) == 0)[0]
lens = nul - starts + 1
ki = np.nonzero(keep)[0]
new_off = np.full(len(starts), NO_DATA, np.uint64)
new_off[ki] = np.concatenate([[0], np.cumsum(lens[ki])[:-1]])
blob = b"".join(core[int(starts[i]):int(starts[i] + lens[i])] for i in ki)


def remap(ids):
    k = np.searchsorted(starts, ids)
    assert np.all(starts[k] == ids), "name id not at a string start"
    out = new_off[k]
    assert np.all(out != NO_DATA), "name id points at a purged string"
    return out.astype(np.uint32)


open(f"{OUT}/strings_core.bin", "wb").write(blob)
lay = json.load(open(f"{SM}/strings_layout.json"))
lay["tiers"][0]["end"] = len(blob)
open(f"{OUT}/strings_layout.json", "wb").write(json.dumps(lay).encode())

ap = load("admin_polygons.bin", ADMIN_DT).copy()
m = ap["name_id"] != NO_DATA
ap["name_id"][m] = remap(ap["name_id"][m])
ap.tofile(f"{OUT}/admin_polygons.bin")

# ---- place nodes, slot stable (V11)
pn = load("place_nodes.bin", PLACE_DT).copy()
live = pn["name_id"] != NO_DATA
pn["name_id"][live] = remap(pn["name_id"][live])
pn["parent_poly_id"] = NO_DATA
pn["lat"][~live] = 0
pn["lng"][~live] = 0
pn["place_type"][~live] = 0
pn.tofile(f"{OUT}/place_nodes.bin")

pc = load("place_cells.bin", CELL_DT)
pe = open(f"{SM}/place_entries.bin", "rb").read()
cells, ent = [], bytearray()
for c, l in zip(pc["cell"], decode_entries(pe, pc["off"])):
    l = l[live[l]]
    if len(l) == 0:
        continue
    cells.append((c, len(ent)))
    ent += len(l).to_bytes(2, "little") + l.astype("<u4").tobytes()
np.array(cells, dtype=CELL_DT).tofile(f"{OUT}/place_cells.bin")
open(f"{OUT}/place_entries.bin", "wb").write(ent)

# ---- admin index dedup without INTERIOR_FLAG (V10)
ac = load("admin_cells.bin", CELL_DT)
eb = open(f"{SM}/admin_entries.bin", "rb").read()
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

os.symlink(f"{SM}/admin_vertices.bin", f"{OUT}/admin_vertices.bin")

# ---- check against the recorded V11 sizes (zstd 1.5.7 -18 -T1)
expect = {"place_nodes.bin": 26278296, "strings_core.bin": 8086466, "admin_cells.bin": 4105308,
          "admin_entries.bin": 3300600, "place_cells.bin": 1666341, "place_entries.bin": 9298431}
for f, e in expect.items():
    z = len(subprocess.run([ZSTD, "-18", "-T1", "-q", "-c", f"{OUT}/{f}"], stdout=subprocess.PIPE, check=True).stdout)
    print(f"{f:20s} raw {os.path.getsize(f'{OUT}/{f}'):>11,d} zst {z:>11,d} recorded V11 {e:>11,d} {'MATCH' if z == e else 'DIFF'}")
print("admin_polygons name ids remapped:", int(m.sum()), "of", len(ap), "slots; live place slots", int(live.sum()), "of", len(pn))
