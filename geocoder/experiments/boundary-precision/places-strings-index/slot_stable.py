"""Slot-stable variant V11 (keeps every record at its strategy-2 slot, so daily patches keep working):
- place_nodes: parent_poly_id written as NO_DATA (the server never reads it), hidden/linked
  nodes zeroed to a tombstone (lat=lng=0, name_id=NO_DATA) instead of keeping their data
- place_entries/place_cells: hidden nodes left out of the cell lists
- strings_core purged + name ids remapped (as V3), admin index deduplicated without flags (as V10)
Reader unchanged."""
import json, os, shutil
from common import *

d = f"{WORK}/variants/V11"
shutil.rmtree(d, ignore_errors=True)
os.makedirs(d)
V10 = f"{WORK}/variants/V10"
for f in ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_vertices.bin",
          "strings_core.bin", "strings_layout.json"]:
    os.symlink(os.path.realpath(f"{V10}/{f}"), f"{d}/{f}")

pn = load("place_nodes.bin", PLACE_DT).copy()
live = pn['name_id'] != NO_DATA
# remap name ids into the purged pool (same mapping as make_variants.py V3)
core = open(f"{SM}/strings_core.bin", 'rb').read()
starts = np.load(f"{WORK}/core_starts.npy")
keep = np.load(f"{WORK}/core_m_min.npy")
nul = np.nonzero(np.frombuffer(core, np.uint8) == 0)[0]
lens = nul - starts + 1
ki = np.nonzero(keep)[0]
new_off = np.concatenate([[0], np.cumsum(lens[ki])[:-1]]).astype(np.uint32)
remap = dict(zip(starts[ki].tolist(), new_off.tolist()))
pn['name_id'][live] = [remap[int(x)] for x in pn['name_id'][live]]
pn['parent_poly_id'] = NO_DATA
pn['lat'][~live] = 0
pn['lng'][~live] = 0
pn['place_type'][~live] = 0
pn.tofile(f"{d}/place_nodes.bin")

pc = load("place_cells.bin", CELL_DT)
pe = open(f"{SM}/place_entries.bin", 'rb').read()
cells, ent = [], bytearray()
for c, l in zip(pc['cell'], decode_entries(pe, pc['off'])):
    l = l[live[l]]
    if len(l) == 0:
        continue
    cells.append((c, len(ent)))
    ent += len(l).to_bytes(2, 'little') + l.astype('<u4').tobytes()
cc = np.array(cells, dtype=CELL_DT)
cc.tofile(f"{d}/place_cells.bin")
open(f"{d}/place_entries.bin", "wb").write(ent)
sizes = {f: zst_file(f"{d}/{f}") for f in ["place_nodes.bin", "strings_core.bin", "admin_cells.bin",
                                            "admin_entries.bin", "place_cells.bin", "place_entries.bin"]}
print("V11 zst:", sizes, "total", sum(sizes.values()), mib(sum(sizes.values())))
print("V11 raw:", {f: os.path.getsize(os.path.realpath(f"{d}/{f}")) for f in sizes})
