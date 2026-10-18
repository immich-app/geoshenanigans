"""Write admin-minimal variants in the CURRENT on-disk format (so the unmodified grid_query
reader can answer from them) to check which shrink steps change answers.

V1 drop hidden place nodes (name_id == NO_DATA) and renumber
V2 V1 + nodes sorted by (L10 cell, lat, lng)  -> a cell's nodes form one contiguous run
V3 V2 + strings_core purged to the strings admin-minimal references
V4 V3 + place coords quantised to 1e-5 deg
V5 V3 + hamlets dropped
V6 V3 + INTERIOR_FLAG cleared in admin_entries
"""
import json, os, shutil
from common import *

OUT = f"{WORK}/variants"
os.makedirs(OUT, exist_ok=True)
FILES = ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_vertices.bin",
         "place_cells.bin", "place_entries.bin", "place_nodes.bin", "strings_core.bin", "strings_layout.json"]


def new_dir(name, src):
    d = f"{OUT}/{name}"
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    for f in FILES:
        os.symlink(os.path.realpath(f"{src}/{f}"), f"{d}/{f}")
    return d


def put(d, name, data):
    p = f"{d}/{name}"
    if os.path.islink(p):
        os.unlink(p)
    with open(p, "wb") as f:
        f.write(data if isinstance(data, (bytes, bytearray)) else data.tobytes())


def write_place(d, nodes, cell_of):
    """nodes: PLACE_DT array; cell_of: L10 cell per node. Writes nodes + cell index in shipped format."""
    order = np.lexsort((np.arange(len(nodes)), cell_of))
    c_sorted = cell_of[order]
    starts = np.nonzero(np.concatenate([[True], c_sorted[1:] != c_sorted[:-1]]))[0]
    ends = np.concatenate([starts[1:], [len(order)]])
    counts = ends - starts
    cells = np.zeros(len(starts), CELL_DT)
    cells['cell'] = c_sorted[starts]
    cells['off'] = np.concatenate([[0], np.cumsum(2 + 4 * counts)[:-1]])
    ent = bytearray()
    for s, e in zip(starts, ends):
        ids = np.sort(order[s:e]).astype('<u4')
        ent += int(e - s).to_bytes(2, 'little') + ids.tobytes()
    put(d, "place_nodes.bin", nodes)
    put(d, "place_cells.bin", cells)
    put(d, "place_entries.bin", bytes(ent))


pn = load("place_nodes.bin", PLACE_DT)
cell_of = np.load(f"{WORK}/place_cell_of.npy")
live = pn['name_id'] != NO_DATA

# V1
d1 = new_dir("V1", SM)
idx1 = np.nonzero(live)[0]
write_place(d1, pn[idx1], cell_of[idx1])

# V2
d2 = new_dir("V2", SM)
idx2 = np.load(f"{WORK}/place_sorted_idx.npy")
write_place(d2, pn[idx2], cell_of[idx2])

# V3: purge strings
d3 = new_dir("V3", d2)
core = open(f"{SM}/strings_core.bin", 'rb').read()
starts = np.load(f"{WORK}/core_starts.npy")
keep = np.load(f"{WORK}/core_m_min.npy")
nul = np.nonzero(np.frombuffer(core, np.uint8) == 0)[0]
lens = nul - starts + 1
ki = np.nonzero(keep)[0]
new_off = np.concatenate([[0], np.cumsum(lens[ki])[:-1]]).astype(np.uint32)
remap = dict(zip(starts[ki].tolist(), new_off.tolist()))
blob = b"".join(core[int(starts[i]):int(starts[i] + lens[i])] for i in ki)
put(d3, "strings_core.bin", blob)
ap = load("admin_polygons.bin", ADMIN_DT).copy()
m = ap['name_id'] != NO_DATA
ap['name_id'][m] = [remap[int(x)] for x in ap['name_id'][m]]
put(d3, "admin_polygons.bin", ap)
p3 = pn[idx2].copy()
p3['name_id'] = [remap[int(x)] for x in p3['name_id']]
put(d3, "place_nodes.bin", p3)
lay = json.load(open(f"{SM}/strings_layout.json"))
lay['tiers'][0]['end'] = len(blob)
put(d3, "strings_layout.json", json.dumps(lay).encode())

# V4: quantise
d4 = new_dir("V4", d3)
p4 = p3.copy()
p4['lat'] = (np.round(p4['lat'].astype(np.float64) * 1e5) / 1e5).astype(np.float32)
p4['lng'] = (np.round(p4['lng'].astype(np.float64) * 1e5) / 1e5).astype(np.float32)
put(d4, "place_nodes.bin", p4)

# V5: no hamlets
d5 = new_dir("V5", d3)
k5 = np.nonzero(p3['place_type'] != 4)[0]
write_place(d5, p3[k5], cell_of[idx2][k5])

# V6: no interior flag
d6 = new_dir("V6", d3)
ae = bytearray(open(f"{SM}/admin_entries.bin", 'rb').read())
cells = load("admin_cells.bin", CELL_DT)
arr = np.frombuffer(bytes(ae), np.uint8).copy()
for o in cells['off']:
    o = int(o)
    c = int(arr[o]) | (int(arr[o + 1]) << 8)
    # clear the top bit of each u32 (little endian: byte 3)
    arr[o + 2 + 3:o + 2 + 4 * c:4] &= 0x7F
put(d6, "admin_entries.bin", arr)
print("variants written to", OUT)
for v in ["V1", "V2", "V3", "V4", "V5", "V6"]:
    print(v, {f: os.path.getsize(f"{OUT}/{v}/{f}") for f in FILES if not os.path.islink(f"{OUT}/{v}/{f}")})
