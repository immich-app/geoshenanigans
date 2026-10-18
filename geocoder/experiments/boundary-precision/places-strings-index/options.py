"""Per-option zstd sizes (zstd 1.5.7 -18 -T1 = builder/tools/compress_index.py; reproduces the
manifest size_zst of the shipped files byte-exactly) for place_nodes, strings_core and the four
index files of planet/admin-minimal. Writes work/options.json."""
import json, os
from concurrent.futures import ThreadPoolExecutor
from common import *
from place_encode_lib import varint, zigzag

V = f"{WORK}/variants"
pn = load("place_nodes.bin", PLACE_DT)
live = pn["name_id"] != NO_DATA
jobs = {}
def J(key, data):
    jobs[key] = data if isinstance(data, (bytes, bytearray)) else (data.tobytes() if isinstance(data, np.ndarray) else b"".join(x.tobytes() if isinstance(x, np.ndarray) else x for x in data))
def F(key, path):
    jobs[key] = open(path, "rb").read()

for f in ["place_nodes.bin", "strings_core.bin", "admin_cells.bin", "admin_entries.bin", "place_cells.bin", "place_entries.bin"]:
    F(f"base {f}", f"{SM}/{f}")

# ---- place_nodes
p1 = pn.copy(); p1["parent_poly_id"] = NO_DATA
J("PN1 parent_poly_id=NO_DATA (slot-stable)", p1)
p2 = p1.copy(); p2["lat"][~live] = 0; p2["lng"][~live] = 0; p2["place_type"][~live] = 0
J("PN2 PN1 + hidden slots zeroed (slot-stable)", p2)
L = p1[live]
J("PN3 live only, shipped order, AoS 20 B, parent NO_DATA", L)
a13 = np.zeros(len(L), np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("t", "u1")]))
a13["lat"], a13["lng"], a13["name_id"], a13["t"] = L["lat"], L["lng"], L["name_id"], L["place_type"]
J("PN4 live, shipped order, AoS 13 B packed", a13)
a16 = np.zeros(len(L), np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("t", "u1"), ("p", "V3")]))
a16["lat"], a16["lng"], a16["name_id"], a16["t"] = L["lat"], L["lng"], L["name_id"], L["place_type"]
J("PN5 live, shipped order, AoS 16 B", a16)
J("PN6 live, shipped order, SoA f32|f32|u32|u8", [L["lat"].copy(), L["lng"].copy(), L["name_id"].copy(), L["place_type"].copy()])
# same with purged name ids (V3 remap) in shipped order
core = open(f"{SM}/strings_core.bin", "rb").read()
starts = np.load(f"{WORK}/core_starts.npy"); keep = np.load(f"{WORK}/core_m_min.npy")
nul = np.nonzero(np.frombuffer(core, np.uint8) == 0)[0]; lens = nul - starts + 1
ki = np.nonzero(keep)[0]
new_off = np.concatenate([[0], np.cumsum(lens[ki])[:-1]]).astype(np.uint32)
remap = np.full(len(starts), NO_DATA, np.uint32); remap[ki] = new_off
ordn = np.searchsorted(starts, L["name_id"])
Lp = L.copy(); Lp["name_id"] = remap[ordn]
J("PN6p live, shipped order, SoA, purged-pool name ids", [Lp["lat"].copy(), Lp["lng"].copy(), Lp["name_id"].copy(), Lp["place_type"].copy()])
a13p = a13.copy(); a13p["name_id"] = Lp["name_id"]
J("PN4p live, shipped order, AoS 13 B, purged-pool name ids", a13p)
P3 = load("place_nodes.bin", PLACE_DT, f"{V}/V3")
J("PN7 V3 file: live, sorted by (L10 cell, lat, lng), AoS 20 B, purged ids", P3)
# transfer encodings, cell order (V3), lossless 1e-7 and 1e-6
o3 = np.searchsorted(np.concatenate([[0], np.nonzero(np.frombuffer(open(f"{V}/V3/strings_core.bin", "rb").read(), np.uint8) == 0)[0][:-1] + 1]), P3["name_id"]).astype("<u4")
ob = o3.view(np.uint8).reshape(-1, 4)
for sc, lab in [(1e7, "1e-7 (lossless vs f32)"), (1e6, "1e-6")]:
    ql = np.round(P3["lat"].astype(np.float64) * sc).astype(np.int64); qg = np.round(P3["lng"].astype(np.float64) * sc).astype(np.int64)
    J(f"PN8 cell order, coords {lab} delta zigzag varint + name ordinal byte-planes + type", [varint(zigzag(np.diff(ql, prepend=0))), varint(zigzag(np.diff(qg, prepend=0))), ob[:, 0].copy(), ob[:, 1].copy(), ob[:, 2].copy(), P3["place_type"].copy()])
# shipped order lossless coords delta (to compare orders)
ql = np.round(Lp["lat"].astype(np.float64) * 1e7).astype(np.int64); qg = np.round(Lp["lng"].astype(np.float64) * 1e7).astype(np.int64)
op = np.searchsorted(np.concatenate([[0], np.nonzero(np.frombuffer(open(f"{V}/V3/strings_core.bin", "rb").read(), np.uint8) == 0)[0][:-1] + 1]), Lp["name_id"]).astype("<u4").view(np.uint8).reshape(-1, 4)
J("PN8s shipped order, coords 1e-7 delta zigzag varint + name ordinal byte-planes + type", [varint(zigzag(np.diff(ql, prepend=0))), varint(zigzag(np.diff(qg, prepend=0))), op[:, 0].copy(), op[:, 1].copy(), op[:, 2].copy(), Lp["place_type"].copy()])
# drop hamlets
nh = live & (pn["place_type"] != 4)
J("PN9 no hamlets: live non-hamlet, shipped order, AoS 20 B parent NO_DATA", p1[nh])
Lh = Lp[Lp["place_type"] != 4]
J("PN9b no hamlets: SoA, purged ids", [Lh["lat"].copy(), Lh["lng"].copy(), Lh["name_id"].copy(), Lh["place_type"].copy()])

# ---- strings
F("ST1 strings purged to admin-minimal refs (V3)", f"{V}/V3/strings_core.bin")
ap = load("admin_polygons.bin", ADMIN_DT)
def ref(ids):
    ids = ids[ids != NO_DATA]; m = np.zeros(len(starts), bool); m[np.searchsorted(starts, ids)] = True; return m
m_nh = ref(ap["name_id"]) | ref(pn["name_id"][nh])
blob = b"".join(core[int(starts[i]):int(starts[i] + lens[i])] for i in np.nonzero(m_nh)[0])
J("ST2 strings purged, hamlets dropped too", blob)
print("ST2 raw", len(blob), "strings", int(m_nh.sum()), flush=True)

# ---- index
F("IX1 place_cells, hidden dropped (V1)", f"{V}/V1/place_cells.bin"); F("IX1 place_entries, hidden dropped (V1)", f"{V}/V1/place_entries.bin")
pc = load("place_cells.bin", CELL_DT, f"{V}/V3"); pe = open(f"{V}/V3/place_entries.bin", "rb").read()
counts = np.array([int.from_bytes(pe[o:o + 2], "little") for o in pc["off"]], np.int64)
pcB = np.zeros(len(pc), CELL_DT); pcB["cell"] = pc["cell"]; pcB["off"] = np.concatenate([[0], np.cumsum(counts)[:-1]])
J("IX3 place_cells as (cell, first node) for cell-sorted nodes; no entries file", pcB)
F("IX4 admin_entries, INTERIOR_FLAG cleared (V6)", f"{V}/V6/admin_entries.bin")
ac = load("admin_cells.bin", CELL_DT)
J("IX6 admin_cells u64 cell ids only (offsets prefix-summed at load)", ac["cell"].copy())
NC = 6 * 4 ** 10
bm = np.zeros(NC, bool); bm[(ac["cell"] >> np.uint64(41)).astype(np.int64)] = True
J("IX7 admin cell presence bitmap (6*4^10 bits)", np.packbits(bm))
bm2 = np.zeros(NC, bool); bm2[(pc["cell"] >> np.uint64(41)).astype(np.int64)] = True
J("IX7 place cell presence bitmap", np.packbits(bm2))
J("IX7 place per-cell counts varint", varint(counts))

keys = list(jobs)
with ThreadPoolExecutor(12) as ex:
    sizes = dict(zip(keys, ex.map(lambda k: zst(jobs[k]), keys)))
res = {k: {"raw": len(jobs[k]), "zst": sizes[k]} for k in keys}
for k in keys:
    print(f"{k:95s} raw {res[k]["raw"]:>11,d} zst {res[k]["zst"]:>11,d} ({mib(res[k]["zst"])} MiB)")
json.dump(res, open(f"{WORK}/options.json", "w"), indent=1)
