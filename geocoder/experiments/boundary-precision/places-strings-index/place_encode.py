"""Re-encode place_nodes.bin (admin-minimal planet) several ways and measure zstd -18 sizes.

Every variant keeps exactly the information find_places() reads
(lat, lng, name, place_type) for the live nodes, except where a row says
otherwise.  Sizes are bytes of `zstd -18` output (the level
geocoder/builder/tools/compress_index.py uses for published files).
"""
import sys
from common import *

pn = load("place_nodes.bin", PLACE_DT)
n = len(pn)
pc = load("place_cells.bin", CELL_DT)
pe = open(f"{SM}/place_entries.bin", 'rb').read()
lists = decode_entries(pe, pc['off'])
cell_of = np.full(n, np.uint64(0xFFFFFFFFFFFFFFFF), dtype=np.uint64)
for c, l in zip(pc['cell'], lists):
    cell_of[l] = c
live = pn['name_id'] != NO_DATA

rows = []


def rec(label, parts, note=""):
    raw = sum(len(p.tobytes()) if isinstance(p, np.ndarray) else len(p) for p in parts)
    blob = b"".join(p.tobytes() if isinstance(p, np.ndarray) else p for p in parts)
    z = zst(blob)
    rows.append((label, raw, z, note))
    print(f"{label:70s} raw {raw:>11,d}  zst {z:>11,d} ({mib(z)} MiB) {note}", flush=True)
    return z


def zigzag(d):
    d = d.astype(np.int64)
    return ((d << 1) ^ (d >> 63)).astype(np.uint64)


def varint(u):
    """LEB128-encode an array of non-negative ints (vectorised)."""
    u = u.astype(np.uint64)
    out = []
    nbytes = np.ones(len(u), dtype=np.int64)
    t = u >> np.uint64(7)
    while True:
        m = t > 0
        if not m.any():
            break
        nbytes += m
        t = t >> np.uint64(7)
    total = int(nbytes.sum())
    buf = np.zeros(total, dtype=np.uint8)
    pos = np.concatenate([[0], np.cumsum(nbytes)[:-1]])
    v = u.copy()
    for k in range(int(nbytes.max())):
        m = nbytes > k
        byte = (v[m] & np.uint64(0x7F)).astype(np.uint8)
        more = nbytes[m] > k + 1
        byte = byte | (more.astype(np.uint8) << 7)
        buf[pos[m] + k] = byte
        v[m] = v[m] >> np.uint64(7)
    return buf


# 0. baseline
rec("A0 baseline AoS 20 B (as shipped)", [pn])

# 1. drop parent_poly_id + padding: AoS 13 B, same order, all slots
aos13 = np.zeros(n, dtype=np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name_id', '<u4'), ('t', 'u1')]))
aos13['lat'], aos13['lng'], aos13['name_id'], aos13['t'] = pn['lat'], pn['lng'], pn['name_id'], pn['place_type']
rec("A1 AoS 13 B (drop parent_poly_id + pad), same order", [aos13])

# 2. SoA same order
rec("A2 SoA f32 lat|lng|name_id|type, same order, all slots",
    [pn['lat'].copy(), pn['lng'].copy(), pn['name_id'].copy(), pn['place_type'].copy()])

# 3. SoA live only same order
L = pn[live]
rec("A3 SoA, live only (drop 272,880 hidden/tombstone), same order",
    [L['lat'].copy(), L['lng'].copy(), L['name_id'].copy(), L['place_type'].copy()])

# 4. live, sorted by L10 cell then lat then lng (cell grouping = implicit place index)
idx = np.nonzero(live)[0]
order = np.lexsort((pn['lng'][idx], pn['lat'][idx], cell_of[idx]))
S = pn[idx[order]]
S_cell = cell_of[idx[order]]
rec("A4 SoA f32, live, sorted by (L10 cell, lat, lng)",
    [S['lat'].copy(), S['lng'].copy(), S['name_id'].copy(), S['place_type'].copy()])
rec("A4b AoS 20 B as shipped layout, live, sorted by (L10 cell, lat, lng)", [S])

# 5. quantised int coords, delta + zigzag, fixed 4-byte and varint
for scale, lab in [(1e7, "1e-7 (1.1 cm)"), (1e6, "1e-6 (11 cm)"), (1e5, "1e-5 (1.1 m)")]:
    qlat = np.round(S['lat'].astype(np.float64) * scale).astype(np.int64)
    qlng = np.round(S['lng'].astype(np.float64) * scale).astype(np.int64)
    dlat = np.diff(qlat, prepend=0)
    dlng = np.diff(qlng, prepend=0)
    rec(f"A5 {lab}: delta i32 lat|lng + name_id u32 + type u8",
        [dlat.astype('<i4'), dlng.astype('<i4'), S['name_id'].copy(), S['place_type'].copy()])
    rec(f"A6 {lab}: delta zigzag varint lat|lng + name_id u32 + type u8",
        [varint(zigzag(dlat)), varint(zigzag(dlng)), S['name_id'].copy(), S['place_type'].copy()])

# 6. name_id as string ordinal instead of byte offset
core = open(f"{SM}/strings_core.bin", 'rb').read()
starts = np.concatenate([[0], np.nonzero(np.frombuffer(core, np.uint8) == 0)[0][:-1] + 1])
ordinal = np.searchsorted(starts, S['name_id'])
assert np.all(starts[ordinal] == S['name_id'])
qlat = np.round(S['lat'].astype(np.float64) * 1e6).astype(np.int64)
qlng = np.round(S['lng'].astype(np.float64) * 1e6).astype(np.int64)
dlat, dlng = np.diff(qlat, prepend=0), np.diff(qlng, prepend=0)
rec("A7 1e-6 delta varint coords + name ORDINAL u32 + type",
    [varint(zigzag(dlat)), varint(zigzag(dlng)), ordinal.astype('<u4'), S['place_type'].copy()])
rec("A7b 1e-6 delta varint coords + name ORDINAL as 3-byte + type",
    [varint(zigzag(dlat)), varint(zigzag(dlng)),
     ordinal.astype('<u4').view(np.uint8).reshape(-1, 4)[:, :3].copy(), S['place_type'].copy()])
rec("A7c 1e-6 delta varint coords + name ORDINAL varint + type",
    [varint(zigzag(dlat)), varint(zigzag(dlng)), varint(ordinal), S['place_type'].copy()])
# byte-plane split of ordinal (helps zstd)
o4 = ordinal.astype('<u4').view(np.uint8).reshape(-1, 4)
rec("A7d 1e-6 delta varint coords + name ORDINAL byte-planes(3) + type",
    [varint(zigzag(dlat)), varint(zigzag(dlng)), o4[:, 0].copy(), o4[:, 1].copy(), o4[:, 2].copy(), S['place_type'].copy()])

# 7. inline names instead of name_id (names in node order, NUL separated)
def get(off):
    e = core.index(b"\0", off)
    return core[off:e]
inline = b"\0".join(get(int(o)) for o in S['name_id']) + b"\0"
rec("A8 1e-6 delta varint coords + INLINE names + type (no name_id)",
    [varint(zigzag(dlat)), varint(zigzag(dlng)), inline, S['place_type'].copy()],
    note="names then need not be in strings_core")

np.save(f"{WORK}/place_sorted_idx.npy", idx[order])
np.save(f"{WORK}/place_cell_of.npy", cell_of)
import json
json.dump(rows, open(f"{WORK}/place_encode_rows.json", "w"), indent=1)
