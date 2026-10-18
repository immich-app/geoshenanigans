"""Per-column zstd -18 sizes of live place nodes under two orders, plus name-storage alternatives.

Order N = shipped slot order (≈ sorted by type then name, strategy-2 stable slots).
Order S = sorted by (L10 cell, lat, lng) — groups each L10 cell's nodes into one run,
          which is what an implicit (rangeable) place index needs.
"""
from common import *
from place_encode_lib import varint, zigzag

pn = load("place_nodes.bin", PLACE_DT)
live = pn['name_id'] != NO_DATA
sorted_idx = np.load(f"{WORK}/place_sorted_idx.npy")
core = open(f"{SM}/strings_core.bin", 'rb').read()
starts = np.load(f"{WORK}/core_starts.npy")


def cols(P, label):
    out = {}
    out['lat f32'] = zst(P['lat'].copy())
    out['lng f32'] = zst(P['lng'].copy())
    for scale, s in [(1e6, '1e-6'), (1e5, '1e-5')]:
        ql = np.round(P['lat'].astype(np.float64) * scale).astype(np.int64)
        qg = np.round(P['lng'].astype(np.float64) * scale).astype(np.int64)
        out[f'lat+lng {s} delta varint'] = zst(np.concatenate([varint(zigzag(np.diff(ql, prepend=0))), varint(zigzag(np.diff(qg, prepend=0)))]))
        out[f'lat+lng {s} i32 (no delta)'] = zst(np.concatenate([ql.astype('<i4'), qg.astype('<i4')]))
    out['name_id u32 offset'] = zst(P['name_id'].copy())
    ordn = np.searchsorted(starts, P['name_id']).astype('<u4')
    o4 = ordn.view(np.uint8).reshape(-1, 4)
    out['name ordinal byte-planes(3)'] = zst(np.concatenate([o4[:, 0], o4[:, 1], o4[:, 2]]))
    out['name ordinal delta zigzag varint'] = zst(varint(zigzag(np.diff(ordn.astype(np.int64), prepend=0))))
    out['type u8'] = zst(P['place_type'].copy())
    print(label)
    for k, v in out.items():
        print(f"   {k:40s} {v:>11,d}  ({mib(v)} MiB)")
    return out


N = pn[live]
S = pn[sorted_idx]
cols(N, "Order N (shipped order, live only)")
cols(S, "Order S (L10 cell, lat, lng; live only)")

# Name strings in node order (inline, with repeats) vs alphabetical pool of the same set
def get(off):
    e = core.index(b"\0", off)
    return core[off:e + 1]
inline_S = b"".join(get(int(o)) for o in S['name_id'])
print(f"inline names, order S (with repeats): raw {len(inline_S):,} zst {zst(inline_S):,}")
uniq = np.unique(S['name_id'])
alpha = b"".join(get(int(o)) for o in uniq)
print(f"place-name pool, alphabetical, distinct: raw {len(alpha):,} zst {zst(alpha):,}")
# first-occurrence order (spatial) distinct pool
_, first = np.unique(S['name_id'], return_index=True)
fo = S['name_id'][np.sort(first)]
spatial_pool = b"".join(get(int(o)) for o in fo)
print(f"place-name pool, first-occurrence spatial order, distinct: raw {len(spatial_pool):,} zst {zst(spatial_pool):,}")
ap = load("admin_polygons.bin", ADMIN_DT)
au = np.unique(ap['name_id'][ap['name_id'] != NO_DATA])
apool = b"".join(get(int(o)) for o in au)
print(f"admin-name pool, alphabetical, distinct: raw {len(apool):,} zst {zst(apool):,}")
only_admin = np.setdiff1d(au, uniq)
apool2 = b"".join(get(int(o)) for o in only_admin)
print(f"admin-only names (not also a place name), alphabetical: raw {len(apool2):,} zst {zst(apool2):,}")
