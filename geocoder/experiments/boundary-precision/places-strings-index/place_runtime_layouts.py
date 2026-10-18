"""Runtime (mmap-able, fixed-width) place_nodes layouts without parent_poly_id, on the V3 data
(live nodes, sorted by L10 cell, purged string offsets). zstd -18 bytes."""
from common import *

P = load("place_nodes.bin", PLACE_DT, f"{WORK}/variants/V3")
a13 = np.zeros(len(P), np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name_id', '<u4'), ('t', 'u1')]))
a13['lat'], a13['lng'], a13['name_id'], a13['t'] = P['lat'], P['lng'], P['name_id'], P['place_type']
print("AoS 13 B:", a13.nbytes, zst(a13))
soa = [P['lat'].copy(), P['lng'].copy(), P['name_id'].copy(), P['place_type'].copy()]
print("SoA f32|f32|u32|u8:", sum(x.nbytes for x in soa), zst(b"".join(x.tobytes() for x in soa)))
