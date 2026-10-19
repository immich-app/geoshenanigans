"""dump_ring.py <set dir> <poly_id> : the admin ring as the reader decodes it (f32 after the f64
sum), one "lat lng" per vertex, 9 decimals; also prints level, area, encoding, bbox to stderr."""
import sys

import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}

d, pid = sys.argv[1], int(sys.argv[2])
p = np.fromfile(f"{d}/admin_polygons.bin", PDT)[pid]
v = np.memmap(f"{d}/admin_vertices.bin", np.uint8, "r")
off, n = int(p["off"]), int(p["cnt"])
enc = int(v[off])
blat, blng = np.frombuffer(v[off + 2:off + 10].tobytes(), "<f4").astype(np.float64)
body = v[off + 10:]
if enc == 4:
    a = np.frombuffer(body[:8 * n].tobytes(), "<f4").reshape(-1, 2).astype(np.float64)
    lat, lng = a[:, 0], a[:, 1]
else:
    a = np.frombuffer(body[:(8 if enc == 3 else 4) * n].tobytes(), "<u4" if enc == 3 else "<u2").reshape(-1, 2).astype(np.float64)
    lat = (blat + a[:, 0] * SCALE[enc]).astype(np.float32).astype(np.float64)
    lng = (blng + a[:, 1] * SCALE[enc]).astype(np.float32).astype(np.float64)
print(f"poly {pid} level {int(p['lvl'])} area {float(p['area'])} enc {enc} n {n} bbox lat {lat.min():.6f}..{lat.max():.6f} lng {lng.min():.6f}..{lng.max():.6f}", file=sys.stderr)
sys.stdout.write("".join(f"{a:.9f} {b:.9f}\n" for a, b in zip(lat, lng)))
