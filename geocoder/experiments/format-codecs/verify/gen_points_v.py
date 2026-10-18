"""Fresh verifier query points (seed 777013, not used by any track).
10k: random addr_points records jittered +-30 m; 10k: random live place_nodes jittered +-2 km.
Writes /ssd/shrink/max/verify/pts_v20k.txt ("lat lng")."""
import numpy as np
rng = np.random.default_rng(777013)
MAX = "/ssd/shrink/idx/max"


def jitter(lat, lng, m, n):
    out = []
    while len(out) < n:
        i = rng.integers(0, len(lat), n)
        a, b = lat[i].astype(np.float64), lng[i].astype(np.float64)
        ok = ~((a == 0) & (b == 0)) & (np.abs(a) <= 85)
        a, b = a[ok], b[ok]
        a = a + rng.uniform(-m, m, len(a)) / 111320.0
        b = b + rng.uniform(-m, m, len(b)) / (111320.0 * np.cos(np.radians(a)))
        b = (b + 180.0) % 360.0 - 180.0
        out.extend(zip(a, b))
    return out[:n]


ap = np.memmap(f"{MAX}/addr_points.bin", np.uint8, mode="r")
na = len(ap) // 28
alat = np.ndarray((na,), '<f4', ap, 0, (28,))
alng = np.ndarray((na,), '<f4', ap, 4, (28,))
pn = np.fromfile(f"{MAX}/place_nodes.bin", np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name', '<u4'), ('t', 'u1'),
                                                      ('p1', 'u1'), ('p2', '<u2'), ('pp', '<u4')]))
live = pn['name'] != 0xFFFFFFFF
pts = jitter(alat, alng, 30.0, 10000) + jitter(pn['lat'][live], pn['lng'][live], 2000.0, 10000)
with open("/ssd/shrink/max/verify/pts_v20k.txt", "w") as f:
    for a, b in pts:
        f.write(f"{a:.6f} {b:.6f}\n")
print("points", len(pts), "addr records", na, "live places", int(live.sum()))
