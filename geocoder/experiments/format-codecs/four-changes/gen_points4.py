"""Street/address-dense query points for the full-mode presets (deterministic seed).
Writes work/pts_addr.txt (20k: random addr_points records, jittered +-30 m),
work/pts_street.txt (10k: random street_nodes, +-30 m), work/pts_interp.txt (5k: random interp_nodes,
+-30 m), work/pts_poi.txt (5k: random poi_records, +-30 m). Zero (tombstone) coordinates skipped."""
import numpy as np
from common4 import *

rng = np.random.default_rng(20261013)


def sample(path, stride, n, label):
    m = np.memmap(path, dtype=np.uint8, mode="r")
    cnt = len(m) // stride
    lat = np.ndarray((cnt,), '<f4', m, 0, (stride,))
    lng = np.ndarray((cnt,), '<f4', m, 4, (stride,))
    out = []
    while len(out) < n:
        i = rng.integers(0, cnt, n)
        a, b = lat[i].astype(np.float64), lng[i].astype(np.float64)
        ok = ~((a == 0) & (b == 0)) & (np.abs(a) <= 85)
        a, b = a[ok], b[ok]
        a = a + rng.uniform(-30, 30, len(a)) / 111320.0
        b = b + rng.uniform(-30, 30, len(b)) / (111320.0 * np.cos(np.radians(a)))
        out.extend(zip(a, b))
    out = out[:n]
    with open(f"{W}/work/pts_{label}.txt", "w") as f:
        for a, b in out:
            f.write(f"{a:.6f} {b:.6f}\n")
    print(label, len(out), "from", cnt, "records")


sample(f"{MAX}/addr_points.bin", 28, 20000, "addr")
sample(f"{MAX}/street_nodes.bin", 8, 10000, "street")
sample(f"{MAX}/interp_nodes.bin", 8, 5000, "interp")
sample(f"{MAX}/poi_records.bin", 36, 5000, "poi")
