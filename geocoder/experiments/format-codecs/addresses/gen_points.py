#!/usr/bin/env python3
"""Address-dense query points (seeded):
  addr20k : 20k random addr points jittered +-30 m
  exact5k : 5k addr points queried at their exact f32 position (ties, 0 m)
  street10k: 10k random street nodes jittered +-20 m
  interp5k : 5k random interpolation nodes jittered +-25 m (TIGER/OSM interp)
"""
import numpy as np
D = '/ssd/shrink/idx/max'
W = '/ssd/shrink/max/addresses'
rng = np.random.default_rng(20261013)

def jitter(lat, lng, m):
    dl = rng.uniform(-m, m, len(lat)) / 111320.0
    dg = rng.uniform(-m, m, len(lat)) / (111320.0 * np.maximum(np.cos(np.radians(lat)), 0.05))
    return np.clip(lat + dl, -89.9, 89.9), ((lng + dg + 180) % 360) - 180

def write(name, lat, lng):
    with open(f'{W}/pts_{name}.txt', 'w') as f:
        for a, b in zip(lat, lng):
            f.write(f'{a:.8f} {b:.8f}\n')
    print(name, len(lat))

ap = np.memmap(f'{D}/addr_points.bin', np.uint32, 'r').reshape(-1, 7)
live = np.nonzero(ap[:, 4] != 0xFFFFFFFF)[0]  # excludes tombstones (and unparented points)
pick = rng.choice(len(ap), 40000, replace=False)
pick = pick[(ap[pick, 2] != 0) | (ap[pick, 3] != 0)][:25000]
lat = ap[pick, 0].view(np.float32).astype(np.float64); lng = ap[pick, 1].view(np.float32).astype(np.float64)
write('addr20k', *jitter(lat[:20000], lng[:20000], 30))
write('exact5k', lat[20000:25000], lng[20000:25000])

sn = np.memmap(f'{D}/street_nodes.bin', np.float32, 'r').reshape(-1, 2)
k = rng.choice(len(sn), 10000, replace=False)
write('street10k', *jitter(sn[k, 0].astype(np.float64), sn[k, 1].astype(np.float64), 20))
inn = np.memmap(f'{D}/interp_nodes.bin', np.float32, 'r').reshape(-1, 2)
k = rng.choice(len(inn), 5000, replace=False)
write('interp5k', *jitter(inn[k, 0].astype(np.float64), inn[k, 1].astype(np.float64), 25))
