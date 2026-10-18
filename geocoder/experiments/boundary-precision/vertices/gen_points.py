#!/usr/bin/env python3
"""Test points within 200 m of today's admin boundaries.

For each pool (levels 2, 4, 8 and all levels), pick ring edges with
probability proportional to their length in metres, a uniform position
along the edge, and a perpendicular offset uniform in [-200, 200] m.
Writes pts_<pool>.txt as "lat lng" lines (seeded, reproducible)."""
import numpy as np
rng = np.random.default_rng(20261013)
z = np.load('decoded.npz')
cnt, vstart, level = z['cnt'], z['vstart'], z['level']
lat, lng, vp = z['lat'].astype(np.float64), z['lng'].astype(np.float64), z['vpoly']
N = len(lat)
last = np.zeros(N, bool); last[vstart + cnt - 1] = True
a = np.nonzero(~last)[0]; b = a + 1                      # edges inside closed rings
la, ga, lb, gb = lat[a], lng[a], lat[b], lng[b]
coslat = np.cos(np.radians((la + lb) / 2))
dy = (lb - la) * 111320.0; dx = (gb - ga) * 111320.0 * coslat
L = np.hypot(dx, dy)
ok = L > 0
for name, sel in (('L2', level[vp[a]] == 2), ('L4', level[vp[a]] == 4), ('L8', level[vp[a]] == 8), ('all', np.ones(len(a), bool))):
    s = np.nonzero(sel & ok)[0]
    w = L[s] / L[s].sum()
    pick = s[rng.choice(len(s), size=20000, p=w)]
    t = rng.random(len(pick)); off = rng.uniform(-200, 200, len(pick))
    ux, uy = dx[pick] / L[pick], dy[pick] / L[pick]       # unit edge direction (east, north)
    nx, ny = -uy, ux                                       # left normal
    plat = la[pick] + t * (lb[pick] - la[pick]) + off * ny / 111320.0
    plng = ga[pick] + t * (gb[pick] - ga[pick]) + off * nx / (111320.0 * coslat[pick])
    with open(f'pts_{name}.txt', 'w') as f:
        for x, y in zip(plat, plng):
            f.write(f'{x:.7f} {y:.7f}\n')
    print(name, len(pick), 'edges in pool', len(s), f'total length {L[s].sum()/1000:.0f} km')
