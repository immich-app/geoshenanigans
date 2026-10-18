#!/usr/bin/env python3
"""Cross-check for topo.py: share of ring EDGES (consecutive vertex pairs,
undirected, on share.py's clusters) that also occur in another ring. Vertices
can coincide while edges do not when two polygons' independent DP passes
keep different in-between vertices along the same border."""
import numpy as np, sys
z = np.load('decoded.npz'); s = np.load('share.npz')
cnt, level = z['cnt'], z['level']
idx = s['idx']; vp = z['vpoly'][idx]
P = len(cnt); m = cnt - 1; N = len(idx)
rs = np.concatenate([[0], np.cumsum(m)[:-1]])
pos = np.arange(N) - rs[vp]
nxt = np.where(pos == m[vp] - 1, np.arange(N) - m[vp] + 1, np.arange(N) + 1)
for name in ('lab_lvl', 'lab'):
    lab = s[name].astype(np.int64)
    a, b = np.minimum(lab, lab[nxt]), np.maximum(lab, lab[nxt])
    ek = a * (1 << 27) + b
    u, inv, c = np.unique(ek, return_inverse=True, return_counts=True)
    shared = c[inv.ravel()] > 1
    print(f'{name}: edges {N}, unique {len(u)} ({len(u)/N:.1%}); edges also in another ring {shared.mean():.1%}')
    for L in (2, 4, 6, 8):
        sel = level[vp] == L
        print(f'   level {L}: shared edges {shared[sel].mean():.1%}')
