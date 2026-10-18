#!/usr/bin/env python3
"""Chance-coincidence control for share.py's tolerance match: match every
vertex against all vertices displaced by SHIFT degrees of latitude (no real
shared vertex can survive the shift), with the same tolerances. The match
rate here is the false-positive rate of the tolerance test."""
import numpy as np, sys
SHIFT = float(sys.argv[1]) if len(sys.argv) > 1 else 0.0013
z = np.load('decoded.npz')
enc, cnt, vstart = z['enc'], z['cnt'], z['vstart']
lat, lng, vpoly = z['lat'].astype(np.float64), z['lng'].astype(np.float64), z['vpoly']
keep = np.ones(len(lat), bool); keep[vstart + cnt - 1] = False
idx = np.nonzero(keep)[0]
lat, lng, vp = lat[idx], lng[idx], vpoly[idx]
N = len(lat)
half = np.array([0.5e-5, 0.5e-4, 0.5e-6, 0.5e-7])[enc][vp] + 1e-5
C = 1.5e-4
def keyof(la, ln):
    return (np.floor(la / C).astype(np.int64) + 700000) * 4000000 + np.floor(ln / C).astype(np.int64) + 1300000
slat = lat + SHIFT
ck_s = keyof(slat, lng)
o = np.argsort(ck_s, kind='stable'); cks = ck_s[o]
ck = keyof(lat, lng)
hit = np.zeros(N, bool)
for dx in (-1, 0, 1):
    for dy in (-1, 0, 1):
        nk = ck + dx * 4000000 + dy
        lo = np.searchsorted(cks, nk, 'left'); hi = np.searchsorted(cks, nk, 'right')
        n = hi - lo; act = np.nonzero(n > 0)[0]; j = 0
        while len(act):
            cand = o[lo[act] + j]
            ok = (vp[cand] != vp[act]) & (np.abs(slat[cand] - lat[act]) <= half[cand] + half[act]) & (np.abs(lng[cand] - lng[act]) <= half[cand] + half[act])
            hit[act[ok]] = True
            j += 1; act = act[n[act] > j]
print(f'shift {SHIFT} deg: chance matches {hit.mean():.3%} of occurrences')
