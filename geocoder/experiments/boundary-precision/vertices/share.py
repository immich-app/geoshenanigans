#!/usr/bin/env python3
"""Item 2a: shared-vertex duplication across polygons.

A vertex position decodes slightly differently in two polygons (each
polygon quantises relative to its own f32 bbox_min on its own grid), so
"same coordinate" is tested three ways:
  exact  : identical decoded f32 (lat,lng) bit patterns
  tol    : |dlat|,|dlng| <= half-grid(A) + half-grid(B) + 2e-5 deg slack
           (f32 rounding of bbox_min and of the decoded value)
Closing vertices (last == first) are excluded throughout.
Also clusters matched vertices (union-find) to count unique positions,
globally and within each admin level.
"""
import numpy as np, sys
z = np.load('decoded.npz')
enc, cnt, vstart, level = z['enc'], z['cnt'], z['vstart'], z['level']
lat, lng, vpoly = z['lat'].astype(np.float64), z['lng'].astype(np.float64), z['vpoly']
keep = np.ones(len(lat), bool); keep[vstart + cnt - 1] = False
idx = np.nonzero(keep)[0]
lat, lng, vp = lat[idx], lng[idx], vpoly[idx]
lv = level[vp]
N = len(lat)
half = np.array([0.5e-5, 0.5e-4, 0.5e-6, 0.5e-7])[enc][vp] + 1e-5   # per-vertex slack
print(f'open-ring vertex occurrences: {N}')

# exact
key = (lat.astype(np.float32).view(np.uint32).astype(np.uint64) << np.uint64(32)) | lng.astype(np.float32).view(np.uint32).astype(np.uint64)
o = np.argsort(key, kind='stable'); ks = key[o]
grp_start = np.r_[True, ks[1:] != ks[:-1]]
gid = np.cumsum(grp_start) - 1
gsize = np.bincount(gid)
# count of distinct polygons in group
pg = vp[o]
pair = np.unique(np.stack([gid, pg], 1), axis=0)
npoly = np.bincount(pair[:, 0], minlength=len(gsize))
ex_shared = npoly[gid] > 1
print(f'exact f32 match with another polygon: {ex_shared.mean():.1%} of occurrences; unique exact positions {len(gsize)} ({len(gsize)/N:.1%})')

# tolerance match via 1.5e-4 deg grid, 3x3 neighbourhood
C = 1.5e-4
cx = np.floor(lat / C).astype(np.int64) + 700000
cy = np.floor(lng / C).astype(np.int64) + 1300000
ck = cx * 4000000 + cy
o = np.argsort(ck, kind='stable'); cks = ck[o]
pairs_i, pairs_j = [], []
any_match = np.zeros(N, bool); same_lvl = np.zeros(N, bool); finer = np.zeros(N, bool); coarser = np.zeros(N, bool)
for dx in (-1, 0, 1):
    for dy in (-1, 0, 1):
        nk = ck + dx * 4000000 + dy
        lo = np.searchsorted(cks, nk, 'left'); hi = np.searchsorted(cks, nk, 'right')
        n = hi - lo
        act = np.nonzero(n > 0)[0]
        j = 0
        while len(act):
            cand = o[lo[act] + j]
            ok = (vp[cand] != vp[act]) & (np.abs(lat[cand] - lat[act]) <= half[cand] + half[act]) & (np.abs(lng[cand] - lng[act]) <= half[cand] + half[act])
            a, c = act[ok], cand[ok]
            any_match[a] = True
            same_lvl[a[lv[a] == lv[c]]] = True
            finer[a[lv[c] > lv[a]]] = True
            coarser[a[lv[c] < lv[a]]] = True
            sel = a < c
            pairs_i.append(a[sel]); pairs_j.append(c[sel])
            j += 1
            act = act[n[act] > j]
pi = np.concatenate(pairs_i); pj = np.concatenate(pairs_j)
print(f'tolerance match: {any_match.mean():.1%} of occurrences match a vertex of another polygon; pairs {len(pi)}')
print(f'  ...of a same-level polygon {same_lvl.mean():.1%}; of a finer (higher admin_level) polygon {finer.mean():.1%}; of a coarser one {coarser.mean():.1%}')

def clusters(pi, pj, n):
    lab = np.arange(n)
    while True:
        m = np.minimum(lab[pi], lab[pj])
        new = lab.copy()
        np.minimum.at(new, pi, m); np.minimum.at(new, pj, m)
        new = new[new]  # pointer jump
        new = new[new]
        if np.array_equal(new, lab):
            return lab
        lab = new
lab = clusters(pi, pj, N)
U = len(np.unique(lab))
print(f'unique positions (all levels merged): {U} = {U/N:.1%} of occurrences')
# per level: only pairs within the same level
sl = lv[pi] == lv[pj]
lab2 = clusters(pi[sl], pj[sl], N)
print('level  occurrences  shared_any  shared_same_level  unique_within_level  ratio')
tot = 0
for L in np.unique(lv):
    s = lv == L
    u = len(np.unique(lab2[s])); tot += u
    print(f'{L:5d} {s.sum():12d} {any_match[s].mean():10.1%} {same_lvl[s].mean():18.1%} {u:20d} {u/s.sum():6.1%}')
print(f'sum of per-level unique positions: {tot} = {tot/N:.1%}')
np.savez('share.npz', lab=lab, lab_lvl=lab2, any_match=any_match, same_lvl=same_lvl, idx=idx)
