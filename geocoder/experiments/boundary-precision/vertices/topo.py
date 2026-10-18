#!/usr/bin/env python3
"""Item 2b: TopoJSON-style shared-arc encoding of TODAY's simplified rings.

Uses share.py's clusters (vertices of different polygons within today's
own quantisation error count as one point). Each cluster gets one position:
the member on the finest grid. Junctions = clusters whose occurrences
disagree on their {prev,next} neighbour pair. Rings are cut at junctions
into arcs; arcs are deduplicated (either direction). Every ring then
decodes to the same vertex sequence as today, only with shared points
snapped to one representative (moves <= today's half-grid, <= ~5.5 m).

Writes topo/<grid>.bin streams and prints bytes.
"""
import numpy as np, os, sys
z = np.load('decoded.npz'); s = np.load('share.npz')
enc, cnt, vstart = z['enc'], z['cnt'], z['vstart']
MODE = sys.argv[1] if len(sys.argv) > 1 else 'all'
idx, lab = s['idx'], (s['lab'] if MODE == 'all' else s['lab_lvl'])
lab = np.unique(lab, return_inverse=True)[1].ravel()
lat = z['lat'][idx].astype(np.float64); lng = z['lng'][idx].astype(np.float64)
vp = z['vpoly'][idx]
P = len(cnt); m = cnt - 1; N = len(idx)
rs = np.concatenate([[0], np.cumsum(m)[:-1]])
pos = np.arange(N) - rs[vp]
prev = np.where(pos == 0, np.arange(N) + m[vp] - 1, np.arange(N) - 1)
nxt = np.where(pos == m[vp] - 1, np.arange(N) - m[vp] + 1, np.arange(N) + 1)
lp, ln_ = lab[prev], lab[nxt]
a, b = np.minimum(lp, ln_), np.maximum(lp, ln_)
pk = a.astype(np.int64) * (1 << 26) + b
# distinct neighbour pairs per cluster
u = np.unique(np.stack([lab.astype(np.int64), pk], 1), axis=0)
npairs = np.bincount(u[:, 0], minlength=lab.max() + 1)
occ = np.bincount(lab, minlength=lab.max() + 1)
isJ = npairs[lab] > 1
print(f'occurrences {N}, clusters {len(np.unique(lab))}, junction occurrences {isJ.mean():.1%}')

# representative position per cluster: finest grid member
scale = np.array([1e-5, 1e-4, 1e-6, 1e-7])[enc][vp]
o = np.lexsort((scale, lab))
first = np.r_[True, lab[o][1:] != lab[o][:-1]]
rep = np.full(lab.max() + 1, -1, np.int64); rep[lab[o][first]] = o[first]
rlat, rlng = lat[rep[lab]], lng[rep[lab]]
moved = np.maximum(np.abs(rlat - lat), np.abs(rlng - lng) * np.cos(np.radians(lat))) * 111320
print(f'snap to representative: moved >0: {(moved > 0).mean():.1%}; p99 {np.percentile(moved, 99):.2f} m; max {moved.max():.1f} m')

# rotate each ring to start at its first junction, append closing point
hasJ = np.bincount(vp, weights=isJ, minlength=P) > 0
fj = np.full(P, 0, np.int64)
jpos = np.nonzero(isJ)[0]
# first junction per ring
fjr = np.full(P, np.iinfo(np.int64).max); np.minimum.at(fjr, vp[jpos], pos[jpos]); fj[hasJ] = fjr[hasJ]
rot = rs[vp] + (pos + fj[vp]) % m[vp]          # occurrence at rotated position
seq = np.empty(N + P, np.int64); seqJ = np.empty(N + P, bool)
dst = np.arange(N) + vp                          # leave a slot after each ring
seq[dst] = lab[rot]; seqJ[dst] = isJ[rot]
close = rs + m + np.arange(P)
seq[close] = seq[close - m]; seqJ[close] = True  # closing point ends the last arc
ring_of = np.repeat(np.arange(P), m + 1)
start_flag = seqJ.copy(); start_flag[close] = False
start_flag[rs + np.arange(P)] = True             # ring start always opens an arc
starts = np.nonzero(start_flag)[0]
ends = np.r_[starts[1:], len(seq) - 1]
# an arc ends at the next start in the same ring, or at that ring's closing slot
nxt_close = close[ring_of[starts]]
ends = np.minimum(ends, nxt_close)
L = ends - starts + 1
print(f'arc occurrences {len(starts)} (mean {N/len(starts):.1f} vertices/arc)')
# order-independent arc key (direction-free): endpoints, length, sums
M61 = (1 << 61) - 1
v = seq.astype(np.uint64)
h1 = (v * np.uint64(0x9E3779B97F4A7C15)) ^ (v >> np.uint64(7))
cs1 = np.concatenate([np.zeros(1, np.uint64), np.cumsum(h1, dtype=np.uint64)])
cs0 = np.concatenate([np.zeros(1, np.uint64), np.cumsum(v, dtype=np.uint64)])
S1 = cs1[ends + 1] - cs1[starts]; S0 = cs0[ends + 1] - cs0[starts]
e1, e2 = np.minimum(seq[starts], seq[ends]), np.maximum(seq[starts], seq[ends])
key = np.stack([e1, e2, L, S0.view(np.int64), S1.view(np.int64)], 1)
uk, arc_id, arc_cnt = np.unique(key, axis=0, return_inverse=True, return_counts=True)
arc_id = arc_id.ravel()
NA = len(uk)
first_occ = np.full(NA, -1, np.int64)
order = np.arange(len(starts))[::-1]; first_occ[arc_id[order]] = order
stored = int(L[first_occ].sum())
print(f'unique arcs {NA}; stored vertices (endpoints per arc) {stored} = {stored/N:.1%} of today\'s open-ring occurrences')
print(f'  arcs used once {np.mean(arc_cnt == 1):.1%}, twice {np.mean(arc_cnt == 2):.1%}, 3+ {np.mean(arc_cnt >= 3):.1%}')

def zz(x): return np.where(x >= 0, 2 * x, -2 * x - 1).astype(np.uint64)
def vlen(v):
    n = np.ones(len(v), np.int64); t = v >> np.uint64(7)
    while np.any(t): n += t > 0; t >>= np.uint64(7)
    return n
def varint(v):
    v = v.astype(np.uint64); ln = vlen(v); Lm = int(ln.max())
    mat = np.zeros((len(v), Lm), np.uint8)
    for k in range(Lm):
        mat[:, k] = (((v >> np.uint64(7 * k)) & np.uint64(0x7f)).astype(np.uint8) | np.where(ln > k + 1, 0x80, 0).astype(np.uint8))
    return mat[np.arange(Lm)[None, :] < ln[:, None]]

os.makedirs('topo', exist_ok=True)
# canonical arc vertex sequences in order of first appearance (renumber arcs)
fo_sorted = np.sort(first_occ)
newid = np.empty(NA, np.int64); newid[arc_id[fo_sorted]] = np.arange(NA)
aS, aL = starts[fo_sorted], L[fo_sorted]
arc_of_vertex = np.repeat(np.arange(NA), aL)
vidx = np.repeat(aS, aL) + (np.arange(aL.sum()) - np.repeat(np.cumsum(aL) - aL, aL))
cl = seq[vidx]
# ring arc references: arc id + direction bit (reversed if stored arc's first point != this occurrence's first point)
rid = newid[arc_id]
canon_first = seq[aS][rid]
rev = (seq[starts] != canon_first).astype(np.int64)
ref = rid * 2 + rev
# delta of ref against previous ref in the same ring (first ref absolute)
rfirst = np.r_[True, ring_of[starts][1:] != ring_of[starts][:-1]]
dref = np.diff(ref, prepend=0); dref[rfirst] = ref[rfirst]
refs = varint(zz(dref))
nref = np.bincount(ring_of[starts], minlength=P)
refcount = varint(nref.astype(np.uint64))
print(f'ring arc refs: {len(starts)} refs, {len(refs)} B varint (+{len(refcount)} B counts)')

for G in (1e-5, 1e-6):
    qa = np.round(rlat / G).astype(np.int64); qg = np.round(rlng / G).astype(np.int64)
    # per-cluster grid values
    clat = np.zeros(lab.max() + 1, np.int64); clng = np.zeros(lab.max() + 1, np.int64)
    clat[lab] = qa; clng[lab] = qg
    A, B = clat[cl], clng[cl]
    afirst = np.r_[True, arc_of_vertex[1:] != arc_of_vertex[:-1]]
    dA = np.diff(A, prepend=0); dB = np.diff(B, prepend=0)
    dA[afirst] = A[afirst]; dB[afirst] = B[afirst]          # absolute first point
    # better: first point as delta from previous arc's first point
    fA = A[afirst]; fB = B[afirst]
    dA[afirst] = np.diff(fA, prepend=0); dB[afirst] = np.diff(fB, prepend=0)
    sA, sB = varint(zz(dA)), varint(zz(dB))
    alen = varint(aL.astype(np.uint64))
    tag = 'g1e-5' if G == 1e-5 else 'g1e-6'
    np.concatenate([sA, sB]).tofile(f'topo/{MODE}_{tag}_coords_split.bin')
    np.concatenate([alen, refcount, refs]).tofile(f'topo/{MODE}_{tag}_struct.bin')
    total = len(sA) + len(sB) + len(alen) + len(refs) + len(refcount) + 4 * NA + 4 * P
    print(f'grid {G:g}: coords {len(sA)+len(sB)} B, arc lengths {len(alen)} B, refs {len(refs)+len(refcount)} B, '
          f'arc offset table 4*{NA} B, polygon ref offsets 4*{P} B => total raw {total} B ({total/2**20:.2f} MiB)')
    np.concatenate([sA, sB, alen, refcount, refs]).tofile(f'topo/{MODE}_{tag}_all.bin')
