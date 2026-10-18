import numpy as np
z = np.load('decoded.npz'); s = np.load('share.npz')
cnt, level = z['cnt'], z['level']
idx = s['idx']; vp = z['vpoly'][idx]
lab = s['lab_lvl'].astype(np.int64)
P = len(cnt); m = cnt - 1; N = len(idx)
rs = np.concatenate([[0], np.cumsum(m)[:-1]])
pos = np.arange(N) - rs[vp]
prev = np.where(pos == 0, np.arange(N) + m[vp] - 1, np.arange(N) - 1)
nxt = np.where(pos == m[vp] - 1, np.arange(N) - m[vp] + 1, np.arange(N) + 1)
a, b = np.minimum(lab[prev], lab[nxt]), np.maximum(lab[prev], lab[nxt])
pk = a * (1 << 26) + b
u = np.unique(np.stack([lab, pk], 1), axis=0)
npairs = np.bincount(u[:, 0], minlength=lab.max() + 1)
isJ = npairs[lab] > 1
# pick an L8 polygon with ~30 vertices
r = np.nonzero((level == 8) & (m > 20) & (m < 40))[0][1000]
seqA = lab[rs[r]:rs[r] + m[r]]
print('ring', r, 'm', m[r])
print('labs', seqA.tolist())
print('isJ ', isJ[rs[r]:rs[r] + m[r]].astype(int).tolist())
# for a few vertices, show other occurrences
for k in range(rs[r], rs[r] + 6):
    occ = np.nonzero(lab == lab[k])[0]
    print('vertex', lab[k], 'occ rings', vp[occ].tolist(), 'nbr pairs', [(int(lab[prev[o]]), int(lab[nxt[o]])) for o in occ])
for rr in (246866,):
    sq = lab[rs[rr]:rs[rr] + m[rr]]; jj = isJ[rs[rr]:rs[rr] + m[rr]]
    k = int(np.nonzero(sq == 775192)[0][0])
    print('ring', rr, 'level', level[rr], 'm', m[rr], 'area', z['area'][rr], 'vs', z['area'][r])
    lo, hi = max(0, k - 12), min(m[rr], k + 4)
    print(list(zip(sq[lo:hi].tolist(), jj[lo:hi].astype(int).tolist())))
    print('ring', r, list(zip(seqA.tolist(), isJ[rs[r]:rs[r] + m[r]].astype(int).tolist()))[18:])
