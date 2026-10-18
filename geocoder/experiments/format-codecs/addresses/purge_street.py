#!/usr/bin/env python3
"""Today's-format variant: strings_street.bin purged to the strings some
shipped file references (the rest are the builder's original-way-name strings,
used at build time only). Street-tier offsets are remapped monotonically in
every file that holds them; strings_layout.json is unchanged (the tier keeps
its base, the file just ends early), so other tiers' offsets do not move.
usage: purge_street.py <out dir>  (other files hardlinked from idx/max)"""
import json, os, sys, numpy as np
D = '/ssd/shrink/idx/max'
OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)
lay = {t['name']: t for t in json.load(open(f'{D}/strings_layout.json'))['tiers']}
A0, A1 = lay['street']['start'], lay['street']['end']
ss = np.fromfile(f'{D}/strings_street.bin', np.uint8)
starts = np.concatenate([[0], np.nonzero(ss == 0)[0][:-1] + 1]).astype(np.int64)
ends = np.concatenate([starts[1:], [len(ss)]])

# (file, record width in u32, columns holding string offsets)
files = [('street_ways.bin', 3, [2]), ('addr_points.bin', 7, [2, 3]), ('interp_ways.bin', 6, [2]),
         ('poi_records.bin', 9, [4, 6, 7])]
arrs = {}
used = np.zeros(len(starts), bool)
for f, wdt, cols in files:
    a = np.fromfile(f'{D}/{f}', np.uint32).reshape(-1, wdt)
    arrs[f] = a
    for c in cols:
        v = a[:, c]
        m = (v >= A0) & (v < A1)
        k = np.searchsorted(starts, v[m].astype(np.int64) - A0)
        assert np.array_equal(starts[k], v[m].astype(np.int64) - A0)
        used[k] = True
print('strings', len(starts), 'referenced', int(used.sum()), 'dropped bytes', int((ends - starts)[~used].sum()))
newlen = np.where(used, ends - starts, 0)
newstart = np.cumsum(newlen) - newlen
keep = np.repeat(used, ends - starts)
ss[keep].tofile(f'{OUT}/strings_street.bin')
for f, wdt, cols in files:
    a = arrs[f]
    for c in cols:
        v = a[:, c]
        m = (v >= A0) & (v < A1)
        k = np.searchsorted(starts, v[m].astype(np.int64) - A0)
        v[m] = (A0 + newstart[k]).astype(np.uint32)
    a.tofile(f'{OUT}/{f}')
for f in os.listdir(D):
    p = f'{OUT}/{f}'
    if not os.path.exists(p):
        os.link(f'{D}/{f}', p)
print('written', OUT, os.path.getsize(f'{OUT}/strings_street.bin'))
