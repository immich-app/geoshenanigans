#!/usr/bin/env python3
"""pbf_sample.py sample|check: verify the exact dumps against the planet PBF with an
independent reader (osmium-tool / libosmium, not the builder's pbf_reader.cpp).

sample: picks random OSM address nodes, street ways, OSM interpolation ways and closed-way
admin rings from out-nocap + dumps, writes ids.txt for `osmium getid -r` and sample.pkl.
check: parses osmium's OPL output and compares every sampled coordinate as integers 1e-7."""
import pickle
import sys
from decimal import Decimal

import numpy as np

B = '/ssd/precision/build'
F = f'{B}/out-nocap/planet/full'
D = f'{B}/dumps'
V = '/ssd/precision/verify/pbf'
SC = np.dtype([('ot', 'u1'), ('fl', 'u1'), ('tier', 'u1'), ('res', 'u1'), ('sid', '<u8')])
WH = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad', '<u2'), ('name', '<u4')])
IW = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('p', '<u2'), ('st', '<u4'), ('s', '<u4'), ('e', '<u4'),
               ('i', 'u1'), ('q', 'u1', 3)])
rng = np.random.default_rng(20261014)


def sidecar(path):
    return np.fromfile(path, dtype=SC, offset=12)


def e7(path):
    return np.memmap(path, dtype='<i4', mode='r').reshape(-1, 2)


def sample():
    s = {}
    # address points that are OSM nodes (kind 0)
    sc = sidecar(f'{F}/addr_points.osm_ids')
    kind = np.fromfile(f'{D}/addr_points.kind', dtype='u1')
    cand = np.flatnonzero((kind == 0) & (sc['ot'] == 1))
    pick = np.sort(rng.choice(cand, 5000, replace=False))
    ap = e7(f'{D}/addr_points.e7')
    s['addr'] = [(int(sc['sid'][i]), int(ap[i, 0]), int(ap[i, 1])) for i in pick]
    # street ways
    sc = sidecar(f'{F}/street_ways.osm_ids')
    wh = np.fromfile(f'{F}/street_ways.bin', dtype=WH)
    assert len(wh) == len(sc)
    cand = np.flatnonzero(sc['ot'] == 2)
    pick = np.sort(rng.choice(cand, 500, replace=False))
    sn = e7(f'{D}/street_nodes.e7')
    s['street'] = [(int(sc['sid'][i]), sn[wh['off'][i]:wh['off'][i] + wh['cnt'][i]].tolist()) for i in pick]
    # OSM interpolation ways (TIGER ones are synthetic keys)
    sc = sidecar(f'{F}/interp_ways.osm_ids')
    iw = np.fromfile(f'{F}/interp_ways.bin', dtype=IW)
    assert len(iw) == len(sc)
    cand = np.flatnonzero(sc['ot'] == 2)
    pick = np.sort(rng.choice(cand, min(200, len(cand)), replace=False))
    inn = e7(f'{D}/interp_nodes.e7')
    s['interp'] = [(int(sc['sid'][i]), inn[iw['off'][i]:iw['off'][i] + iw['cnt'][i]].tolist()) for i in pick]
    # closed-way admin rings, raw (before DP)
    rows = []
    with open(f'{D}/admin_meta.tsv') as f:
        hdr = f.readline().rstrip('\n').split('\t')
        for line in f:
            r = dict(zip(hdr, line.rstrip('\n').split('\t')))
            rows.append(r)
    print('admin_meta columns', hdr)
    idx = np.fromfile(f'{D}/admin_raw.e7.idx', dtype='<u8')
    ar = e7(f'{D}/admin_raw.e7')
    cw = [r for r in rows if r['source'] in ('1', 'closed way', 'way') and int(r['n_raw']) <= 300]
    print('closed-way admin rings <=300 raw', len(cw), 'of', len(rows))
    pick = rng.choice(len(cw), min(200, len(cw)), replace=False)
    s['admin'] = []
    for j in pick:
        r = cw[j]
        p = int(r['polygon'])
        s['admin'].append((int(r['osm_id']), ar[idx[p]:idx[p + 1]].tolist()))
    with open(f'{V}/sample.pkl', 'wb') as f:
        pickle.dump(s, f)
    with open(f'{V}/ids.txt', 'w') as f:
        for nid, _, _ in s['addr']:
            f.write(f'n{nid}\n')
        for k in ('street', 'interp', 'admin'):
            for wid, _ in s[k]:
                f.write(f'w{wid}\n')
    ncoord = len(s['addr']) + sum(len(c) for k in ('street', 'interp', 'admin') for _, c in s[k])
    print({k: len(v) for k, v in s.items()}, 'coordinates', ncoord)


def deg7(t):
    return int(Decimal(t) * 10_000_000)


def check():
    s = pickle.load(open(f'{V}/sample.pkl', 'rb'))
    nodes, ways = {}, {}
    for line in open(f'{V}/sample.opl'):
        f = line.split(' ')
        if f[0][0] == 'n':
            x = next(t for t in f if t.startswith('x'))[1:]
            y = next(t for t in f if t.startswith('y'))[1:].rstrip('\n')
            if x and y:
                nodes[int(f[0][1:])] = (deg7(y), deg7(x))
        elif f[0][0] == 'w':
            refs = next(t for t in f if t.startswith('N'))[1:].rstrip('\n')
            ways[int(f[0][1:])] = [int(r[1:]) for r in refs.split(',') if r]
    res = {}
    bad = []
    n = ok = 0
    for nid, la, lo in s['addr']:
        n += 1
        if nodes.get(nid) == (la, lo):
            ok += 1
        else:
            bad.append(('addr', nid, (la, lo), nodes.get(nid)))
    res['addr'] = (n, ok)
    for k in ('street', 'interp'):
        n = ok = 0
        for wid, coords in s[k]:
            pbf = [nodes.get(r) for r in ways.get(wid, [])]
            for i, c in enumerate(coords):
                n += 1
                if i < len(pbf) and pbf[i] == tuple(c):
                    ok += 1
                else:
                    bad.append((k, wid, i, tuple(c), pbf[i] if i < len(pbf) else None))
        res[k] = (n, ok)
    n = ok = seq = 0
    for wid, coords in s['admin']:
        pbf = [nodes.get(r) for r in ways.get(wid, [])]
        pset = set(pbf)
        for c in coords:
            n += 1
            if tuple(c) in pset:
                ok += 1
            else:
                bad.append(('admin', wid, tuple(c)))
        # ring equals the way's node sequence up to rotation / reversal (closing node dropped)
        ring = [tuple(c) for c in coords]
        if ring and ring[0] == ring[-1]:
            ring = ring[:-1]
        w = pbf[:-1] if pbf and pbf[0] == pbf[-1] else pbf
        dd = w + w
        rr = w[::-1] + w[::-1]
        L = len(w)
        if len(ring) == L and any(dd[i:i + L] == ring or rr[i:i + L] == ring for i in range(L)):
            seq += 1
    res['admin'] = (n, ok)
    res['admin_rings_equal_way_up_to_rotation'] = (len(s['admin']), seq)
    tot = sum(v[0] for k, v in res.items() if k != 'admin_rings_equal_way_up_to_rotation')
    good = sum(v[1] for k, v in res.items() if k != 'admin_rings_equal_way_up_to_rotation')
    print(res)
    print(f'coordinates checked {tot}, equal to the PBF integer 1e-7 value {good}, mismatched {tot - good}')
    for b in bad[:20]:
        print('BAD', b)


if __name__ == '__main__':
    {'sample': sample, 'check': check}[sys.argv[1]]()
