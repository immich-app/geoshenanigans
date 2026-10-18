#!/usr/bin/env python3
"""Slot identity across two published days (strategy-2 stable slots).

identity.py PREV_DATE CUR_DATE

For street ways, interp ways and addr points: compares the published
*.osm_ids sidecars (slot -> (object type, osm id)) of two consecutive days
with the record content of the published .bin files, to check that a slot
live on both days holds the same entity, and how a client that only sees the
.bin files (no osm ids) can tell.
"""
import sys
import numpy as np

DAYS = '/ssd/shrink/days'
SC = '/ssd/shrink/sortpatch/sort/sidecars'
NO = np.uint32(0xFFFFFFFF)
SLOT = np.dtype([('type', 'u1'), ('flags', 'u1'), ('tier', 'u1'), ('res', 'u1'), ('id', '<u8')])
WAY = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad', '<u2'), ('name', '<u4')])
IW = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad1', '<u2'), ('street', '<u4'),
               ('start', '<u4'), ('end', '<u4'), ('interp', 'u1'), ('pad4', 'u1', 3)])
ADDR = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('hn', '<u4'), ('street', '<u4'),
                 ('parent', '<u4'), ('voff', '<u4'), ('vcnt', '<u4')])
TYPES = {0: 'NONE', 1: 'NODE', 2: 'WAY', 3: 'RELATION', 4: 'POSTCODE', 5: 'SYNTHETIC'}

prev_date, cur_date = sys.argv[1], sys.argv[2]


def sidecar(date, fam):
    raw = np.fromfile(f'{SC}/{date}/{fam}.osm_ids', np.uint8)
    hdr = raw[:12].view('<u4')
    assert hdr[0] == 0xD0510EAD and hdr[1] == 1, hdr
    s = raw[12:].view(SLOT)
    assert len(s) == hdr[2]
    return s


def load(date, fam):
    d = f'{DAYS}/{date}/planet/full'
    if fam == 'street_ways':
        r = np.memmap(f'{d}/street_ways.bin', WAY, 'r')
        nodes = np.memmap(f'{d}/street_nodes.bin', '<f4', 'r').reshape(-1, 2)
        tomb = (r['cnt'] == 0) & (r['off'] == 0) & (r['name'] == NO)
        first = nodes[np.where(r['cnt'] > 0, r['off'], 0)]
        sig = [r['cnt'], first[:, 0], first[:, 1]]
    elif fam == 'interp_ways':
        r = np.memmap(f'{d}/interp_ways.bin', IW, 'r')
        nodes = np.memmap(f'{d}/interp_nodes.bin', '<f4', 'r').reshape(-1, 2)
        tomb = (r['cnt'] == 0) & (r['off'] == 0) & (r['street'] == NO) & (r['start'] == 0) & (r['end'] == 0)
        first = nodes[np.where(r['cnt'] > 0, r['off'], 0)]
        sig = [r['cnt'], first[:, 0], first[:, 1], r['start'], r['end'], r['interp']]
    else:
        r = np.memmap(f'{d}/addr_points.bin', ADDR, 'r')
        tomb = ((r['lat'] == 0) & (r['lng'] == 0) & (r['hn'] == 0) & (r['street'] == 0) & (r['parent'] == NO)
                & (r['voff'] == NO) & (r['vcnt'] == 0))
        sig = [r['lat'], r['lng'], r['vcnt']]
    return np.asarray(tomb), [np.asarray(x) for x in sig]


def pct(a, b):
    return f'{a} ({100.0 * a / b:.4f}%)' if b else f'{a}'


for fam in ('street_ways', 'interp_ways', 'addr_points'):
    print(f'=== {fam}: {prev_date} -> {cur_date}')
    sp, sc = sidecar(prev_date, fam), sidecar(cur_date, fam)
    tp, gp = load(prev_date, fam)
    tc, gc = load(cur_date, fam)
    assert len(sp) == len(tp) and len(sc) == len(tc), (len(sp), len(tp), len(sc), len(tc))
    for name, s, t in ((prev_date, sp, tp), (cur_date, sc, tc)):
        flag = (s['flags'] & 1) != 0
        live = ~t
        zombie = t & ~flag            # tombstone content, sidecar still names an entity
        types = {TYPES.get(int(k), k): int(v) for k, v in zip(*np.unique(s['type'][live], return_counts=True))}
        print(f'  {name}: slots {len(s)}, live (by content) {int(live.sum())}, tombstone content {int(t.sum())} '
              f'(sidecar-flagged {int((t & flag).sum())}, unflagged {int(zombie.sum())}), '
              f'flagged but live content {int((flag & live).sum())}; live types {types}')
        key = (s['type'].astype(np.uint64) << np.uint64(56)) | s['id']
        k = np.sort(key[live])
        dup = k[1:] == k[:-1]
        ndup_keys = int(np.count_nonzero(dup & np.concatenate([[True], ~dup[:-1]])))
        print(f'    live slots sharing an osm identity with another live slot: {int(dup.sum()) + ndup_keys} '
              f'slots over {ndup_keys} identities')
    n = min(len(sp), len(sc))
    lp, lc = ~tp[:n], ~tc[:n]
    kp = (sp['type'][:n].astype(np.uint64) << np.uint64(56)) | sp['id'][:n]
    kc = (sc['type'][:n].astype(np.uint64) << np.uint64(56)) | sc['id'][:n]
    both = lp & lc
    same = both & (kp == kc)
    print(f'  slots < {n}: live both days {int(both.sum())}, same osm identity {pct(int(same.sum()), int(both.sum()))}, '
          f'DIFFERENT identity {int((both & (kp != kc)).sum())}')
    print(f'    live -> dead (deleted) {int((lp & ~lc).sum())}, dead -> live (slot reused by a new record) '
          f'{int((~lp & lc).sum())}, appended live slots {int((~tc[n:]).sum())}')
    reused = ~lp & lc
    print(f'    reused slots: prev sidecar tombstone-flagged {int((reused & ((sp["flags"][:n] & 1) != 0)).sum())}, '
          f'prev identity == new identity {int((reused & (kp == kc)).sum())}')
    unchanged = both.copy()
    for a, b in zip(gp, gc):
        unchanged &= a[:n] == b[:n]
    print(f'    live both days with identical content signature (geometry start / point, counts): '
          f'{pct(int(unchanged.sum()), int(both.sum()))}')
