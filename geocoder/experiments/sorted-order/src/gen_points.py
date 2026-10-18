#!/usr/bin/env python3
"""Query points for the sorted-vs-published answer check (seeded).

gen_points.py DATE OUT_DIR
  pts_mix100k.txt : 100k points
     30k addr points jittered +-30 m      5k addr points at their own position
     15k street nodes jittered +-30 m     5k street first nodes (shared-node ties)
     15k interp nodes jittered +-30 m     5k interp nodes (end points) exact
     15k POIs jittered +-30 m             10k place nodes jittered +-30 m
  pts_ties.txt : targeted exact-tie queries for the reader tie classes whose
     winner could change with the new order (same name + same first node,
     new order != slot order): per class the shared node itself and points
     5 / 15 / 40 m behind it (opposite the members' mean direction), so the
     shared node is the nearest point of every member.
  ties_report.txt : those classes, and whether their members differ in what
     the reader reports for the winner (way_parents / way_postcodes / being an
     addr point's parent way; interp numbers / interp_postcodes).
"""
import os, sys
import numpy as np

date, OUT = sys.argv[1], sys.argv[2]
D = f'/ssd/shrink/days/{date}/planet'
META = f'/ssd/shrink/sortpatch/sort/meta/{date}'
os.makedirs(OUT, exist_ok=True)
rng = np.random.default_rng(20261014)
NO = np.uint32(0xFFFFFFFF)
WAY = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad', '<u2'), ('name', '<u4')])
IW = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad1', '<u2'), ('street', '<u4'),
               ('start', '<u4'), ('end', '<u4'), ('interp', 'u1'), ('pad4', 'u1', 3)])
ADDR = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('hn', '<u4'), ('street', '<u4'),
                 ('parent', '<u4'), ('voff', '<u4'), ('vcnt', '<u4')])
POI = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('rest', 'u1', 28)])
PLACE = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('rest', 'u1', 12)])


def jitter(lat, lng, m):
    lat = np.asarray(lat, np.float64); lng = np.asarray(lng, np.float64)
    dl = rng.uniform(-m, m, len(lat)) / 111320.0
    dg = rng.uniform(-m, m, len(lat)) / (111320.0 * np.maximum(np.cos(np.radians(lat)), 0.05))
    return np.clip(lat + dl, -89.9, 89.9), ((lng + dg + 180) % 360) - 180


def write(path, parts):
    lat = np.concatenate([p[0] for p in parts]); lng = np.concatenate([p[1] for p in parts])
    with open(path, 'w') as f:
        for a, b in zip(lat, lng):
            f.write(f'{a:.8f} {b:.8f}\n')
    return len(lat)


ways = np.memmap(f'{D}/full/street_ways.bin', WAY, 'r')
sn = np.memmap(f'{D}/full/street_nodes.bin', '<f4', 'r').reshape(-1, 2)
iws = np.memmap(f'{D}/full/interp_ways.bin', IW, 'r')
inn = np.memmap(f'{D}/full/interp_nodes.bin', '<f4', 'r').reshape(-1, 2)
ap = np.memmap(f'{D}/full/addr_points.bin', ADDR, 'r')
poi = np.memmap(f'{D}/poi/all/poi_records.bin', POI, 'r')
pl = np.memmap(f'{D}/full/place_nodes.bin', PLACE, 'r')


def live_pick(n, ok, k):
    idx = rng.choice(n, int(k * 1.3) + 100, replace=False)
    idx = idx[ok(idx)]
    return np.sort(idx[:k]) if len(idx) >= k else idx


parts = []
a = live_pick(len(ap), lambda i: (ap['hn'][i] != 0) | (ap['street'][i] != 0), 35000)
parts.append(jitter(ap['lat'][a[:30000]], ap['lng'][a[:30000]], 30))
parts.append((ap['lat'][a[30000:]].astype(np.float64), ap['lng'][a[30000:]].astype(np.float64)))
w = live_pick(len(ways), lambda i: ways['cnt'][i] >= 2, 20000)
node = ways['off'][w[:15000]].astype(np.int64) + (rng.random(15000) * ways['cnt'][w[:15000]]).astype(np.int64)
parts.append(jitter(sn[node, 0], sn[node, 1], 30))
first = ways['off'][w[15000:]].astype(np.int64)
parts.append((sn[first, 0].astype(np.float64), sn[first, 1].astype(np.float64)))
iw = live_pick(len(iws), lambda i: iws['cnt'][i] >= 2, 20000)
node = iws['off'][iw[:15000]].astype(np.int64) + (rng.random(15000) * iws['cnt'][iw[:15000]]).astype(np.int64)
parts.append(jitter(inn[node, 0], inn[node, 1], 30))
end = iws['off'][iw[15000:]].astype(np.int64) + np.where(rng.random(5000) < 0.5, 0, iws['cnt'][iw[15000:]] - 1)
parts.append((inn[end, 0].astype(np.float64), inn[end, 1].astype(np.float64)))
p = live_pick(len(poi), lambda i: (poi['lat'][i] != 0) | (poi['lng'][i] != 0), 15000)
parts.append(jitter(poi['lat'][p], poi['lng'][p], 30))
q = live_pick(len(pl), lambda i: (pl['lat'][i] != 0) | (pl['lng'][i] != 0), 10000)
parts.append(jitter(pl['lat'][q], pl['lng'][q], 30))
print('mix', write(f'{OUT}/pts_mix100k.txt', parts), [len(x[0]) for x in parts])


# ---------------------------------------------------------------- tie classes
def tie_classes(recs, nodes, name_field, slot_file):
    perm = np.fromfile(slot_file, '<u4')           # new id -> slot
    live = perm
    cnt = recs['cnt'][live]
    ok = cnt > 0
    live = live[ok]
    newid = np.nonzero(ok)[0].astype(np.int64)
    off = recs['off'][live].astype(np.int64)
    lat = nodes[off, 0]; lng = nodes[off, 1]
    name = recs[name_field][live]
    order = np.lexsort((live, lng.view('<u4'), lat.view('<u4'), name))
    k = np.stack([name[order], lat[order].view('<u4'), lng[order].view('<u4')], 1)
    brk = np.concatenate([[True], (k[1:] != k[:-1]).any(1)])
    gid = np.cumsum(brk) - 1
    size = np.bincount(gid)
    multi = size[gid] > 1
    # within a class (sorted by slot) the new ids must increase
    nid = newid[order]
    dec = np.zeros(len(order), bool)
    dec[1:] = (nid[1:] < nid[:-1]) & (gid[1:] == gid[:-1])
    flipped = np.unique(gid[dec])
    starts = np.nonzero(brk)[0]
    members = [order[starts[g]:starts[g] + size[g]] for g in flipped]
    return live, members, int(multi.sum()), int((size > 1).sum())


rep = open(f'{OUT}/ties_report.txt', 'w')
tie_pts = []


def behind_points(nodes, recs, slots, base_lat, base_lng):
    # direction from the shared node to each member's second node, averaged
    d = []
    for s in slots:
        o = int(recs['off'][s])
        d.append((float(nodes[o + 1, 0]) - base_lat, (float(nodes[o + 1, 1]) - base_lng) * np.cos(np.radians(base_lat))))
    d = np.array(d)
    v = d.mean(0)
    nv = np.hypot(*v)
    v = (v / nv) if nv > 0 else np.array([1.0, 0.0])
    out = [(base_lat, base_lng)]
    for m in (5.0, 15.0, 40.0):
        out.append((base_lat - v[0] * m / 111320.0, base_lng - v[1] * m / (111320.0 * np.cos(np.radians(base_lat)))))
    return out


wp = np.memmap(f'{D}/full/way_parents.bin', '<u4', 'r')
wpc = np.memmap(f'{D}/full/way_postcodes.bin', '<u4', 'r')
ipc = np.memmap(f'{D}/full/interp_postcodes.bin', '<u4', 'r')
pw = np.asarray(ap['parent'])
is_parent = np.zeros(len(ways), bool)
is_parent[pw[pw != NO]] = True
del pw

for fam, recs, nodes, field, slotf in (('street', ways, sn, 'name', f'{META}/street.slot.u32'),
                                       ('interp', iws, inn, 'street', f'{META}/interp.slot.u32')):
    live, members, n_in, n_cls = tie_classes(recs, nodes, field, slotf)
    observable = 0
    for mem in members:
        slots = live[mem]
        o = int(recs['off'][slots[0]])
        base_lat, base_lng = float(nodes[o, 0]), float(nodes[o, 1])
        if fam == 'street':
            diff = (len(set(wp[slots].tolist())) > 1 or len(set(wpc[slots].tolist())) > 1
                    or is_parent[slots].any())
        else:
            diff = (len(set(zip(recs['start'][slots].tolist(), recs['end'][slots].tolist(),
                                recs['interp'][slots].tolist()))) > 1 or len(set(ipc[slots].tolist())) > 1)
        observable += bool(diff)
        rep.write(f'{fam} class at {base_lat:.7f},{base_lng:.7f} slots {slots.tolist()} '
                  f'observable_difference {bool(diff)}\n')
        tie_pts.extend(behind_points(nodes, recs, slots, base_lat, base_lng))
    msg = (f'{fam}: tie classes {n_cls} ({n_in} records); new order != slot order in {len(members)} classes, '
           f'{observable} of them with members the reader can tell apart')
    print(msg)
    rep.write(msg + '\n')
rep.close()
with open(f'{OUT}/pts_ties.txt', 'w') as f:
    for a, b in tie_pts:
        f.write(f'{a:.8f} {b:.8f}\n')
print('ties', len(tie_pts))
