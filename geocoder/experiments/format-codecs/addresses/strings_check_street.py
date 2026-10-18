#!/usr/bin/env python3
"""strings_street.bin: which strings does anything reference? (every file that
holds string offsets is scanned; offsets in the addr tier are collected)."""
import json, numpy as np
D = '/ssd/shrink/idx/max'
lay = {t['name']: t for t in json.load(open(f'{D}/strings_layout.json'))['tiers']}
A0, A1 = lay["street"]['start'], lay["street"]["end"]
sa = np.fromfile(f'{D}/strings_street.bin', np.uint8)
starts = np.concatenate([[0], np.nonzero(sa == 0)[0][:-1] + 1])
print('strings in street tier', len(starts), 'bytes', len(sa))
refs = []
def add(name, arr):
    a = arr[(arr >= A0) & (arr < A1)].astype(np.int64) - A0
    print(f'  {name:28s} refs into street tier {len(a):>12d}')
    refs.append(np.unique(a))
ap = np.memmap(f'{D}/addr_points.bin', np.uint32, 'r').reshape(-1, 7)
add('addr_points.housenumber', np.asarray(ap[:, 2])); add('addr_points.street', np.asarray(ap[:, 3]))
add('addr_postcodes', np.fromfile(f'{D}/addr_postcodes.bin', np.uint32))
iw = np.fromfile(f'{D}/interp_ways.bin', np.uint32).reshape(-1, 6)
add('interp_ways.street', iw[:, 2])
add('interp_postcodes', np.fromfile(f'{D}/interp_postcodes.bin', np.uint32))
add('way_postcodes', np.fromfile(f'{D}/way_postcodes.bin', np.uint32))
sw = np.fromfile(f'{D}/street_ways.bin', np.uint32).reshape(-1, 3)
add('street_ways.name', sw[:, 2])
pr = np.fromfile(f'{D}/poi_records.bin', np.uint32).reshape(-1, 9)
add('poi.name', pr[:, 4]); add('poi.parent_street', pr[:, 6]); add('poi.parent_postcode', pr[:, 7])
adm = np.fromfile(f'{D}/admin_polygons.bin', np.uint32).reshape(-1, 6)
add('admin.name', adm[:, 2])
pn = np.fromfile(f'{D}/place_nodes.bin', np.uint32).reshape(-1, 5)
add('place.name', pn[:, 2])
pcc = np.fromfile(f'{D}/postcode_centroids.bin', np.uint32).reshape(-1, 4)
add('postcode_centroids', pcc[:, 2])
r = np.unique(np.concatenate(refs))
print('distinct referenced offsets', len(r), 'all at string starts', bool(np.isin(r, starts).all()))
unref = ~np.isin(starts, r)
ends = np.concatenate([starts[1:], [len(sa)]])
print('unreferenced strings', int(unref.sum()), 'bytes', int((ends - starts)[unref].sum()))
