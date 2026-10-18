#!/usr/bin/env python3
"""Decode admin_polygons.bin + admin_vertices.bin into numpy arrays.

Mirrors server/src/lib.rs decode_polygon_verts (POLY_HEADER_BYTES=10,
encodings 0/1/2 = u16 deltas from bbox_min at 1e-5/1e-4/1e-6, 3 = u32 at
1e-7, 4 = raw f32). Saves to decoded.npz:
  poly_*      per polygon (only slots with vertex_count > 0)
  q_lat,q_lng raw integer grid values as stored (int64)
  lat,lng     decoded f32 exactly as the reader produces them
  vpoly       polygon index per vertex
"""
import sys, numpy as np

D = sys.argv[1] if len(sys.argv) > 1 else '/ssd/shrink/idx/smallest'
OUT = sys.argv[2] if len(sys.argv) > 2 else 'decoded.npz'

NO_DATA = 0xFFFFFFFF
pdt = np.dtype([('vertex_offset', '<u4'), ('vertex_count', '<u4'), ('name_id', '<u4'),
                ('admin_level', 'u1'), ('pto', 'u1'), ('p2', 'u1'), ('p3', 'u1'),
                ('area', '<f4'), ('cc', '<u2'), ('p4', '<u2')])
polys = np.fromfile(f'{D}/admin_polygons.bin', dtype=pdt)
vb = np.fromfile(f'{D}/admin_vertices.bin', dtype=np.uint8)
live = (polys['vertex_count'] > 0) & (polys['vertex_offset'] != NO_DATA)
slot = np.nonzero(live)[0]
P = polys[live]
print('slots', len(polys), 'live', len(P), file=sys.stderr)

off = P['vertex_offset'].astype(np.int64)
cnt = P['vertex_count'].astype(np.int64)
enc = vb[off]
stride = np.where(enc <= 2, 4, 8)
# check back-to-back layout
order = np.argsort(off)
assert np.all(order == np.arange(len(off))), 'offsets not ascending'
end = off + 10 + cnt * stride
assert np.all(end[:-1] == off[1:]), 'gaps between polygons'
assert end[-1] == len(vb), f'file tail {end[-1]} vs {len(vb)}'
bmin_lat = vb[(off[:, None] + np.arange(2, 6)).ravel()].view('<f4')
bmin_lng = vb[(off[:, None] + np.arange(6, 10)).ravel()].view('<f4')

N = int(cnt.sum())
q_lat = np.empty(N, np.int64); q_lng = np.empty(N, np.int64)
vpoly = np.repeat(np.arange(len(P)), cnt)
vstart = np.concatenate([[0], np.cumsum(cnt)[:-1]])
for e, st, dt in ((0, 4, '<u2'), (1, 4, '<u2'), (2, 4, '<u2'), (3, 8, '<u4')):
    sel = np.nonzero(enc == e)[0]
    if len(sel) == 0:
        continue
    # gather bytes for these polygons
    bo = np.repeat(off[sel] + 10, cnt[sel]) + (np.arange(cnt[sel].sum()) - np.repeat(np.cumsum(cnt[sel]) - cnt[sel], cnt[sel])) * st
    vi = np.repeat(vstart[sel], cnt[sel]) + (np.arange(cnt[sel].sum()) - np.repeat(np.cumsum(cnt[sel]) - cnt[sel], cnt[sel]))
    h = st // 2
    raw = vb[(bo[:, None] + np.arange(st)).ravel()].reshape(-1, st)
    q_lat[vi] = raw[:, :h].copy().view(dt).ravel()
    q_lng[vi] = raw[:, h:].copy().view(dt).ravel()
assert not np.any(enc == 4), 'f32 encoding present'
scale = np.array([1e-5, 1e-4, 1e-6, 1e-7])[enc]
lat = (bmin_lat.astype(np.float64)[vpoly] + q_lat * scale[vpoly]).astype(np.float32)
lng = (bmin_lng.astype(np.float64)[vpoly] + q_lng * scale[vpoly]).astype(np.float32)
np.savez(OUT, slot=slot, level=P['admin_level'], cc=P['cc'], area=P['area'], name_id=P['name_id'],
         off=off, cnt=cnt, enc=enc, bmin_lat=bmin_lat, bmin_lng=bmin_lng,
         q_lat=q_lat, q_lng=q_lng, lat=lat, lng=lng, vpoly=vpoly, vstart=vstart)
print('vertices', N, 'bytes', len(vb), file=sys.stderr)
