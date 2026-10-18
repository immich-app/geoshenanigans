#!/usr/bin/env python3
"""addr_vertices facts: closure, spans, delta widths, header predictability,
duplicate polygon blobs. Saves decoded arrays to poly.npz for the encoders."""
import numpy as np

D = '/ssd/shrink/idx/max'
W = '/ssd/shrink/max/addresses'
NO = np.uint32(0xFFFFFFFF)

ap = np.fromfile(f'{D}/addr_points.bin', np.uint32).reshape(-1, 7)
av = np.fromfile(f'{D}/addr_vertices.bin', np.uint8)
vc_all = ap[:, 6]
pi = np.nonzero(vc_all > 0)[0]
off = ap[pi, 5].astype(np.int64)
cnt = vc_all[pi].astype(np.int64)
P = len(pi)
enc = av[off]
assert np.all(enc <= 2)
hdr = av[(off[:, None] + np.arange(10)).ravel()].reshape(P, 10)
bmin_lat_bits = hdr[:, 2:6].copy().view(np.uint32).ravel()
bmin_lng_bits = hdr[:, 6:10].copy().view(np.uint32).ravel()

mask = np.ones(len(av), bool)
mask[(off[:, None] + np.arange(10)).ravel()] = False
vb = av[mask]
q = vb.view('<u2').reshape(-1, 2)
q_lat = q[:, 0].copy(); q_lng = q[:, 1].copy()
V = len(q_lat)
assert V == cnt.sum()
vstart = np.concatenate([[0], np.cumsum(cnt)[:-1]])
vend = vstart + cnt - 1
vpoly = np.repeat(np.arange(P), cnt)
print('polygons', P, 'vertices', V, 'mean', V / P)
print('enc counts', np.bincount(enc))

closed = (q_lat[vstart] == q_lat[vend]) & (q_lng[vstart] == q_lng[vend])
print('closed rings', int(closed.sum()), 'open', int((~closed).sum()), 'count<4', int((cnt < 4).sum()))
mnl = np.minimum.reduceat(q_lat, vstart); mng = np.minimum.reduceat(q_lng, vstart)
mxl = np.maximum.reduceat(q_lat, vstart); mxg = np.maximum.reduceat(q_lng, vstart)
print('min q_lat==0', int((mnl == 0).sum()), 'min q_lng==0', int((mng == 0).sum()), 'of', P)
span = np.maximum(mxl, mxg)
for b in (16, 64, 128, 256, 512, 1024, 4096, 65536):
    print(f'  span < {b:6d}: {np.mean(span < b):.4f}')

dl = np.diff(q_lat.astype(np.int32), prepend=0); dl[vstart] = q_lat[vstart]
dg = np.diff(q_lng.astype(np.int32), prepend=0); dg[vstart] = q_lng[vstart]
inner = np.ones(V, bool); inner[vstart] = False
m = np.maximum(np.abs(dl[inner]), np.abs(dg[inner]))
for b in (8, 16, 64, 128, 256, 1024):
    print(f'  |seq delta| < {b:5d}: {np.mean(m < b):.4f}')
pm = np.maximum.reduceat(np.maximum(np.abs(dl), np.abs(dg)) * inner, vstart)
for b in (8, 64, 128, 2048, 32768):
    print(f'  polygon max |delta| < {b:5d}: {np.mean(pm < b):.4f}')

# header vs the addr point's own lat/lng
def fbits(u):  # IEEE total-order key
    u = u.astype(np.int64)
    return np.where(u & 0x80000000, 0xFFFFFFFF - u, u ^ 0x80000000)
plat = ap[pi, 0]; plng = ap[pi, 1]
d_lat = fbits(bmin_lat_bits) - fbits(plat)
d_lng = fbits(bmin_lng_bits) - fbits(plng)
for name, d in (('bmin_lat - point.lat (ulps)', d_lat), ('bmin_lng - point.lng (ulps)', d_lng)):
    a = np.abs(d)
    print(name, 'pct', np.percentile(a, [50, 90, 99, 99.9, 100]).astype(np.int64),
          'fits i8', np.mean(a < 128), 'i16', np.mean(a < 32768), '>0 count', int((d > 0).sum()))

# duplicate polygon blobs (whole header + vertices)
blob_hash = np.zeros(P, np.uint64)
h = np.uint64(1469598103934665603)
# polynomial hash over per-vertex u32 words, per polygon
w = (q_lat.astype(np.uint64) << np.uint64(16)) | q_lng.astype(np.uint64)
mult = np.uint64(1099511628211)
pos_w = (np.arange(V) - vstart[vpoly]).astype(np.uint64)
mix = (w + np.uint64(0x9E3779B97F4A7C15) * (pos_w + np.uint64(1))) * mult
mix ^= mix >> np.uint64(29)
s = np.add.reduceat(mix, vstart)
s ^= (bmin_lat_bits.astype(np.uint64) << np.uint64(32)) | bmin_lng_bits.astype(np.uint64)
s ^= (cnt.astype(np.uint64) << np.uint64(48)) ^ enc.astype(np.uint64)
u, inv, c = np.unique(s, return_inverse=True, return_counts=True)
dup_polys = P - len(u)
dup_bytes = int(((c - 1) * np.bincount(inv, weights=10 + 4 * cnt) / c).sum())
print('duplicate polygon blobs (hash)', dup_polys, 'bytes saved if shared', dup_bytes)

np.savez(f'{W}/poly.npz', pi=pi, cnt=cnt, enc=enc, bmin_lat_bits=bmin_lat_bits, bmin_lng_bits=bmin_lng_bits,
         q_lat=q_lat, q_lng=q_lng, closed=closed, hash=s)
print('saved')
