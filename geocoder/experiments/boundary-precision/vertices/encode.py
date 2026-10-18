#!/usr/bin/env python3
"""Item 3: lossless re-encodings of admin_vertices.bin (same q values, same
bbox_min/encoding per polygon, so the reader decodes identical f32).

Writes enc/<name>.bin; compress.sh then zstd's each.
"""
import os, numpy as np
z = np.load('decoded.npz')
enc, cnt, vstart = z['enc'], z['cnt'], z['vstart']
q_lat, q_lng = z['q_lat'], z['q_lng']
bmin_lat, bmin_lng = z['bmin_lat'], z['bmin_lng']
P, N = len(cnt), len(q_lat)
os.makedirs('enc', exist_ok=True)
vpoly = z['vpoly']

def header_bytes(pad=True):
    h = np.zeros((P, 10 if pad else 9), np.uint8)
    h[:, 0] = enc
    o = 2 if pad else 1
    h[:, o:o+4] = bmin_lat.view(np.uint8).reshape(-1, 4)
    h[:, o+4:o+8] = bmin_lng.view(np.uint8).reshape(-1, 4)
    return h

def zz(x):
    x = x.astype(np.int64)
    return np.where(x >= 0, 2 * x, -2 * x - 1).astype(np.uint64)

def varint(v):
    """LEB128 of uint64 array -> (bytes, length per value)."""
    v = v.astype(np.uint64)
    ln = np.ones(len(v), np.int64)
    t = v >> np.uint64(7)
    while np.any(t):
        ln += (t > 0); t >>= np.uint64(7)
    L = int(ln.max())
    mat = np.zeros((len(v), L), np.uint8)
    for k in range(L):
        b = ((v >> np.uint64(7 * k)) & np.uint64(0x7f)).astype(np.uint8)
        b |= np.where(ln > k + 1, 0x80, 0).astype(np.uint8)
        mat[:, k] = b
    mask = np.arange(L)[None, :] < ln[:, None]
    return mat[mask], ln

def interleave_with_headers(hdr, stream, per_poly_len):
    """header_i + block_i for each polygon, blocks contiguous in stream."""
    H = hdr.shape[1]
    total = P * H + len(stream)
    out = np.empty(total, np.uint8)
    block_start = np.concatenate([[0], np.cumsum(per_poly_len)[:-1]])
    poly_out = block_start + H * np.arange(P)
    out[(poly_out[:, None] + np.arange(H)).ravel()] = hdr.ravel()
    byte_poly = np.repeat(np.arange(P), per_poly_len)
    out[np.arange(len(stream)) + H * (byte_poly + 1)] = stream
    return out

def seq_delta(q, keep):
    """delta from previous vertex in same polygon; first vertex absolute."""
    d = np.diff(q, prepend=0)
    d[vstart] = q[vstart]
    return d[keep]

def write(name, arr):
    arr.tofile(f'enc/{name}.bin')
    print(f'{name:40s} {len(arr):>11d} bytes  {len(arr)/N:.3f} B/vertex')

all_v = np.ones(N, bool)
open_v = all_v.copy(); open_v[vstart + cnt - 1] = False   # drop closing vertex
cnt_open = cnt - 1
wide = (enc == 3)[vpoly]

for tag, keep, c in (('closed', all_v, cnt), ('open', open_v, cnt_open)):
    # --- E1: zigzag LEB128 deltas, lat/lng interleaved per vertex, header inline
    dl, dg = seq_delta(q_lat, keep), seq_delta(q_lng, keep)
    vals = np.empty(2 * len(dl), np.uint64); vals[0::2] = zz(dl); vals[1::2] = zz(dg)
    s, ln = varint(vals)
    vp = np.repeat(np.arange(P), 2 * c)
    per_poly = np.bincount(vp, weights=ln, minlength=P).astype(np.int64)
    write(f'e1_varint_inter_{tag}', interleave_with_headers(header_bytes(), s, per_poly))

    # --- E2: separate streams: headers | lat varints | lng varints
    sl, _ = varint(zz(dl)); sg, _ = varint(zz(dg))
    write(f'e2_varint_split_{tag}', np.concatenate([header_bytes().ravel(), sl, sg]))

    # --- E4: second-order deltas (delta of delta), first two vertices as E1
    def dd(q):
        d = seq_delta(q, keep)
        # positions of each polygon's first/second kept vertex in kept array
        st = np.concatenate([[0], np.cumsum(c)[:-1]])
        d2 = np.diff(d, prepend=0)
        d2[st] = d[st]
        second = st + 1
        second = second[c > 1]
        d2[second] = d[second]
        return d2
    d2l, d2g = dd(q_lat), dd(q_lng)
    vals = np.empty(2 * len(d2l), np.uint64); vals[0::2] = zz(d2l); vals[1::2] = zz(d2g)
    s, ln = varint(vals)
    per_poly = np.bincount(vp, weights=ln, minlength=P).astype(np.int64)
    write(f'e4_dd_varint_inter_{tag}', interleave_with_headers(header_bytes(), s, per_poly))

# --- E3: fixed-width (current stride) variants, byte-plane shuffled
def planes_u16(vals16):
    b = vals16.astype('<u2').view(np.uint8).reshape(-1, 2)
    return [b[:, 0], b[:, 1]]
def planes_u32(vals32):
    b = vals32.astype('<u4').view(np.uint8).reshape(-1, 4)
    return [b[:, k] for k in range(4)]

# E3a: today's absolute values, shuffled: headers | u16 lat lo,hi | u16 lng lo,hi | u32 lat b0..3 | u32 lng b0..3
n16 = ~wide
parts = [header_bytes().ravel()]
parts += planes_u16(q_lat[n16]) + planes_u16(q_lng[n16]) + planes_u32(q_lat[wide]) + planes_u32(q_lng[wide])
write('e3a_abs_shuffle', np.concatenate(parts))

# E3b: wrapping deltas (mod 2^16 / 2^32) zigzagged, same fixed width, interleaved like today
def wrap_delta(q, bits):
    d = np.diff(q, prepend=0); d[vstart] = q[vstart]
    m = 1 << bits
    d = np.mod(d, m); d = np.where(d >= m // 2, d - m, d)   # signed
    return zz(d)
dl16, dg16 = wrap_delta(q_lat, 16), wrap_delta(q_lng, 16)
dl32, dg32 = wrap_delta(q_lat, 32), wrap_delta(q_lng, 32)
dl = np.where(wide, dl32, dl16); dg = np.where(wide, dg32, dg16)
# E3b interleaved per vertex, header inline, fixed stride (4 or 8)
per_vertex = []
b16 = np.empty((n16.sum(), 4), np.uint8)
b16[:, 0:2] = dl[n16].astype('<u2').view(np.uint8).reshape(-1, 2)
b16[:, 2:4] = dg[n16].astype('<u2').view(np.uint8).reshape(-1, 2)
b32 = np.empty((wide.sum(), 8), np.uint8)
b32[:, 0:4] = dl[wide].astype('<u4').view(np.uint8).reshape(-1, 4)
b32[:, 4:8] = dg[wide].astype('<u4').view(np.uint8).reshape(-1, 4)
stride = np.where(wide, 8, 4)
vbytes = np.empty(int(stride.sum()), np.uint8)
vpos = np.concatenate([[0], np.cumsum(stride)[:-1]])
vbytes[(vpos[n16][:, None] + np.arange(4)).ravel()] = b16.ravel()
vbytes[(vpos[wide][:, None] + np.arange(8)).ravel()] = b32.ravel()
per_poly = np.bincount(vpoly, weights=stride, minlength=P).astype(np.int64)
write('e3b_wrapdelta_fixed_inter', interleave_with_headers(header_bytes(), vbytes, per_poly))
# E3c: same wrapping deltas, byte-plane shuffled, split lat/lng
parts = [header_bytes().ravel()]
parts += planes_u16(dl[n16]) + planes_u16(dg[n16]) + planes_u32(dl[wide]) + planes_u32(dg[wide])
write('e3c_wrapdelta_shuffle', np.concatenate(parts))

# --- round-trip check: decode the E2 'closed' lat stream back to q_lat
def unvarint(b):
    term = b < 0x80
    vid = np.concatenate([[0], np.cumsum(term)[:-1]])
    first = np.concatenate([[0], np.nonzero(term)[0][:-1] + 1])
    k = np.arange(len(b)) - first[vid]
    v = np.zeros(int(term.sum()), np.uint64)
    np.add.at(v, vid, (b & 0x7f).astype(np.uint64) << (7 * k).astype(np.uint64))
    return v
def unzz(u):
    u = u.astype(np.int64)
    return np.where(u & 1, -((u + 1) >> 1), u >> 1)
dlc = seq_delta(q_lat, all_v)
sl, _ = varint(zz(dlc))
back = unzz(unvarint(sl))
assert np.array_equal(back, dlc)
grp = np.repeat(np.arange(P), cnt)
cs = np.cumsum(back)
base = np.concatenate([[0], cs[vstart[1:] - 1]])
rec = cs - base[grp]
assert np.array_equal(rec, q_lat), 'roundtrip failed'
print('roundtrip ok: varint zigzag deltas reproduce q_lat exactly')
