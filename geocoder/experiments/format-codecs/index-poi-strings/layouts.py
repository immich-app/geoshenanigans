#!/usr/bin/env python3
"""Lossless transfer layouts for admin_vertices.bin and their decoders back to today's format.

usage: layouts.py encode <fmt> <set_dir> <out.bin>
       layouts.py decode <fmt> <set_dir> <in.bin> <out_admin_vertices.bin>
<set_dir>/admin_polygons.bin gives vertex_offset/vertex_count (no layout changes it); encode
also reads <set_dir>/admin_vertices.bin (today's format). decode reads ONLY the polygons file
and <in.bin>, so a byte-for-byte cmp of its output with today's file proves exact inversion.

Today's format (server/src/lib.rs decode_polygon_verts, builder vertex_pack.h): per polygon a
10-byte header (enc, pad, bbox_min lat f32, lng f32), then per vertex q_lat,q_lng as u16
(enc 0/1/2 = 1e-5/1e-4/1e-6 deg) or u32 (enc 3 = 1e-7 deg), polygons back to back.

fmt
  B   per-polygon block at the same offset and size as today: header verbatim, then wrapping
      zigzag deltas (first vertex absolute) at the polygon's own width, stored as byte planes
      inside the block: lat b0 | lat b1 [| b2 | b3] | lng b0 | lng b1 [| b2 | b3]
      (vertices track encode_local.py e3d_local_wrapdelta_planes)
  Bg  B + per-polygon divisor g in the header pad byte (largest divisor <= 255 of the gcd of
      the polygon's q values); planes hold deltas of q / g. Extra, not one of the four changes.
  A   vertices track encode_variant.py "gshuffle" at G = 1e-4 deg: one global grid, open rings,
      no per-polygon header, first vertex zigzag delta from the previous polygon's first vertex,
      u16 where the whole polygon fits else u32 (1 width byte per polygon), byte planes over the
      whole file. Decoding repacks with the builder's plan_polygon/pack_polygon rules.
  An  vertices track "shuffle": native grids, all headers in one block, wrapping zigzag deltas,
      byte planes over the whole file."""
import sys, time
import numpy as np

NO_DATA = 0xFFFFFFFF
H = 10
PDT = np.dtype([('vertex_offset', '<u4'), ('vertex_count', '<u4'), ('name_id', '<u4'), ('admin_level', 'u1'),
                ('pto', 'u1'), ('p2', 'u1'), ('p3', 'u1'), ('area', '<f4'), ('cc', '<u2'), ('p4', '<u2')])
SCALE = np.array([1e-5, 1e-4, 1e-6, 1e-7])
G = 1e-4


def polygons(d):
    p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
    live = (p['vertex_count'] > 0) & (p['vertex_offset'] != NO_DATA)
    off, cnt = p['vertex_offset'][live].astype(np.int64), p['vertex_count'][live].astype(np.int64)
    assert np.all(np.diff(off) > 0), "offsets not ascending"
    return off, cnt


def geom(cnt, w):
    vstart = np.concatenate([[0], np.cumsum(cnt)[:-1]])
    vpoly = np.repeat(np.arange(len(cnt)), cnt)
    k = np.arange(int(cnt.sum())) - vstart[vpoly]
    return vstart, vpoly, k, w[vpoly]


def width(enc):
    assert np.isin(enc, [0, 1, 2, 3]).all(), "unknown vertex encoding"
    return np.where(enc == 3, 4, 2).astype(np.int64)


def check_today_layout(off, cnt, w, n):
    end = off + H + cnt * 2 * w
    assert off[0] == 0 and np.array_equal(end[:-1], off[1:]) and end[-1] == n, "today's blocks not back to back"


def get_bytes(buf, pos, wv):
    """little-endian value of wv bytes at pos (vectorised)."""
    v = np.zeros(len(pos), np.int64)
    for b in range(4):
        s = b < wv
        v[s] |= buf[pos[s] + b].astype(np.int64) << (8 * b)
    return v


def put_planes(out, pos_of_byte, vals, wv):
    for b in range(4):
        s = b < wv
        out[pos_of_byte(b, s)] = ((vals[s] >> (8 * b)) & 0xFF).astype(np.uint8)


def read_today(vb, off, cnt):
    enc = vb[off].astype(np.int64)
    w = width(enc)
    check_today_layout(off, cnt, w, len(vb))
    vstart, vpoly, k, wv = geom(cnt, w)
    tb = off[vpoly] + H + k * 2 * wv
    q = [np.zeros(len(k), np.int64), np.zeros(len(k), np.int64)]
    for comp in range(2):
        for b in range(4):
            s = b < wv
            q[comp][s] |= vb[tb[s] + comp * wv[s] + b].astype(np.int64) << (8 * b)
    return enc, w, vstart, vpoly, k, wv, q


def write_today(out, off, k, vpoly, wv, q):
    tb = off[vpoly] + H + k * 2 * wv
    for comp in range(2):
        put_planes(out, lambda b, s: tb[s] + comp * wv[s] + b, q[comp], wv)


def zz(d):
    return np.where(d >= 0, 2 * d, -2 * d - 1)


def unzz(z):
    return np.where(z & 1, -((z + 1) >> 1), z >> 1)


def wrap_delta(q, vstart, wv):
    m = np.int64(1) << (8 * wv)
    d = np.diff(q, prepend=0)
    d[vstart] = q[vstart]
    d = np.mod(d, m)
    return zz(np.where(d >= m // 2, d - m, d))


def unwrap_delta(z, vstart, vpoly, wv):
    cs = np.cumsum(unzz(z))
    before = np.concatenate([[0], cs[vstart[1:] - 1]])
    return np.mod(cs - before[vpoly], np.int64(1) << (8 * wv))


def divisor(q, vstart):
    g = np.gcd(np.gcd.reduceat(q[0], vstart), np.gcd.reduceat(q[1], vstart))
    g[g == 0] = 1
    for v in np.unique(g[g > 255]):
        g[g == v] = max(x for x in range(1, 256) if v % x == 0)
    return g


# ---------------- B / Bg
def encode_B(vb, off, cnt, with_g):
    enc, w, vstart, vpoly, k, wv, q = read_today(vb, off, cnt)
    out = np.zeros_like(vb)
    hp = (off[:, None] + np.arange(H)).ravel()
    out[hp] = vb[hp]
    if with_g:
        assert np.all(vb[off + 1] == 0), "header pad byte in use"
        g = divisor(q, vstart)
        out[off + 1] = g
        gv = g[vpoly]
        assert all(np.all(x % gv == 0) for x in q)
        q = [x // gv for x in q]
        gu, gc = np.unique(g, return_counts=True)
        print("Bg most common divisors (g: polygons):", {int(a): int(b) for a, b in sorted(zip(gu, gc), key=lambda t: -t[1])[:8]})
    c = cnt[vpoly]
    base = off[vpoly] + H
    for comp in range(2):
        z = wrap_delta(q[comp], vstart, wv)
        put_planes(out, lambda b, s: base[s] + (comp * wv[s] + b) * c[s] + k[s], z, wv)
    return out


def decode_B(bb, off, cnt, with_g):
    enc = bb[off].astype(np.int64)
    w = width(enc)
    check_today_layout(off, cnt, w, len(bb))
    vstart, vpoly, k, wv = geom(cnt, w)
    out = np.zeros_like(bb)
    hp = (off[:, None] + np.arange(H)).ravel()
    out[hp] = bb[hp]
    c = cnt[vpoly]
    base = off[vpoly] + H
    q = []
    for comp in range(2):
        z = np.zeros(len(k), np.int64)
        for b in range(4):
            s = b < wv
            z[s] |= bb[base[s] + (comp * wv[s] + b) * c[s] + k[s]].astype(np.int64) << (8 * b)
        q.append(unwrap_delta(z, vstart, vpoly, wv))
    if with_g:
        g = bb[off + 1].astype(np.int64)
        out[off + 1] = 0
        q = [x * g[vpoly] for x in q]
    write_today(out, off, k, vpoly, wv, q)
    return out


# ---------------- An (native grids, whole-file planes)
def encode_An(vb, off, cnt):
    enc, w, vstart, vpoly, k, wv, q = read_today(vb, off, cnt)
    parts = [vb[(off[:, None] + np.arange(H)).ravel()]]
    z = [wrap_delta(x, vstart, wv) for x in q]
    for width_ in (2, 4):
        s = wv == width_
        for comp in range(2):
            for b in range(width_):
                parts.append(((z[comp][s] >> (8 * b)) & 0xFF).astype(np.uint8))
    return np.concatenate(parts)


def decode_An(ab, off, cnt):
    P = len(cnt)
    hdr = ab[:P * H].reshape(P, H)
    w = width(hdr[:, 0].astype(np.int64))
    vstart, vpoly, k, wv = geom(cnt, w)
    pos = P * H
    z = [np.zeros(len(k), np.int64), np.zeros(len(k), np.int64)]
    for width_ in (2, 4):
        s = wv == width_
        n = int(s.sum())
        for comp in range(2):
            for b in range(width_):
                z[comp][s] |= ab[pos:pos + n].astype(np.int64) << (8 * b)
                pos += n
    assert pos == len(ab)
    q = [unwrap_delta(x, vstart, vpoly, wv) for x in z]
    toff = np.concatenate([[0], np.cumsum(H + 2 * w * cnt)[:-1]])
    assert np.array_equal(toff, off), "derived offsets differ from admin_polygons"
    out = np.zeros(int(toff[-1] + H + 2 * w[-1] * cnt[-1]), np.uint8)
    out[(off[:, None] + np.arange(H)).ravel()] = hdr.ravel()
    write_today(out, off, k, vpoly, wv, q)
    return out


# ---------------- A (global 1e-4 grid, open rings, whole-file planes)
def encode_A(vb, off, cnt):
    enc, w, vstart, vpoly, k, wv, q = read_today(vb, off, cnt)
    bml = vb[(off[:, None] + np.arange(2, 6)).ravel()].view('<f4').astype(np.float64)
    bmg = vb[(off[:, None] + np.arange(6, 10)).ravel()].view('<f4').astype(np.float64)
    sc = SCALE[enc][vpoly]
    lat = (bml[vpoly] + q[0] * sc).astype(np.float32)     # what the reader returns
    lng = (bmg[vpoly] + q[1] * sc).astype(np.float32)
    A = [np.round(lat.astype(np.float64) / G).astype(np.int64), np.round(lng.astype(np.float64) / G).astype(np.int64)]
    assert cnt.min() >= 2, "A needs at least 2 vertices per ring"
    last = vstart + cnt - 1
    assert all(np.array_equal(x[last], x[vstart]) for x in A), "ring not closed on the grid"
    keep = np.ones(len(k), bool)
    keep[last] = False
    m = cnt - 1
    st = np.concatenate([[0], np.cumsum(m)[:-1]])
    d = []
    for x in A:
        x = x[keep]
        dd = np.diff(x, prepend=0)
        dd[st] = np.diff(x[st], prepend=0)
        d.append(dd)
    pk = np.repeat(np.arange(len(cnt)), m)
    big = np.zeros(len(cnt), bool)
    np.logical_or.at(big, pk, (np.abs(d[0]) > 32767) | (np.abs(d[1]) > 32767))
    bv = big[pk]
    parts = [big.astype(np.uint8)]
    for width_, s in ((2, ~bv), (4, bv)):
        for comp in range(2):
            zv = zz(d[comp][s])
            assert zv.max(initial=0) < (1 << (8 * width_))
            for b in range(width_):
                parts.append(((zv >> (8 * b)) & 0xFF).astype(np.uint8))
    return np.concatenate(parts)


def decode_A(ab, off, cnt):
    P = len(cnt)
    big = ab[:P].astype(bool)
    m = cnt - 1
    st = np.concatenate([[0], np.cumsum(m)[:-1]])
    pk = np.repeat(np.arange(P), m)
    bv = big[pk]
    pos = P
    z = [np.zeros(len(pk), np.int64), np.zeros(len(pk), np.int64)]
    for width_, s in ((2, ~bv), (4, bv)):
        n = int(s.sum())
        for comp in range(2):
            for b in range(width_):
                z[comp][s] |= ab[pos:pos + n].astype(np.int64) << (8 * b)
                pos += n
    assert pos == len(ab)
    vstart, vpoly, k, _ = geom(cnt, np.zeros(P, np.int64))
    pts = []
    for zc in z:
        d = unzz(zc)
        first = np.cumsum(d[st])                      # each polygon's first vertex
        d[st] = 0
        cs = np.cumsum(d)
        rel = cs - cs[st][pk]                         # offset from the polygon's first vertex
        x = first[pk] + rel
        full = np.empty(len(k), np.int64)
        open_pos = np.ones(len(k), bool)
        open_pos[vstart + cnt - 1] = False
        full[open_pos] = x
        full[vstart + cnt - 1] = first                # closing vertex = first
        pts.append(full.astype(np.float64) * G)       # == std::round(a / 1e-4) * 1e-4 in variant.cpp
    # builder plan_polygon + pack_polygon
    mn = [np.minimum.reduceat(p, vstart) for p in pts]
    mx = [np.maximum.reduceat(p, vstart) for p in pts]
    span = np.maximum(mx[0] - mn[0], mx[1] - mn[1])
    enc = np.full(P, 3, np.int64)
    enc[span < 65535.0 * 1e-4] = 1
    enc[span < 65535.0 * 1e-5] = 0
    enc[span < 65535.0 * 1e-6] = 2
    w = width(enc)
    wv = w[vpoly]
    sc = SCALE[enc][vpoly]
    q = [np.floor((pts[c] - mn[c][vpoly]) / sc + 0.5).astype(np.int64) for c in range(2)]   # lround, values >= 0
    toff = np.concatenate([[0], np.cumsum(H + 2 * w * cnt)[:-1]])
    assert np.array_equal(toff, off), "derived offsets differ from admin_polygons"
    out = np.zeros(int(toff[-1] + H + 2 * w[-1] * cnt[-1]), np.uint8)
    hdr = np.zeros((P, H), np.uint8)
    hdr[:, 0] = enc
    hdr[:, 2:6] = mn[0].astype(np.float32).view(np.uint8).reshape(-1, 4)
    hdr[:, 6:10] = mn[1].astype(np.float32).view(np.uint8).reshape(-1, 4)
    out[(off[:, None] + np.arange(H)).ravel()] = hdr.ravel()
    write_today(out, off, k, vpoly, wv, q)
    return out


ENC = {"B": lambda vb, o, c: encode_B(vb, o, c, False), "Bg": lambda vb, o, c: encode_B(vb, o, c, True),
       "A": encode_A, "An": encode_An}
DEC = {"B": lambda b, o, c: decode_B(b, o, c, False), "Bg": lambda b, o, c: decode_B(b, o, c, True),
       "A": decode_A, "An": decode_An}

if __name__ == "__main__":
    mode, fmt, d = sys.argv[1:4]
    off, cnt = polygons(d)
    t = time.time()
    if mode == "encode":
        out = ENC[fmt](np.fromfile(f"{d}/admin_vertices.bin", np.uint8), off, cnt)
        out.tofile(sys.argv[4])
    else:
        out = DEC[fmt](np.fromfile(sys.argv[4], np.uint8), off, cnt)
        out.tofile(sys.argv[5])
    print(f"{mode} {fmt}: {len(out):,} bytes, {time.time() - t:.1f}s (numpy, 1 thread)")
