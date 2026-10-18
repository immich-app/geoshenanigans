#!/usr/bin/env python3
"""Tier B (format change, lossless, per-record / per-polygon random access).

encode <codec> <out>: from v_sorted (cell-ordered today's format) write
  addr_points.bin   columnar: lat f32 | lng f32 | housenumber u32 | street u32
                    | parent_way u32 | poly_offset u32 (NO_DATA = no footprint)
                    (vertex_count moved into the polygon blob)
  addr_vertices.bin one blob per footprint, record order:
     codec 'abs': tag u8 (enc | wide<<2 | short<<3 | closed<<4), count varint
                  (stored vertices), bmin (short: u16 point-bmin ulps each,
                  else f32 pair), vertices q_lat,q_lng at u8 (span<256) or u16
     codec 'abs2': tag u8 (enc | wide<<2 | small<<3 | closed<<4), count
                  varint, bmin: small -> i8 residual pair (ulps) against
                  f32(point - mean(q of the closed ring) * scale), else f32
                  pair; vertices split per polygon: all q_lat then all q_lng
                  at u8 (span<256) or u16
     codec 'var': tag u8 (enc | closed<<4), count varint, bmin as 2 zigzag
                  varints (ulps below the point), vertices as zigzag varint
                  residuals (pred 'seq' or 'pgram'), lat,lng interleaved
     closed rings drop the closing vertex (decoder re-appends the first one)
decode <codec> <out>: rebuild v_sorted's addr_points.bin + addr_vertices.bin
  from them and compare bytes with v_sorted."""
import os, sys, hashlib, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from codecs_addr import fkey, zz, unzz, varint, unvarint, interleave, Polys, NO

W = '/ssd/shrink/max/addresses'
S = f'{W}/v_sorted'
CH = 1 << 24  # records per chunk


def unfkey(k):
    k = k.astype(np.int64)
    return np.where(k & 0x80000000, k ^ 0x80000000, 0xFFFFFFFF - k).astype(np.uint32)


SCALE = np.array([1e-5, 1e-4, 1e-6])


def bmin_pred(p_bits, sum_q, n, enc):
    """f32(point - (sum_q / n) * scale) computed in f64, as the reader would."""
    mean = sum_q.astype(np.float64) / n.astype(np.float64)
    return (p_bits.view(np.float32).astype(np.float64) - mean * SCALE[enc]).astype(np.float32).view(np.uint32)


def bmin_residual(p):
    """stored bmin minus its prediction, in ulps (sums over the stored ring,
    closing vertex included)."""
    sl = np.add.reduceat(p.ql.astype(np.int64), p.vs); sg = np.add.reduceat(p.qg.astype(np.int64), p.vs)
    pl = bmin_pred(p.plat, sl, p.cnt, p.enc); pg = bmin_pred(p.plng, sg, p.cnt, p.enc)
    return fkey(p.bl) - fkey(pl), fkey(p.bg) - fkey(pg)


def enc_chunk(rec, blob, codec, pred):
    p = Polys(rec, blob)
    P = p.P
    encb = p.enc.astype(np.uint8)
    cntv, cntl = varint(p.ocnt)
    closed = p.closed.astype(np.uint8) << 4
    if codec == 'abs':
        short, bm, bml = p.hdr_bmin_delta()
        wa, pay, payl = p.abs_width()
        tag = (encb | ((wa == 2).astype(np.uint8) << 2) | (short.astype(np.uint8) << 3) | closed).astype(np.uint8)
        parts = [(tag, np.ones(P, np.int64)), (cntv, cntl), (bm, bml), (pay, payl)]
    elif codec == 'abs2':
        rl, rg = bmin_residual(p)
        small = (np.abs(rl) < 128) & (np.abs(rg) < 128)
        b8 = np.zeros((P, 8), np.uint8)
        b8[:, 0] = rl.astype(np.int8).view(np.uint8); b8[:, 1] = rg.astype(np.int8).view(np.uint8)
        raw = np.zeros((P, 8), np.uint8)
        raw[:, 0:4] = p.bl.view(np.uint8).reshape(P, 4); raw[:, 4:8] = p.bg.view(np.uint8).reshape(P, 4)
        b8 = np.where(small[:, None], b8, raw)
        bml = np.where(small, 2, 8)
        bm = b8[np.arange(8)[None, :] < bml[:, None]]
        wa, pay, payl = p.abs_width(split=True)
        tag = (encb | ((wa == 2).astype(np.uint8) << 2) | (small.astype(np.uint8) << 3) | closed).astype(np.uint8)
        parts = [(tag, np.ones(P, np.int64)), (cntv, cntl), (bm, bml), (pay, payl)]
    else:
        bm, bml = p.hdr_bmin_varint()
        pay, payl = p.varint_payload(pred)
        tag = (encb | closed).astype(np.uint8)
        parts = [(tag, np.ones(P, np.int64)), (cntv, cntl), (bm, bml), (pay, payl)]
    out, size = interleave(parts, P)
    return p.ri, out, size


def encode(codec, out, pred='seq'):
    os.makedirs(out, exist_ok=True)
    ap = np.memmap(f'{S}/addr_points.bin', np.uint32, 'r').reshape(-1, 7)
    av = np.memmap(f'{S}/addr_vertices.bin', np.uint8, 'r')
    N = len(ap)
    vo = np.full(N, NO, np.uint32)
    base = 0
    with open(f'{out}/addr_vertices.bin', 'wb') as fv:
        for s in range(0, N, CH):
            rec = np.array(ap[s:s + CH])
            pol = rec[:, 6] > 0
            if not pol.any():
                continue
            o0 = int(rec[pol, 5][0])
            last = np.nonzero(pol)[0][-1]
            o1 = int(rec[last, 5]) + 10 + 4 * int(rec[last, 6])
            blob = np.array(av[o0:o1])
            r = rec.copy(); r[pol, 5] -= o0
            ri, b, size = enc_chunk(r, blob, codec, pred)
            vo[s + ri] = (base + np.concatenate([[0], np.cumsum(size)[:-1]])).astype(np.uint32)
            assert base + int(size.sum()) < 2**32
            base += int(size.sum())
            b.tofile(fv)
            print('chunk', s, 'bytes', base, flush=True)
    with open(f'{out}/addr_points.bin', 'wb') as f:
        for k in range(5):
            np.ascontiguousarray(ap[:, k]).tofile(f)
        vo.tofile(f)
    print('encoded', codec, pred, 'points', os.path.getsize(f'{out}/addr_points.bin'),
          'vertices', os.path.getsize(f'{out}/addr_vertices.bin'))


def read_varint_at(s, pos):
    """varint at byte positions pos (<= 3 bytes) -> (value, length)."""
    b0 = s[pos].astype(np.int64)
    v = b0 & 0x7f; ln = np.ones(len(pos), np.int64)
    m = b0 >= 0x80
    b1 = np.zeros(len(pos), np.int64); b1[m] = s[pos[m] + 1]
    v[m] |= (b1[m] & 0x7f) << 7; ln[m] = 2
    m2 = m & (b1 >= 0x80)
    b2 = np.zeros(len(pos), np.int64); b2[m2] = s[pos[m2] + 2]
    assert np.all(b2 < 0x80)
    v[m2] |= b2[m2] << 14; ln[m2] = 3
    return v, ln


def decode(codec, out, pred='seq'):
    """Rebuild today's-format records + vertex blob and compare with v_sorted."""
    col = np.fromfile(f'{out}/addr_points.bin', np.uint32)
    N = len(col) // 6
    col = col.reshape(6, N)
    s = np.fromfile(f'{out}/addr_vertices.bin', np.uint8)
    vo = col[5]
    pi = np.nonzero(vo != NO)[0]
    st = vo[pi].astype(np.int64)
    P = len(pi)
    tag = s[st]
    enc = tag & 3
    closed = (tag >> 4) & 1
    cnt, cl = read_varint_at(s, st + 1)
    h = st + 1 + cl
    plat = col[0][pi]; plng = col[1][pi]
    if codec == 'abs':
        wide = (tag >> 2) & 1; short = (tag >> 3) & 1
        pos = h
        dl = (s[pos].astype(np.int64) | (s[pos + 1].astype(np.int64) << 8))
        dg = (s[pos + 2].astype(np.int64) | (s[pos + 3].astype(np.int64) << 8))
        rawl = np.zeros(P, np.uint32); rawg = np.zeros(P, np.uint32)
        nm = short == 0
        for k in range(4):
            rawl[nm] |= s[pos[nm] + k].astype(np.uint32) << np.uint32(8 * k)
            rawg[nm] |= s[pos[nm] + 4 + k].astype(np.uint32) << np.uint32(8 * k)
        bl = np.where(short == 1, unfkey(fkey(plat) - dl), rawl)
        bg = np.where(short == 1, unfkey(fkey(plng) - dg), rawg)
        vstart = h + np.where(short == 1, 4, 8)
        w = 1 + wide.astype(np.int64)
        V = int(cnt.sum())
        vs = np.concatenate([[0], np.cumsum(cnt)[:-1]])
        idx = np.arange(V) - np.repeat(vs, cnt)
        wv = np.repeat(w, cnt)
        base = np.repeat(vstart, cnt) + idx * 2 * wv
        ql = s[base].astype(np.int64); qg = s[base + wv].astype(np.int64)
        two = wv == 2
        ql[two] |= s[base[two] + 1].astype(np.int64) << 8
        qg[two] |= s[base[two] + 3].astype(np.int64) << 8
    elif codec == 'abs2':
        wide = (tag >> 2) & 1; small = (tag >> 3) & 1
        w = 1 + wide.astype(np.int64)
        vstart = h + np.where(small == 1, 2, 8)
        V = int(cnt.sum())
        vs = np.concatenate([[0], np.cumsum(cnt)[:-1]])
        idx = np.arange(V) - np.repeat(vs, cnt)
        wv = np.repeat(w, cnt); cv = np.repeat(cnt, cnt)
        bl_ = np.repeat(vstart, cnt) + idx * wv
        bg_ = np.repeat(vstart, cnt) + cv * wv + idx * wv
        ql = s[bl_].astype(np.int64); qg = s[bg_].astype(np.int64)
        two = wv == 2
        ql[two] |= s[bl_[two] + 1].astype(np.int64) << 8
        qg[two] |= s[bg_[two] + 1].astype(np.int64) << 8
        n = cnt + closed
        sl = np.add.reduceat(ql, vs) + closed * ql[vs]
        sg = np.add.reduceat(qg, vs) + closed * qg[vs]
        pl = bmin_pred(plat, sl, n, enc); pg = bmin_pred(plng, sg, n, enc)
        r8l = s[h].view(np.int8).astype(np.int64); r8g = s[h + 1].view(np.int8).astype(np.int64)
        rawl = np.zeros(P, np.uint32); rawg = np.zeros(P, np.uint32)
        nm = small == 0
        for k in range(4):
            rawl[nm] |= s[h[nm] + k].astype(np.uint32) << np.uint32(8 * k)
            rawg[nm] |= s[h[nm] + 4 + k].astype(np.uint32) << np.uint32(8 * k)
        bl = np.where(small == 1, unfkey(fkey(pl) + r8l), rawl)
        bg = np.where(small == 1, unfkey(fkey(pg) + r8g), rawg)
    else:
        # bmin: two zigzag varints right after the count
        d1, l1 = read_varint_at(s, h)
        d2, l2 = read_varint_at(s, h + l1)
        bl = unfkey(fkey(plat) - unzz(d1)); bg = unfkey(fkey(plng) - unzz(d2))
        pstart = h + l1 + l2
        pend = np.append(st[1:], len(s))
        plen = pend - pstart
        first = np.cumsum(plen) - plen
        g = np.repeat(pstart, plen) + (np.arange(int(plen.sum())) - np.repeat(first, plen))
        vals = unzz(unvarint(s[g]))
        V = int(cnt.sum())
        assert len(vals) == 2 * V
        rl, rg = vals[0::2], vals[1::2]
        vs = np.concatenate([[0], np.cumsum(cnt)[:-1]])
        idx = np.arange(V) - np.repeat(vs, cnt)
        def rebuild(r):
            if pred == 'seq':
                c = np.cumsum(r)
                base = np.repeat(np.concatenate([[0], c[vs[1:] - 1]]), cnt)
                return c - base
            q = np.empty(V, np.int64)
            # sequential within polygon; vectorize over polygons by vertex index
            byk = np.argsort(idx, kind='stable')
            kb = np.concatenate([[0], np.cumsum(np.bincount(idx))])
            for k in range(int(cnt.max())):
                m = byk[kb[k]:kb[k + 1]]
                if k == 0:
                    q[m] = r[m]
                elif k < 3:
                    q[m] = q[m - 1] + r[m]
                else:
                    q[m] = r[m] + q[m - 1] + q[m - 3] - q[m - 2]
            return q
        ql, qg = rebuild(rl), rebuild(rg)
    # today's format: header + all vertices + closing vertex
    full = cnt + closed
    size = 10 + 4 * full
    off = np.concatenate([[0], np.cumsum(size)[:-1]])
    blob = np.zeros(int(size.sum()), np.uint8)
    blob[off] = enc
    blob[(off[:, None] + 2 + np.arange(4)).ravel()] = bl.astype('<u4').view(np.uint8)
    blob[(off[:, None] + 6 + np.arange(4)).ravel()] = bg.astype('<u4').view(np.uint8)
    V = int(cnt.sum())
    vs = np.concatenate([[0], np.cumsum(cnt)[:-1]])
    idx = np.arange(V) - np.repeat(vs, cnt)
    vp = np.repeat(off + 10, cnt) + 4 * idx
    for qq, sh in ((ql, 0), (qg, 2)):
        assert qq.min() >= 0 and qq.max() < 65536
        blob[vp + sh] = qq & 0xff; blob[vp + sh + 1] = qq >> 8
    cv = off[closed == 1] + 10 + 4 * cnt[closed == 1]
    fv = off[closed == 1] + 10
    for k in range(4):
        blob[cv + k] = blob[fv + k]
    rec = np.empty((N, 7), np.uint32)
    for k in range(5):
        rec[:, k] = col[k]
    rec[:, 5] = NO; rec[:, 6] = 0
    rec[pi, 5] = off.astype(np.uint32); rec[pi, 6] = full.astype(np.uint32)
    ok_p = np.array_equal(rec, np.fromfile(f'{S}/addr_points.bin', np.uint32).reshape(-1, 7))
    ok_v = np.array_equal(blob, np.fromfile(f'{S}/addr_vertices.bin', np.uint8))
    print('decode', codec, pred, 'addr_points identical:', ok_p, ' addr_vertices identical:', ok_v)
    return ok_p and ok_v


if __name__ == '__main__':
    op, codec, out = sys.argv[1], sys.argv[2], sys.argv[3]
    pred = sys.argv[4] if len(sys.argv) > 4 else 'seq'
    (encode if op == 'encode' else decode)(codec, out, pred)
