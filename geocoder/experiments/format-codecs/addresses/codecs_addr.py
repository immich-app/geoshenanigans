"""Encoders (and decoders for the chosen ones) for addr_points / addr_vertices.

All on-disk encodings keep one contiguous blob per polygon (random access by
a per-record byte offset) and fixed-width per-record columns.
"""
import numpy as np

NO = np.uint32(0xFFFFFFFF)


# ------------------------------------------------------------------ helpers
def fkey(bits):
    """f32 bits -> int64 total-order key (monotone in the float value)."""
    u = bits.astype(np.int64)
    return np.where(u & 0x80000000, 0xFFFFFFFF - u, u ^ 0x80000000)


def zz(x):
    x = x.astype(np.int64)
    return np.where(x >= 0, 2 * x, -2 * x - 1).astype(np.uint64)


def unzz(u):
    u = u.astype(np.int64)
    return np.where(u & 1, -((u + 1) >> 1), u >> 1)


def varint(v):
    """LEB128 of uint64 array -> (bytes, length per value)."""
    v = v.astype(np.uint64)
    ln = np.ones(len(v), np.int64)
    t = v >> np.uint64(7)
    while np.any(t):
        ln += (t > 0); t >>= np.uint64(7)
    L = int(ln.max()) if len(v) else 1
    out = np.empty(int(ln.sum()), np.uint8)
    start = np.concatenate([[0], np.cumsum(ln)[:-1]])
    for k in range(L):
        m = ln > k
        b = ((v[m] >> np.uint64(7 * k)) & np.uint64(0x7f)).astype(np.uint8)
        b |= np.where(ln[m] > k + 1, 0x80, 0).astype(np.uint8)
        out[start[m] + k] = b
    return out, ln


def unvarint(b):
    term = b < 0x80
    vid = np.concatenate([[0], np.cumsum(term)[:-1]])
    first = np.concatenate([[0], np.nonzero(term)[0][:-1] + 1])
    k = np.arange(len(b)) - first[vid]
    v = np.zeros(int(term.sum()), np.uint64)
    np.add.at(v, vid, (b & 0x7f).astype(np.uint64) << (7 * k).astype(np.uint64))
    return v


def interleave(parts, P):
    """parts: list of (bytes, per_polygon_len). Concatenate per polygon:
    part0[p] part1[p] ... for p in 0..P. Returns (stream, per_polygon_size)."""
    lens = np.stack([np.asarray(l, np.int64) for _, l in parts])
    tot = lens.sum(0)
    start = np.concatenate([[0], np.cumsum(tot)[:-1]])
    out = np.empty(int(tot.sum()), np.uint8)
    inner = np.zeros(P, np.int64)
    for (b, l), ln in zip(parts, lens):
        if ln.sum() == 0:
            continue
        first = np.cumsum(ln) - ln
        pos = np.repeat(start + inner, ln) + (np.arange(int(ln.sum())) - np.repeat(first, ln))
        out[pos] = b
        inner += ln
    return out, tot


def u_bytes(v, width):
    return v.astype(f'<u{width}').view(np.uint8) if width in (1, 2, 4, 8) else None


# ------------------------------------------------------------------ parse
class Polys:
    """Today's addr_vertices blob parsed for the polygons of `rec` (records in
    order; rec[:,5] offsets into blob, rec[:,6] counts)."""

    def __init__(self, rec, blob):
        vc = rec[:, 6].astype(np.int64)
        self.ri = np.nonzero(vc > 0)[0]
        self.cnt = vc[self.ri]
        off = rec[self.ri, 5].astype(np.int64)
        P = self.P = len(self.ri)
        self.enc = blob[off]
        assert np.all(self.enc <= 2), 'u32/f32 encodings not handled'
        assert np.all(blob[off + 1] == 0)
        h = blob[(off[:, None] + np.arange(2, 10)).ravel()].reshape(P, 8)
        self.bl = h[:, 0:4].copy().view(np.uint32).ravel()
        self.bg = h[:, 4:8].copy().view(np.uint32).ravel()
        self.plat = rec[self.ri, 0]; self.plng = rec[self.ri, 1]
        V = int(self.cnt.sum())
        vs = np.concatenate([[0], np.cumsum(self.cnt)[:-1]])
        pos = np.repeat(off + 10, self.cnt) + 4 * (np.arange(V) - np.repeat(vs, self.cnt))
        self.ql = (blob[pos].astype(np.uint16) | (blob[pos + 1].astype(np.uint16) << 8))
        self.qg = (blob[pos + 2].astype(np.uint16) | (blob[pos + 3].astype(np.uint16) << 8))
        self.vs = vs
        self.vpoly = np.repeat(np.arange(P), self.cnt)
        last = vs + self.cnt - 1
        self.closed = (self.ql[vs] == self.ql[last]) & (self.qg[vs] == self.qg[last])
        # open ring (drop closing vertex) when closed
        keep = np.ones(V, bool); keep[last[self.closed]] = False
        self.keep = keep
        self.ocnt = self.cnt - self.closed

    def today(self):
        P = self.P
        hdr = np.zeros((P, 10), np.uint8)
        hdr[:, 0] = self.enc
        hdr[:, 2:6] = self.bl.view(np.uint8).reshape(P, 4)
        hdr[:, 6:10] = self.bg.view(np.uint8).reshape(P, 4)
        v = np.stack([self.ql, self.qg], 1).astype('<u2').view(np.uint8).ravel()
        return interleave([(hdr.ravel(), np.full(P, 10)), (v, 4 * self.cnt)], P)

    # ---- header pieces
    def hdr_bmin_delta(self):
        """bmin as point - bmin in ulps (always >= 0): u16 each when both fit,
        else escape to raw f32 pair. Returns (flag per polygon, bytes, lens)."""
        dl = fkey(self.plat) - fkey(self.bl)
        dg = fkey(self.plng) - fkey(self.bg)
        short = (dl >= 0) & (dg >= 0) & (dl < 65536) & (dg < 65536)
        P = self.P
        lens = np.where(short, 4, 8)
        b = np.zeros((P, 8), np.uint8)
        b[:, 0:2] = np.where(short, dl, 0).astype('<u2').view(np.uint8).reshape(P, 2)
        b[:, 2:4] = np.where(short, dg, 0).astype('<u2').view(np.uint8).reshape(P, 2)
        raw = np.zeros((P, 8), np.uint8)
        raw[:, 0:4] = self.bl.view(np.uint8).reshape(P, 4)
        raw[:, 4:8] = self.bg.view(np.uint8).reshape(P, 4)
        b = np.where(short[:, None], b, raw)
        m = np.arange(8)[None, :] < lens[:, None]
        return short, b[m], lens

    def hdr_bmin_varint(self):
        dl = fkey(self.plat) - fkey(self.bl)
        dg = fkey(self.plng) - fkey(self.bg)
        v = np.empty(2 * self.P, np.int64); v[0::2] = dl; v[1::2] = dg
        b, ln = varint(zz(v))
        return b, ln[0::2] + ln[1::2]

    def hdr_bmin_raw(self):
        P = self.P
        raw = np.zeros((P, 8), np.uint8)
        raw[:, 0:4] = self.bl.view(np.uint8).reshape(P, 4)
        raw[:, 4:8] = self.bg.view(np.uint8).reshape(P, 4)
        return raw.ravel(), np.full(P, 8)

    # ---- vertex payloads
    def abs_width(self, split=False):
        """open ring, absolute q, per-polygon width 1 (span<256) or 2 bytes.
        split: per polygon all lat then all lng."""
        k = self.keep
        ql, qg, vp = self.ql[k], self.qg[k], self.vpoly[k]
        span = np.maximum(np.maximum.reduceat(self.ql, self.vs), np.maximum.reduceat(self.qg, self.vs))
        w = np.where(span < 256, 1, 2)
        wv = w[vp]
        n = len(ql)
        out_len = 2 * wv
        if split:
            # per polygon: lat values then lng values, each at width w
            oc = self.ocnt
            vs = np.concatenate([[0], np.cumsum(oc)[:-1]])
            idx = np.arange(n) - np.repeat(vs, oc)
            pstart = np.concatenate([[0], np.cumsum(2 * oc * w)[:-1]])
            out = np.zeros(int((2 * oc * w).sum()), np.uint8)
            base_l = np.repeat(pstart, oc) + idx * wv
            base_g = np.repeat(pstart + oc * w, oc) + idx * wv
            out[base_l] = (ql & 0xff).astype(np.uint8); out[base_g] = (qg & 0xff).astype(np.uint8)
            two = wv == 2
            out[base_l[two] + 1] = (ql[two] >> 8).astype(np.uint8)
            out[base_g[two] + 1] = (qg[two] >> 8).astype(np.uint8)
            return w, out, 2 * oc * w
        pos = np.concatenate([[0], np.cumsum(out_len)[:-1]])
        out = np.zeros(int(out_len.sum()), np.uint8)
        out[pos] = (ql & 0xff).astype(np.uint8)
        two = wv == 2
        out[pos[two] + 1] = (ql[two] >> 8).astype(np.uint8)
        out[pos + wv] = (qg & 0xff).astype(np.uint8)
        out[pos[two] + 3] = (qg[two] >> 8).astype(np.uint8)
        return w, out, np.bincount(vp, weights=out_len, minlength=self.P).astype(np.int64)

    def residuals(self, pred='seq'):
        """open ring residuals: 'seq' = delta from previous vertex (first
        vertex absolute), 'pgram' = parallelogram v[k-1]+v[k-3]-v[k-2] for k>=3."""
        k = self.keep
        ql = self.ql[k].astype(np.int64); qg = self.qg[k].astype(np.int64)
        oc = self.ocnt
        vs = np.concatenate([[0], np.cumsum(oc)[:-1]])
        idx = np.arange(len(ql)) - np.repeat(vs, oc)
        def res(q):
            r = np.diff(q, prepend=0)
            r[idx == 0] = q[idx == 0]
            if pred == 'pgram':
                m = idx >= 3
                j = np.nonzero(m)[0]
                r[j] = q[j] - (q[j - 1] + q[j - 3] - q[j - 2])
            return r
        return res(ql), res(qg), idx, oc

    def varint_payload(self, pred='seq'):
        rl, rg, idx, oc = self.residuals(pred)
        v = np.empty(2 * len(rl), np.int64); v[0::2] = rl; v[1::2] = rg
        b, ln = varint(zz(v))
        vp = np.repeat(np.arange(self.P), 2 * oc)
        return b, np.bincount(vp, weights=ln, minlength=self.P).astype(np.int64)

    def fixed_res_payload(self, pred='seq', planes=True):
        """layout-B-like: zigzag residuals at a per-polygon width of 1, 2 or 4
        bytes; planes: per polygon lat byte0s | lat byte1s .. | lng byte0s ..;
        else interleaved per vertex (lat, lng)."""
        rl, rg, idx, oc = self.residuals(pred)
        zl, zg = zz(rl), zz(rg)
        vs = np.concatenate([[0], np.cumsum(oc)[:-1]])
        vp = np.repeat(np.arange(self.P), oc)
        mx = np.maximum(np.maximum.reduceat(zl, vs), np.maximum.reduceat(zg, vs))
        w = np.where(mx < 256, 1, np.where(mx < 65536, 2, 4))
        size = 2 * oc * w
        pstart = np.concatenate([[0], np.cumsum(size)[:-1]])
        out = np.zeros(int(size.sum()), np.uint8)
        wv = w[vp]; ocv = oc[vp]; ps = pstart[vp]
        for b in range(4):
            m = wv > b
            bl = ((zl[m] >> np.uint64(8 * b)) & np.uint64(0xff)).astype(np.uint8)
            bg = ((zg[m] >> np.uint64(8 * b)) & np.uint64(0xff)).astype(np.uint8)
            if planes:
                out[ps[m] + b * ocv[m] + idx[m]] = bl
                out[ps[m] + (wv[m] + b) * ocv[m] + idx[m]] = bg
            else:
                base = ps[m] + idx[m] * 2 * wv[m]
                out[base + b] = bl
                out[base + wv[m] + b] = bg
        return w, out, size
