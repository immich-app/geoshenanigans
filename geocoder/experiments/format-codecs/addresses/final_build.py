#!/usr/bin/env python3
"""Final whole-file variants built from tb/abs2 (col6 points + abs2 footprints),
each checked to invert exactly, written to final/<name>.bin one at a time:

  col      on-disk: lat as 4 byte planes | lng 4 planes | hn | st | pw |
           poly_offset as 4 byte planes (per-record random access: 4 single-
           byte reads per planed field)
  pax170   on-disk: blocks of 170 records (4080 B, one page): inside a block
           the same layout as col (lat planes, lng planes, hn, st, pw, vo planes)
  dlpts    download-only: lat, lng as zigzag deltas of the f32 order key (5 byte
           planes) | hn | st | pw | has-footprint bitmap (poly_offset rebuilt
           at install from the footprint sizes)
  dlverts  download-only footprints: tags | counts | bmin bytes | u8 lat | u8 lng
           | u16 lat lo | hi | u16 lng lo | hi (abs2 fields regrouped)
usage: final_build.py <name>"""
import os, sys, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from codecs_addr import fkey, zz, unzz, NO

W = '/ssd/shrink/max/addresses'
O = f'{W}/final'
os.makedirs(O, exist_ok=True)
name = sys.argv[1]
col = np.fromfile(f'{W}/tb/abs2/addr_points.bin', np.uint32)
N = len(col) // 6
col = col.reshape(6, N)


def planes(v):
    return v.view(np.uint8).reshape(-1, 4).T


def unplanes(p):
    return np.ascontiguousarray(p.T).view(np.uint32).ravel()


def unfkey(k):
    k = k.astype(np.int64)
    return np.where(k & 0x80000000, k ^ 0x80000000, 0xFFFFFFFF - k).astype(np.uint32)


if name == 'col':
    out = np.concatenate([planes(col[0]).ravel(), planes(col[1]).ravel(),
                          col[2].view(np.uint8), col[3].view(np.uint8), col[4].view(np.uint8),
                          planes(col[5]).ravel()])
    out.tofile(f'{O}/col.bin')
    b = out
    s = 4 * N
    back = [unplanes(b[0:s].reshape(4, N)), unplanes(b[s:2 * s].reshape(4, N))]
    back += [b[(2 + k) * s:(3 + k) * s].view(np.uint32) for k in range(3)]
    back += [unplanes(b[5 * s:6 * s].reshape(4, N))]
    assert all(np.array_equal(back[k], col[k]) for k in range(6))
elif name == 'pax170':
    B = 170
    nb = -(-N // B)
    pad = np.zeros((6, nb * B), np.uint32); pad[:, :N] = col
    b8 = pad.view(np.uint8).reshape(6, nb, B, 4)
    blk = np.concatenate([b8[0].transpose(0, 2, 1).reshape(nb, -1), b8[1].transpose(0, 2, 1).reshape(nb, -1),
                          b8[2].reshape(nb, -1), b8[3].reshape(nb, -1), b8[4].reshape(nb, -1),
                          b8[5].transpose(0, 2, 1).reshape(nb, -1)], 1)
    assert blk.shape[1] == 24 * B
    blk.tofile(f'{O}/pax170.bin')
    # random-access inverse: record i, field k
    i = np.arange(N); blo = i // B; j = i % B
    base = blo * 24 * B
    def planed(off):  # 4 planes of B bytes at block offset off
        return (blk.ravel()[base + off + j].astype(np.uint32) | (blk.ravel()[base + off + B + j].astype(np.uint32) << 8) |
                (blk.ravel()[base + off + 2 * B + j].astype(np.uint32) << 16) | (blk.ravel()[base + off + 3 * B + j].astype(np.uint32) << 24))
    flat = blk.ravel()
    def plain(off):
        p = base + off + 4 * j
        return flat[p].astype(np.uint32) | (flat[p + 1].astype(np.uint32) << 8) | (flat[p + 2].astype(np.uint32) << 16) | (flat[p + 3].astype(np.uint32) << 24)
    back = [planed(0), planed(4 * B), plain(8 * B), plain(12 * B), plain(16 * B), planed(20 * B)]
    assert all(np.array_equal(back[k], col[k]) for k in range(6))
elif name == 'dlpts':
    parts = []
    for k in range(2):
        z = zz(np.diff(fkey(col[k]), prepend=0))
        assert z.max() < 2**40
        parts.append(z.astype('<u8').view(np.uint8).reshape(N, 8)[:, :5].T.ravel())
    parts += [col[2].view(np.uint8), col[3].view(np.uint8), col[4].view(np.uint8)]
    has = col[5] != NO
    parts.append(np.packbits(has))
    out = np.concatenate(parts)
    out.tofile(f'{O}/dlpts.bin')
    # inverse: lat/lng; poly_offset from the footprint sizes in abs2
    s5 = 5 * N
    for k in range(2):
        z = out[k * s5:(k + 1) * s5].reshape(5, N).T
        zz8 = np.zeros((N, 8), np.uint8); zz8[:, :5] = z
        v = np.cumsum(unzz(zz8.view('<u8').ravel()))
        assert np.array_equal(unfkey(v), col[k])
    has2 = np.unpackbits(out[2 * s5 + 12 * N:])[:N].astype(bool)
    assert np.array_equal(has2, has)
    s = np.fromfile(f'{W}/tb/abs2/addr_vertices.bin', np.uint8)
    # walk footprints in order: size = 1 + len(varint count) + (2|8) + 2*count*width
    # (vectorised by trusting starts; check against stored offsets)
    st = col[5][has].astype(np.int64)
    tag = s[st]
    b0 = s[st + 1].astype(np.int64); b1 = s[st + 2].astype(np.int64)
    two = b0 >= 0x80
    cnt = np.where(two, (b0 & 0x7f) | (b1 << 7), b0); cl = np.where(two, 2, 1)
    assert np.all(~two | (b1 < 0x80))
    size = 1 + cl + np.where((tag >> 3) & 1, 2, 8) + 2 * cnt * (1 + ((tag >> 2) & 1))
    rebuilt = np.concatenate([[0], np.cumsum(size)[:-1]])
    assert np.array_equal(rebuilt, st) and int(rebuilt[-1] + size[-1]) == len(s)
elif name == 'dlverts':
    s = np.fromfile(f'{W}/tb/abs2/addr_vertices.bin', np.uint8)
    st = col[5][col[5] != NO].astype(np.int64)
    P = len(st)
    tag = s[st]
    b0 = s[st + 1].astype(np.int64); b1 = s[st + 2].astype(np.int64)
    two = b0 >= 0x80
    cnt = np.where(two, (b0 & 0x7f) | (b1 << 7), b0); cl = np.where(two, 2, 1)
    bml = np.where((tag >> 3) & 1, 2, 8)
    wide = ((tag >> 2) & 1).astype(bool)
    def gather(start, ln):
        first = np.cumsum(ln) - ln
        return s[np.repeat(start, ln) + (np.arange(int(ln.sum())) - np.repeat(first, ln))]
    cntb = gather(st + 1, cl)
    bmb = gather(st + 1 + cl, bml)
    pay0 = st + 1 + cl + bml
    nar = ~wide
    u8lat = gather(pay0[nar], cnt[nar]); u8lng = gather(pay0[nar] + cnt[nar], cnt[nar])
    wl = gather(pay0[wide], 2 * cnt[wide]); wg = gather(pay0[wide] + 2 * cnt[wide], 2 * cnt[wide])
    out = np.concatenate([tag, cntb, bmb, u8lat, u8lng, wl[0::2], wl[1::2], wg[0::2], wg[1::2]])
    out.tofile(f'{O}/dlverts.bin')
    # inverse: re-interleave per footprint from the streams and the tags/counts
    assert len(out) == len(s)
    rb = np.empty(len(s), np.uint8)
    rb[st] = tag
    def scatter(start, ln, vals):
        first = np.cumsum(ln) - ln
        rb[np.repeat(start, ln) + (np.arange(int(ln.sum())) - np.repeat(first, ln))] = vals
    scatter(st + 1, cl, cntb); scatter(st + 1 + cl, bml, bmb)
    scatter(pay0[nar], cnt[nar], u8lat); scatter(pay0[nar] + cnt[nar], cnt[nar], u8lng)
    wl2 = np.empty(len(wl), np.uint8); wl2[0::2] = out[len(out) - len(wg) - len(wl):len(out) - len(wg) - len(wl) // 2]
    wl2[1::2] = out[len(out) - len(wg) - len(wl) // 2:len(out) - len(wg)]
    wg2 = np.empty(len(wg), np.uint8); wg2[0::2] = out[len(out) - len(wg):len(out) - len(wg) // 2]
    wg2[1::2] = out[len(out) - len(wg) // 2:]
    scatter(pay0[wide], 2 * cnt[wide], wl2); scatter(pay0[wide] + 2 * cnt[wide], 2 * cnt[wide], wg2)
    assert np.array_equal(rb, s)
print(name, 'written and inverted exactly', os.path.getsize(f'{O}/{name}.bin'))
