#!/usr/bin/env python3
"""Structural facts about the street / interp files (today's format).

Usage: stats.py <idx dir>
Prints: record counts, offset contiguity, padding, tombstones, dead records
(cnt<2, unreferenced by any cell entry), f32 specials, delta magnitudes.
"""
import sys, numpy as np

D = sys.argv[1]
WAY = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad', '<u2'), ('name', '<u4')])
IW = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad1', '<u2'), ('street', '<u4'),
               ('start', '<u4'), ('end', '<u4'), ('interp', 'u1'), ('pad4', 'u1', 3)])
NO_DATA = 0xFFFFFFFF


def entries_ids(path, cells_offsets):
    """All ids referenced from cell offsets (u16 count + u32 ids)."""
    b = np.fromfile(path, np.uint8)
    offs = np.unique(cells_offsets[cells_offsets != NO_DATA]).astype(np.int64)
    cnt = b[offs].astype(np.int64) | (b[offs + 1].astype(np.int64) << 8)
    tot = int(cnt.sum())
    # byte positions of each id
    rep = np.repeat(offs + 2, cnt)
    k = np.arange(tot) - np.repeat(np.cumsum(cnt) - cnt, cnt)
    pos = rep + 4 * k
    ids = (b[pos].astype(np.uint32) | (b[pos + 1].astype(np.uint32) << 8)
           | (b[pos + 2].astype(np.uint32) << 16) | (b[pos + 3].astype(np.uint32) << 24))
    covered = 2 * len(offs) + 4 * tot
    return ids, cnt, offs, len(b), covered


def orderable(bits):
    bits = bits.astype(np.uint32)
    neg = (bits >> 31).astype(bool)
    return np.where(neg, ~bits, bits | np.uint32(0x80000000)).astype(np.int64)


def describe_nodes(tag, nodes_bits, off, cnt):
    N = len(nodes_bits)
    f = nodes_bits.view(np.float32)
    print(f'[{tag}] nodes {N}')
    for j, nm in ((0, 'lat'), (1, 'lng')):
        v = f[:, j]
        print(f'  {nm}: nan {np.isnan(v).sum()} inf {np.isinf(v).sum()} '
              f'-0.0 {(nodes_bits[:, j] == 0x80000000).sum()} +0.0 {(nodes_bits[:, j] == 0).sum()} '
              f'min {v.min()} max {v.max()} |v|<0.5 {(np.abs(v) < 0.5).sum()}')
    live = cnt > 0
    first = off[live].astype(np.int64)
    k = orderable(nodes_bits)
    d = np.diff(k, axis=0, prepend=k[:1])
    isfirst = np.zeros(N, bool); isfirst[first] = True
    dd = d[~isfirst]
    for j, nm in ((0, 'lat'), (1, 'lng')):
        a = np.abs(dd[:, j])
        z = 2 * a  # zigzag magnitude
        bl = np.zeros(len(z), np.int64)
        nz = z > 0
        bl[nz] = np.floor(np.log2(z[nz])).astype(np.int64) + 1
        h = np.bincount(bl, minlength=40)
        tot = h.sum()
        print(f'  {nm} delta(ulp) zigzag bitlen pct: ' +
              ' '.join(f'{i}:{100*h[i]/tot:.1f}' for i in range(33) if h[i]))
        print(f'  {nm} zz<128 {100*(z<128).mean():.1f}% <32768 {100*(z<32768).mean():.1f}% '
              f'<2^23 {100*(z<(1<<23)).mean():.2f}% max {a.max()}')
    zero = (dd[:, 0] == 0) & (dd[:, 1] == 0)
    print(f'  consecutive identical nodes within a way: {zero.sum()}')


def main():
    # ---------------- streets
    ways = np.fromfile(f'{D}/street_ways.bin', WAY)
    nodes = np.fromfile(f'{D}/street_nodes.bin', '<u4').reshape(-1, 2)
    W, N = len(ways), len(nodes)
    print(f'street ways {W} nodes {N}')
    off, cnt = ways['off'].astype(np.int64), ways['cnt'].astype(np.int64)
    exp = np.concatenate([[0], np.cumsum(cnt)[:-1]])
    print(f'  offsets == prefix sum of counts: {np.array_equal(off, exp)}; '
          f'sum(cnt)==N: {cnt.sum() == N}; mismatches {(off != exp).sum()}')
    print(f'  pad nonzero {(ways["pad"] != 0).sum()}')
    print(f'  cnt==0 {(cnt == 0).sum()}  cnt==1 {(cnt == 1).sum()}  cnt<2 nodes {cnt[cnt < 2].sum()} '
          f'cnt max {cnt.max()} cnt==65535 {(cnt == 65535).sum()} mean {cnt.mean():.2f}')
    print(f'  name NO_DATA {(ways["name"] == NO_DATA).sum()} distinct names {len(np.unique(ways["name"]))}')
    h = np.bincount(np.minimum(cnt, 64))
    print('  cnt hist (<=64): ' + ' '.join(f'{i}:{h[i]}' for i in range(len(h)) if h[i]))

    gc = np.fromfile(f'{D}/geo_cells.bin', np.uint8).reshape(-1, 20)
    st_off = gc[:, 8:12].copy().view('<u4').ravel()
    ad_off = gc[:, 12:16].copy().view('<u4').ravel()
    ip_off = gc[:, 16:20].copy().view('<u4').ravel()
    print(f'geo_cells {len(gc)}  street NO_DATA {(st_off == NO_DATA).sum()} interp NO_DATA {(ip_off == NO_DATA).sum()}')
    ids, ecnt, eoffs, elen, cov = entries_ids(f'{D}/street_entries.bin', st_off)
    print(f'street_entries bytes {elen} covered-by-distinct-cell-offsets {cov} lists {len(eoffs)} '
          f'ids {len(ids)} max list {ecnt.max()} shared lists {(st_off != NO_DATA).sum() - len(eoffs)}')
    srt = True
    starts = np.cumsum(ecnt) - ecnt
    dif = np.diff(ids.astype(np.int64))
    inlist = np.ones(len(ids), bool); inlist[starts] = False
    print(f'  ids sorted within each list: {(dif[inlist[1:]] > 0).all()}  dup-in-list {(dif[inlist[1:]] == 0).sum()}')
    ref = np.zeros(W, bool); ref[ids[ids < W]] = True
    print(f'  ids >= W: {(ids >= W).sum()}  ways referenced {ref.sum()}  unreferenced {(~ref).sum()} '
          f'(their nodes {cnt[~ref].sum()})  entries pointing at cnt<2 ways {(cnt[ids[ids < W]] < 2).sum()}')
    print(f'  unreferenced & cnt>=2: {((~ref) & (cnt >= 2)).sum()}')
    describe_nodes('street', nodes, off, cnt)
    del nodes

    # ---------------- interps
    iw = np.fromfile(f'{D}/interp_ways.bin', IW)
    inodes = np.fromfile(f'{D}/interp_nodes.bin', '<u4').reshape(-1, 2)
    I, M = len(iw), len(inodes)
    print(f'interp ways {I} nodes {M}')
    off, cnt = iw['off'].astype(np.int64), iw['cnt'].astype(np.int64)
    exp = np.concatenate([[0], np.cumsum(cnt)[:-1]])
    print(f'  offsets == prefix sum: {np.array_equal(off, exp)} sum==M {cnt.sum() == M}')
    print(f'  pad1 nonzero {(iw["pad1"] != 0).sum()} pad4 nonzero {(iw["pad4"] != 0).any(axis=1).sum()}')
    print(f'  cnt==0 {(cnt == 0).sum()} cnt==1 {(cnt == 1).sum()} max {cnt.max()} mean {cnt.mean():.2f}')
    h = np.bincount(np.minimum(cnt, 64))
    print('  cnt hist (<=64): ' + ' '.join(f'{i}:{h[i]}' for i in range(len(h)) if h[i]))
    print(f'  interpolation values: {dict(zip(*np.unique(iw["interp"], return_counts=True)))}')
    print(f'  street NO_DATA {(iw["street"] == NO_DATA).sum()} distinct streets {len(np.unique(iw["street"]))}')
    s, e = iw['start'].astype(np.int64), iw['end'].astype(np.int64)
    print(f'  start max {s.max()} end max {e.max()} start>end {(s > e).sum()} start==end {(s == e).sum()} '
          f'start<2^16 {(s < 65536).mean():.4f} end-start<2^16 {((e - s) >= 0).mean():.4f}/{((e - s) < 65536).mean():.4f}')
    ids, ecnt, eoffs, elen, cov = entries_ids(f'{D}/interp_entries.bin', ip_off)
    print(f'interp_entries bytes {elen} covered {cov} lists {len(eoffs)} ids {len(ids)} max list {ecnt.max()}')
    ref = np.zeros(I, bool); ref[ids[ids < I]] = True
    print(f'  unreferenced interps {(~ref).sum()} (nodes {cnt[~ref].sum()}) entries->cnt<2 {(cnt[ids[ids < I]] < 2).sum()}')
    ipc = np.fromfile(f'{D}/interp_postcodes.bin', '<u4')
    print(f'interp_postcodes {len(ipc)} NO_DATA {(ipc == NO_DATA).sum()}')
    describe_nodes('interp', inodes, off, cnt)

    wp = np.fromfile(f'{D}/way_parents.bin', '<u4')
    wpc = np.fromfile(f'{D}/way_postcodes.bin', '<u4')
    print(f'way_parents {len(wp)} NO_DATA {(wp == NO_DATA).sum()} max(non-NO_DATA) {wp[wp != NO_DATA].max()} '
          f'distinct {len(np.unique(wp))} runs {(np.diff(wp.astype(np.int64)) != 0).sum() + 1}')
    print(f'way_postcodes {len(wpc)} NO_DATA {(wpc == NO_DATA).sum()} max {wpc[wpc != NO_DATA].max()} '
          f'distinct {len(np.unique(wpc))} runs {(np.diff(wpc.astype(np.int64)) != 0).sum() + 1}')


main()
