#!/usr/bin/env python3
"""u32-per-way sidecars (way_parents, way_postcodes, interp_postcodes):
value ranges, NO_DATA share, and a lossless u24 repack (NO_DATA -> 0xFFFFFF)
written next to the source for zstd measurement. usage: sidecars.py <src_dir> <out_dir>"""
import sys, os, numpy as np
src, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
ND = np.uint32(0xFFFFFFFF)
for f in ('way_parents.bin', 'way_postcodes.bin', 'interp_postcodes.bin'):
    p = f'{src}/{f}'
    if not os.path.exists(p):
        continue
    v = np.fromfile(p, '<u4')
    live = v != ND
    mx = int(v[live].max()) if live.any() else 0
    print(f'{f}: n {len(v)} NO_DATA {(~live).sum()} ({100*(~live).mean():.1f}%) max {mx} '
          f'fits_u24 {mx < 0xFFFFFF} distinct {len(np.unique(v))} runs {(np.diff(v.astype(np.int64)) != 0).sum() + 1}', flush=True)
    if mx < 0xFFFFFF:
        w = np.where(live, v, np.uint32(0xFFFFFF)).astype('<u4')
        b = w.view(np.uint8).reshape(-1, 4)[:, :3].copy()
        b.tofile(f'{out}/{f}.u24')
        back = b[:, 0].astype(np.uint32) | (b[:, 1].astype(np.uint32) << 8) | (b[:, 2].astype(np.uint32) << 16)
        back = np.where(back == 0xFFFFFF, ND, back)
        assert np.array_equal(back, v), f
        print(f'  u24 roundtrip ok, {b.size} bytes', flush=True)
    # u16 dictionary: per-record u16 index into a table of the distinct u32
    # values (0xFFFF = NO_DATA); random access = two reads, the table is tiny.
    d = np.unique(v[live])
    if len(d) < 0xFFFF:
        idx = np.where(live, np.searchsorted(d, v), 0xFFFF).astype('<u2')
        idx.tofile(f'{out}/{f}.u16dict')
        d.astype('<u4').tofile(f'{out}/{f}.dict')
        back = np.where(idx == 0xFFFF, ND, d[np.minimum(idx, len(d) - 1)])
        assert np.array_equal(back, v), f
        print(f'  u16 dict roundtrip ok, {idx.nbytes} + {d.nbytes} bytes', flush=True)
