#!/usr/bin/env python3
"""Interp record fields: end stored as (end - start) mod 2^32 (lossless; end = start + span).
zstd -18 of the span column vs the end column. usage: interp_span.py <i_g16.ways2> <tag>"""
import sys, subprocess, numpy as np
Z = '/ssd/shrink/zstd157/bin/zstd'
dt = np.dtype([('cnt', '<u2'), ('code', '<u2'), ('street', '<u4'), ('start', '<u4'), ('end', '<u4'), ('interp', 'u1')])
w = np.fromfile(sys.argv[1], dt)
span = (w['end'].astype(np.int64) - w['start'].astype(np.int64)) % (1 << 32)
assert np.array_equal(((w['start'].astype(np.int64) + span) % (1 << 32)).astype(np.uint32), w['end'])
for name, arr in (('end', w['end']), ('span', span.astype('<u4'))):
    z = subprocess.run([Z, '-18', '-T8', '-q', '-c'], input=np.ascontiguousarray(arr).tobytes(), capture_output=True).stdout
    print(f'{sys.argv[2]}\t{name}\traw {arr.nbytes}\tzst {len(z)}', flush=True)
