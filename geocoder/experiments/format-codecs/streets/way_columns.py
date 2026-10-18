#!/usr/bin/env python3
"""Split a street_ways.bin into its columns (off, cnt, name) and print each
column's zstd -18 size, to see which field drives the ways file size.
usage: way_columns.py <street_ways.bin> <tag>"""
import sys, subprocess, numpy as np
WAY = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad', '<u2'), ('name', '<u4')])
w = np.fromfile(sys.argv[1], WAY)
tag = sys.argv[2]
Z = '/ssd/shrink/zstd157/bin/zstd'
for col in ('off', 'cnt', 'name'):
    raw = np.ascontiguousarray(w[col]).tobytes()
    z = subprocess.run([Z, '-18', '-T8', '-q', '-c'], input=raw, capture_output=True).stdout
    print(f'{tag}\t{col}\traw {len(raw)}\tzst {len(z)}', flush=True)
