#!/usr/bin/env python3
"""zstd -18 sizes of a packed ways2 file split into column files.
usage: ways2_columns.py <file.ways2> street|interp <tag>
street ways2 = cnt u16 | code u16 | name u32 (8 B); interp = cnt | code | street u32 | start u32 | end u32 | interp u8 (17 B).
Writes nothing; prints per-column and grouped sizes."""
import sys, subprocess, numpy as np
path, kind, tag = sys.argv[1:4]
Z = '/ssd/shrink/zstd157/bin/zstd'
if kind == 'street':
    dt = np.dtype([('cnt', '<u2'), ('code', '<u2'), ('name', '<u4')])
    groups = {'cnt': ['cnt'], 'code': ['code'], 'name': ['name'], 'cnt+code': ['cnt', 'code']}
else:
    dt = np.dtype([('cnt', '<u2'), ('code', '<u2'), ('street', '<u4'), ('start', '<u4'), ('end', '<u4'), ('interp', 'u1')])
    groups = {'cnt+code': ['cnt', 'code'], 'street': ['street'], 'start': ['start'], 'end': ['end'], 'interp': ['interp'],
              'street+start+end+interp': ['street', 'start', 'end', 'interp']}
w = np.fromfile(path, dt)
for name, cols in groups.items():
    sub = np.empty(len(w), np.dtype([(c, dt[c]) for c in cols]))
    for c in cols:
        sub[c] = w[c]
    raw = sub.tobytes()
    z = subprocess.run([Z, '-18', '-T8', '-q', '-c'], input=raw, capture_output=True).stdout
    print(f'{tag}\t{name}\traw {len(raw)}\tzst {len(z)}', flush=True)
