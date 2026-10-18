"""Tier B addr_points columns (lat|lng|hn|st|pw|vo, N u32 each) with parent_way_id mapped to the
streets track's spatial way order (old -> new id table), so the two tracks' changes are measured
together. usage: pw_remap.py <in columns> <remap_street.u32> <out columns>"""
import os
import sys
import numpy as np

src, remap, dst = sys.argv[1:4]
col = np.fromfile(src, np.uint32)
N = len(col) // 6
col = col.reshape(6, N)
rm = np.fromfile(remap, "<u4")
pw = col[4]
has = pw != 0xFFFFFFFF
assert (pw[has] < len(rm)).all()
pw[has] = rm[pw[has]]
os.makedirs(os.path.dirname(dst), exist_ok=True)
col.tofile(dst)
print("records", N, "parent_way remapped", int(has.sum()))
