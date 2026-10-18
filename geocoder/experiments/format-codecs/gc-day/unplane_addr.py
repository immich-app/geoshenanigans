"""addr_points.col (lat, lng, poly_offset as 4 byte planes; hn, st, pw plain u32) -> the addresses
track's Tier B plain columns (lat | lng | hn | st | pw | poly_offset, N u32 each), the input of its
independent C decoder abs2dec. usage: unplane_addr.py <addr_points.col> <out>"""
import sys
import numpy as np

src, dst = sys.argv[1:3]
b = np.fromfile(src, np.uint8)
N = len(b) // 24
assert 24 * N == len(b)
s = 4 * N


def unplanes(p):
    return np.ascontiguousarray(p.reshape(4, N).T).view(np.uint32).ravel()


cols = [unplanes(b[0:s]), unplanes(b[s:2 * s])]
cols += [b[(2 + k) * s:(3 + k) * s].view(np.uint32) for k in range(3)]
cols += [unplanes(b[5 * s:6 * s])]
with open(dst, "wb") as f:
    for c in cols:
        c.astype("<u4").tofile(f)
print("records", N)
