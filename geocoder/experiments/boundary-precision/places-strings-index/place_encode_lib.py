import numpy as np


def zigzag(d):
    d = d.astype(np.int64)
    return ((d << 1) ^ (d >> 63)).astype(np.uint64)


def varint(u):
    """LEB128-encode an array of non-negative ints (vectorised)."""
    u = u.astype(np.uint64)
    out = []
    nbytes = np.ones(len(u), dtype=np.int64)
    t = u >> np.uint64(7)
    while True:
        m = t > 0
        if not m.any():
            break
        nbytes += m
        t = t >> np.uint64(7)
    total = int(nbytes.sum())
    buf = np.zeros(total, dtype=np.uint8)
    pos = np.concatenate([[0], np.cumsum(nbytes)[:-1]])
    v = u.copy()
    for k in range(int(nbytes.max())):
        m = nbytes > k
        byte = (v[m] & np.uint64(0x7F)).astype(np.uint8)
        more = nbytes[m] > k + 1
        byte = byte | (more.astype(np.uint8) << 7)
        buf[pos[m] + k] = byte
        v[m] = v[m] >> np.uint64(7)
    return buf


