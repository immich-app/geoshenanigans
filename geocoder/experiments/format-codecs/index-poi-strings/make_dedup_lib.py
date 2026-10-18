"""write_lists shared by make_dedup.py / make_clean.py."""
import numpy as np


def write_lists(path, cnt, ids, starts, order):
    """Write lists `order` (indices) back to back as [u16 count][u32 ids]; return byte offset of each."""
    c = cnt[order]
    sizes = 2 + 4 * c
    offs = np.concatenate([[0], np.cumsum(sizes)[:-1]]).astype(np.int64)
    total = int(sizes.sum())
    assert total < 2 ** 32, "entries file would exceed u32 offsets"
    buf = np.empty(total, np.uint8)
    buf[offs] = (c & 0xFF).astype(np.uint8)
    buf[offs + 1] = (c >> 8).astype(np.uint8)
    lid = np.repeat(np.arange(len(order)), c)
    k = np.arange(int(c.sum()), dtype=np.int64) - np.repeat(np.concatenate([[0], np.cumsum(c)[:-1]]), c)
    src = ids[starts[order][lid] + k]
    pos = offs[lid] + 2 + 4 * k
    del lid, k
    b = src.view(np.uint8).reshape(-1, 4)
    for j in range(4):
        buf[pos + j] = b[:, j]
    buf.tofile(path)
    return offs
