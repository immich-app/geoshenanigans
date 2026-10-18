"""Shared helpers: entries files ([u16 count][count x u32 id] lists back to back), list identity."""
import numpy as np
import subprocess

D = "/ssd/shrink/idx/max"
W = "/ssd/shrink/max/index-poi-strings/work"
O = "/ssd/shrink/max/index-poi-strings/out"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
NO = 0xFFFFFFFF
MiB = 1048576
GEO_DT = np.dtype([("cell", "<u8"), ("s", "<u4"), ("a", "<u4"), ("i", "<u4")])
CELL_DT = np.dtype([("cell", "<u8"), ("off", "<u4")])


def u32_at(e, pos):
    return (e[pos].astype(np.uint32) | (e[pos + 1].astype(np.uint32) << 8)
            | (e[pos + 2].astype(np.uint32) << 16) | (e[pos + 3].astype(np.uint32) << 24))


def load_lists(e, offsets):
    """e: uint8 array of the entries file; offsets: byte offset of each list.
    Returns counts (int64), flat ids (u32), starts (int64 index into ids)."""
    offsets = np.asarray(offsets, np.int64)
    cnt = e[offsets].astype(np.int64) | (e[offsets + 1].astype(np.int64) << 8)
    tot = int(cnt.sum())
    starts = np.concatenate([[0], np.cumsum(cnt)[:-1]]).astype(np.int64)
    lid = np.repeat(np.arange(len(cnt)), cnt)
    pos = offsets[lid] + 2 + 4 * (np.arange(tot, dtype=np.int64) - starts[lid])
    del lid
    return cnt, u32_at(e, pos), starts


def walk_offsets(e):
    """Byte offsets of every list in a back-to-back entries file (sequential walk)."""
    out = []
    p, n = 0, len(e)
    while p < n:
        out.append(p)
        p += 2 + 4 * (int(e[p]) | (int(e[p + 1]) << 8))
    assert p == n
    return np.array(out, np.int64)


def list_keys(cnt, ids, starts):
    """Exact list identity. Returns inv (list -> distinct id), first (a representative list per
    distinct id, in distinct-id order) and the distinct count."""
    n = len(cnt)
    inv = np.empty(n, np.int64)
    first = []
    base = 0
    for k in np.unique(cnt):
        sel = np.nonzero(cnt == k)[0]
        if k == 1:
            u, fi, iv = np.unique(ids[starts[sel]], return_index=True, return_inverse=True)
        else:
            mat = np.ascontiguousarray(ids[(starts[sel][:, None] + np.arange(k))].astype(">u4"))
            u, fi, iv = np.unique(mat.view(np.dtype((np.void, 4 * int(k)))).ravel(),
                                  return_index=True, return_inverse=True)
        inv[sel] = base + iv.ravel()
        first.append(sel[fi])
        base += len(u)
    return inv, np.concatenate(first), base


def zst_bytes(data, level=18, threads=1):
    if isinstance(data, np.ndarray):
        data = data.tobytes()
    p = subprocess.run([ZSTD, f"-{level}", f"-T{threads}", "-q", "-c"], input=data,
                       stdout=subprocess.PIPE, check=True)
    return len(p.stdout)


def zst_file(path, level=18, threads=16):
    p = subprocess.run(f"{ZSTD} -{level} -T{threads} -q -c {path} | wc -c", shell=True,
                       stdout=subprocess.PIPE, check=True)
    return int(p.stdout)


def varint(v):
    """LEB128 of a uint64 array -> bytes (uint8 array) and length per value."""
    v = np.asarray(v).astype(np.uint64)
    ln = np.ones(len(v), np.int64)
    t = v >> np.uint64(7)
    while np.any(t):
        ln += (t > 0)
        t >>= np.uint64(7)
    L = int(ln.max()) if len(v) else 1
    out = np.empty(int(ln.sum()), np.uint8)
    pos = np.concatenate([[0], np.cumsum(ln)[:-1]])
    for k in range(L):
        s = ln > k
        b = ((v[s] >> np.uint64(7 * k)) & np.uint64(0x7F)).astype(np.uint8)
        b |= np.where(ln[s] > k + 1, 0x80, 0).astype(np.uint8)
        out[pos[s] + k] = b
    return out, ln


def zz(x):
    x = np.asarray(x, np.int64)
    return np.where(x >= 0, 2 * x, -2 * x - 1).astype(np.uint64)
