"""Shared readers for the address track: entries lists, S2 cells, polygons."""
import numpy as np

NO = np.uint32(0xFFFFFFFF)
STREET_LEVEL = 17

# ---------------------------------------------------------------- S2 (numpy)
_SWAP, _INV = 1, 2
_POS_TO_IJ = [[0, 1, 3, 2], [0, 2, 3, 1], [3, 2, 0, 1], [3, 1, 0, 2]]
_POS_TO_OR = [_SWAP, 0, 0, _INV | _SWAP]
_LOOKUP_POS = np.zeros(1 << 10, np.int64)


def _init(level, i, j, orig, pos, orient):
    if level == 4:
        ij = (i << 4) + j
        _LOOKUP_POS[(ij << 2) + orig] = (pos << 2) + orient
        return
    level += 1; i <<= 1; j <<= 1; pos <<= 2
    r = _POS_TO_IJ[orient]
    for k in range(4):
        _init(level, i + (r[k] >> 1), j + (r[k] & 1), orig, pos + k, orient ^ _POS_TO_OR[k])


for _o in (0, _SWAP, _INV, _SWAP | _INV):
    _init(0, 0, 0, _o, 0, _o)


def _st(u):
    return np.where(u >= 0, 0.5 * np.sqrt(1 + 3 * u), 1 - 0.5 * np.sqrt(1 - 3 * u))


def cell_of_points(lat_deg, lng_deg, level=STREET_LEVEL, chunk=1 << 24):
    out = np.empty(len(lat_deg), np.uint64)
    for s in range(0, len(lat_deg), chunk):
        out[s:s + chunk] = _cells(np.asarray(lat_deg[s:s + chunk], np.float64),
                                  np.asarray(lng_deg[s:s + chunk], np.float64), level)
    return out


def _cells(lat_deg, lng_deg, level):
    la, lo = np.radians(lat_deg), np.radians(lng_deg)
    x = np.cos(la) * np.cos(lo); y = np.cos(la) * np.sin(lo); z = np.sin(la)
    ax, ay, az = np.abs(x), np.abs(y), np.abs(z)
    # S2 LargestAbsComponent: ties go to the later axis
    f = np.where(ax > ay, np.where(ax > az, 0, 2), np.where(ay > az, 1, 2))
    comp = np.choose(f, [x, y, z])
    face = np.where(comp < 0, f + 3, f)
    u = np.empty_like(x); v = np.empty_like(x)
    for fc, (uu, vv) in enumerate((
            (lambda: y / x, lambda: z / x), (lambda: -x / y, lambda: z / y), (lambda: -x / z, lambda: -y / z),
            (lambda: z / x, lambda: y / x), (lambda: z / y, lambda: -x / y), (lambda: -y / z, lambda: -x / z))):
        m = face == fc
        if m.any():
            with np.errstate(divide='ignore', invalid='ignore'):
                u[m] = uu()[m]; v[m] = vv()[m]
    MAX = 1 << 30
    i = np.clip(np.floor(MAX * _st(u)), 0, MAX - 1).astype(np.int64)
    j = np.clip(np.floor(MAX * _st(v)), 0, MAX - 1).astype(np.int64)
    n = face.astype(np.uint64) << np.uint64(60)
    bits = (face & _SWAP).astype(np.int64)
    for k in range(7, -1, -1):
        bits = bits + (((i >> (k * 4)) & 15) << 6) + (((j >> (k * 4)) & 15) << 2)
        bits = _LOOKUP_POS[bits]
        n |= (bits >> 2).astype(np.uint64) << np.uint64(k * 8)
        bits &= (_SWAP | _INV)
    leaf = n * np.uint64(2) + np.uint64(1)
    lsb = np.uint64(1) << np.uint64(2 * (30 - level))
    return (leaf & ~(lsb - np.uint64(1)) & ~lsb) | lsb


# ---------------------------------------------------------------- files
def geo_cells(d):
    gc = np.fromfile(f'{d}/geo_cells.bin', np.uint8).reshape(-1, 20)
    cell = gc[:, 0:8].copy().view(np.uint64).ravel()
    cols = gc[:, 8:20].copy().view(np.uint32).reshape(-1, 3)
    return cell, cols[:, 0], cols[:, 1], cols[:, 2]


def read_lists(blob, offs):
    """u16 count + u32 ids lists at offs -> (counts, ids concatenated)."""
    o = offs.astype(np.int64)
    cnt = blob[o].astype(np.int64) | (blob[o + 1].astype(np.int64) << 8)
    tot = int(cnt.sum())
    first = np.cumsum(cnt) - cnt
    pos = np.repeat(o + 2, cnt) + 4 * (np.arange(tot) - np.repeat(first, cnt))
    ids = (blob[pos].astype(np.uint32) | (blob[pos + 1].astype(np.uint32) << 8) |
           (blob[pos + 2].astype(np.uint32) << 16) | (blob[pos + 3].astype(np.uint32) << 24))
    return cnt, ids


def load_entries(d, which=1, name='addr_entries.bin'):
    cell, s, a, i = geo_cells(d)
    col = (s, a, i)[which - 0] if which != 1 else a
    has = col != NO
    blob = np.fromfile(f'{d}/{name}', np.uint8)
    cnt, ids = read_lists(blob, col[has])
    rows = np.nonzero(has)[0]
    return np.repeat(cell[has], cnt), ids, np.repeat(rows, cnt)


def write_lists(lists_cnt, ids):
    """Inverse of read_lists for consecutive lists: returns blob, offsets."""
    cnt = np.asarray(lists_cnt, np.int64)
    sizes = 2 + 4 * cnt
    offs = np.concatenate([[0], np.cumsum(sizes)[:-1]])
    blob = np.empty(int(sizes.sum()), np.uint8)
    blob[offs] = (cnt & 0xff).astype(np.uint8)
    blob[offs + 1] = (cnt >> 8).astype(np.uint8)
    first = np.cumsum(cnt) - cnt
    pos = np.repeat(offs + 2, cnt) + 4 * (np.arange(int(cnt.sum())) - np.repeat(first, cnt))
    b = ids.astype('<u4').view(np.uint8).reshape(-1, 4)
    for k in range(4):
        blob[pos + k] = b[:, k]
    return blob, offs


def polygons(d, ap):
    """Per-polygon (record index, offset, count, enc, bmin_lat_bits, bmin_lng_bits)."""
    av = np.fromfile(f'{d}/addr_vertices.bin', np.uint8)
    vc = ap[:, 6]
    pi = np.nonzero(vc > 0)[0]
    off = ap[pi, 5].astype(np.int64)
    return av, pi, off
