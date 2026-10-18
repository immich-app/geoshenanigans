#!/usr/bin/env python3
"""Recount what the 20-vertex building outline cap removes, independently of the
builder's counters: uncapped outline sizes from the exact dump's CSR index and from
out-nocap's AddrPoint.vertex_count, capped sizes from out-base's AddrPoint.vertex_count,
records joined by the *.osm_ids sidecar key (object_type << 56 | stable_id)."""
import numpy as np

B = '/ssd/precision/build'
AP = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('hn', '<u4'), ('st', '<u4'), ('pw', '<u4'),
               ('voff', '<u4'), ('vcnt', '<u4')])
SC = np.dtype([('ot', 'u1'), ('fl', 'u1'), ('tier', 'u1'), ('res', 'u1'), ('sid', '<u8')])


def load(run):
    ap = np.fromfile(f'{B}/{run}/planet/full/addr_points.bin', dtype=AP)
    sc = np.fromfile(f'{B}/{run}/planet/full/addr_points.osm_ids', dtype=SC, offset=12)
    assert len(ap) == len(sc), (len(ap), len(sc))
    key = (sc['ot'].astype(np.uint64) << np.uint64(56)) | sc['sid']
    return ap['vcnt'].astype(np.int64), key


vn, kn = load('out-nocap')
vb, kb = load('out-base')
idx = np.fromfile(f'{B}/dumps/addr_vertices.e7.idx', dtype='<u8')
cnt = np.diff(idx).astype(np.int64)
print('records nocap', len(vn), 'base', len(vb), 'dump idx', len(cnt))
print('dump idx counts == nocap vertex_count for every record:', bool(np.array_equal(cnt, vn)))
print('dump total vertices', int(idx[-1]), 'size check', int(idx[-1]) * 8)


def summary(name, v):
    o = v > 0
    print(f'{name}: outlines {int(o.sum()):,} vertices {int(v.sum()):,} '
          f'>20: {int((v > 20).sum()):,} holding {int(v[v > 20].sum()):,} max {int(v.max())}')


summary('uncapped (out-nocap)', vn)
summary('capped (out-base)', vb)
print('removed, totals difference:', f'{int(vn.sum() - vb.sum()):,}')

# join by key
print('unique keys nocap', len(np.unique(kn)) == len(kn), 'base', len(np.unique(kb)) == len(kb))
common, i_n, i_b = np.intersect1d(kn, kb, assume_unique=True, return_indices=True)
print('joined records', len(common), 'only nocap', len(kn) - len(common), 'only base', len(kb) - len(common))
a, b = vn[i_n], vb[i_b]
over = a > 20
print(f'joined: outlines over 20 (uncapped) {int(over.sum()):,}, their vertices {int(a[over].sum()):,} -> capped {int(b[over].sum()):,}'
      f' removed {int((a[over] - b[over]).sum()):,} ({(a[over] - b[over]).sum() / a[a > 0].sum() * 100:.3f}% of joined outline vertices)')
print('joined, <=20 unchanged:', bool(np.array_equal(a[~over], b[~over])), ' capped max', int(b.max()),
      ' capped all <=20:', bool((b <= 20).all()))
print('capped size of trimmed outlines: min', int(b[over].min()), 'p50', int(np.median(b[over])), 'max', int(b[over].max()))
edges = [0, 8, 12, 16, 20, 32, 64, 128, 256, 1024, 1 << 30]
print('bucket\toutlines\tuncapped_vertices\tcapped_vertices')
for lo, hi in zip(edges[:-1], edges[1:]):
    m = (a > lo) & (a <= hi) if lo else (a > 0) & (a <= hi)
    print(f'{lo + 1}-{hi}\t{int(m.sum()):,}\t{int(a[m].sum()):,}\t{int(b[m].sum()):,}')
