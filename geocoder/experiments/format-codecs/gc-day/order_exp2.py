"""[order_exp2: adds within-cell keys led by street id] Why addr_points.col compresses worse than the addresses track's measurement: the same records in
two orders, column by column, zstd -18 -T16 (streamed to zstd, nothing written to disk).
  canon : the sort step's order (first row listing the point, hilbert, lat, lng, hn, street, slot)
  slot  : first row listing the point, then the published slot (the addresses track's build_sorted.py
          order: geo_cells row, old id) -- same records, same cells, only the order inside a cell differs
usage: order_exp.py"""
import subprocess
import numpy as np

D = "/ssd/shrink/sortpatch/sort/days/2026-10-13/planet/full"
M = "/ssd/shrink/sortpatch/sort/meta/2026-10-13"
Z = "/ssd/shrink/zstd157/bin/zstd"
NO = 0xFFFFFFFF

ap = np.fromfile(f"{D}/addr_points.bin", np.uint32).reshape(-1, 7)
N = len(ap)
slot = np.fromfile(f"{M}/addr.slot.u32", np.uint32)
assert len(slot) == N
g = np.fromfile(f"{D}/geo_cells.bin", np.dtype([("cell", "<u8"), ("s", "<u4"), ("a", "<u4"), ("i", "<u4")]))
a = g["a"].copy()
del g
rows = np.nonzero(a != NO)[0]
offs = a[rows].astype(np.int64)
del a
e = np.fromfile(f"{D}/addr_entries.bin", np.uint8)
cnt = e[offs].astype(np.int64) | (e[offs + 1].astype(np.int64) << 8)
tot = int(cnt.sum())
st = np.concatenate([[0], np.cumsum(cnt)[:-1]])
lid = np.repeat(np.arange(len(cnt)), cnt)
pos = offs[lid] + 2 + 4 * (np.arange(tot) - st[lid])
ids = e[pos].astype(np.uint32) | (e[pos + 1].astype(np.uint32) << 8) | (e[pos + 2].astype(np.uint32) << 16) | (e[pos + 3].astype(np.uint32) << 24)
del e, pos
row_of = np.full(N, np.iinfo(np.int64).max, np.int64)
np.minimum.at(row_of, ids, rows[lid])
del ids, lid
assert np.all(np.diff(row_of) >= 0), "canonical order is not row-major"
order2 = np.lexsort((slot, row_of))
print("records", N, "moved by the slot order:", int((order2 != np.arange(N)).sum()), flush=True)

# abs2 blob size per record (from gc's encoded poly_offset column) to rebuild poly_offset in order 2
col = np.fromfile("/ssd/shrink/sortpatch/codecs/try/enc/2026-10-13/planet/full/addr_points.col", np.uint8)
assert len(col) == 24 * N
vo = np.ascontiguousarray(col[20 * N:24 * N].reshape(4, N).T).view(np.uint32).ravel()
del col
abs2 = 1931965995
has = vo != NO
vp = vo[has].astype(np.int64)
sz = np.zeros(N, np.int64)
sz[has] = np.diff(np.append(vp, abs2))


def zsize(b):
    return len(subprocess.run([Z, "-18", "-T16", "-q", "-c"], input=b, stdout=subprocess.PIPE, check=True).stdout)


def planes(v):
    return np.ascontiguousarray(v.view(np.uint8).reshape(-1, 4).T).tobytes()


canon_rank = np.arange(N)  # canonical order already holds (hilbert, lat, lng, hn, street, slot) inside a row
order3 = np.lexsort((canon_rank, ap[:, 2], ap[:, 3], row_of))          # row, street, housenumber, canonical
order4 = np.lexsort((canon_rank, ap[:, 3], row_of))                    # row, street, canonical (hilbert ...)
order5 = np.lexsort((canon_rank, ap[:, 4], row_of))                    # row, parent way, canonical
res = {}
for name, o in (("canon", None), ("slot", order2), ("street_hn", order3), ("street", order4), ("parent_way", order5)):
    p = ap if o is None else ap[o]
    if o is None:
        v = vo
    else:
        s2 = sz[o]
        v = np.where(s2 > 0, np.cumsum(s2) - s2, NO).astype(np.uint32)
    parts = {"lat planes": planes(p[:, 0].copy()), "lng planes": planes(p[:, 1].copy()),
             "housenumber": p[:, 2].copy().tobytes(), "street": p[:, 3].copy().tobytes(),
             "parent_way": p[:, 4].copy().tobytes(), "poly_offset planes": planes(v)}
    for k, b in parts.items():
        res[(name, k)] = zsize(b)
        print(name, k, len(b), res[(name, k)], flush=True)
names = ("canon", "slot", "street_hn", "street", "parent_way")
print("\n| column | " + " | ".join(names) + " |")
print("|---|" + "---:|" * len(names))
tot = {n: 0 for n in names}
for k in ("lat planes", "lng planes", "housenumber", "street", "parent_way", "poly_offset planes"):
    for n in names:
        tot[n] += res[(n, k)]
    print(f"| {k} | " + " | ".join(f"{res[(n, k)]:,}" for n in names) + " |")
print("| total | " + " | ".join(f"{tot[n]:,}" for n in names) + " |")
