"""Today's-format variants of the geo index (geo_cells.bin + *_entries.bin), readable by
today's reader unchanged:
  dedup_global : every distinct list stored once (first-occurrence order); rows point at it
  dedup_adj    : only runs of identical lists in consecutive rows collapsed
addr_entries has no duplicate lists (2 of 57.8M) and stays as shipped in both.
Also a POI index variant with identical lists stored once (poi_cells offsets).
usage: make_dedup.py <variant> ; writes work/v_<variant>/ (changed files only)"""
import os
import sys
import numpy as np
from lists import *

variant = sys.argv[1]
out = f"{W}/v_{variant}"
os.makedirs(out, exist_ok=True)


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


g = np.fromfile(f"{D}/geo_cells.bin", GEO_DT)
for key, fn in [("s", "street_entries.bin"), ("i", "interp_entries.bin")]:
    e = np.fromfile(f"{D}/{fn}", np.uint8)
    rows = np.nonzero(g[key] != NO)[0]
    cnt, ids, starts = load_lists(e, g[key][rows])
    del e
    inv = np.load(f"{W}/inv_{key}.npy")
    if variant == "dedup_global":
        _, first_occ = np.unique(inv, return_index=True)       # per distinct id: first list using it
        order = np.sort(first_occ)                              # distinct lists in first-occurrence order
        rank = np.empty(len(first_occ), np.int64)
        rank[np.argsort(first_occ)] = np.arange(len(first_occ))  # distinct id -> output position
        offs = write_lists(f"{out}/{fn}", cnt, ids, starts, order)
        new_off = offs[rank[inv]]
    elif variant == "dedup_adj":
        run_start = np.concatenate([[True], inv[1:] != inv[:-1]])
        order = np.nonzero(run_start)[0]
        offs = write_lists(f"{out}/{fn}", cnt, ids, starts, order)
        new_off = offs[np.cumsum(run_start) - 1]
    else:
        raise SystemExit(variant)
    g[key][rows] = new_off.astype(np.uint32)
    print(f"{variant} {fn}: {len(cnt):,} lists -> {len(order):,} stored, {os.path.getsize(f'{out}/{fn}'):,} bytes", flush=True)
    del cnt, ids, starts, inv
g.tofile(f"{out}/geo_cells.bin")
print("geo_cells written", flush=True)
