"""Today's-format geo index with dead entries removed:
  - an id repeated inside one cell's list (addr: 4,479,573; street: 707) is dropped; the reader
    already skips repeated street ids (seen_streets) and a repeated addr point can't beat itself
    (beats() breaks exact ties by content)
Writes work/v_clean/ (no list dedup) and work/v_clean_dg/ (clean + identical lists stored once),
and work/clean_<t>.npz (counts, ids, inv) for the new-layout encoders.
usage: make_clean.py"""
import os
import numpy as np
from lists import *
from make_dedup_lib import write_lists

out1, out2 = f"{W}/v_clean", f"{W}/v_clean_dg"
os.makedirs(out1, exist_ok=True)
os.makedirs(out2, exist_ok=True)
g = np.fromfile(f"{D}/geo_cells.bin", GEO_DT)
g1, g2 = g.copy(), g.copy()
for key, fn in [("s", "street_entries.bin"), ("a", "addr_entries.bin"), ("i", "interp_entries.bin")]:
    e = np.fromfile(f"{D}/{fn}", np.uint8)
    rows = np.nonzero(g[key] != NO)[0]
    cnt, ids, starts = load_lists(e, g[key][rows])
    del e
    lid = np.repeat(np.arange(len(cnt)), cnt)
    o = np.lexsort((ids, lid))  # by list, then id; stable, so the first copy comes first
    dup_sorted = np.zeros(len(ids), bool)
    dup_sorted[1:] = (lid[o][1:] == lid[o][:-1]) & (ids[o][1:] == ids[o][:-1])
    dup = np.zeros(len(ids), bool)
    dup[o] = dup_sorted
    del o, dup_sorted
    # the first copy keeps its place: dup marks a later copy only if lexsort put the first copy first
    # (stable on equal keys -> original order) -> later positions are marked
    keep = ~dup
    ids2 = ids[keep]
    cnt2 = np.bincount(lid[keep], minlength=len(cnt)).astype(np.int64)
    del lid, keep, ids
    assert cnt2.min() >= 1
    starts2 = np.concatenate([[0], np.cumsum(cnt2)[:-1]]).astype(np.int64)
    print(f"{fn}: removed {int(dup.sum()):,} repeated ids inside lists", flush=True)
    del dup
    allr = np.arange(len(cnt2))
    offs = write_lists(f"{out1}/{fn}", cnt2, ids2, starts2, allr)
    g1[key][rows] = offs.astype(np.uint32)
    inv, first, nd = list_keys(cnt2, ids2, starts2)
    _, first_occ = np.unique(inv, return_index=True)
    order = np.sort(first_occ)
    rank = np.empty(len(first_occ), np.int64)
    rank[np.argsort(first_occ)] = np.arange(len(first_occ))
    offs2 = write_lists(f"{out2}/{fn}", cnt2, ids2, starts2, order)
    g2[key][rows] = offs2[rank[inv]].astype(np.uint32)
    print(f"   clean {os.path.getsize(f'{out1}/{fn}'):,} B; clean+dedup {os.path.getsize(f'{out2}/{fn}'):,} B ({nd:,} distinct lists)", flush=True)
    np.savez(f"{W}/clean_{key}.npz", cnt=cnt2.astype(np.uint16), ids=ids2, inv=inv)
    del cnt2, ids2, starts2, inv, first, offs, offs2
g1.tofile(f"{out1}/geo_cells.bin")
g2.tofile(f"{out2}/geo_cells.bin")
print("done", flush=True)
