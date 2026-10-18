"""GX: a compact on-disk geo index replacing geo_cells.bin (20 B/row) with blocks of B rows.

  gx_top.bin   one record per block: u64 first cell id, u32 byte offset of the block in
               gx_rows.bin, then per table (street, addr, interp) the byte offset of the list
               in effect at block start ("cur", NO_DATA if none yet) and, for the global variant,
               where the next newly stored list starts ("next"). 24 B (adj) / 36 B (global).
  gx_rows.bin  per row: header byte = s | a<<2 | i<<4 | d<<6, then varint(delta-4) if d == 3,
               then varint(byte distance back) for each table whose state is 3, in s,a,i order.
                 table state: 0 absent, 1 new list (the next stored list), 2 same list as this
                 table's previous row, 3 back-reference to an earlier stored list (global only)
                 d: cell delta from the previous row in level-17 steps (2^27): 0 -> 1, 1 -> 2,
                    2 -> 3, 3 -> escape; the first row of a block takes its cell from gx_top
  <t>_entries  stored lists back to back as today ([u16 count][u32 ids]); a new list starts
               where the previous stored one ends (cur + 2 + 4*count), so no per-row offsets.
  adj variant: only consecutive identical lists are shared (stored lists in row order)
  global     : every distinct list stored once in first-occurrence order (= work/v_clean_dg)
Lookup: binary-search gx_top, decode <= B rows of one block (hopping list counts in the
entries files), read the target lists. Inputs: work/clean_<t>.npz from make_clean.py.
usage: geo_gx.py <adj|global> <B>"""
import os
import sys
import numpy as np
from lists import *
from make_dedup_lib import write_lists

variant, B = sys.argv[1], int(sys.argv[2])
out = f"{W}/gx_{variant}_{B}"
os.makedirs(out, exist_ok=True)
g = np.fromfile(f"{D}/geo_cells.bin", GEO_DT)
cell = g["cell"].copy()
present = {k: g[k] != NO for k in "sai"}
del g
R = len(cell)
blk_first = np.arange(0, R, B)
nb = len(blk_first)
state = {}
backref = {}
cur_at_block = {}
next_at_block = {}
TABLES = [("s", "street_entries.bin"), ("a", "addr_entries.bin"), ("i", "interp_entries.bin")]
for key, fn in TABLES:
    z = np.load(f"{W}/clean_{key}.npz")
    cnt, ids, inv = z["cnt"].astype(np.int64), z["ids"], z["inv"]
    starts = np.concatenate([[0], np.cumsum(cnt)[:-1]]).astype(np.int64)
    rows = np.nonzero(present[key])[0]
    J = len(rows)
    assert J == len(cnt)
    if variant == "adj":
        new = np.concatenate([[True], inv[1:] != inv[:-1]])
        order = np.nonzero(new)[0]
        st = np.where(new, 1, 2).astype(np.uint8)
    else:
        _, first_occ = np.unique(inv, return_index=True)
        new = np.zeros(J, bool)
        new[first_occ] = True
        order = np.nonzero(new)[0]
        same = np.concatenate([[False], inv[1:] == inv[:-1]])
        st = np.where(new, 1, np.where(same, 2, 3)).astype(np.uint8)
    offs_stored = write_lists(f"{out}/{fn}", cnt, ids, starts, order)
    size_stored = 2 + 4 * cnt[order]
    # byte offset of every row's list; for "new" rows it is the next stored offset
    stored_idx = np.empty(J, np.int64)
    if variant == "adj":
        stored_idx = np.cumsum(new) - 1
    else:
        rank = np.empty(len(order), np.int64)
        # distinct id of each stored list (in stored order) -> its stored position
        rank_of_inv = np.empty(int(inv.max()) + 1, np.int64)
        rank_of_inv[inv[order]] = np.arange(len(order))
        stored_idx = rank_of_inv[inv]
    off_row = offs_stored[stored_idx]
    # next offset before row j = bytes of stored lists that started before j
    next_before = np.concatenate([[0], np.cumsum(np.where(new, 2 + 4 * cnt, 0))[:-1]])
    br = np.zeros(J, np.int64)
    if variant == "global":
        m = st == 3
        br[m] = next_before[m] - off_row[m]
        assert np.all(br[m] > 0)
    s_full = np.zeros(R, np.uint8)
    s_full[rows] = st
    b_full = np.zeros(R, np.int64)
    b_full[rows] = br
    state[key], backref[key] = s_full, b_full
    # per block: cur = offset of the list of the last present row before the block, next = next_before
    # of the first present row at/after the block start (= bytes stored before the block)
    j_at = np.searchsorted(rows, blk_first)            # first present row index >= block start
    cur = np.where(j_at > 0, off_row[np.maximum(j_at - 1, 0)], NO).astype(np.uint32)
    nxt_total = np.concatenate([next_before, [int(size_stored.sum())]])
    nxt = nxt_total[j_at].astype(np.uint32)
    if variant == "adj":
        # derivable: next = cur + size(cur list), 0 before the first list
        chk = np.where(cur == NO, 0, cur.astype(np.int64) + 2 + 4 * cnt[np.maximum(j_at - 1, 0)])
        assert np.array_equal(chk, nxt.astype(np.int64))
    cur_at_block[key], next_at_block[key] = cur, nxt
    print(f"{key}: rows {J:,} stored lists {len(order):,} entries {os.path.getsize(f'{out}/{fn}'):,} B "
          f"states {np.bincount(st, minlength=4).tolist()}", flush=True)
    del cnt, ids, inv, starts, z, off_row, next_before, br, stored_idx

# cell delta codes
du = np.zeros(R, np.int64)
du[1:] = ((cell[1:] - cell[:-1]) >> np.uint64(27)).astype(np.int64)
assert np.all(du[1:] >= 1)
first_of_block = np.zeros(R, bool)
first_of_block[blk_first] = True
dcode = np.where(first_of_block, 0, np.minimum(du - 1, 3)).astype(np.uint8)
hdr = (state["s"] | (state["a"] << 2) | (state["i"] << 4) | (dcode << 6)).astype(np.uint8)
assert np.all((state["s"] | state["a"] | state["i"]) != 0)
esc = dcode == 3
vals = [(esc, (du - 4))]
for key in "sai":
    vals.append((state[key] == 3, backref[key]))
rowlen = np.ones(R, np.int64)
enc = []
for m, v in vals:
    if not m.any():
        continue
    b, ln = varint(v[m].astype(np.uint64))
    full_ln = np.zeros(R, np.int64)
    full_ln[m] = ln
    enc.append((m, b, full_ln))
    rowlen += full_ln
rowpos = np.concatenate([[0], np.cumsum(rowlen)[:-1]])
buf = np.empty(int(rowlen.sum()), np.uint8)
buf[rowpos] = hdr
at = rowpos + 1
for m, b, full_ln in enc:
    ln = full_ln[m]
    starts_v = at[m]
    pos = np.repeat(starts_v, ln) + (np.arange(int(ln.sum())) - np.repeat(np.concatenate([[0], np.cumsum(ln)[:-1]]), ln))
    buf[pos] = b
    at = at + full_ln
buf.tofile(f"{out}/gx_rows.bin")
if variant == "adj":
    top = np.zeros(nb, np.dtype([("cell", "<u8"), ("roff", "<u4"), ("s", "<u4"), ("a", "<u4"), ("i", "<u4")]))
    for k in "sai":
        top[k] = cur_at_block[k]
else:
    top = np.zeros(nb, np.dtype([("cell", "<u8"), ("roff", "<u4"), ("s", "<u4"), ("sn", "<u4"), ("a", "<u4"),
                                 ("an", "<u4"), ("i", "<u4"), ("in", "<u4")]))
    for k in "sai":
        top[k] = cur_at_block[k]
        top[k + "n"] = next_at_block[k]
top["cell"] = cell[blk_first]
assert rowpos[blk_first].max() < 2 ** 32
top["roff"] = rowpos[blk_first]
top.tofile(f"{out}/gx_top.bin")
print(f"gx_{variant}_{B}: rows {len(buf):,} B ({len(buf)/R:.3f} B/row), top {top.nbytes:,} B; "
      f"delta codes {np.bincount(dcode, minlength=4).tolist()}", flush=True)
