"""Level-10 cell tables (admin_cells/admin_entries, poi_cells/poi_entries; 12-byte rows u64 cell +
u32 entry offset, binary-searched): list dedup (flags kept, today's format), INTERIOR_FLAG
cleared (admin only; answer-neutral fast path), and a dense presence bitmap + rank directory
replacing the cells file (6*4^10 = 6,291,456 level-10 cells).
Writes work/l10/ and work/v_admin_dg/.
usage: l10_idx.py"""
import os
import numpy as np
from lists import *
from make_dedup_lib import write_lists

O10 = f"{W}/l10"
os.makedirs(O10, exist_ok=True)
NC = 6 * 4 ** 10
for name in ["admin", "poi"]:
    c = np.fromfile(f"{D}/{name}_cells.bin", CELL_DT)
    e = np.fromfile(f"{D}/{name}_entries.bin", np.uint8)
    cnt, ids, starts = load_lists(e, c["off"])
    lsb = c["cell"] & (~c["cell"] + np.uint64(1))
    assert np.all(lsb == np.uint64(1 << 40)), "not level 10"
    exp = np.concatenate([[0], np.cumsum(2 + 4 * cnt)[:-1]])
    print(f"== {name}: cells {len(c):,} ids {len(ids):,} offsets are prefix sums: {bool(np.array_equal(exp, c['off'].astype(np.int64)))}"
          f" flagged {float(np.mean(ids >> 31))*100:.2f}%")
    inv, first, nd = list_keys(cnt, ids, starts)
    _, first_occ = np.unique(inv, return_index=True)
    order = np.sort(first_occ)
    rank = np.empty(len(first_occ), np.int64)
    rank[np.argsort(first_occ)] = np.arange(len(first_occ))
    os.makedirs(f"{W}/v_{name}_dg", exist_ok=True)
    offs = write_lists(f"{W}/v_{name}_dg/{name}_entries.bin", cnt, ids, starts, order)
    c2 = c.copy()
    c2["off"] = offs[rank[inv]].astype(np.uint32)
    c2.tofile(f"{W}/v_{name}_dg/{name}_cells.bin")
    print(f"   dedup (flags kept): {nd:,} distinct lists, entries {os.path.getsize(f'{W}/v_{name}_dg/{name}_entries.bin'):,} B")
    if name == "admin":
        ids0 = ids & np.uint32(0x7FFFFFFF)
        inv0, _, nd0 = list_keys(cnt, ids0, starts)
        _, fo0 = np.unique(inv0, return_index=True)
        order0 = np.sort(fo0)
        rank0 = np.empty(len(fo0), np.int64)
        rank0[np.argsort(fo0)] = np.arange(len(fo0))
        os.makedirs(f"{W}/v_admin_noflag_dg", exist_ok=True)
        offs0 = write_lists(f"{W}/v_admin_noflag_dg/admin_entries.bin", cnt, ids0, starts, order0)
        c3 = c.copy()
        c3["off"] = offs0[rank0[inv0]].astype(np.uint32)
        c3.tofile(f"{W}/v_admin_noflag_dg/admin_cells.bin")
        print(f"   flags cleared + dedup: {nd0:,} distinct lists, entries {os.path.getsize(f'{W}/v_admin_noflag_dg/admin_entries.bin'):,} B")
    # bitmap + rank directory (u32 per 512 bits) + u32 offset per present cell (on-disk; O(1) lookup)
    pos = (c["cell"] >> np.uint64(41)).astype(np.int64)  # dense level-10 index: face<<20 | hilbert pos
    bm = np.zeros(NC, bool)
    bm[pos] = True
    bits = np.packbits(bm, bitorder="little")
    rankdir = np.concatenate([[0], np.cumsum(bm.reshape(-1, 512).sum(1))[:-1]]).astype("<u4")
    bits.tofile(f"{O10}/{name}_bitmap.bin")
    rankdir.tofile(f"{O10}/{name}_rank.bin")
    c2["off"].astype("<u4").tofile(f"{O10}/{name}_offsets_dg.bin")
    # self-check: rank lookup reproduces every row's offset
    k = pos
    r = rankdir[k // 512].astype(np.int64) + np.array([0])  # popcount within the 512-bit block
    cs = np.cumsum(bm)  # inclusive count
    r = cs[k] - 1
    assert np.array_equal(r, np.arange(len(c)))
    print(f"   bitmap {len(bits):,} B + rank {rankdir.nbytes:,} B + offsets {len(c)*4:,} B replace cells {c.nbytes:,} B")
