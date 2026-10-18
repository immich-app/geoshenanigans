"""admin_cells/admin_entries and place_cells/place_entries: structure + compact re-encodings (zstd -18 sizes).

File format (server/src/lib.rs:1114-1150, builder/src/cell_index_io.h):
  *_cells.bin   : sorted rows of (u64 S2 cell id at level 10, u32 byte offset into *_entries.bin)
  *_entries.bin : per cell, u16 count then count u32 ids (admin ids carry INTERIOR_FLAG 0x80000000)
"""
import sys
from common import *
from place_encode_lib import varint, zigzag

INTERIOR = np.uint32(0x80000000)
LVL = 10
STEP_SHIFT = 2 * (30 - LVL) + 1     # L10 cell ids differ by multiples of 2**41
NCELLS_L10 = 6 * 4 ** LVL


def analyse(prefix, cells_name, entries_name, is_admin):
    cells = load(cells_name, CELL_DT)
    eb = open(f"{SM}/{entries_name}", 'rb').read()
    n = len(cells)
    counts = np.frombuffer(eb, np.uint8)  # placeholder
    lists = decode_entries(eb, cells['off'])
    counts = np.array([len(l) for l in lists], dtype=np.int64)
    exp_off = np.concatenate([[0], np.cumsum(2 + 4 * counts)[:-1]])
    print(f"== {prefix}: {n:,} L10 cells, {counts.sum():,} ids, mean {counts.mean():.2f}, max {counts.max()}")
    print(f"   offsets == prefix sum of entry sizes (i.e. derivable): {bool(np.all(exp_off == cells['off']))}")
    lv = cells['cell'] & np.uint64((1 << STEP_SHIFT) - 1)
    print(f"   all ids are level-10 cells: {bool(np.all(lv == np.uint64(1 << (STEP_SHIFT - 1))))}")
    pos = (cells['cell'] >> np.uint64(STEP_SHIFT)).astype(np.int64)   # 0 .. 6*4^10-1 in Hilbert order
    base = zst(cells) + zst(eb)
    print(f"   baseline zst: cells {zst(cells):,} + entries {zst(eb):,} = {base:,} ({mib(base)} MiB)")
    res = {"baseline": base}

    # --- cell-id encodings
    dpos = np.diff(pos, prepend=-1)
    print(f"   cell gap==1 share: {(dpos == 1).mean():.1%}")
    z_ids_u64 = zst(cells['cell'].copy())
    z_ids_dv = zst(varint(dpos.astype(np.uint64)))
    bm = np.zeros(NCELLS_L10, bool); bm[pos] = True
    z_bitmap = zst(np.packbits(bm))
    print(f"   cell ids only u64: {z_ids_u64:,}; delta varint: {z_ids_dv:,}; presence bitmap (6*4^10 bits = {NCELLS_L10 // 8:,} B raw): {z_bitmap:,}")

    # --- entries encodings
    raw_ids = np.concatenate(lists)
    cnt16 = counts.astype('<u2')
    z_flat = zst(np.concatenate([cnt16.view(np.uint8), raw_ids.view(np.uint8)]))
    # SoA: counts then ids
    z_soa = zst(cnt16) + zst(raw_ids)
    # per-list delta of sorted ids (strip flag into separate bit-plane)
    if is_admin:
        flags = (raw_ids & INTERIOR) != 0
        ids = raw_ids & np.uint32(0x7FFFFFFF)
    else:
        flags = np.zeros(len(raw_ids), bool)
        ids = raw_ids
    # sort each list by id (flag stored separately)
    starts = np.concatenate([[0], np.cumsum(counts)[:-1]])
    owner = np.repeat(np.arange(n), counts)
    o = np.lexsort((ids, owner))
    ids_s, flags_s = ids[o], flags[o]
    first = np.zeros(len(ids_s), bool); first[starts] = True
    d = ids_s.astype(np.int64) - np.concatenate([[0], ids_s[:-1].astype(np.int64)])
    d[first] = ids_s[first].astype(np.int64)
    z_delta = zst(varint(counts)) + zst(varint(zigzag(d))) + (zst(np.packbits(flags_s)) if is_admin else 0)
    print(f"   entries: as-is {zst(eb):,}; SoA counts|ids {z_soa:,}; per-list delta varint (+flag bitplane) {z_delta:,}")

    # dedup identical lists
    keys = [l.tobytes() for l in lists]
    uniq = {}
    lid = np.empty(n, np.int64)
    for i, k in enumerate(keys):
        lid[i] = uniq.setdefault(k, len(uniq))
    nu = len(uniq)
    ul = [np.frombuffer(k, '<u4') for k in uniq]
    ucounts = np.array([len(x) for x in ul], np.int64)
    uids = np.concatenate(ul)
    print(f"   distinct lists: {nu:,} of {n:,} ({nu / n:.1%}); ids in distinct lists {ucounts.sum():,} of {counts.sum():,}")
    z_dedup_tbl = zst(np.concatenate([ucounts.astype('<u2').view(np.uint8), uids.view(np.uint8)]))
    z_lid_u32 = zst(lid.astype('<u4'))
    z_lid_dv = zst(varint(zigzag(np.diff(lid, prepend=0))))
    # list ids renumbered in first-use order already (setdefault in Hilbert order)
    print(f"   dedup: list table {z_dedup_tbl:,} + per-cell list id u32 {z_lid_u32:,} / delta-zigzag-varint {z_lid_dv:,}")
    best_cells = min(z_ids_dv, z_bitmap)
    best = best_cells + z_dedup_tbl + min(z_lid_u32, z_lid_dv)
    best_nodedup = best_cells + min(z_flat, z_soa, z_delta)
    print(f"   BEST no-dedup: cell ids {best_cells:,} + entries {min(z_flat, z_soa, z_delta):,} = {best_nodedup:,} ({mib(best_nodedup)} MiB)")
    print(f"   BEST dedup   : cell ids {best_cells:,} + table {z_dedup_tbl:,} + list ids {min(z_lid_u32, z_lid_dv):,} = {best:,} ({mib(best)} MiB)")
    if is_admin:
        # drop interior flag entirely (query still PIP-verifies every winner, see find_admin)
        ul2 = {}
        lid2 = np.empty(n, np.int64)
        for i, l in enumerate(lists):
            lid2[i] = ul2.setdefault(np.sort(l & np.uint32(0x7FFFFFFF)).tobytes(), len(ul2))
        u2 = [np.frombuffer(k, '<u4') for k in ul2]
        t2 = zst(np.concatenate([np.array([len(x) for x in u2], '<u2').view(np.uint8), np.concatenate(u2).view(np.uint8)]))
        l2 = min(zst(lid2.astype('<u4')), zst(varint(zigzag(np.diff(lid2, prepend=0)))))
        print(f"   no-interior-flag dedup: {len(ul2):,} distinct lists; table {t2:,} + list ids {l2:,} + cells {best_cells:,} = {t2 + l2 + best_cells:,} ({mib(t2 + l2 + best_cells)} MiB)")
    return res


analyse("admin", "admin_cells.bin", "admin_entries.bin", True)
analyse("place", "place_cells.bin", "place_entries.bin", False)
