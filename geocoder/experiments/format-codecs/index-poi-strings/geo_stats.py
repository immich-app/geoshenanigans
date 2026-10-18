"""geo_cells.bin structure: 20-byte rows (u64 level-17 S2 cell id, u32 byte offsets into
street/addr/interp_entries.bin or NO_DATA). Checks what is derivable."""
import numpy as np, time
D = "/ssd/shrink/idx/max"
W = "/ssd/shrink/max/index-poi-strings/work"
NO = np.uint32(0xFFFFFFFF)
t = time.time()
g = np.fromfile(f"{D}/geo_cells.bin", np.dtype([("cell", "<u8"), ("s", "<u4"), ("a", "<u4"), ("i", "<u4")]))
n = len(g); print("rows", n, f"{time.time()-t:.0f}s", flush=True)
cell = g["cell"]
assert np.all(cell[1:] > cell[:-1]), "not strictly ascending"
lsb = cell & (~cell + np.uint64(1))
print("level-17 lsb (1<<25):", bool(np.all(lsb == np.uint64(1 << 25))))
np.save(f"{W}/geo_cell.npy", cell)
# presence pattern
pres = np.zeros(n, np.uint8)
for b, k in enumerate("sai"):
    pres |= ((g[k] != NO).astype(np.uint8) << b)
u, c = np.unique(pres, return_counts=True)
print("presence pattern (bit0 street, bit1 addr, bit2 interp):", dict(zip(u.tolist(), c.tolist())))
np.save(f"{W}/geo_pres.npy", pres)
# delta in cell units
k = (cell >> np.uint64(25))
d = np.diff(k.astype(np.int64))
print("delta (in level-17 cells) percentiles", np.percentile(d, [10, 25, 50, 75, 90, 99, 99.9]).tolist(), "max", int(d.max()))
for lim in [1, 2, 4, 16, 128, 1 << 14, 1 << 21]:
    print(f"  delta <= {lim}: {np.mean(d <= lim)*100:.2f}%")
# offsets derivable from counts?
for key, fn in [("s", "street_entries.bin"), ("a", "addr_entries.bin"), ("i", "interp_entries.bin")]:
    e = np.memmap(f"{D}/{fn}", np.uint8, "r")
    off = g[key][g[key] != NO].astype(np.int64)
    # u32 wrap check: file size
    print(fn, "size", len(e), "lists", len(off), "offsets ascending:", bool(np.all(np.diff(off) > 0)))
    cnt = e[off].astype(np.int64) | (e[off + 1].astype(np.int64) << 8)
    exp = np.concatenate([[0], np.cumsum(2 + 4 * cnt)[:-1]])
    print("   offsets == prefix sum of list sizes:", bool(np.array_equal(exp, off)), " last end == file size:", int(exp[-1] + 2 + 4 * cnt[-1]) == len(e))
    print("   count pctl", np.percentile(cnt, [50, 90, 99, 99.9]).tolist(), "max", int(cnt.max()), "zero-count lists", int((cnt == 0).sum()), "total ids", int(cnt.sum()))
    np.save(f"{W}/cnt_{key}.npy", cnt.astype(np.uint16))
