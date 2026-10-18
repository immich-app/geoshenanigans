"""Are node_offset / vertex_offset derivable on a day (each record's block
starts where the previous record's ends)? If so a patcher could recompute
every offset from the replayed counts/blocks and the offset fixups could go.

Usage: check_packed.py DAY_FULL_DIR"""
import os
import sys

import numpy as np

d = sys.argv[1]
NO = 0xFFFFFFFF
for fname, words, nodes in (("street_ways.bin", 3, "street_nodes.bin"), ("interp_ways.bin", 6, "interp_nodes.bin")):
    r = np.memmap(os.path.join(d, fname), dtype=np.uint32, mode="r").reshape(-1, words)
    off = r[:, 0].astype(np.uint64)
    cnt = (r[:, 1] & 0xFFFF).astype(np.uint64)
    live = cnt > 0
    o, c = off[live], cnt[live]
    expect = np.concatenate(([0], np.cumsum(c)[:-1])).astype(np.uint64)
    bad = int((o != expect).sum())
    end = int(o[-1] + c[-1]) if len(o) else 0
    n_nodes = os.path.getsize(os.path.join(d, nodes)) // 8
    print(f"{fname}: {len(r)} records, {int(live.sum())} with nodes, offsets != running sum: {bad}, "
          f"last block ends at {end} of {n_nodes} nodes")
a = np.memmap(os.path.join(d, "addr_points.bin"), dtype=np.uint32, mode="r").reshape(-1, 7)
voff = a[:, 5]
has = voff != NO
v = voff[has].astype(np.int64)
print(f"addr_points.bin: {len(a)} records, {int(has.sum())} with a vertex block, "
      f"offsets non-decreasing: {bool((np.diff(v) >= 0).all())}, first {int(v[0]) if len(v) else -1}, "
      f"addr_vertices bytes {os.path.getsize(os.path.join(d, 'addr_vertices.bin'))} (block sizes are implicit: next offset - offset)")
