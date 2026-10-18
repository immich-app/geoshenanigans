"""Per geo table: list sizes, singletons, exact duplicate lists, adjacency of duplicates."""
import numpy as np
import time
from lists import *

g = np.fromfile(f"{D}/geo_cells.bin", GEO_DT)
for key, fn in [("i", "interp_entries.bin"), ("a", "addr_entries.bin"), ("s", "street_entries.bin")]:
    t = time.time()
    e = np.fromfile(f"{D}/{fn}", np.uint8)
    rows = np.nonzero(g[key] != NO)[0]
    cnt, ids, starts = load_lists(e, g[key][rows])
    inv, first, nd = list_keys(cnt, ids, starts)
    dc = cnt[first]
    same_prev = np.concatenate([[False], inv[1:] == inv[:-1]])
    lid = np.repeat(np.arange(len(cnt)), cnt)
    inner = lid[1:] == lid[:-1]
    asc = bool(np.all(ids[1:][inner] > ids[:-1][inner]))
    del lid, inner
    single = cnt == 1
    nds = len(np.unique(ids[starts[single]]))
    print(f"== {fn}: lists {len(cnt):,} ids {int(cnt.sum()):,} max id {int(ids.max()):,}\n"
          f"   singleton lists {single.mean()*100:.1f}% ({int(single.sum()):,}; {nds:,} distinct)\n"
          f"   distinct lists {nd:,} ({nd/len(cnt)*100:.1f}%) holding {int(dc.sum()):,} ids\n"
          f"   bytes today {len(cnt)*2+int(cnt.sum())*4:,}  with identical lists stored once {nd*2+int(dc.sum())*4:,}\n"
          f"   list identical to the previous row's list {same_prev.mean()*100:.1f}%\n"
          f"   ids strictly ascending within each list: {asc}   ({time.time()-t:.0f}s)", flush=True)
    np.save(f"{W}/inv_{key}.npy", inv)
    np.save(f"{W}/first_{key}.npy", first)
    del e, cnt, ids, starts, inv, first
