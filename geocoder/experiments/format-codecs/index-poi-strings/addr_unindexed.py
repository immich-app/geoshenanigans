"""addr points that no geo cell lists (rebuild-at-install exceptions) and points listed in more
than one cell."""
import numpy as np
from lists import *

z = np.load(f"{W}/clean_a.npz")
ids = z["ids"]
ADDR_DT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("hn", "<u4"), ("st", "<u4"), ("pw", "<u4"), ("vo", "<u4"), ("vc", "<u4")])
a = np.fromfile(f"{D}/addr_points.bin", ADDR_DT)
occ = np.bincount(ids, minlength=len(a))
un = np.nonzero(occ == 0)[0]
print(f"addr points {len(a):,}; never listed {len(un):,}; listed in 2+ cells {int((occ > 1).sum()):,}")
u = a[un]
print("  unlisted: housenumber NO_DATA", int((u["hn"] == NO).sum()), "street NO_DATA", int((u["st"] == NO).sum()),
      "lat=lng=0", int(((u["lat"] == 0) & (u["lng"] == 0)).sum()))
print("  all points with housenumber NO_DATA:", int((a["hn"] == NO).sum()), "with lat=lng=0:", int(((a["lat"] == 0) & (a["lng"] == 0)).sum()))
