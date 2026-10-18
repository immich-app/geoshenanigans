"""Side measurements (zstd 1.5.7 -18):
1. admin_polygons.bin zst before/after the strings_core purge remaps its name_ids (V3).
2. parent_poly_id := NO_DATA on the recommended (admin mode, all place types) place_nodes.bin,
   i.e. the same one-field change for the enhanced set."""
from common import *

a0 = zst_file(f"{SM}/admin_polygons.bin")
a3 = zst_file(f"{WORK}/variants/V3/admin_polygons.bin")
print(f"admin_polygons zst shipped {a0:,} -> purged-pool name ids {a3:,} ({a3 - a0:+,})")

pr = load("place_nodes.bin", PLACE_DT, REC)
live = pr["name_id"] != NO_DATA
print("recommended place_nodes records", len(pr), "live", int(live.sum()),
      {int(t): int((pr["place_type"] == t).sum()) for t in np.unique(pr["place_type"])})
r0 = zst(pr)
p = pr.copy()
p["parent_poly_id"] = NO_DATA
r1 = zst(p)
p["lat"][~live] = 0
p["lng"][~live] = 0
p["place_type"][~live] = 0
r2 = zst(p)
print(f"recommended place_nodes zst {r0:,} -> parent NO_DATA {r1:,} ({r1 - r0:+,}) -> + hidden zeroed {r2:,} ({r2 - r0:+,})")
