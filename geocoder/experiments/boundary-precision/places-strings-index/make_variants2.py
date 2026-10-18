"""More admin-minimal variants in the current on-disk format (reader unchanged), on top of V3
(live place nodes sorted by L10 cell, purged strings_core):
V7  no place group at all (place_nodes/cells/entries absent: Index::load accepts the whole group missing)
V9  V3 + place coords quantised to 1e-6 deg
V12 V3 + admin_cells/admin_entries rebuilt from the shipped q2.5 vertices (cover_bench, RECT_FALLBACK)
"""
import os, shutil
from common import *
OUT = f"{WORK}/variants"
V3 = f"{OUT}/V3"
def clone(name, skip=()):
    d = f"{OUT}/{name}"
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    for f in os.listdir(V3):
        if f not in skip:
            os.symlink(os.path.realpath(f"{V3}/{f}"), f"{d}/{f}")
    return d
clone("V7", skip=("place_nodes.bin", "place_cells.bin", "place_entries.bin"))
d9 = clone("V9", skip=("place_nodes.bin",))
p = load("place_nodes.bin", PLACE_DT, V3).copy()
p["lat"] = (np.round(p["lat"].astype(np.float64) * 1e6) / 1e6).astype(np.float32)
p["lng"] = (np.round(p["lng"].astype(np.float64) * 1e6) / 1e6).astype(np.float32)
p.tofile(f"{d9}/place_nodes.bin")
rb = f"{WORK}/rebuilt_admin_fb"
if os.path.exists(f"{rb}/admin_cells.bin"):
    d12 = clone("V12", skip=("admin_cells.bin", "admin_entries.bin"))
    for f in ("admin_cells.bin", "admin_entries.bin"):
        os.symlink(f"{rb}/{f}", f"{d12}/{f}")
print("ok", sorted(os.listdir(OUT)))
