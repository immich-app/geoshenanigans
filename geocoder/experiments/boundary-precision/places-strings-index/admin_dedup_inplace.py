"""Builder-only dedup of admin_entries: identical per-cell lists stored once, admin_cells offsets
point at the shared copy. The reader (lib.rs for_each_entry_fb) already follows any offset, so
no reader change. Written as variant V10 (on top of V3 + V6) and checked with grid_query."""
import os, shutil
from common import *

V3 = f"{WORK}/variants/V3"
d = f"{WORK}/variants/V10"
shutil.rmtree(d, ignore_errors=True)
os.makedirs(d)
for f in os.listdir(V3):
    if f not in ("admin_cells.bin", "admin_entries.bin"):
        os.symlink(os.path.realpath(f"{V3}/{f}"), f"{d}/{f}")
cells = load("admin_cells.bin", CELL_DT)
eb = open(f"{SM}/admin_entries.bin", 'rb').read()
lists = decode_entries(eb, cells['off'])
table = bytearray()
where = {}
newc = cells.copy()
for i, l in enumerate(lists):
    k = np.sort(l & np.uint32(0x7FFFFFFF)).astype('<u4').tobytes()
    if k not in where:
        where[k] = len(table)
        table += (len(k) // 4).to_bytes(2, 'little') + k
    newc['off'][i] = where[k]
newc.tofile(f"{d}/admin_cells.bin")
open(f"{d}/admin_entries.bin", "wb").write(table)
print(f"V10 admin_cells raw {newc.nbytes:,} zst {zst(newc):,}; admin_entries raw {len(table):,} zst {zst(bytes(table)):,}")
