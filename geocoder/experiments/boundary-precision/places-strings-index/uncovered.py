"""Admin polygons of the shipped admin-minimal set that no admin_entries list references
(their cover_polygon covering came back empty, e.g. the full-res loop failed S2 validation),
so find_admin can never return them."""
from common import *

ap = load("admin_polygons.bin", ADMIN_DT)
cells = load("admin_cells.bin", CELL_DT)
eb = open(f"{SM}/admin_entries.bin", "rb").read()
ids = np.concatenate(decode_entries(eb, cells["off"])) & np.uint32(0x7FFFFFFF)
seen = np.zeros(len(ap), bool)
seen[ids] = True
live = (ap["name_id"] != NO_DATA) & (ap["vertex_count"] > 0)
un = live & ~seen
core = open(f"{SM}/strings_core.bin", "rb").read()


def name(o):
    o = int(o)
    return core[o:core.index(b"\0", o)].decode()


print(f"live polygons {live.sum():,}; never referenced by any cell: {un.sum():,}")
for L in np.unique(ap["admin_level"][un]):
    m = un & (ap["admin_level"] == L)
    sub = ap[m]
    big = np.argsort(-sub["area"])[:8]
    names = [name(sub[i]["name_id"]) for i in big]
    print(f"  L{L}: {m.sum():,} rings; largest: {names}")
