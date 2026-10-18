"""place_nodes.bin + place_cells/place_entries stats for the admin-minimal set."""
from common import *

pn = load("place_nodes.bin", PLACE_DT)
n = len(pn)
print(f"records: {n} ({n * 20} bytes)")
live = pn['name_id'] != NO_DATA
print(f"live (name_id != NO_DATA): {live.sum()}  hidden/tombstone: {(~live).sum()}")
names = {0: 'city', 1: 'town', 2: 'village', 3: 'suburb', 4: 'hamlet', 5: 'neighbourhood', 6: 'quarter'}
for t in np.unique(pn['place_type']):
    m = pn['place_type'] == t
    print(f"  type {t} {names.get(int(t), '?')}: {m.sum()} (live {(m & live).sum()})")
print("pad1 nonzero:", (pn['pad1'] != 0).sum(), " pad2 nonzero:", (pn['pad2'] != 0).sum())
pp = pn['parent_poly_id']
print("parent_poly_id NO_DATA:", (pp == NO_DATA).sum())
# tombstones: name NO_DATA and lat/lng 0?
tomb = (~live) & (pn['lat'] == 0) & (pn['lng'] == 0)
print("hidden with lat=lng=0 (strategy-2 tombstone slots):", tomb.sum())
print("distinct name_ids (live):", len(np.unique(pn['name_id'][live])))

# ordering check
print("sorted by (type,name_id)?", bool(np.all(np.diff(pn['place_type'].astype(np.int64)) >= 0)))

# place cells
pc = load("place_cells.bin", CELL_DT)
pe = open(f"{SM}/place_entries.bin", 'rb').read()
print(f"place_cells: {len(pc)} cells")
lists = decode_entries(pe, pc['off'])
counts = np.array([len(l) for l in lists])
print(f"place_entries: total ids {counts.sum()}, max per cell {counts.max()}, mean {counts.mean():.2f}")
allids = np.concatenate(lists)
print("ids unique?", len(np.unique(allids)) == len(allids), " ids covered:", len(np.unique(allids)), "of", n)
cover = np.zeros(n, bool); cover[allids] = True
print("records not in any cell:", (~cover).sum(), " of which hidden:", ((~cover) & (~live)).sum())
print("hidden records still in cells:", (cover & ~live).sum())
# does the entries file have 2+4k byte layout with no sharing?
print("entries bytes expected", int((2 + 4 * counts).sum()), "actual", len(pe))
