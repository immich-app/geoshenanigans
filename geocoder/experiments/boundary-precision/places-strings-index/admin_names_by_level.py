"""Bytes of strings_core names that ONLY admin polygons of a given admin_level reference (not a
place node, not another level), admin-minimal set. Shows what an Immich-only purge of names
Immich never stores (country names come from the code; county/state_district names) could save."""
from common import *
core = open(f"{SM}/strings_core.bin", "rb").read()
starts = np.load(f"{WORK}/core_starts.npy")
nul = np.nonzero(np.frombuffer(core, np.uint8) == 0)[0]; lens = nul - starts + 1
ap = load("admin_polygons.bin", ADMIN_DT); pn = load("place_nodes.bin", PLACE_DT)
ok = ap["name_id"] != NO_DATA
k = np.searchsorted(starts, ap["name_id"][ok]); lv = ap["admin_level"][ok]
place = np.zeros(len(starts), bool); place[np.searchsorted(starts, pn["name_id"][pn["name_id"] != NO_DATA])] = True
levels_of = {}
for kk, l in zip(k.tolist(), lv.tolist()): levels_of.setdefault(kk, set()).add(l)
print("polygons by admin_level:", {int(l): int((lv == l).sum()) for l in np.unique(lv)})
for L in sorted(set(lv.tolist())):
    only = [kk for kk, s in levels_of.items() if s == {L} and not place[kk]]
    b = b"".join(core[int(starts[i]):int(starts[i] + lens[i])] for i in sorted(only))
    print(f"L{L}: names only this level uses: {len(only):,} strings, raw {len(b):,}, zst {zst(b):,}")
