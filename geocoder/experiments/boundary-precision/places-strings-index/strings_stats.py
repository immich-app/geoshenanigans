"""strings_core.bin: who references what, and what a purge to admin-minimal's own references would save."""
import json
from common import *

core = open(f"{SM}/strings_core.bin", 'rb').read()
b = np.frombuffer(core, np.uint8)
nul = np.nonzero(b == 0)[0]
starts = np.concatenate([[0], nul[:-1] + 1]).astype(np.int64)
lens = (nul - starts).astype(np.int64)          # bytes excl NUL
ns = len(starts)
print(f"strings_core: {len(core):,} bytes, {ns:,} strings, mean len {lens.mean():.1f}")
print("trailing byte is NUL:", core[-1] == 0)


def ref_mask(ids, label):
    ids = ids[(ids != NO_DATA) & (ids < len(core))]
    k = np.searchsorted(starts, ids)
    ok = (k < ns) & (starts[np.minimum(k, ns - 1)] == ids)
    if (~ok).any():
        print(f"  WARN {label}: {(~ok).sum()} ids not at a string start")
    m = np.zeros(ns, bool)
    m[k[ok]] = True
    print(f"  {label}: {len(ids):,} refs -> {m.sum():,} distinct strings, {int((lens[m] + 1).sum()):,} bytes")
    return m


print("References from the admin-minimal set:")
ap = load("admin_polygons.bin", ADMIN_DT)
pn = load("place_nodes.bin", PLACE_DT)
m_admin = ref_mask(ap['name_id'], "admin-minimal admin_polygons.name_id")
live = pn['name_id'] != NO_DATA
m_place = ref_mask(pn['name_id'][live], "admin-minimal place_nodes.name_id (live)")
m_place_hidden_only = ref_mask(pn['name_id'][~live], "(hidden place nodes: name_id is NO_DATA)")
m_min = m_admin | m_place
used = int((lens[m_min] + 1).sum())
print(f"  UNION used by admin-minimal: {m_min.sum():,} strings, {used:,} bytes ({used / len(core):.1%})")
print(f"  admin-only {(m_admin & ~m_place).sum():,}, place-only {(m_place & ~m_admin).sum():,}, both {(m_admin & m_place).sum():,}")

print("Where the rest comes from (references in the recommended set, same build):")
ap_r = load("admin_polygons.bin", ADMIN_DT, REC)
pn_r = load("place_nodes.bin", PLACE_DT, REC)
m_admin_r = ref_mask(ap_r['name_id'], "recommended admin_polygons (q2.5, all levels)")
m_place_r = ref_mask(pn_r['name_id'][pn_r['name_id'] != NO_DATA], "recommended place_nodes (all types, live)")
POI_DT = np.dtype([('raw', 'V36')])
pr = np.fromfile(f"{REC}/poi_records.bin", dtype=np.uint8).reshape(-1, 36)
# PoiRecord layout: read field offsets from types.h / lib.rs (verified below)
print("  poi_records:", len(pr))
m_poi_r = ref_mask(pr[:, 16:20].copy().view('<u4').ravel(), "recommended poi_records.name_id (major tier)")
rest = ~m_min
print(f"Unused by admin-minimal: {rest.sum():,} strings, {int((lens[rest] + 1).sum()):,} bytes")
cats = {
    "admin polygons outside admin-minimal (L9-11, L15, postal etc)": rest & m_admin_r,
    "place nodes outside admin-minimal (suburb/neighbourhood/quarter)": rest & ~m_admin_r & m_place_r,
    "names of major-tier POIs (recommended set) that also name something outside core": rest & m_poi_r,
}
acc = np.zeros(ns, bool)
for k, m in cats.items():
    m = m & ~acc
    acc |= m
    print(f"  {k}: {m.sum():,} strings, {int((lens[m] + 1).sum()):,} bytes")
other = rest & ~acc
print(f"  other (POI names shared with streets/postcodes, unreferenced, ...): {other.sum():,} strings, {int((lens[other] + 1).sum()):,} bytes")
np.save(f"{WORK}/core_starts.npy", starts)
np.save(f"{WORK}/core_m_min.npy", m_min)
np.save(f"{WORK}/core_m_other.npy", other)

# Purged pool: keep only strings admin-minimal references, same (alphabetical) order.
keep = np.nonzero(m_min)[0]
blob = b"".join(core[int(starts[i]):int(starts[i] + lens[i] + 1)] for i in keep)
print(f"purged pool raw {len(blob):,}  zst {zst(blob):,}  (orig zst {zst(core):,})")
open(f"{WORK}/strings_core_purged.bin", "wb").write(blob)
# exact duplicate check (pool is interned, so expect none)
s = [core[int(starts[i]):int(starts[i] + lens[i])] for i in range(ns)]
print("exact duplicate strings in pool:", ns - len(set(s)))
# case/whitespace-insensitive near duplicates among used strings
used_s = [s[i] for i in keep]
norm = {}
for x in used_s:
    norm.setdefault(x.decode('utf-8', 'replace').strip().casefold(), 0)
    norm[x.decode('utf-8', 'replace').strip().casefold()] += 1
print("used strings that collapse under strip+casefold:", len(used_s) - len(norm))
# sample of 'other'
oi = np.nonzero(other)[0]
rng = np.random.default_rng(1)
print("sample of 'other':", [s[i].decode('utf-8', 'replace') for i in rng.choice(oi, 25, replace=False)])
mi = np.nonzero(m_min)[0]
print("sample of used:", [s[i].decode('utf-8', 'replace') for i in rng.choice(mi, 25, replace=False)])
# non-ASCII share
nonascii = np.array([any(c > 127 for c in s[i]) for i in mi])
print(f"used strings with non-ASCII bytes: {nonascii.mean():.1%}")
