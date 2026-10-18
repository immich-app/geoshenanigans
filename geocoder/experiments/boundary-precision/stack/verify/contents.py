"""Independent content check of the shipped set (final/) vs the baseline (idx/smallest), and of
todays-format/ (the set queried). Written from the reader structs in server/src/lib.rs, not from
the stack scripts. Also writes verify/v11_polys.bin (baseline geometry + final name ids) and
verify/snaponly/ (baseline + snapped geometry with baseline name ids) for size / attribution."""
import os, sys, json, numpy as np

B = "/ssd/shrink/idx/smallest"
F = "/ssd/shrink/stack/final"
T = "/ssd/shrink/stack/todays-format"
V = "/ssd/shrink/stack/verify"
NO = 0xFFFFFFFF
FLAG = 0x80000000
PLACE_DT = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name_id', '<u4'), ('place_type', 'u1'),
                     ('pad1', 'u1'), ('pad2', '<u2'), ('parent_poly_id', '<u4')])
ADMIN_DT = np.dtype([('vertex_offset', '<u4'), ('vertex_count', '<u4'), ('name_id', '<u4'),
                     ('admin_level', 'u1'), ('pto', 'u1'), ('pad2', 'u1'), ('pad3', 'u1'),
                     ('area', '<f4'), ('cc', '<u2'), ('pad4', '<u2')])
CELL_DT = np.dtype([('cell', '<u8'), ('off', '<u4')])
bad = []


def check(label, cond, info=""):
    print(f"{'OK  ' if cond else 'FAIL'} {label} {info}")
    if not cond:
        bad.append(label)


def same(a, b):
    return open(a, 'rb').read() == open(b, 'rb').read()


# ---------------- file lists
fb = sorted(os.listdir(B))
ff = sorted(x for x in os.listdir(F) if x != "SHA256SUMS")
ft = sorted(os.listdir(T))
print("baseline:", fb)
print("final   :", ff)
print("final - baseline:", sorted(set(ff) - set(fb)), " baseline - final:", sorted(set(fb) - set(ff)))
check("final == today's minus place_cells/place_entries", set(ft) - set(ff) == {"place_cells.bin", "place_entries.bin"} and set(ff) <= set(ft))
for f in ff:
    if f != "admin_vertices.bin":
        check(f"final/{f} == todays-format/{f}", same(f"{F}/{f}", f"{T}/{f}"))

# ---------------- strings
def pool(d):
    b = np.fromfile(f"{d}/strings_core.bin", np.uint8)
    nul = np.flatnonzero(b == 0)
    starts = np.concatenate([[0], nul[:-1] + 1]).astype(np.int64)
    assert nul[-1] == len(b) - 1, "pool does not end in NUL"
    return b, starts, nul


ob, ost, onul = pool(B)
nb, nst, nnul = pool(F)
print(f"strings: baseline {len(ost):,} strings / {len(ob):,} B ; final {len(nst):,} strings / {len(nb):,} B")
lb = json.load(open(f"{B}/strings_layout.json"))
lf = json.load(open(f"{F}/strings_layout.json"))
check("layout core end == final strings_core size", lf["tiers"][0]["end"] == len(nb))
check("layout other tiers unchanged", lf["tiers"][1:] == lb["tiers"][1:])

ap_b = np.fromfile(f"{B}/admin_polygons.bin", ADMIN_DT)
ap_f = np.fromfile(f"{F}/admin_polygons.bin", ADMIN_DT)
pn_b = np.fromfile(f"{B}/place_nodes.bin", PLACE_DT)
pn_f = np.fromfile(f"{F}/place_nodes.bin", PLACE_DT)
check("admin_polygons slot count unchanged", len(ap_b) == len(ap_f), f"{len(ap_f):,}")
check("place_nodes slot count unchanged", len(pn_b) == len(pn_f), f"{len(pn_f):,}")


def text(buf, st, nul, ids):
    k = np.searchsorted(st, ids)
    ok = (k < len(st)) & (st[np.minimum(k, len(st) - 1)] == ids)
    return ok, k


def compare_names(label, old_ids, new_ids):
    m = old_ids != NO
    check(f"{label}: NO_DATA name ids stay NO_DATA", np.array_equal(old_ids == NO, new_ids == NO),
          f"({int((~m).sum()):,} NO_DATA)")
    o, n = old_ids[m].astype(np.int64), new_ids[m].astype(np.int64)
    ok_o, ko = text(ob, ost, onul, o)
    ok_n, kn = text(nb, nst, nnul, n)
    check(f"{label}: every baseline id at a string start", ok_o.all())
    check(f"{label}: every final id at a string start", ok_n.all())
    pairs = np.unique(np.stack([ko, kn], 1), axis=0)
    obb, nbb = ob.tobytes(), nb.tobytes()
    diff = 0
    for a, b in pairs:
        if obb[ost[a]:onul[a]] != nbb[nst[b]:nnul[b]]:
            diff += 1
    check(f"{label}: resolved name text identical for every slot", diff == 0,
          f"({len(pairs):,} distinct old->new pairs, {diff} differ, {len(o):,} named slots)")
    return set(kn.tolist()), set(ko.tolist())


ref_n_a, ref_o_a = compare_names("admin_polygons", ap_b["name_id"], ap_f["name_id"])
live = pn_b["name_id"] != NO
print(f"place_nodes: {int(live.sum()):,} live, {int((~live).sum()):,} hidden (baseline name_id NO_DATA)")
ref_n_p, ref_o_p = compare_names("place_nodes", pn_b["name_id"], pn_f["name_id"])
ref_n = ref_n_a | ref_n_p
check("final pool: every string is referenced (purge complete)", len(ref_n) == len(nst),
      f"(referenced {len(ref_n):,} of {len(nst):,})")
ref_o = ref_o_a | ref_o_p
print(f"baseline pool: referenced {len(ref_o):,} of {len(ost):,} strings")
# referenced baseline strings that vanished = would be a loss
check("no referenced baseline string lost", len(ref_o) == len(ref_n),
      f"(baseline referenced {len(ref_o):,} vs final {len(ref_n):,})")
nbt = nb.tobytes()
nset = {nbt[nst[i]:nnul[i]] for i in range(len(nst))}
print(f"final pool distinct texts {len(nset):,} of {len(nst):,} strings (duplicates kept {len(nst) - len(nset):,})")

# ---------------- place nodes
check("place_nodes: parent_poly_id all NO_DATA", np.all(pn_f["parent_poly_id"] == NO))
print(f"  baseline parent_poly_id NO_DATA: {int((pn_b['parent_poly_id'] == NO).sum()):,} of {len(pn_b):,}")
for f in ("lat", "lng", "place_type", "pad1", "pad2"):
    check(f"place_nodes live slots: {f} unchanged", np.array_equal(pn_b[f][live], pn_f[f][live]))
hid = pn_f[~live]
check("place_nodes hidden slots zeroed (lat=lng=type=0, name NO_DATA)",
      np.all(hid["lat"] == 0) and np.all(hid["lng"] == 0) and np.all(hid["place_type"] == 0) and np.all(hid["name_id"] == NO))
print(f"  hidden slots in baseline with lat=lng=0 already: {int(((pn_b['lat'] == 0) & (pn_b['lng'] == 0) & ~live).sum()):,}"
      f"; hidden pad1/pad2 nonzero in final: {int(((hid['pad1'] != 0) | (hid['pad2'] != 0)).sum())}")

# ---------------- admin polygons (other fields)
for f in ("vertex_offset", "vertex_count", "admin_level", "pto", "pad2", "pad3", "cc", "pad4"):
    check(f"admin_polygons {f} unchanged", np.array_equal(ap_b[f], ap_f[f]))
ac = ap_b["area"] != ap_f["area"]
rel = np.abs(ap_f["area"].astype(np.float64) - ap_b["area"]) / np.maximum(np.abs(ap_b["area"].astype(np.float64)), 1e-30)
print(f"  area changed in {int(ac.sum()):,} slots; relative change median {np.median(rel[ac]):.2e}, p99 {np.percentile(rel[ac], 99):.2e}, max {rel[ac].max():.3e}")
pos_b, pos_f = ap_b["area"] > 0, ap_f["area"] > 0
print(f"  polygons with area > 0: baseline {int(pos_b.sum()):,}, final {int(pos_f.sum()):,}; "
      f"became <= 0 (reader skips them): {int((pos_b & ~pos_f).sum())}; became > 0: {int((~pos_b & pos_f).sum())}")
gone = np.flatnonzero(pos_b & ~pos_f)
if len(gone):
    for i in gone[:10]:
        a, b = ap_b[i], ap_f[i]
        j = np.searchsorted(ost, a['name_id'])
        nm = bytes(ob[ost[j]:onul[j]]).decode() if a['name_id'] != NO else None
        print(f"    slot {i} L{a['admin_level']} '{nm}' area {a['area']:.3e} -> {b['area']:.3e} verts {a['vertex_count']}")
np.save(f"{V}/area_gone.npy", gone)

# derived sets for sizes / attribution
v11 = ap_b.copy(); v11["name_id"] = ap_f["name_id"]; v11.tofile(f"{V}/v11_polys.bin")
os.makedirs(f"{V}/snaponly", exist_ok=True)
so = ap_f.copy(); so["name_id"] = ap_b["name_id"]; so.tofile(f"{V}/snaponly/admin_polygons.bin")
for f in fb:
    if f not in ("admin_polygons.bin", "admin_vertices.bin"):
        p = f"{V}/snaponly/{f}"
        if not os.path.lexists(p):
            os.symlink(f"{B}/{f}", p)
if not os.path.lexists(f"{V}/snaponly/admin_vertices.bin"):
    os.symlink(f"{T}/admin_vertices.bin", f"{V}/snaponly/admin_vertices.bin")

# ---------------- admin index
def entries(d):
    c = np.fromfile(f"{d}/admin_cells.bin", CELL_DT)
    e = np.fromfile(f"{d}/admin_entries.bin", np.uint8)
    return c, e


def lst(e, o):
    n = int(e[o]) | int(e[o + 1]) << 8
    return e[o + 2:o + 2 + 4 * n].view('<u4')


cb, eb = entries(B)
cf, ef = entries(F)
check("admin_cells: same cell ids in same order", np.array_equal(cb["cell"], cf["cell"]), f"({len(cf):,} cells)")
mism = flag = 0
for ob_, of_ in zip(cb["off"], cf["off"]):
    a = np.sort(lst(eb, int(ob_)) & np.uint32(0x7FFFFFFF))
    b = lst(ef, int(of_))
    flag += int((b & np.uint32(FLAG) != 0).sum())
    if not np.array_equal(a, np.sort(b)):
        mism += 1
check("admin entries: every cell keeps the same polygon set (flag masked)", mism == 0, f"({mism} cells differ)")
check("admin entries: INTERIOR_FLAG cleared everywhere", flag == 0, f"({flag} flagged)")
uo = np.unique(cf["off"])
# walk the entries table: every list stored back to back, each distinct
pos, lists = 0, []
while pos < len(ef):
    n = int(ef[pos]) | int(ef[pos + 1]) << 8
    lists.append((pos, ef[pos + 2:pos + 2 + 4 * n].tobytes()))
    pos += 2 + 4 * n
check("admin entries: table fully tiled by lists", pos == len(ef))
check("admin entries: every stored list referenced", {p for p, _ in lists} == set(uo.tolist()),
      f"({len(lists):,} stored lists, {len(uo):,} distinct offsets)")
check("admin entries: no list stored twice", len({b for _, b in lists}) == len(lists))
bf = int((np.concatenate([lst(eb, int(o)) for o in np.unique(cb['off'])]) & np.uint32(FLAG) != 0).sum())
print(f"  baseline: {len(np.unique(cb['off'])):,} distinct list offsets, {bf:,} flagged entries")

print("FAILED:" if bad else "ALL CHECKS OK", bad)
