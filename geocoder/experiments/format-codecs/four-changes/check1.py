"""Independent content checks of change (1) for one preset: baseline dir vs sets/<preset>1.
- strings: layout core end == new pool size, other tiers unchanged; every new-pool string referenced
  by some file of the set (purge complete); no referenced baseline string lost
- every string reference field of every file: non-core values unchanged; core values resolve to the
  IDENTICAL text in the new pool as in the old one (checked on every distinct old->new pair)
- every byte outside the reference fields unchanged (chunked compare of all records), except the
  place_nodes fields change (1) rewrites: parent_poly_id all NO_DATA, hidden slots zeroed, live
  lat/lng/type/pads unchanged
- admin index: same cells in the same order, same polygon SET per cell (flag masked), no flags,
  table fully used, no list stored twice
- place index: same (cell, live ids) content as the baseline minus hidden ids
usage: check1.py <max|withaddr>"""
import sys
from common4 import *

preset = sys.argv[1]
B = {"max": MAX, "withaddr": WA}[preset]
S = f"{W}/sets/{preset}1"
ok = True


def check(label, cond, extra=""):
    global ok
    ok &= bool(cond)
    print(f"{'OK  ' if cond else 'FAIL'} {label} {extra}", flush=True)


def pool(d):
    c = np.fromfile(f"{d}/strings_core.bin", np.uint8)
    nul = np.flatnonzero(c == 0)
    st = np.concatenate([[0], nul[:-1] + 1]).astype(np.int64)
    return c, st, nul


oc, ost, onul = pool(B)
nc, nst, nnul = pool(S)
OCL, NCL = len(oc), len(nc)
lb, ls = layout(B)["tiers"], layout(S)["tiers"]
check("layout core end == new strings_core size", ls[0]["end"] == NCL, f"({OCL:,} -> {NCL:,})")
check("layout other tiers unchanged", lb[1:] == ls[1:] and ls[0]["start"] == lb[0]["start"])
check("new pool: no duplicate strings", len(set(bytes(nc[a:b]) for a, b in zip(nst, nnul))) == len(nst))
n_used = np.zeros(len(nst), bool)
o_used = np.zeros(len(ost), bool)
o_idx = np.full(OCL, -1, np.int32); o_idx[ost] = np.arange(len(ost), dtype=np.int32)
n_idx = np.full(NCL, -1, np.int32); n_idx[nst] = np.arange(len(nst), dtype=np.int32)
map_new = np.full(len(ost), -1, np.int64)
conflicts = 0
CH = 8_000_000
for f, stride, fields in STRING_REFS:
    if not os.path.exists(f"{B}/{f}"):
        continue
    if os.path.samefile(f"{B}/{f}", f"{S}/{f}"):
        print(f"     {f}: same inode as baseline (no core refs)")
        continue
    mo = np.memmap(f"{B}/{f}", np.uint8, mode="r")
    mn = np.memmap(f"{S}/{f}", np.uint8, mode="r")
    check(f"{f}: same size", len(mo) == len(mn))
    n = len(mo) // stride
    refcols = sorted(o for _, o in fields)
    other = np.ones(stride, bool)
    for o in refcols:
        other[o:o + 4] = False
    if f == "place_nodes.bin":
        other[16:20] = False  # parent_poly_id, checked below
    bad_other = bad_noncore = 0
    for r0 in range(0, n, CH):
        r1 = min(n, r0 + CH)
        a = np.asarray(mo[r0 * stride:r1 * stride]).reshape(-1, stride)
        b = np.asarray(mn[r0 * stride:r1 * stride]).reshape(-1, stride)
        if f == "place_nodes.bin":
            live = a[:, 8:12].copy().view('<u4').ravel() != NO_DATA
            bad_other += int((a[live][:, other] != b[live][:, other]).any(axis=1).sum())
            hid = b[~live]
            bad_other += int((hid[:, 0:8] != 0).any(axis=1).sum() + (hid[:, 12:16] != 0).any(axis=1).sum())
            check("place_nodes: parent_poly_id all NO_DATA (chunk)", (b[:, 16:20] == 0xFF).all())
        else:
            bad_other += int((a[:, other] != b[:, other]).any(axis=1).sum())
        for o in refcols:
            va = a[:, o:o + 4].copy().view('<u4').ravel()
            vb = b[:, o:o + 4].copy().view('<u4').ravel()
            core = (va != NO_DATA) & (va < OCL)
            bad_noncore += int((va[~core] != vb[~core]).sum())
            vb_c = vb[core]
            assert (vb_c < NCL).all(), "remapped id outside new core"
            oi, ni = o_idx[va[core]], n_idx[vb_c]
            assert (oi >= 0).all() and (ni >= 0).all(), "id not at a string start"
            o_used[oi] = True
            n_used[ni] = True
            prev = map_new[oi]
            conflicts += int(((prev >= 0) & (prev != ni)).sum())
            map_new[oi] = ni
            conflicts += int((map_new[oi] != ni).sum())  # same old id mapped to two new ids within the chunk
    check(f"{f}: bytes outside the string fields unchanged", bad_other == 0, f"({bad_other} records differ)")
    check(f"{f}: non-core references unchanged", bad_noncore == 0)
check("each old string maps to exactly one new string", conflicts == 0)
xs = np.flatnonzero(map_new >= 0)
diff = sum(bytes(oc[ost[x]:onul[x]]) != bytes(nc[nst[map_new[x]]:nnul[map_new[x]]]) for x in xs)
check("every remapped reference resolves to identical text", diff == 0, f"({len(xs):,} distinct pairs, {diff} differ)")
check("new pool: every string referenced (purge complete)", n_used.all(), f"({int(n_used.sum()):,} of {len(nst):,})")
check("no referenced baseline string lost", int(o_used.sum()) == len(nst), f"(baseline referenced {int(o_used.sum()):,})")

# admin index
ac, nac = np.fromfile(f"{B}/admin_cells.bin", CELL_DT), np.fromfile(f"{S}/admin_cells.bin", CELL_DT)
check("admin_cells: same cell ids, same order", np.array_equal(ac["cell"], nac["cell"]), f"({len(ac):,} cells)")
eo, en = open(f"{B}/admin_entries.bin", "rb").read(), open(f"{S}/admin_entries.bin", "rb").read()
lo, ln = decode_entries(eo, ac["off"]), decode_entries(en, nac["off"])
check("admin entries: every cell keeps the same polygon set (flag masked)",
      all(np.array_equal(np.sort(x & 0x7FFFFFFF), y) for x, y in zip(lo, ln)))
check("admin entries: no INTERIOR_FLAG left", all(((y & 0x80000000) == 0).all() for y in ln))
offs = sorted(set(nac["off"].tolist()))
tiled = 0
for o in offs:
    check_at = tiled == o
    if not check_at:
        break
    tiled += 2 + 4 * int.from_bytes(en[o:o + 2], "little")
check("admin entries: table fully tiled by referenced lists", tiled == len(en), f"({len(offs):,} stored lists)")
check("admin entries: no list stored twice", len({en[o:o + 2 + 4 * int.from_bytes(en[o:o + 2], 'little')] for o in offs}) == len(offs))
print(f"     baseline flagged entries {sum(int(((x & 0x80000000) != 0).sum()) for x in lo):,}; lists whose masked order != sorted order "
      f"{sum(not np.array_equal(x & 0x7FFFFFFF, np.sort(x & 0x7FFFFFFF)) for x in lo):,}")

# place index
pn = np.fromfile(f"{B}/place_nodes.bin", PLACE_DT)
live = pn["name_id"] != NO_DATA
pc, npc = np.fromfile(f"{B}/place_cells.bin", CELL_DT), np.fromfile(f"{S}/place_cells.bin", CELL_DT)
pe, npe = open(f"{B}/place_entries.bin", "rb").read(), open(f"{S}/place_entries.bin", "rb").read()
want = {}
for c, l in zip(pc["cell"], decode_entries(pe, pc["off"])):
    l = l[live[l]]
    if len(l):
        want[int(c)] = l.tolist()
got = {int(c): l.tolist() for c, l in zip(npc["cell"], decode_entries(npe, npc["off"]))}
check("place index == baseline minus hidden slots (same order)", want == got, f"({len(pc):,} -> {len(npc):,} cells)")
check("place index: no hidden id left", all(live[np.array(v)].all() for v in got.values()))
print("ALL OK" if ok else "SOME CHECKS FAILED")
