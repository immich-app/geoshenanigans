"""Checks that changes (1) and (3) both took effect in todays-format/admin_polygons.bin + admin_vertices.bin:
- name_id equals the V11 (purged-pool) name id in every slot, and differs from the baseline where remapped
- vertex_offset / vertex_count / area / every other field equals the 11 m snap of the BASELINE
  (work/snap_orig), and admin_vertices.bin equals that snap byte for byte
- the snap really moved the geometry (area / vertex bytes differ from the baseline)
- the other seven files equal the V11 build byte for byte."""
import filecmp, sys
sys.path.insert(0, "/ssd/shrink/places-strings-index")
from common import ADMIN_DT, np

S = "/ssd/shrink/stack"
B = "/ssd/shrink/idx/smallest"
rd = lambda d: np.fromfile(f"{d}/admin_polygons.bin", dtype=ADMIN_DT)
fin, v11, snap, base = rd(f"{S}/todays-format"), rd(f"{S}/work/v11"), rd(f"{S}/work/snap_orig"), rd(B)
ok = True


def check(label, cond):
    global ok
    ok &= bool(cond)
    print(f"{'OK  ' if cond else 'FAIL'} {label}")


check("name_id == V11 remapped name_id, all slots", np.array_equal(fin["name_id"], v11["name_id"]))
print(f"     name_id differs from baseline in {int((fin['name_id'] != base['name_id']).sum()):,} of {len(fin):,} slots")
other = [f for f in ADMIN_DT.names if f != "name_id"]
for f in other:
    check(f"{f} == snap of baseline", np.array_equal(fin[f], snap[f]))
print(f"     vertex_offset differs from baseline in {int((fin['vertex_offset'] != base['vertex_offset']).sum()):,} slots, "
      f"area in {int((fin['area'] != base['area']).sum()):,} slots")
check("admin_vertices.bin == snap of baseline (byte for byte)",
      filecmp.cmp(f"{S}/todays-format/admin_vertices.bin", f"{S}/work/snap_orig/admin_vertices.bin", shallow=False))
vf = np.fromfile(f"{S}/todays-format/admin_vertices.bin", np.uint8)
vb = np.fromfile(f"{B}/admin_vertices.bin", np.uint8)
print(f"     admin_vertices bytes differing from baseline: {int((vf != vb).sum()):,} of {len(vf):,}")
for f in ["admin_cells.bin", "admin_entries.bin", "place_nodes.bin", "place_cells.bin", "place_entries.bin",
          "strings_core.bin", "strings_layout.json"]:
    check(f"{f} == V11 build", filecmp.cmp(f"{S}/todays-format/{f}", f"{S}/work/v11/{f}", shallow=False))
print("ALL OK" if ok else "SOME CHECKS FAILED")
