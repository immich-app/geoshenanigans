#!/usr/bin/env python3
"""Build dirs/rec_nopc_nol11: rec_nopc with the boundary=postal_code
(admin_level 11) polygons also removed from the shared admin files. The
builder keeps L11 polygons in admin_polygons/admin_vertices and the admin cell
index AND duplicates them into postal_polygons/postal_vertices
(builder/src/cell_index.cpp:1177-1203); the reader skips L11 in every admin
walk (server/src/lib.rs:1782, 2696; 2029/2069 filter 8..10 / 7..8).

admin_vertices: L11 byte spans dropped, kept offsets rewritten.
admin_polygons: L11 slots tombstoned (slots stay id-aligned: POI parent_poly_id
and place parent_poly_id index this table).
admin_cells/admin_entries: L11 ids dropped from each entry list; cells left
empty dropped. Then zstd -18 sizes old vs new.
"""
import collections, mmap, os, struct, subprocess

REC = "/ssd/shrink/idx/recommended"
OUT = "/ssd/shrink/no-postcodes/dirs/rec_nopc_nol11"
SRC = "/ssd/shrink/no-postcodes/dirs/rec_nopc"
NO_DATA = 0xFFFFFFFF
POLY = struct.Struct("<IIIBBBBfHH")
os.makedirs(OUT, exist_ok=True)
CHANGED = ("admin_vertices.bin", "admin_polygons.bin", "admin_cells.bin", "admin_entries.bin")
for name in os.listdir(SRC):
    if name not in CHANGED and not os.path.exists(f"{OUT}/{name}"):
        os.link(f"{SRC}/{name}", f"{OUT}/{name}")


def mm(path):
    f = open(path, "rb")
    return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)


def zsize(path):
    p = subprocess.run(["zstd", "-18", "-T16", "-c", path], stdout=subprocess.PIPE, check=True)
    return len(p.stdout)


polys, verts = mm(f"{REC}/admin_polygons.bin"), mm(f"{REC}/admin_vertices.bin")
n = len(polys) // POLY.size
recs = [POLY.unpack_from(polys, i * POLY.size) for i in range(n)]
levels = collections.Counter(r[3] for r in recs if r[0] != NO_DATA)
is11 = bytearray(n)
spans = []
for i, r in enumerate(recs):
    if r[0] == NO_DATA:
        continue
    spans.append((r[0], i))
    if r[3] == 11:
        is11[i] = 1
spans.sort()
bytes_by_level = collections.Counter()
keep, new_off, pos = [], {}, 0
for k, (off, i) in enumerate(spans):
    end = spans[k + 1][0] if k + 1 < len(spans) else len(verts)
    bytes_by_level[recs[i][3]] += end - off
    if not is11[i]:
        keep.append((off, end))
        new_off[i] = pos
        pos += end - off
print("polygon slots:", n, " live by level:", dict(sorted(levels.items())))
print("vertex bytes by level:", dict(sorted(bytes_by_level.items())))
with open(f"{OUT}/admin_vertices.bin", "wb") as f:
    for off, end in keep:
        f.write(verts[off:end])
tomb = POLY.pack(NO_DATA, 0, NO_DATA, 0, 0, 0, 0, 0.0, 0, 0)
with open(f"{OUT}/admin_polygons.bin", "wb") as f:
    for i in range(n):
        if is11[i]:
            f.write(tomb)
        elif i in new_off:
            f.write(struct.pack("<I", new_off[i]) + polys[i * POLY.size + 4:(i + 1) * POLY.size])
        else:
            f.write(polys[i * POLY.size:(i + 1) * POLY.size])

cells, ents = mm(f"{REC}/admin_cells.bin"), mm(f"{REC}/admin_entries.bin")
nc = len(cells) // 12
seen = {}
new_cells, new_ents = bytearray(), bytearray()
ids_total = ids_l11 = cells_dropped = 0
for c in range(nc):
    cid, off = struct.unpack_from("<QI", cells, c * 12)
    if off not in seen:
        cnt = struct.unpack_from("<H", ents, off)[0]
        ids = struct.unpack_from(f"<{cnt}I", ents, off + 2)
        kept = [x for x in ids if not is11[x & 0x7FFFFFFF]]
        ids_total += cnt
        ids_l11 += cnt - len(kept)
        if kept:
            seen[off] = len(new_ents)
            new_ents += struct.pack("<H", len(kept)) + struct.pack(f"<{len(kept)}I", *kept)
        else:
            seen[off] = None
    if seen[off] is None:
        cells_dropped += 1
        continue
    new_cells += struct.pack("<QI", cid, seen[off])
print(f"cells {nc}, distinct entry lists {len(seen)}, entry ids {ids_total}, L11 ids {ids_l11}, cells dropped (only L11) {cells_dropped}")
open(f"{OUT}/admin_cells.bin", "wb").write(new_cells)
open(f"{OUT}/admin_entries.bin", "wb").write(new_ents)

MiB = 1 << 20
tot = [0, 0, 0, 0]
print(f'{"file":20s} {"raw old":>9s} {"raw new":>9s} {"zst old":>9s} {"zst new":>9s} (MiB, zstd -18)')
for name in CHANGED:
    a, b = f"{REC}/{name}", f"{OUT}/{name}"
    row = [os.path.getsize(a), os.path.getsize(b), zsize(a), zsize(b)]
    tot = [x + y for x, y in zip(tot, row)]
    print(f"{name:20s} " + " ".join(f"{x/MiB:9.2f}" for x in row))
print(f'{"total":20s} ' + " ".join(f"{x/MiB:9.2f}" for x in tot))
