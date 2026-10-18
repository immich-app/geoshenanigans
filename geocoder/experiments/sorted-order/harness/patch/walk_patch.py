"""Where a .gcpatch's bytes go, by what they carry (v7 and v8 layouts).

Walks the decompressed patch section by section and assigns every byte to a
(category, file) stream: record INSERT / REPLACE bytes, child-stream
(nodes / vertex bytes) INSERT bytes, merge opcodes, offset fixups, id remap
pairs, cell changes, cell-list corrections, side-file values, strings,
headers. Each category's bytes are concatenated and compressed alone with
zstd -19 (an estimate of its share of the whole-stream compressed patch).

Usage: walk_patch.py PATCH.gcpatch [--files]"""
import collections
import struct
import subprocess
import sys

ZSTD = "/ssd/shrink/zstd157/bin/zstd"
NAMES = ["strings.bin", "street_ways.bin", "street_nodes.bin", "addr_points.bin", "interp_ways.bin", "interp_nodes.bin",
         "admin_polygons.bin", "admin_vertices.bin", "geo_cells.bin", "street_entries.bin", "addr_entries.bin",
         "interp_entries.bin", "admin_cells.bin", "admin_entries.bin", "poi_records.bin", "poi_vertices.bin",
         "poi_cells.bin", "poi_entries.bin", "place_nodes.bin", "place_cells.bin", "place_entries.bin",
         "addr_postcodes.bin", "admin_parents.bin", "way_parents.bin", "way_postcodes.bin", "postcode_centroids.bin",
         "postcode_centroid_cells.bin", "postcode_centroid_entries.bin", "postal_polygons.bin", "postal_vertices.bin",
         "addr_vertices.bin", "strings_core.bin", "strings_street.bin", "strings_addr.bin", "strings_postcode.bin",
         "strings_poi.bin", "interp_postcodes.bin"]
CHILD = {"street_nodes.bin", "interp_nodes.bin", "addr_vertices.bin", "admin_vertices.bin", "poi_vertices.bin",
         "postal_vertices.bin"}

path = sys.argv[1]
show_files = "--files" in sys.argv
raw = subprocess.run([ZSTD, "-dc", path], capture_output=True, check=True).stdout
whole = len(open(path, "rb").read())
streams = collections.defaultdict(bytearray)  # (category, file) -> bytes
pos = 0


def take(n, cat, f="-"):
    global pos
    b = raw[pos:pos + n]
    assert len(b) == n, (cat, f, pos, n)
    streams[(cat, f)] += b
    pos += n
    return b


def u16(cat, f="-"):
    return struct.unpack("<H", take(2, cat, f))[0]


def u32(cat, f="-"):
    return struct.unpack("<I", take(4, cat, f))[0]


def u64(cat, f="-"):
    return struct.unpack("<Q", take(8, cat, f))[0]


# header, client files, old sizes
take(8, "header")
version = u32("header")
u32("header")
assert u32("header") == 0xFFFFFFF2
for _ in range(u32("header")):
    ln = u16("header")
    take(ln, "header")
    size = u64("header")
    inl = take(1, "header")[0]
    if inl:
        take(size, "header")
for _ in range(u32("header")):
    take(12, "header")
# strings
assert u32("strings") == 0xFFFFFFF6
for _ in range(5):
    take(8, "strings")
shipped, sent = u32("strings"), u32("strings")
for t in range(5):
    if sent >> t & 1:
        u32("strings")
        take(u32("strings"), "strings")
        continue
    if not shipped >> t & 1:
        continue
    take(16, "strings")
    na, nd = u32("strings"), u32("strings")
    for _ in range(na):
        end = raw.index(b"\0", pos)
        take(end - pos + 1, "strings")
    take(nd * 4, "strings")
take(u32("strings") * 8, "strings")

ops_count = collections.Counter()
while True:
    fid = u32("section headers")
    if fid == 0xFFFFFFFF:
        break
    if fid in (0xFFFFFFFB, 0xFFFFFFFA, 0xFFFFFFF5, 0xFFFFFFF4):
        na, nr = u32("cell changes"), u32("cell changes")
        take((na + nr) * 8, "cell changes")
        continue
    if fid == 0xFFFFFFF0:
        f = NAMES[u32("cell corrections")]
        for _ in range(u32("cell corrections", f)):
            take(8, "cell corrections", f)
            nl, ng = u16("cell corrections", f), u16("cell corrections", f)
            if nl == 0xFFFF:
                nl = 0
            take((nl + ng) * 4, "cell corrections", f)
        continue
    if fid == 0xFFFFFFF6:  # secondary remap
        for _ in range(u32("id pairs")):
            f = NAMES[u32("id pairs")]
            take(u32("id pairs", f) * 8, "id pairs", f)
        continue
    if fid == 0xFFFFFFF3:  # parent-id remap
        take(u32("id pairs", "admin (parent remap)") * 8, "id pairs", "admin (parent remap)")
        take(u32("id pairs", "street (parent remap)") * 8, "id pairs", "street (parent remap)")
        take(u32("id pairs") * 8, "id pairs")
        continue
    if fid == 0xFFFFFFF7:
        f = NAMES[u32("cell corrections")]
        take(u64("cell corrections", f), "cell corrections", f)
        continue
    if fid == 0xFFFFFFF8:
        f = NAMES[u32("cell corrections")]
        for _ in range(u32("cell corrections", f)):
            take(8, "cell corrections", f)
            take(u16("cell corrections", f) * 4, "cell corrections", f)
        continue
    f = NAMES[fid]
    stride = u32("section headers", f)
    u64("section headers", f)
    u64("section headers", f)
    if stride == 0:
        u32("section headers", f)
        take(u64("section headers", f), "full replacement", f)
    elif stride == 0xFD:
        u32("section headers", f)
        u64("section headers", f)
    elif stride == 0xFB:
        take(u64("section headers", f), "cell corrections", f)
    elif stride == 0xFC:
        vs, _, n = u32("section headers", f), u32("section headers", f), u32("section headers", f)
        take(n * (4 + vs), "side-file values", f)
    elif stride == 0xFA:
        u32("section headers", f)
        u32("section headers", f)
        u32("section headers", f)
        take(u32("section headers", f) * 4, "side-file values", f)
        take(u32("section headers", f) * 8, "side-file values", f)
    else:
        u32("section headers", f)
        u32("section headers", f)
        rs, vs = u32("section headers", f), u32("section headers", f)
        take(rs + vs, "offset fixups", f)
        end = pos + 8 + u64("section headers", f)
        child = f in CHILD
        while pos < end:
            op = take(1, "merge opcodes", f)[0]
            count = u32("merge opcodes", f)
            ops_count[(f, op)] += 1
            if op == 1:
                take(count * stride, "child INSERT bytes" if child else "record INSERT", f)
            elif op == 3:
                take(count * stride, "record REPLACE", f)
take(len(raw) - pos, "end")


def zlen(b):
    if not b:
        return 0
    return len(subprocess.run([ZSTD, "-19", "-T4", "-c"], input=bytes(b), capture_output=True, check=True).stdout)


cat_raw = collections.Counter()
cat_bytes = collections.defaultdict(bytearray)
for (c, f), b in streams.items():
    cat_raw[c] += len(b)
    cat_bytes[c] += b
rows = [(c, cat_raw[c], zlen(cat_bytes[c])) for c in cat_raw]
total_z = sum(r[2] for r in rows)
print(f"{path}: v{version}, raw {len(raw)} B, whole-stream zstd {whole} B, categories zstd'd alone sum {total_z} B")
print(f"  {'zstd B':>10} {'%':>6} {'raw B':>11}  category")
for c, r, z in sorted(rows, key=lambda x: -x[2]):
    print(f"  {z:>10} {100 * z / total_z:5.1f}% {r:>11}  {c}")
if show_files:
    print("  per file:")
    for (c, f), b in sorted(streams.items(), key=lambda kv: -len(kv[1])):
        if f != "-" and len(b) > 1000:
            print(f"  {zlen(b):>10}        {len(b):>11}  {c}: {f}")
