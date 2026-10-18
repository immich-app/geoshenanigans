#!/usr/bin/env python3
"""Shipped-byte costs of the search design (zstd 1.5.7 -19 unless noted).

1. The zero-byte builder changes: record files before / after (E1 primary-ring flag, E2 label
   link, E3 POI secondary-ring flag, E4 hidden nodes keep name_id).
2. The importance byte: dense per record slot vs sparse (delta-varint slot, byte) for the
   non-zero slots.
3. ISO codes; 4. the optional extra-names file."""
import os
import subprocess
import sys

import numpy as np

Z = "/ssd/shrink/zstd157/bin/zstd"
P = "/ssd/precision/build/out-nocap/planet/"
S = "/ssd/search-prod/sets/"
E = "/ssd/search-prod/extras/"
T = "/ssd/search-prod/tmp/sizes/"
os.makedirs(T, exist_ok=True)


def zsize(path_or_bytes, level=19):
    if isinstance(path_or_bytes, bytes):
        p = T + "blob"
        open(p, "wb").write(path_or_bytes)
        path_or_bytes = p
    out = subprocess.run([Z, f"-{level}", "-T16", "-c", "-q", path_or_bytes], capture_output=True, check=True).stdout
    return len(out)


def varint(v, out):
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def sparse(arr):
    nz = np.nonzero(arr)[0]
    out = bytearray()
    prev = -1
    for i in nz.tolist():
        varint(i - prev - 1, out)
        out.append(int(arr[i]))
        prev = i
    return bytes(out), len(nz)


rows = []
print("## builder-change file sizes (bytes raw / zstd-19)")
for s, files in (("minimal", [("admin-minimal/place_nodes.bin", "place_nodes.e3.bin"), ("admin-minimal/place_nodes.bin", "place_nodes.bin"),
                              ("admin-minimal/admin_polygons.bin", "admin_polygons.bin")]),
                 ("recommended", [("admin/place_nodes.bin", "place_nodes.e3.bin"), ("admin/place_nodes.bin", "place_nodes.bin"),
                                  ("quality/q2.5/admin_polygons.bin", "admin_polygons.bin"), ("poi/major/poi_records.bin", "poi_records.bin")]),
                 ("max", [("quality/uncapped/admin_polygons.bin", "admin_polygons.bin"), ("poi/all/poi_records.bin", "poi_records.bin")])):
    for orig, mod in files:
        a, b = P + orig, S + s + "/" + mod
        za, zb = zsize(a), zsize(b)
        print(f"{s} {orig} -> {mod}: raw {os.path.getsize(a)} -> {os.path.getsize(b)}; zstd {za} -> {zb} ({zb - za:+d})")

print("## importance byte")
for s in ("minimal", "recommended", "max"):
    for f in ("imp_places.u8", "imp_polys.u8", "imp_poi.u8"):
        p = E + s + "/" + f
        if not os.path.exists(p):
            continue
        arr = np.fromfile(p, dtype=np.uint8)
        sp, nnz = sparse(arr)
        print(f"{s} {f}: slots {len(arr)}, non-zero {nnz}; dense zstd {zsize(p)}; sparse raw {len(sp)} zstd {zsize(sp)}")

print("## ISO codes, extra names")
for s in ("minimal", "recommended"):
    p = E + s + "/iso_codes.tsv"
    print(f"{s} iso_codes.tsv raw {os.path.getsize(p)} zstd {zsize(p)}")
    p = E + s + "/extra_names.bin"
    print(f"{s} extra_names.bin raw {os.path.getsize(p)} zstd-19 {zsize(p)}")
