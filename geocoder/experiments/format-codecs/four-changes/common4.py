"""Shared layouts and helpers for the four-changes track (max / withaddr presets).
Record layouts: builder/src/types.h, server/src/lib.rs:170-320."""
import json, os, subprocess
import numpy as np

W = "/ssd/shrink/max/four-changes"
MAX = "/ssd/shrink/idx/max"
WA = "/ssd/shrink/idx/withaddr"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
NO_DATA = 0xFFFFFFFF
MiB = 1 << 20

PLACE_DT = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name_id', '<u4'), ('place_type', 'u1'),
                     ('pad1', 'u1'), ('pad2', '<u2'), ('parent_poly_id', '<u4')])
ADMIN_DT = np.dtype([('vertex_offset', '<u4'), ('vertex_count', '<u4'), ('name_id', '<u4'),
                     ('admin_level', 'u1'), ('pto', 'u1'), ('pad2', 'u1'), ('pad3', 'u1'),
                     ('area', '<f4'), ('cc', '<u2'), ('pad4', '<u2')])
CELL_DT = np.dtype([('cell', '<u8'), ('off', '<u4')])

# Every u32 string reference the reader can follow, per file: (file, record stride, [(field, byte offset)]).
# Sidecars are one u32 per record.
STRING_REFS = [
    ("admin_polygons.bin", 24, [("name_id", 8)]),
    ("postal_polygons.bin", 24, [("name_id", 8)]),
    ("place_nodes.bin", 20, [("name_id", 8)]),
    ("street_ways.bin", 12, [("name_id", 8)]),
    ("addr_points.bin", 28, [("housenumber_id", 8), ("street_id", 12)]),
    ("interp_ways.bin", 24, [("street_id", 8)]),
    ("poi_records.bin", 36, [("name_id", 16), ("parent_street_id", 24), ("parent_postcode_id", 28)]),
    ("postcode_centroids.bin", 16, [("postcode_id", 8)]),
    ("way_postcodes.bin", 4, [("pc", 0)]),
    ("addr_postcodes.bin", 4, [("pc", 0)]),
    ("interp_postcodes.bin", 4, [("pc", 0)]),
]


def u32col(path, stride, off, mode="r"):
    """Strided u32 view of one field over a whole record file (memmap)."""
    m = np.memmap(path, dtype=np.uint8, mode=mode)
    assert len(m) % stride == 0, (path, len(m), stride)
    n = len(m) // stride
    return np.ndarray((n,), dtype='<u4', buffer=m, offset=off, strides=(stride,))


def layout(d):
    return json.load(open(f"{d}/strings_layout.json"))


def zst_file(path, threads=1):
    """zstd 1.5.7 -18 size of a file, compressing the file by path (= compress_index.py)."""
    p = subprocess.run([ZSTD, "-18", f"-T{threads}", "-q", "-c", path], stdout=subprocess.PIPE, check=True)
    return len(p.stdout)


def decode_entries(entries_bytes, offsets):
    out = []
    for o in offsets:
        o = int(o)
        c = int.from_bytes(entries_bytes[o:o + 2], 'little')
        out.append(np.frombuffer(entries_bytes, dtype='<u4', count=c, offset=o + 2))
    return out
