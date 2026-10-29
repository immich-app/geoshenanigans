#!/usr/bin/env python3
"""Writes the identity sidecars geocoder-diff --identity reads (GCPATCH v8)
for a variant dir of a stable-slot (strategy-2) build.

Usage: gen_identity_sidecar.py <variant dir> <out dir>

Writes <out dir>/{street,interp,addr}.slot.u32: one u32 entity key per record
of street_ways.bin, interp_ways.bin and addr_points.bin. In a stable-slot file
record id == slot, and a slot live on two days holds the same OSM entity, so
a live record's key is its index; a tombstone's key is NO_DATA, which never
matches. A tombstone is the strategy-2 fill (as u32 words):
  street_ways  (off, cnt | pad, name)                          = 0, 0, NO_DATA
  interp_ways  (off, cnt | pad, street, start, end, type | pad) = 0, 0, NO_DATA, 0, 0, 0
  addr_points  (lat, lng, hn, street, parent, voff, vcnt)      = 0, 0, 0, 0, NO_DATA, NO_DATA, 0

A family whose record file the dir lacks (no-addresses has street ways only;
admin, quality and POI dirs borrow full's) gets no sidecar: the diff reads
none for it. tools/generate_patches.sh passes both days' sidecar dirs to the
diff: --identity OLD_DIR NEW_DIR. A family whose sidecar is missing, or
doesn't hold one key per record, merges by content (the diff warns). Days
kept in a canonical content order need keys from the OSM ids instead
(geocoder/experiments/sorted-order/identity.py).
"""
import mmap
import os
import struct
import sys
from array import array

NO_DATA = 0xFFFFFFFF
CHUNK_RECORDS = 2**20
# family -> (record file, tombstone record as u32 words)
TOMBSTONES = dict(
    street=("street_ways.bin", (0, 0, NO_DATA)),
    interp=("interp_ways.bin", (0, 0, NO_DATA, 0, 0, 0)),
    addr=("addr_points.bin", (0, 0, 0, 0, NO_DATA, NO_DATA, 0)),
)


def tombstone_indices(data, tombstone):
    """Record indices of the records equal to the tombstone, ascending."""
    stride = len(tombstone)
    at = data.find(tombstone)
    while at != -1:
        if at % stride == 0:
            yield at // stride
            at = data.find(tombstone, at + stride)
        else:
            # A hit off the record grid straddles two records; resume at the
            # next record boundary.
            at = data.find(tombstone, (at // stride + 1) * stride)


def write_sidecar(src_path, tombstone_words, out_path):
    tombstone = struct.pack("<%dI" % len(tombstone_words), *tombstone_words)
    stride = len(tombstone)
    size = os.path.getsize(src_path)
    if size % stride:
        raise ValueError("%s: size %d is not a multiple of %d" % (src_path, size, stride))

    n = size // stride
    dead = 0
    with open(src_path, "rb") as f, open(out_path, "wb") as out:
        data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) if size else b""
        dead_iter = tombstone_indices(data, tombstone)
        next_dead = next(dead_iter, n)
        for start in range(0, n, CHUNK_RECORDS):
            end = min(start + CHUNK_RECORDS, n)
            keys = array("I", range(start, end))
            while next_dead < end:
                keys[next_dead - start] = NO_DATA
                dead += 1
                next_dead = next(dead_iter, n)
            if sys.byteorder != "little":
                keys.byteswap()
            keys.tofile(out)
        if size:
            data.close()
    return n, dead


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2

    src, out = argv[1], argv[2]
    os.makedirs(out, exist_ok=True)
    for family, (name, tombstone) in TOMBSTONES.items():
        path = os.path.join(src, name)
        if not os.path.exists(path):
            continue

        n, dead = write_sidecar(path, tombstone, os.path.join(out, family + ".slot.u32"))
        print("%s: %d records, %d tombstones" % (family, n, dead))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
