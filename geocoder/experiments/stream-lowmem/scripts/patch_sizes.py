"""patch_sizes.py PATCH.gcpatch: the client file list of a GCPATCH (v7/v8) as "name size" lines:
the new size of every file the patch's output dir holds (the streaming encoders' --size). Reads the
decompressed patch only up to the end of the list."""
import struct
import subprocess
import sys

zstd = subprocess.Popen(["/ssd/shrink/zstd157/bin/zstd", "-dc", sys.argv[1]], stdout=subprocess.PIPE)
raw = zstd.stdout


def take(n):
    b = raw.read(n)
    assert len(b) == n, f"patch ends inside the file list ({len(b)} of {n} bytes)"
    return b


magic = take(8)
assert magic == b"GCPATCH\x00" or magic[:7] == b"GCPATCH", magic
version, flags, marker = struct.unpack("<III", take(12))
assert marker == 0xFFFFFFF2, hex(marker)
(n,) = struct.unpack("<I", take(4))
for _ in range(n):
    (ln,) = struct.unpack("<H", take(2))
    name = take(ln).decode()
    size, inl = struct.unpack("<QB", take(9))
    if inl:
        left = size
        while left:
            left -= len(take(min(left, 1 << 20)))
    print(name, size)
zstd.kill()
zstd.wait()
