"""patch_sizes.py PATCH.gcpatch: the client file list of a GCPATCH (v7/v8) as "name size" lines:
the new size of every file the patch's output dir holds (the streaming encoders' --size)."""
import struct
import subprocess
import sys

raw = subprocess.run(["/ssd/shrink/zstd157/bin/zstd", "-dc", sys.argv[1]], capture_output=True, check=True).stdout
pos = 8
version, flags, marker = struct.unpack_from("<III", raw, pos)
pos += 12
assert raw[:8] == b"GCPATCH\x00" or raw[:7] == b"GCPATCH", raw[:8]
assert marker == 0xFFFFFFF2, hex(marker)
(n,) = struct.unpack_from("<I", raw, pos)
pos += 4
for _ in range(n):
    (ln,) = struct.unpack_from("<H", raw, pos)
    pos += 2
    name = raw[pos:pos + ln].decode()
    pos += ln
    size, inl = struct.unpack_from("<QB", raw, pos)
    pos += 9
    if inl:
        pos += size
    print(name, size)
