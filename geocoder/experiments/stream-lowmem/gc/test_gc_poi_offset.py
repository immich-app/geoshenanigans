"""Run against a built gc executable: a POI set whose B-open file ends past 4 GiB, with a point record
after the last polygon, must fail to encode (R3 stores that point's offset, the file size, in 32 bits)
rather than write a set its decoder rejects. A small set with a trailing point round-trips.

The big case writes a ~4 GiB poi_vertices.bopen under TMPDIR (removed at the end); the input is a
sparse file. 1024 closed all-zero rings of 2^20 vertices: every polygon starts below 2^32, the
B-open file ends at 4,294,973,440 bytes."""
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

NO = 0xFFFFFFFF
binary = str(Path(sys.argv[1]).resolve())


def record(offset, count):
    return struct.pack('<QIIIIIII', 7, offset, count, 0, 0, 0, 0, 0)


def write_set(d, polygons, trailing_points):
    """polygons: vertex counts of all-zero closed rings (enc 0), back to back."""
    d.mkdir()
    recs, off = [], 0
    for vc in polygons:
        recs.append(record(off, vc))
        off += 10 + 4 * vc
    recs += [record(NO, 0)] * trailing_points
    (d / 'poi_records.bin').write_bytes(b''.join(recs))
    with open(d / 'poi_vertices.bin', 'wb') as f:
        f.truncate(off)


errors = []
with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as scratch:
    root = Path(scratch)
    small = root / 'small'
    write_set(small, [3, 5], 2)
    enc, dec = root / 'small.enc', root / 'small.dec'
    r = subprocess.run([binary, 'encode', str(small), str(enc), '--only', 'poi', '--no-link'], capture_output=True)
    r2 = subprocess.run([binary, 'decode', str(enc), str(dec), '--only', 'poi', '--no-link'], capture_output=True)
    if r.returncode or r2.returncode or any(
            (small / f).read_bytes() != (dec / f).read_bytes() for f in ('poi_records.bin', 'poi_vertices.bin')):
        errors.append('small set with a trailing point did not round-trip')

    big = root / 'big'
    write_set(big, [1 << 20] * 1024, 1)
    out = root / 'big.enc'
    r = subprocess.run([binary, 'encode', str(big), str(out), '--only', 'poi', '--no-link'], capture_output=True)
    if r.returncode == 0 or b'offset overflow' not in r.stderr:
        errors.append(f'4 GiB set with a trailing point encoded: rc={r.returncode} {r.stderr.decode()[-300:]}')
assert not errors, '\n'.join(errors)
