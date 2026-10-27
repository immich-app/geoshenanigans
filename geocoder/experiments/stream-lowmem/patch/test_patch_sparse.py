"""test_patch_sparse.py PATCHER DIFF: malformed sparse delta sections are refused.

A diff of two way_parents.bin (4096 u32 records, three changed) gives a sparse delta, which applies.
The section is then edited, with the client file list kept in step so only the section is wrong:
a new size that ends in part of a record (a change writes that record whole), and a change past
the new file's last record (it was dropped unread). Both fail with "Malformed sparse delta"; a
change at the last record applies. zstd must be on PATH.
"""
import os
from pathlib import Path
import random
import struct
import subprocess
import sys
import tempfile

PATCHER, DIFF = (str(Path(a).resolve()) for a in sys.argv[1:3])
SPARSE_DELTA_STRIDE = 0xFC
N = 4096
errors = []


def zstd(args, data):
    return subprocess.run(['zstd', '-q', '-c'] + args, input=data, capture_output=True, check=True).stdout


with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as scratch:
    root = Path(scratch)
    rng = random.Random(20261027)
    old = [rng.randrange(1000) for _ in range(N)]
    new = list(old)
    for p, v in ((10, 7777), (2000, 8888), (4000, 9999)):
        new[p] = v
    for d, vals in (('a', old), ('b', new)):
        (root / d).mkdir()
        (root / d / 'way_parents.bin').write_bytes(struct.pack(f'<{N}I', *vals))
    r = subprocess.run([DIFF, str(root / 'a'), str(root / 'b'), '-o', str(root / 'p.gcpatch')], capture_output=True)
    assert r.returncode == 0, r.stderr.decode()[-500:]
    raw = zstd(['-d'], (root / 'p.gcpatch').read_bytes())
    head = struct.pack('<IQQII', SPARSE_DELTA_STRIDE, 4 * N, 4 * N, 4, 1)
    at = raw.find(head)
    assert at > 0 and raw.find(head, at + 1) < 0, 'no single way_parents sparse section'
    new_size_at = at + 12
    changes_at = at + len(head) + 4
    assert struct.unpack_from('<I', raw, changes_at - 4)[0] == 3
    listed = b'\x0f\x00way_parents.bin' + struct.pack('<Q', 4 * N)
    assert raw.count(listed) == 1, 'way_parents.bin not in the client file list once'

    def apply(name, new_size=4 * N, last_pos=4000):
        b = bytearray(raw)
        struct.pack_into('<Q', b, new_size_at, new_size)
        struct.pack_into('<I', b, changes_at + 2 * 8, last_pos)
        i = b.find(listed)
        struct.pack_into('<Q', b, i + len(listed) - 8, new_size)
        patch = root / f'{name}.gcpatch'
        patch.write_bytes(zstd(['-3'], bytes(b)))
        out = root / f'{name}.out'
        r = subprocess.run([PATCHER, str(root / 'a'), str(patch), '-o', str(out)], capture_output=True)
        return r.returncode, r.stderr.decode(), out / 'way_parents.bin'

    rc, err, got = apply('valid')
    if rc != 0 or got.read_bytes() != (root / 'b' / 'way_parents.bin').read_bytes():
        errors.append(f'valid: rc={rc} {err[-300:]}')
    rc, err, got = apply('last-record', last_pos=N - 1)
    want = list(new)
    want[4000], want[N - 1] = old[4000], 9999
    if rc != 0 or got.read_bytes() != struct.pack(f'<{N}I', *want):
        errors.append(f'a change at the last record: rc={rc} {err[-300:]}')
    for name, kw in (('partial-record', dict(new_size=4 * N - 1, last_pos=N - 1)),
                     ('past-the-end', dict(last_pos=N + 904))):
        rc, err, _ = apply(name, **kw)
        if rc == 0 or 'Malformed sparse delta' not in err:
            errors.append(f'{name}: not refused: rc={rc} {err[-300:]}')

assert not errors, '\n'.join(errors)
print('PASS')
