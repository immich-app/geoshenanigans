"""test_patch_e2e.py PATCHER GC DIFF: end-to-end applies of small synthetic sets.

PATCHER: the experiment's geocoder-patch; GC: gc; DIFF: a geocoder-diff (v7 or v8). Each case makes
an old and a new today's-format dir, diffs them, encodes the old one with gc, applies the patch to it
(sync and --threads) and compares the new encoded set byte for byte with gc's encoding of the new dir.
"""
import os
from pathlib import Path
import random
import struct
import subprocess
import sys
import tempfile

PATCHER, GC, DIFF = (str(Path(a).resolve()) for a in sys.argv[1:4])
NO = 0xFFFFFFFF
ENV = dict(os.environ)
errors = []


def poi_dir(d, blocks):
    """blocks: per record, None (a point) or the bytes of its vertex block (enc 0, 4-byte vertices)."""
    d.mkdir(parents=True)
    recs, verts = [], bytearray()
    for i, b in enumerate(blocks):
        if b is None:
            recs.append(struct.pack('<QIIIIIII', 1000 + i, NO, 0, 0, 0, 0, 0, 0))
        else:
            recs.append(struct.pack('<QIIIIIII', 1000 + i, len(verts), (len(b) - 10) // 4, 0, 0, 0, 0, 0))
            verts += b
    (d / 'poi_records.bin').write_bytes(b''.join(recs))
    (d / 'poi_vertices.bin').write_bytes(bytes(verts))


def block(rng, vc):
    return bytes(10) + bytes(rng.randrange(256) for _ in range(4 * vc))


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, env=ENV, **kw)


def same_dirs(a, b):
    fa = sorted(p.name for p in a.iterdir())
    fb = sorted(p.name for p in b.iterdir())
    if fa != fb:
        return f'files {fa} != {fb}'
    bad = [f for f in fa if (a / f).read_bytes() != (b / f).read_bytes()]
    return f'differ: {bad}' if bad else ''


def apply_case(root, name, old, new, check=None):
    """Diff old -> new, apply to gc's encoding of old in both modes, compare with gc's encoding of new.
    check(stderr, threads): extra checks on the patcher's log, returning an error or ''."""
    r = run([DIFF, str(old), str(new), '-o', str(root / f'{name}.gcpatch')])
    if r.returncode:
        errors.append(f'{name}: diff failed: {r.stderr.decode()[-500:]}')
        return
    for d, enc in ((old, root / f'{name}.old.enc'), (new, root / f'{name}.new.enc')):
        r = run([GC, 'encode', str(d), str(enc)])
        if r.returncode:
            errors.append(f'{name}: gc encode {d} failed: {r.stderr.decode()[-300:]}')
            return
    for threads in (False, True):
        out = root / f'{name}.out{".t" if threads else ""}'
        r = run([PATCHER, str(root / f'{name}.old.enc'), str(root / f'{name}.gcpatch'), '-o', str(out)]
                + (['--threads'] if threads else []))
        mode = 'threads' if threads else 'sync'
        if r.returncode:
            errors.append(f'{name} ({mode}): patcher rc={r.returncode}: {r.stderr.decode()[-500:]}')
            continue
        diff = same_dirs(root / f'{name}.new.enc', out)
        if diff:
            errors.append(f'{name} ({mode}): new set {diff}')
        if check:
            e = check(r.stderr.decode(), threads)
            if e:
                errors.append(f'{name} ({mode}): {e}')


with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as scratch:
    root = Path(scratch)
    rng = random.Random(20261027)

    # An unchanged parent of an encoded family (COPY_OLD poi_records.bin) replays with its child:
    # both files unchanged, then the records unchanged and one vertex block edited.
    blocks = [block(rng, rng.randrange(1, 9)) if i % 3 else None for i in range(300)]
    poi_dir(root / 'same.a', blocks)
    poi_dir(root / 'same.b', blocks)
    apply_case(root, 'unchanged', root / 'same.a', root / 'same.b')
    edited = list(blocks)
    edited[100] = bytes(10) + bytes(4 * ((len(blocks[100]) - 10) // 4))
    poi_dir(root / 'edit.b', edited)
    apply_case(root, 'unchanged-records', root / 'same.a', root / 'edit.b')

assert not errors, '\n'.join(errors)
