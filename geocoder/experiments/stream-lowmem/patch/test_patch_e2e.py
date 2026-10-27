"""test_patch_e2e.py PATCHER GC DIFF: end-to-end applies of small synthetic sets.

PATCHER: the experiment's geocoder-patch; GC: gc; DIFF: a geocoder-diff (v7 or v8). Each case makes
an old and a new today's-format dir, diffs them, encodes the old one with gc, applies the patch to it
(sync and --threads) and compares the new encoded set byte for byte with gc's encoding of the new dir.
"""
import os
from pathlib import Path
import random
import re
import struct
import subprocess
import sys
import tempfile

PATCHER, GC, DIFF = (str(Path(a).resolve()) for a in sys.argv[1:4])
NO = 0xFFFFFFFF
ENV = dict(os.environ)
errors = []


def poi_dir(d, blocks):
    """blocks: per record, (id, None) for a point or (id, bytes of its vertex block: enc 0 to 2, 4-byte
    vertices). The id fills the record's first 8 bytes, so a record kept in the new set matches."""
    d.mkdir(parents=True)
    recs, verts = [], bytearray()
    for rid, b in blocks:
        if b is None:
            recs.append(struct.pack('<QIIIIIII', rid, NO, 0, 0, 0, 0, 0, 0))
        else:
            recs.append(struct.pack('<QIIIIIII', rid, len(verts), (len(b) - 10) // 4, 0, 0, 0, 0, 0))
            verts += b
    (d / 'poi_records.bin').write_bytes(b''.join(recs))
    (d / 'poi_vertices.bin').write_bytes(bytes(verts))


def geo_dir(d, cells, ways=1000):
    """cells: (cell index, street ids) per geo row, rows in cell order; no address or interp lists.
    The lists name `ways` street ways without nodes (a v8 patch drops ids no way replay maps)."""
    d.mkdir(parents=True)
    (d / 'street_ways.bin').write_bytes(b''.join(struct.pack('<IHHI', 0, 0, 0, w) for w in range(ways)))
    (d / 'street_nodes.bin').write_bytes(b'')
    rows, entries = [], bytearray()
    for k, ids in cells:
        rows.append(struct.pack('<QIII', (k + 1) << 27, len(entries), NO, NO))
        entries += struct.pack('<H', len(ids)) + struct.pack(f'<{len(ids)}I', *ids)
    (d / 'geo_cells.bin').write_bytes(b''.join(rows))
    (d / 'street_entries.bin').write_bytes(bytes(entries))
    (d / 'addr_entries.bin').write_bytes(b'')
    (d / 'interp_entries.bin').write_bytes(b'')


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


def decoder_peaks_at_most(family, limit):
    """The family's decoder queue (and --threads ring) peaks, from the patcher's report line."""
    def check(log, threads):
        lines = [line for line in log.splitlines() if line.strip().startswith(f'decode {family}:')]
        if not lines:
            return f'no decode {family} line'
        peaks = [int(v) for group in re.findall(r'peaks ((?:\d+ )+)B', lines[0]) for v in group.split()]
        print(f'{"threads" if threads else "sync"}: {lines[0].strip()}')
        return f'decoder held {max(peaks)} B > {limit} B: {lines[0].strip()}' if max(peaks) > limit else ''
    return check


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
    blocks = [(i, block(rng, rng.randrange(1, 9)) if i % 3 else None) for i in range(300)]
    poi_dir(root / 'same.a', blocks)
    poi_dir(root / 'same.b', blocks)
    apply_case(root, 'unchanged', root / 'same.a', root / 'same.b')
    edited = list(blocks)
    edited[100] = (100, bytes(10) + bytes(4 * ((len(blocks[100][1]) - 10) // 4)))
    poi_dir(root / 'edit.b', edited)
    apply_case(root, 'unchanged-records', root / 'same.a', root / 'edit.b')

    # Old child bytes the replay skips are dropped as the shared decoder produces them, not queued
    # until the child's next read: 4 MiB of vertices of a deleted run of POI polygons (the parent's
    # DELETE run), and of polygons whose records stay but whose blocks all change (an unchanged
    # parent, the child one DELETE and one INSERT run). The decoder holds at most 1 MiB.
    big = [(i, block(rng, 50)) for i in range(20000)]
    poi_dir(root / 'run.a', big)
    poi_dir(root / 'run.b', big[:1000] + big[19000:])
    apply_case(root, 'deleted-run', root / 'run.a', root / 'run.b', decoder_peaks_at_most('poi', 1 << 20))
    # (encoding byte 1 instead of 0, same vertex size: no byte of a block is kept, no MATCH reads)
    moved = [(i, b'\x01' + b[1:2] + bytes(x ^ 0x5A for x in b[2:])) for i, b in big[:19000]] + big[19000:]
    poi_dir(root / 'moved.b', moved)
    apply_case(root, 'replaced-children', root / 'run.a', root / 'moved.b', decoder_peaks_at_most('poi', 1 << 20))
    # The same for the geo index: the lists of a removed stretch of cells (4 MiB of street lists).
    cells = [(k, list(range(k % 950, k % 950 + 50))) for k in range(20000)]
    geo_dir(root / 'geo.a', cells)
    geo_dir(root / 'geo.b', cells[:1000] + cells[19000:])
    apply_case(root, 'removed-cells', root / 'geo.a', root / 'geo.b', decoder_peaks_at_most('gxc', 1 << 20))

assert not errors, '\n'.join(errors)
