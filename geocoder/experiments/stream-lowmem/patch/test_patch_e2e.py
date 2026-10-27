"""test_patch_e2e.py PATCHER GC DIFF: end-to-end applies of small synthetic sets.

PATCHER: the experiment's geocoder-patch; GC: gc; DIFF: a geocoder-diff (v7 or v8; the REPLACE run
case needs v8). Each case makes an old and a new today's-format dir, diffs them, encodes the old one
with gc, applies the patch to it (sync and --threads) and compares the new encoded set byte for byte
with gc's encoding of the new dir. The later cases apply patches that must fail. zstd must be on PATH.
"""
import errno
import fcntl
import os
from pathlib import Path
import random
import re
import struct
import subprocess
import sys
import tempfile
import threading

PATCHER, GC, DIFF = (str(Path(a).resolve()) for a in sys.argv[1:4])
NO = 0xFFFFFFFF
ENV = dict(os.environ)
errors = []


def poi_dir(d, blocks):
    """blocks: per record, (id, None) for a point or (id, bytes of its vertex block: enc 0 to 2, 4-byte
    vertices). The id fills the record's first 8 bytes, so a record kept in the new set matches.
    d may hold other files already."""
    d.mkdir(parents=True, exist_ok=True)
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
        rows.append(struct.pack('<QIII', (k + 1) << 27, len(entries) if ids else NO, NO, NO))
        if ids:
            entries += struct.pack('<H', len(ids)) + struct.pack(f'<{len(ids)}I', *ids)
    (d / 'geo_cells.bin').write_bytes(b''.join(rows))
    (d / 'street_entries.bin').write_bytes(bytes(entries))
    (d / 'addr_entries.bin').write_bytes(b'')
    (d / 'interp_entries.bin').write_bytes(b'')


def ways_dir(d, names):
    """Street ways of two nodes each, named by `names`, in one geo cell; and the identity sidecar
    (record i is entity i) in d.meta, for a v8 diff's --identity."""
    geo_dir(d, [(0, [0])], ways=len(names))
    (d / 'street_ways.bin').write_bytes(b''.join(struct.pack('<IHHI', 2 * w, 2, 0, n) for w, n in enumerate(names)))
    (d / 'street_nodes.bin').write_bytes(
        b''.join(struct.pack('<4f', 47 + w * 1e-5, 8 + w * 1e-5, 47.001 + w * 1e-5, 8.001 + w * 1e-5) for w in range(len(names))))
    meta = d.with_name(d.name + '.meta')
    meta.mkdir()
    (meta / 'street.slot.u32').write_bytes(struct.pack(f'<{len(names)}I', *range(len(names))))


def unprivileged():
    """preexec_fn: nobody, when run as root (a FIFO's pipe counts against its first opener's limit)."""
    if os.geteuid() == 0:
        os.setgroups([])
        os.setgid(65534)
        os.setuid(65534)


def pipe_growth_refusal():
    """Why a pipe of this process can't grow to the 1 MiB a FIFO output needs ('' when it can). The
    patcher runs with this process's credentials, so its pipes are under the same limits."""
    r, w = os.pipe()
    try:
        got = fcntl.fcntl(w, 1031, 1 << 20)  # F_SETPIPE_SZ
        return '' if got >= 1 << 20 else f'it grows to {got} B'
    except OSError as e:
        return os.strerror(e.errno)
    finally:
        os.close(r)
        os.close(w)


def narrow_pipes():
    """preexec_fn: unprivileged() holding pipes of up to 1 MiB until it is past its pipe page limit
    (fs.pipe-user-pages-soft), where a new pipe gets the minimum size and none can grow: the patcher's
    own pipes can't grow. (Stopping at the first refused 1 MiB leaves up to 1 MiB minus a page free.)
    A descriptor limit reached first is a SKIP: the pipes are closed and the patcher runs as is."""
    unprivileged()
    held = []
    for _ in range(4096):
        try:
            r, w = os.pipe()
        except OSError as e:
            if e.errno not in (errno.EMFILE, errno.ENFILE):
                raise
            for fd in held:
                os.close(fd)
            os.write(2, b'SKIP: the descriptor limit comes before the pipe page limit\n')
            return
        held += [r, w]
        os.set_inheritable(r, True)
        os.set_inheritable(w, True)
        if fcntl.fcntl(w, 1032) < 65536:  # F_GETPIPE_SZ below the default: past the limit
            return
        try:
            fcntl.fcntl(w, 1031, 1 << 20)  # F_SETPIPE_SZ
        except OSError:
            pass
    os.write(2, b'SKIP: no pipe page limit reached\n')


def block(rng, vc):
    return bytes(10) + bytes(rng.randrange(256) for _ in range(4 * vc))


def run(cmd, **kw):
    kw.setdefault('env', ENV)
    return subprocess.run(cmd, capture_output=True, **kw)


def zstd(args, data):
    return subprocess.run(['zstd', '-q', '-c'] + args, input=data, capture_output=True, check=True).stdout


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


def apply_case(root, name, old, new, check=None, identity=False):
    """Diff old -> new, apply to gc's encoding of old in both modes, compare with gc's encoding of new.
    check(stderr, threads): extra checks on the patcher's log, returning an error or ''. identity: diff
    with ways_dir's sidecars."""
    ids = ['--identity', str(old.with_name(old.name + '.meta')), str(new.with_name(new.name + '.meta'))] if identity else []
    r = run([DIFF, str(old), str(new), '-o', str(root / f'{name}.gcpatch')] + ids)
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
    # An encoded set whose two address files are empty: the diff sends addr_points.bin's (empty) merge
    # with the streets after it and addr_vertices.bin later, as a full replacement. An empty parent
    # has no child bytes to replay with it.
    cells = [(k, [k % 10]) for k in range(50)]
    for d, rows in (('noaddr.a', cells), ('noaddr.b', cells[:40] + [(60, [3])])):
        geo_dir(root / d, rows)
        (root / d / 'addr_points.bin').write_bytes(b'')
        (root / d / 'addr_vertices.bin').write_bytes(b'')
    apply_case(root, 'empty-addresses', root / 'noaddr.a', root / 'noaddr.b')
    # A new set without an encoded family's files (the POI family, both files gone): its gc.manifest
    # names only the families it ships, as gc's encoding of the new dir does, and it decodes to the
    # new dir. A new set with part of the family (one of its two files) is refused before the replay.
    geo_dir(root / 'fam.a', cells)
    poi_dir(root / 'fam.a', blocks[:40])
    geo_dir(root / 'fam.b', cells)
    apply_case(root, 'removed-family', root / 'fam.a', root / 'fam.b')
    r = run([GC, 'decode', str(root / 'removed-family.out'), str(root / 'removed-family.dec')])
    if r.returncode or same_dirs(root / 'fam.b', root / 'removed-family.dec'):
        errors.append(f'removed-family: the new set does not decode to the new dir: rc={r.returncode} {r.stderr.decode()[-300:]}')
    for kept in ('poi_records.bin', 'poi_vertices.bin'):
        new = root / f'fam.{kept}'
        geo_dir(new, cells)
        poi_dir(new, blocks[:40])
        next(p for p in new.iterdir() if p.name.startswith('poi_') and p.name != kept).unlink()
        r = run([DIFF, str(root / 'fam.a'), str(new), '-o', str(root / f'{new.name}.gcpatch')])
        for threads in ([], ['--threads']):
            r = run([PATCHER, str(root / 'removed-family.old.enc'), str(root / f'{new.name}.gcpatch'), '-o',
                     str(root / f'{new.name}.out{len(threads)}')] + threads)
            err = r.stderr.decode()
            if r.returncode == 0 or 'family poi: the patch ships 1 of its 2 files' not in err:
                errors.append(f'partial-family (only {kept}{", threads" if threads else ""}): not refused up front: '
                              f'rc={r.returncode} {err[-300:]}')
    # A parent's REPLACE run (v8: the same street ways, renamed) whose children stay one MATCH run: the
    # old records are dropped as the node reads make the shared decoder produce them, not queued for
    # the whole run (2.4 MB of them).
    if '--identity' in run([DIFF]).stderr.decode():
        names = list(range(200000))
        ways_dir(root / 'named.a', names)
        ways_dir(root / 'named.b', [n + 1000000 for n in names])
        apply_case(root, 'replaced-parents', root / 'named.a', root / 'named.b', decoder_peaks_at_most('streets', 1 << 20),
                   identity=True)
    else:
        print('replaced-parents: SKIP, a v7 diff writes no REPLACE runs')

    # A staged apply (--only) writes into a dir that may hold files already. One there that is a
    # current file (a hard link, or a symlink to it) is refused before it is truncated, and the current
    # set keeps its bytes: an inline client file, a stage's own output, and an output its stage writes
    # with it (an entries file of the geo index).
    for d, vals in (('wp.a', range(4096)), ('wp.b', [v + (v == 2000) for v in range(4096)])):
        (root / d).mkdir()
        (root / d / 'way_parents.bin').write_bytes(struct.pack('<4096I', *vals))
        (root / d / 'meta.json').write_text(f'{{"set": "{d}"}}\n')
    r = run([DIFF, str(root / 'wp.a'), str(root / 'wp.b'), '-o', str(root / 'wp.gcpatch')])
    for case, cur, patch, only, name, link in (
            ('inline hard link', 'wp.a', 'wp.gcpatch', 'meta.json', 'meta.json', os.link),
            ('output symlink', 'wp.a', 'wp.gcpatch', 'way_parents.bin', 'way_parents.bin', os.symlink),
            ('coupled output hard link', 'geo.a', 'removed-cells.gcpatch', 'geo_cells.bin', 'street_entries.bin', os.link)):
        out = root / f'alias.{case.replace(" ", "-")}'
        out.mkdir()
        link(root / cur / name, out / name)
        before = {p.name: p.read_bytes() for p in (root / cur).iterdir()}
        r = run([PATCHER, str(root / cur), str(root / patch), '-o', str(out), '--only', only])
        err = r.stderr.decode()
        if r.returncode == 0 or f'is the current set\'s {name}' not in err:
            errors.append(f'alias ({case}): an output that is a current file was not refused: rc={r.returncode} {err[-300:]}')
        if {p.name: p.read_bytes() for p in (root / cur).iterdir()} != before:
            errors.append(f'alias ({case}): the current set changed')
    # The same with a current dir the patcher's user can't list (execute only; the patcher runs as
    # nobody when this runs as root): it can't tell which files an output may be, so it fails first.
    os.chmod(root, 0o777)
    cur, out = root / 'xonly.cur', root / 'xonly.out'
    cur.mkdir()
    out.mkdir()
    for f in ('way_parents.bin', 'meta.json'):
        (cur / f).write_bytes((root / 'wp.a' / f).read_bytes())
    os.link(cur / 'meta.json', out / 'meta.json')
    if os.geteuid() == 0:
        for p in (cur, out, cur / 'way_parents.bin', cur / 'meta.json'):
            os.chown(p, 65534, 65534)
    before = {p.name: p.read_bytes() for p in cur.iterdir()}
    cur.chmod(0o111)
    try:
        r = run([PATCHER, str(cur), str(root / 'wp.gcpatch'), '-o', str(out), '--only', 'meta.json'], preexec_fn=unprivileged,
                env=dict(ENV, TMPDIR=str(root)))
    finally:
        cur.chmod(0o755)
    err = r.stderr.decode()
    if r.returncode == 0 or 'Cannot list the current dir' not in err:
        errors.append(f'alias (unlistable current dir): not refused: rc={r.returncode} {err[-300:]}')
    if {p.name: p.read_bytes() for p in cur.iterdir()} != before:
        errors.append('alias (unlistable current dir): the current set changed')

    # A MATCH run longer than what is left of the old file is refused before any of it is written (the
    # replay stopped at the old file's end and went on as if the run was whole, and a FIFO output has
    # no size check after): the last run of POI records (a record by record replay) and of their
    # vertices (a byte stream, copied a run at a time), 5 longer, into a file and into a FIFO.
    match_blocks = [(i, block(rng, 3)) for i in range(300)]
    poi_dir(root / 'match.a', match_blocks)
    poi_dir(root / 'match.b', match_blocks[:100] + match_blocks[101:])
    r = run([DIFF, str(root / 'match.a'), str(root / 'match.b'), '-o', str(root / 'match.gcpatch')])
    r = run([PATCHER, str(root / 'match.a'), str(root / 'match.gcpatch'), '-o', str(root / 'match.out')])
    if r.returncode or same_dirs(root / 'match.b', root / 'match.out'):
        errors.append(f'match-past-end: the unedited patch does not apply: rc={r.returncode} {r.stderr.decode()[-300:]}')
    raw = zstd(['-d'], (root / 'match.gcpatch').read_bytes())
    refusal = pipe_growth_refusal()
    for name, kept, gone, rest in (('poi_records.bin', 100, 1, 199), ('poi_vertices.bin', 2200, 22, 4378)):
        runs = struct.pack('<BIBIBI', 0, kept, 2, gone, 0, rest)  # MATCH, DELETE, MATCH
        at = raw.find(runs)
        if at < 0 or raw.find(runs, at + 1) >= 0:
            errors.append(f'match-past-end ({name}): no single MATCH {kept}, DELETE {gone}, MATCH {rest} in the patch')
            continue
        bad = bytearray(raw)
        struct.pack_into('<I', bad, at + 11, rest + 5)
        patch = root / f'match.{name}.gcpatch'
        patch.write_bytes(zstd(['-3'], bytes(bad)))
        for fifo in (False, True):
            if fifo and refusal:
                print(f"match-past-end ({name}, fifo): SKIP, this user's pipes can't grow to 1 MiB ({refusal})")
                continue
            out = root / f'match.{name}.{"fifo" if fifo else "file"}'
            out.mkdir()
            reader = None
            if fifo:
                os.mkfifo(out / name)
                reader = subprocess.Popen(['cat', str(out / name)], stdout=subprocess.DEVNULL)
            try:
                r = run([PATCHER, str(root / 'match.a'), str(patch), '-o', str(out)], timeout=120)
            finally:
                if reader:
                    reader.kill()
                    reader.wait()
            err = r.stderr.decode()
            if r.returncode == 0 or f'Malformed merge sequence: {name}' not in err:
                errors.append(f'match-past-end ({name}{", fifo" if fifo else ""}): not refused: rc={r.returncode} {err[-300:]}')

    # A streamed output whose reader goes away fails the run (the entry pipeline's four outputs were
    # closed unchecked): geo_cells.bin is a FIFO whose reader closes at once, the patch rewrites 4 MiB
    # of rows (more than the pipe and the stdio buffer hold), SIGPIPE is ignored.
    # Where this user's pipes can't grow to 1 MiB the patcher refuses the FIFO before it writes: a SKIP
    # (the narrow-pipe case below checks that refusal).
    flat = [(k, []) for k in range(200000)]
    geo_dir(root / 'flat', flat)
    r = run([DIFF, str(root / 'flat'), str(root / 'flat'), '-o', str(root / 'flat.gcpatch')])
    if refusal := pipe_growth_refusal():
        print(f"fifo-output: SKIP, this user's pipes can't grow to 1 MiB ({refusal})")
    else:
        out = root / 'fifo.out'
        out.mkdir()
        fifo = out / 'geo_cells.bin'
        os.mkfifo(fifo)
        reader = threading.Thread(target=lambda: os.close(os.open(fifo, os.O_RDONLY)), daemon=True)
        reader.start()
        r = run([PATCHER, str(root / 'flat'), str(root / 'flat.gcpatch'), '-o', str(out)], restore_signals=False, timeout=120)
        if reader.is_alive():
            os.close(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK))
        err = r.stderr.decode()
        if r.returncode == 0 or 'Cannot write geo_cells.bin' not in err:
            errors.append(f'fifo-output: a failed geo_cells.bin was not reported: rc={r.returncode} {err[-300:]}')

    # A FIFO output whose pipe can't grow to 1 MiB fails the run up front (the entry pipeline's group
    # flush needs it; it went on with a 64 KiB or 8 KiB pipe): the patcher runs as a user past its
    # pipe page limit, geo_cells.bin is a FIFO the same user drains.
    os.chmod(root, 0o777)
    out = root / 'narrow.out'
    out.mkdir()
    os.chmod(out, 0o777)
    fifo = out / 'geo_cells.bin'
    os.mkfifo(fifo)
    os.chmod(fifo, 0o666)
    reader = subprocess.Popen(['cat', str(fifo)], stdout=subprocess.DEVNULL, preexec_fn=unprivileged)
    try:
        r = run([PATCHER, str(root / 'flat'), str(root / 'flat.gcpatch'), '-o', str(out)], preexec_fn=narrow_pipes,
                close_fds=False, timeout=120, env=dict(ENV, TMPDIR=str(root)))
    finally:
        reader.kill()
        reader.wait()
    err = r.stderr.decode()
    if 'SKIP' in err:
        print('narrow-pipe:', next(line for line in err.splitlines() if 'SKIP' in line))
    elif r.returncode == 0 or "pipe can't hold 1 MiB" not in err:
        errors.append(f'narrow-pipe: a FIFO output on a small pipe was not refused: rc={r.returncode} {err[-300:]}')

assert not errors, '\n'.join(errors)
