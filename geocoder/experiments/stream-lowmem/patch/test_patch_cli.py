"""Run against a built geocoder-patch (the experiment's, with libzstd or the zstd CLI fallback):
command-line checks that need no patch data. zstd must be on PATH.

--only on an encoded installed set (a gc.manifest in the current dir) is refused before anything is
written: a staged apply would leave a gc.manifest naming every family with only one stage's keys.

The output dir is created as named, quotes and all, and no part of its name runs as a command; the
same for the patch path, which is decompressed as named.

A patch shorter than its 8-byte magic is refused as truncated.

A TMPDIR too small for the decompressed patch fails the run with an error (no SIGBUS from a page of
the mapped patch.raw with no storage behind it) and the scratch dir goes; one with room for the
patch but not for its decompression bound (a frame without a content size bounds every block at
128 KiB) still decompresses it. TMPDIR there is a size-limited tmpfs in a private mount namespace
(unshare), gone with the run; skipped where user namespaces are unavailable."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile

binary = str(Path(sys.argv[1]).resolve())
errors = []
with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as scratch:
    root = Path(scratch)
    cur = root / 'cur'
    cur.mkdir()
    (cur / 'gc.manifest').write_text('gc.families streets\ngc.version 1\n')
    patch = root / 'patch.gcpatch'
    patch.write_bytes(b'not read')
    out = root / 'out'
    r = subprocess.run([binary, str(cur), str(patch), '-o', str(out), '--only', 'street_ways.bin,street_nodes.bin'],
                       capture_output=True)
    err = r.stderr.decode()
    if r.returncode == 0 or '--only' not in err or 'encoded installed set' not in err:
        errors.append(f'--only on an encoded set was not refused: rc={r.returncode} {err[-300:]}')
    if out.exists() and any(out.iterdir()):
        errors.append(f'--only on an encoded set wrote {sorted(p.name for p in out.iterdir())}')

    plain = root / 'plain'
    plain.mkdir()
    for name in ["user's update/new", "x'; touch pwned; 'y/new"]:
        out = root / name
        r = subprocess.run([binary, str(plain), str(patch), '-o', str(out)], capture_output=True, cwd=root)
        if not out.is_dir():
            errors.append(f'output dir {name!r} was not created: rc={r.returncode} {r.stderr.decode()[-300:]}')
        if (root / 'pwned').exists():
            errors.append(f'output dir {name!r} ran a command (pwned exists)')

    def compressed(payload):
        return subprocess.run(['zstd', '-q', '-c'], input=payload, capture_output=True, check=True).stdout

    for i, name in enumerate(["user's patch.gcpatch", "x'; touch pwned; 'y.gcpatch"]):
        named = root / name
        named.write_bytes(compressed(b'NOT A GCPATCH'))
        r = subprocess.run([binary, str(plain), str(named), '-o', str(root / f'named{i}')], capture_output=True, cwd=root)
        err = r.stderr.decode()
        if 'Bad magic' not in err:
            errors.append(f'patch {name!r} was not decompressed as named: rc={r.returncode} {err[-300:]}')
        if (root / 'pwned').exists():
            errors.append(f'patch {name!r} ran a command (pwned exists)')

    short = root / 'short.gcpatch'
    short.write_bytes(compressed(b'GCPATCH'))
    r = subprocess.run([binary, str(plain), str(short), '-o', str(root / 'short')], capture_output=True)
    err = r.stderr.decode()
    if r.returncode == 0 or 'Truncated patch' not in err or 'magic' not in err:
        errors.append(f'a 7-byte patch was not refused as truncated: rc={r.returncode} {err[-300:]}')

    # The patcher's output comes back as stderr; stdout carries its exit status (128 + signal when
    # one killed it) and what the tmpfs still holds after it. None: no namespace or no mount.
    def on_small_tmpdir(size, patch_file, name):
        mnt = root / f'{name}.tmpfs'
        mnt.mkdir()
        script = ('mount -t tmpfs -o size="$1" tmpfs "$2" || exit 99; '
                  'TMPDIR="$2" "$3" "$4" "$5" -o "$6" >&2; echo "rc=$?"; ls -A "$2"')
        r = subprocess.run(['unshare', '--map-root-user', '--mount', 'sh', '-c', script, 'sh', size, str(mnt), binary,
                            str(plain), str(patch_file), str(root / name)], capture_output=True)
        if r.returncode != 0:
            print(f'SKIP {name}: no private tmpfs (rc={r.returncode} {r.stderr.decode()[-200:]})')
            return None
        status, *left = r.stdout.decode().splitlines()
        return int(status.removeprefix('rc=')), r.stderr.decode(), left

    full = root / 'full.gcpatch'
    full.write_bytes(compressed(bytes(8 << 20)))
    if (res := on_small_tmpdir('1m', full, 'full')) is not None:
        rc, err, left = res
        if rc != 1 or 'Failed to decompress' not in err:
            errors.append(f'an 8 MiB patch on a 1 MiB TMPDIR did not fail cleanly: rc={rc} {err[-300:]}')
        if left:
            errors.append(f'an 8 MiB patch on a 1 MiB TMPDIR left {left} behind')

    framed = root / 'framed.gcpatch'
    framed.write_bytes(b''.join(compressed(bytes(16 << 10)) for _ in range(64)))
    if (res := on_small_tmpdir('4m', framed, 'framed')) is not None:
        rc, err, left = res
        if 'Bad magic' not in err:
            errors.append(f'a 1 MiB patch with an 8 MiB bound was not decompressed on a 4 MiB TMPDIR: rc={rc} {err[-300:]}')
        if left:
            errors.append(f'a 1 MiB patch on a 4 MiB TMPDIR left {left} behind')
assert not errors, '\n'.join(errors)
