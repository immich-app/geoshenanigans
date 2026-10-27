"""Run against a built gc executable: a source dir, named as itself or through a symlink, is never
written to. The encode cases use a today's-format dir, the decode cases a valid encoded one (gc's
encoding of it), so only the alias check stops them.

An encoded dir is refused as encode input (whole, or one more family with --only) before the
destination is made: its families have no today's-format files, and its gc.manifest would be
replaced by one naming none of them."""
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

binary = str(Path(sys.argv[1]).resolve())


def snapshot(d):
    return {p.name: p.read_bytes() for p in sorted(d.iterdir())}


with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as scratch:
    root = Path(scratch)
    src = root / 'source'
    src.mkdir()
    (src / 'a.bin').write_bytes(b'keep these source bytes')
    (src / 'street_ways.bin').write_bytes(b''.join(struct.pack('<IHHI', 2 * w, 2, 0, 100 + w) for w in range(3)))
    (src / 'street_nodes.bin').write_bytes(b''.join(struct.pack('<4f', 47 + w * 1e-4, 8, 47.001, 8.001 + w * 1e-4)
                                                    for w in range(3)))
    encoded = root / 'encoded'
    result = subprocess.run([binary, 'encode', str(src), str(encoded)], capture_output=True)
    assert result.returncode == 0 and (encoded / 'gc.manifest').exists(), result.stderr.decode()[-300:]
    errors = []
    for mode, source in [('encode', src), ('decode', encoded)]:
        alias = root / f'{source.name}.alias'
        alias.symlink_to(source, target_is_directory=True)
        for dest in (source, alias):
            before = snapshot(source)
            result = subprocess.run([binary, mode, str(source), str(dest)], capture_output=True)
            if result.returncode == 0 or snapshot(source) != before:
                errors.append(f'{mode} into {dest.name}: the source alias was accepted or changed: rc={result.returncode}')
    for mode, source in [('encode', src), ('decode', encoded)]:
        dest = root / f'separate.{mode}'
        result = subprocess.run([binary, mode, str(source), str(dest)], capture_output=True)
        if result.returncode != 0:
            errors.append(f'{mode} into a separate destination failed: {result.stderr.decode()[-300:]}')
    if snapshot(root / 'separate.encode') != snapshot(encoded) or snapshot(root / 'separate.decode') != snapshot(src):
        errors.append('separate destination did not round trip')
    for name, extra in [('whole', []), ('one family', ['--only', 'way_parents'])]:
        dest = root / f'again.{extra[-1] if extra else "all"}'
        result = subprocess.run([binary, 'encode', str(encoded), str(dest)] + extra, capture_output=True)
        err = result.stderr.decode()
        if result.returncode == 0 or 'already encoded' not in err or dest.exists():
            errors.append(f'encode of an encoded dir ({name}) was not refused up front: rc={result.returncode} '
                          f'wrote={dest.exists()} {err[-300:]}')
    assert not errors, '\n'.join(errors)
