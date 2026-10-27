"""Run against a built gc executable: source aliases must never be overwritten."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile

binary = str(Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as scratch:
    root = Path(scratch)
    src = root / 'source'
    src.mkdir()
    alias = root / 'alias'
    alias.symlink_to(src, target_is_directory=True)
    errors = []
    for mode, dest in [('encode', src), ('encode', alias), ('decode', src)]:
        sample = src / 'a.bin'
        sample.write_bytes(b'keep these source bytes')
        result = subprocess.run([binary, mode, str(src), str(dest)], capture_output=True)
        if result.returncode == 0 or not sample.exists() or sample.read_bytes() != b'keep these source bytes':
            errors.append(f'{mode} source alias was accepted or changed')
    sample.write_bytes(b'keep these source bytes')
    dest = root / 'separate'
    result = subprocess.run([binary, 'encode', str(src), str(dest)], capture_output=True)
    if result.returncode != 0 or (dest / 'a.bin').read_bytes() != sample.read_bytes():
        errors.append('separate destination did not round trip')
    assert not errors, '\n'.join(errors)
