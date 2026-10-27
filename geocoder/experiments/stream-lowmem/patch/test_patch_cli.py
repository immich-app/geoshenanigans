"""Run against a built geocoder-patch (the experiment's): command-line checks that need no patch data.

--only on an encoded installed set (a gc.manifest in the current dir) is refused before anything is
written: a staged apply would leave a gc.manifest naming every family with only one stage's keys."""
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
assert not errors, '\n'.join(errors)
