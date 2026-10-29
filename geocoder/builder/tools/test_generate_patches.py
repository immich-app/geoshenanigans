#!/usr/bin/env python3
"""Tests for generate_patches.sh's diff command line. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import json
import os
import struct
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
NO = 0xFFFFFFFF
STREET_TOMBSTONE = (0, 0, NO)

# Records its command line and the sidecars in its --identity dirs (they are
# gone once the script ends), then writes an empty patch.
FAKE_DIFF = """\
import json, os, struct, sys
args = sys.argv[1:]
def sidecars(d):
    out = {}
    for name in sorted(os.listdir(d)):
        with open(os.path.join(d, name), "rb") as f:
            data = f.read()
        out[name] = list(struct.unpack("<%dI" % (len(data) // 4), data))
    return out
at = args.index("--identity")
seen = {"args": args, "old": sidecars(args[at + 1]), "new": sidecars(args[at + 2])}
with open(os.path.join(os.environ["CAPTURE"], "diff.json"), "w") as f:
    json.dump(seen, f)
open(args[3], "wb").close()
"""

# Copies the new day's files beside the patch, so the verify passes.
FAKE_PATCH = """\
import os, shutil, sys
src, out = os.path.dirname(sys.argv[2]), sys.argv[4]
for name in os.listdir(src):
    if not name.endswith(".gcpatch"):
        shutil.copy(os.path.join(src, name), out)
"""


def write_words(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        for row in rows:
            f.write(struct.pack("<%dI" % len(row), *row))


def write_tool(path, source):
    with open(path, "w") as f:
        f.write("#!%s\n%s" % (sys.executable, source))
    os.chmod(path, 0o755)


def read(path):
    with open(path) as f:
        return f.read()


class GeneratePatchesTest(unittest.TestCase):
    # A no-addresses dir: street ways only, so neither day has interp or
    # addr keys.
    def test_diffs_a_variant_by_both_days_identity_sidecars(self):
        with tempfile.TemporaryDirectory() as root:
            old, new = os.path.join(root, "old/r/no-addresses"), os.path.join(root, "new/r/no-addresses")
            write_words(os.path.join(old, "street_ways.bin"), [(0, 2, 7), (2, 1, 8)])
            write_words(os.path.join(old, "street_nodes.bin"), [(1, 2), (3, 4), (5, 6)])
            write_words(os.path.join(new, "street_ways.bin"), [(0, 2, 7), STREET_TOMBSTONE, (2, 1, 9)])
            write_words(os.path.join(new, "street_nodes.bin"), [(1, 2), (3, 4), (5, 6)])
            diff, patch = os.path.join(root, "diff"), os.path.join(root, "patch")
            write_tool(diff, FAKE_DIFF)
            write_tool(patch, FAKE_PATCH)
            scratch = os.path.join(root, "scratch")
            os.makedirs(scratch)
            env = dict(os.environ, GEOCODER_DIFF=diff, GEOCODER_PATCH=patch, CAPTURE=root, TMPDIR=scratch)
            results, logs = os.path.join(root, "results"), os.path.join(root, "logs")

            subprocess.run(["bash", os.path.join(HERE, "generate_patches.sh"), os.path.join(root, "old"),
                            os.path.join(root, "new"), results, logs], env=env, check=True, capture_output=True)

            seen = json.loads(read(os.path.join(root, "diff.json")))
            self.assertEqual(seen["args"][:5], [old, new, "-o", new + "/patch.gcpatch", "--identity"])
            self.assertEqual([a.startswith(scratch + "/") for a in seen["args"][5:]], [True, True])
            self.assertEqual(seen["old"], {"street.slot.u32": [0, 1]})
            self.assertEqual(seen["new"], {"street.slot.u32": [0, NO, 2]})
            self.assertEqual(read(os.path.join(logs, "r_no-addresses.log")),
                             "street: 2 records, 0 tombstones\nstreet: 3 records, 1 tombstones\n")
            self.assertEqual(read(os.path.join(results, "r_no-addresses")).split()[:2], ["PASS", "2"])
            self.assertEqual(os.listdir(scratch), [])


if __name__ == "__main__":
    unittest.main()
