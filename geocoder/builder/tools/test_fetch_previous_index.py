#!/usr/bin/env python3
"""Tests for fetch_previous_index.sh: yesterday's client files come down as patch generation reads
them, a transport unit decoded into its files once geocoder-transport is built. s5cmd and
geocoder-transport are stubbed out; a directory stands in for the bucket.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import io
import os
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "fetch_previous_index.sh")
PREFIX = "s3://bucket/geocoder/builds/2026-11-16"

# s5cmd's ls, cat and cp over $FAKE_BUCKET, which holds s3://bucket/. A cat of a key in
# $FAKE_CAT_BREAKS (":"-separated) writes half the object, then fails, as a dropped stream does.
FAKE_S5CMD = r'''#!{python}
import os, shutil, sys
args = sys.argv[1:]
while args[0].startswith("--"):
    args = args[2:]
verb, args = args[0], args[1:]
local = lambda url: os.path.join(os.environ["FAKE_BUCKET"], url[len("s3://bucket/"):])
if verb == "ls":
    root = local(args[0].rstrip("*"))
    for d, _, names in os.walk(root):
        for n in sorted(names):
            p = os.path.join(d, n)
            print(f"2026/11/16 06:00:00 {{os.path.getsize(p):>10}}  {{args[0].rstrip('*')}}{{os.path.relpath(p, root)}}")
elif verb == "cat":
    with open(local(args[0]), "rb") as f:
        data = f.read()
    if any(k and args[0].endswith(k) for k in os.environ.get("FAKE_CAT_BREAKS", "").split(":")):
        sys.stdout.buffer.write(data[:len(data) // 2])
        sys.exit(1)
    sys.stdout.buffer.write(data)
elif verb == "cp":
    os.makedirs(os.path.dirname(args[1]), exist_ok=True)
    shutil.copy(local(args[0]), args[1])
'''

# geocoder-transport decode <unit|-> <dir>: the stub's units are tar archives of their files, all
# written or, for a unit it can't read, none. Each call is logged to $FAKE_TRANSPORT_LOG.
FAKE_TRANSPORT = r'''#!{python}
import io, os, sys, tarfile
with open(os.environ["FAKE_TRANSPORT_LOG"], "a") as f:
    f.write(" ".join(sys.argv[1:]) + "\n")
_, unit, d = sys.argv[1:]
data = sys.stdin.buffer.read() if unit == "-" else open(unit, "rb").read()
try:
    t = tarfile.open(fileobj=io.BytesIO(data))
    members = {{m.name: t.extractfile(m).read() for m in t.getmembers()}}
    if not members or any(len(members[m.name]) != m.size for m in t.getmembers()):
        raise EOFError("the unit ends early")
except (tarfile.TarError, OSError, EOFError) as e:
    sys.exit(f"geocoder-transport: {{e}}")
for name, body in members.items():
    with open(os.path.join(d, name), "wb") as f:
        f.write(body)
print("decoded unit")
'''


def tar(files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def content(seed, n=50_000):
    return bytes((seed * 31 + i * 7 + i // 251) % 256 for i in range(n))


class FetchPreviousIndexTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        t = self.tmp.name
        self.bucket = os.path.join(t, "bucket")
        self.dest = os.path.join(t, "old-output")
        self.status = os.path.join(t, "old-output.status")
        self.built = os.path.join(t, "search.status")
        self.log = os.path.join(t, "transport.log")
        os.makedirs(os.path.join(t, "bin"))
        for name, text in (("s5cmd", FAKE_S5CMD), ("geocoder-transport", FAKE_TRANSPORT)):
            with open(os.path.join(t, "bin", name), "w") as f:
                f.write(text.format(python=sys.executable))
            os.chmod(os.path.join(t, "bin", name), 0o755)
        self.places = {"place_cells.bin": content(1, 800), "place_nodes.bin": content(2), "place_hidden.bin": content(3, 64)}
        self.strings = content(4)
        published = {
            "planet/admin/places.tt.zst": tar(self.places),
            "planet/admin/strings_core.bin.zst": subprocess.run(["zstd", "-q", "-c"], input=self.strings,
                                                                 capture_output=True, check=True).stdout,
            "planet/admin/strings_layout.json": b'{"tiers": []}',
            "planet/admin/patch.gcpatch": b"GCPATCH",
            "planet/admin/admin_cells.osm_ids": b"ids",
            "planet/search/full/simplified/all/places.fst.zst": b"index",
            "configurations.json": b"{}",
        }
        for rel, data in published.items():
            path = os.path.join(self.bucket, "geocoder/builds/2026-11-16", rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)

    def tearDown(self):
        self.tmp.cleanup()

    def fetch(self, **env):
        run = subprocess.run(
            ["bash", SCRIPT, PREFIX, self.dest, self.status, os.path.join(self.tmp.name, "bin", "geocoder-transport"), self.built],
            env={"PATH": os.path.join(self.tmp.name, "bin") + os.pathsep + os.environ["PATH"], "S3_ENDPOINT": "http://bucket",
                 "FAKE_BUCKET": self.bucket, "FAKE_TRANSPORT_LOG": self.log, **env},
            capture_output=True, text=True, timeout=120)
        with open(self.status) as f:
            return f.read().strip(), run.stdout + run.stderr

    def held(self, rel):
        path = os.path.join(self.dest, rel)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return f.read()

    def built_with(self, status):
        with open(self.built, "w") as f:
            f.write(f"{status}\n")

    def test_a_unit_is_decoded_into_its_files_beside_the_rest_of_its_dir(self):
        self.built_with(0)

        status, out = self.fetch()

        self.assertEqual(status, "0", out)
        for name, data in self.places.items():
            self.assertEqual(self.held(f"planet/admin/{name}"), data, name)
        self.assertEqual(self.held("planet/admin/strings_core.bin"), self.strings)
        self.assertEqual(self.held("planet/admin/strings_layout.json"), b'{"tiers": []}')
        self.assertEqual(self.held("planet/admin/patch.gcpatch"), b"GCPATCH")
        for rel in ("planet/admin/places.tt.zst", "planet/admin/admin_cells.osm_ids", "planet/search"):
            self.assertFalse(os.path.exists(os.path.join(self.dest, rel)), rel)
        with open(self.log) as f:
            self.assertEqual(f.read().splitlines(), [f"decode - {self.dest}/planet/admin"])

    def test_a_unit_cut_off_mid_stream_is_fetched_whole_and_decoded(self):
        self.built_with(0)

        status, out = self.fetch(FAKE_CAT_BREAKS="planet/admin/places.tt.zst")

        self.assertEqual(status, "0", out)
        for name, data in self.places.items():
            self.assertEqual(self.held(f"planet/admin/{name}"), data, name)
        self.assertFalse(os.path.exists(os.path.join(self.dest, "planet/admin/places.tt.zst")))
        self.assertIn("retrying planet/admin/places.tt.zst as a download", out)

    def test_a_unit_waits_for_geocoder_transport_to_be_built(self):
        later = threading.Timer(2.0, self.built_with, [0])
        later.start()
        try:
            status, out = self.fetch()
        finally:
            later.cancel()

        self.assertEqual(status, "0", out)
        self.assertEqual(self.held("planet/admin/place_nodes.bin"), self.places["place_nodes.bin"])

    def test_a_failed_build_of_geocoder_transport_fails_the_fetch(self):
        self.built_with(101)

        status, out = self.fetch()

        self.assertNotEqual(status, "0", out)
        self.assertIn("geocoder-transport failed to build", out)
        self.assertIsNone(self.held("planet/admin/place_nodes.bin"))
        self.assertFalse(os.path.exists(self.log))


if __name__ == "__main__":
    unittest.main()
