#!/usr/bin/env python3
"""Tests for compress_index.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compress_index as sut  # noqa: E402


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def unzstd(path):
    return subprocess.run(["zstd", "-dc", path], check=True, capture_output=True).stdout


def content(seed, n=200_000):
    return bytes((seed * 31 + i * 7 + i // 251) % 256 for i in range(n))


class CompressIndexTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = os.path.join(self.tmp.name, "output")
        os.makedirs(self.out)
        with open(os.path.join(self.out, "configurations.json"), "w") as f:
            json.dump({"version": 1}, f)
        self.files = {
            "planet/full/geo_cells.bin": content(1),
            "planet/no-addresses/geo_cells.bin": content(1),
            "planet/full/strings_layout.json": b'{"tiers": []}',
            "planet/full/addr_points.bin": content(2),
            "planet/search/full/q2.5/all/places.fst": content(4),
            "planet/search/full/q2.5/all/search_meta.json": b'{"dataset": {}}',
        }
        for rel, data in self.files.items():
            write(os.path.join(self.out, rel), data)
        write(os.path.join(self.out, "planet/full/ways.osm_ids"), content(3))

    def tearDown(self):
        self.tmp.cleanup()

    def run_main(self, *args):
        with mock.patch.object(sys, "argv", ["compress_index.py", self.out, *args]):
            sut.main()
        with open(os.path.join(self.out, "configurations.json")) as f:
            return json.load(f)

    def test_compresses_each_content_once_and_links_its_copies(self):
        cfg = self.run_main()

        self.assertEqual(sorted(cfg["files"]), sorted(self.files))
        for rel, data in self.files.items():
            path = os.path.join(self.out, rel)
            self.assertEqual(unzstd(path + ".zst"), data, rel)
            self.assertEqual(cfg["files"][rel], {"size_zst": os.path.getsize(path + ".zst"), "size_raw": len(data),
                                                 "sha256": hashlib.sha256(data).hexdigest()})
        full = os.stat(os.path.join(self.out, "planet/full/geo_cells.bin.zst"))
        copy = os.stat(os.path.join(self.out, "planet/no-addresses/geo_cells.bin.zst"))
        self.assertEqual(full.st_ino, copy.st_ino)
        self.assertFalse(os.path.exists(os.path.join(self.out, "planet/full/ways.osm_ids.zst")))
        self.assertEqual(cfg["version"], 1)

    def test_streams_every_zst_to_the_upload_command(self):
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = f"{sys.executable} -c \"import sys; open({log!r}, 'a').write(sys.stdin.read())\""

        self.run_main("--upload", "s3://bucket/day/", "--upload-cmd", recorder)

        with open(log) as f:
            sent = f.read().splitlines()
        expected = [f"cp {os.path.join(self.out, rel)}.zst s3://bucket/day/{rel}.zst" for rel in self.files]
        self.assertEqual(sorted(sent), sorted(expected))


if __name__ == "__main__":
    unittest.main()
