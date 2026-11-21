#!/usr/bin/env python3
"""Tests for checksums.sh, which writes the published checksums.sha256 in two parts: the .zst once compression
ends, the rest in Finalize, configurations.json after its splice. And how the nightly runs it.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "checksums.sh")
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")

# Finalize's checksums.sha256 before it came in two parts: every .zst, .gcpatch and .json, hashed in one go and
# sorted by path.
ONE_GO = ('(cd "$1" && find . -type f \\( -name "*.zst" -o -name "*.gcpatch" -o -name "*.json" \\) -print0 '
          '| xargs -0 -P "$(nproc)" -n 8 sha256sum | sort -k2)')


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


class ChecksumsTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.out = os.path.join(self.root, "output")
        files = {
            "configurations.json": b'{"build": {}}',
            "planet/full/geo_cells.bin.zst": b"a" * 5000,
            "planet/full/geo_cells.bin": b"raw",
            "planet/full/places.tt.zst": b"unit",
            "planet/full/patch.gcpatch": b"patch",
            "planet/full/strings_layout.json": b"{}",
            "planet/full/strings_layout.json.zst": b"zl",
            "planet/full/ways.osm_ids": b"ids",
            "planet/search/full/simplified/all/search_meta.json": b'{"t": 1}',
            "planet/search/full/simplified/all/search_meta.json.zst": b"sm",
            "planet/search/full/simplified/all/places.fst.zst": b"fst",
            "europe/full/geo_cells.bin.zst": b"b" * 7000,
            "europe/full/patch.gcpatch": b"",
            "a b/odd name.bin.zst": b"spaces",
        }
        for rel, data in files.items():
            write(os.path.join(self.out, rel), data)
        self.zst = os.path.join(self.root, "zst.sha256")

    def run_script(self, *args):
        return subprocess.run(["bash", SCRIPT, *args], capture_output=True, text=True)

    def one_go(self):
        return subprocess.run(["bash", "-c", ONE_GO, "bash", self.out], capture_output=True, text=True, check=True).stdout

    def hash_zst(self):
        done = self.run_script("zst", self.out)
        self.assertEqual(done.returncode, 0, done.stderr)
        with open(self.zst, "w") as f:
            f.write(done.stdout)
        return done.stdout

    def test_the_two_parts_make_the_one_go_checksums_byte_for_byte(self):
        zst = self.hash_zst()
        # Finalize's splice rewrites configurations.json after the .zst were hashed.
        write(os.path.join(self.out, "configurations.json"), b'{"build": {"date": "2026-11-21"}}')

        done = self.run_script("all", self.out, self.zst)

        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout, self.one_go())
        self.assertEqual(sorted(line.split("  ", 1)[1] for line in zst.splitlines()),
                         sorted(line.split("  ", 1)[1] for line in self.one_go().splitlines() if line.endswith(".zst")))
        self.assertTrue(all(line.endswith(".zst") for line in zst.splitlines()))

    # A .zst written after the early hash (a compression run again) has no line, or a stale one: the merge
    # refuses, rather than publish a checksum that is not the file's.
    def test_a_zst_set_that_changed_after_its_hash_is_refused(self):
        self.hash_zst()
        write(os.path.join(self.out, "planet/full/late.bin.zst"), b"late")

        done = self.run_script("all", self.out, self.zst)

        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(done.stdout, "")
        self.assertIn("planet/full/late.bin.zst", done.stderr)

    # configurations.json must be hashed as it is published, after its splice: a line from before it is refused.
    def test_a_stale_configurations_json_line_is_refused(self):
        stale = self.run_script("zst", self.out).stdout
        stale += subprocess.run(["sha256sum", "./configurations.json"], cwd=self.out, capture_output=True, text=True,
                                check=True).stdout
        with open(self.zst, "w") as f:
            f.write(stale)
        write(os.path.join(self.out, "configurations.json"), b'{"build": {"date": "2026-11-21"}}')

        done = self.run_script("all", self.out, self.zst)

        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(done.stdout, "")
        self.assertIn("configurations.json", done.stderr)


def step_script(name):
    with open(NIGHTLY) as f:
        text = f.read()
    step = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", text, re.M).group(1)
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", step, re.M)
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines())


class NightlyChecksumsTest(unittest.TestCase):
    TOOL = '"$GITHUB_WORKSPACE/geocoder/builder/tools/checksums.sh"'

    # The .zst are hashed as soon as compression ends, in both steps that compress, beside the patches on a patch
    # night; Finalize hashes the rest after its splice, and the .zst itself when that hash is missing.
    def test_the_zst_are_hashed_when_compression_ends_and_the_rest_after_the_splice(self):
        for name in ("Generate patches and verify", "Compress index files"):
            self.assertIn(f'{self.TOOL} zst "$WORKDIR/output" > "$WORKDIR/zst.sha256.part"', step_script(name), name)
        finalize = step_script("Finalize upload and configurations.json")
        splice = finalize.index('mv "$WORKDIR/output/configurations.tmp" "$WORKDIR/output/configurations.json"')
        merge = finalize.index(f'{self.TOOL} all "$WORKDIR/output" "$WORKDIR/zst.sha256" > "$WORKDIR/output/checksums.sha256"')
        self.assertLess(splice, merge)
        self.assertNotIn("xargs -0 -P", finalize)

    # A compression run again in the compress step rewrites the .zst: their earlier hash goes first.
    def test_a_compression_run_again_drops_the_earlier_hash(self):
        script = step_script("Compress index files")
        self.assertLess(script.index('rm -f "$WORKDIR/zst.sha256"'), script.index("compress_index.py"))


if __name__ == "__main__":
    unittest.main()
