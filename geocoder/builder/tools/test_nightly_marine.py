#!/usr/bin/env python3
"""Tests for the marine areas' way into the nightly: Update Marine Areas publishes a run of marine_import.py
under geocoder/marine/<version>/, the repo pins one (builder/data/marine.json), and the nightly downloads
the pinned file, checks its sha256 and passes it to build-index --marine, with yesterday's water kinds
fetched before the build reads them.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import hashlib
import json
import os
import re
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
WORKFLOWS = os.path.join(HERE, "..", "..", "..", ".github", "workflows")
NIGHTLY = os.path.join(WORKFLOWS, "geocoder-build.yml")
MARINE_WORKFLOW = os.path.join(WORKFLOWS, "geocoder-marine.yml")
PIN = os.path.join(HERE, "..", "data", "marine.json")

# curl as the block runs it: -o PATH and a URL; serves $FAKE_CURL_DIR/<the URL's path past /geocoder/>,
# fails (as --fail does) for any other.
FAKE_CURL = r'''#!/bin/sh
out=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift 2 ;;
    --connect-timeout|--max-time|--retry|--retry-delay) shift 2 ;;
    -*) shift ;;
    *) url=$1; shift ;;
  esac
done
echo "$url" >> "$FAKE_CURL_DIR/calls"
src="$FAKE_CURL_DIR/${url#*/geocoder/}"
[ -f "$src" ] || exit 22
cp "$src" "$out"
'''


def read(path):
    with open(path) as f:
        return f.read()


def step(text, name):
    """The lines of the workflow step `name`, but for its `- name:` line."""
    block = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", text, re.M)
    return block.group(1) if block else None


def script(text, name):
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", step(text, name), re.M)
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines()) if run else None


def marine_block():
    """The Download step's fetch of the pinned marine areas, as bash, up to its pid."""
    text = script(read(NIGHTLY), "Download PBF and previous indexes")
    block = re.search(r"^# ── Background: the pinned marine areas ──\n.*?^MARINE_PID=\$!\n", text, re.M | re.S)
    return block.group(0) if block else None


class PinTest(unittest.TestCase):
    def test_the_repo_pins_a_published_version_by_its_sha256_or_none(self):
        pin = json.loads(read(PIN))
        if pin["version"] is None:
            self.assertIsNone(pin["path"])
            self.assertIsNone(pin["sha256"])
            return
        self.assertRegex(pin["version"], r"^[0-9]{8}$")
        self.assertEqual(pin["path"], f"marine/{pin['version']}/marine_areas.bin.zst")
        self.assertRegex(pin["sha256"], r"^[0-9a-f]{64}$")


class NightlyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        t = self.tmp.name
        self.work = os.path.join(t, "work")
        self.served = os.path.join(t, "served")
        self.bin = os.path.join(t, "bin")
        self.repo = os.path.join(t, "repo")
        for d in (self.work, self.served, self.bin, os.path.join(self.repo, "geocoder", "builder", "data")):
            os.makedirs(d)
        with open(os.path.join(self.bin, "curl"), "w") as f:
            f.write(FAKE_CURL)
        os.chmod(os.path.join(self.bin, "curl"), 0o755)
        self.output = os.path.join(t, "github_output")
        open(self.output, "w").close()
        # A published version: its file, zstd-compressed.
        self.raw = b"GCMARINE" + bytes(range(200)) * 50
        path = os.path.join(self.served, "marine", "20261122", "marine_areas.bin.zst")
        os.makedirs(os.path.dirname(path))
        with open(path, "wb") as f:
            f.write(subprocess.run(["zstd", "-q", "-c"], input=self.raw, capture_output=True, check=True).stdout)
        with open(path, "rb") as f:
            self.sha = hashlib.sha256(f.read()).hexdigest()

    def tearDown(self):
        self.tmp.cleanup()

    def pin(self, version, sha=None):
        doc = {"version": version, "path": f"marine/{version}/marine_areas.bin.zst" if version else None,
               "sha256": sha if version else None}
        with open(os.path.join(self.repo, "geocoder", "builder", "data", "marine.json"), "w") as f:
            json.dump(doc, f)

    def run_block(self):
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "WORKDIR": self.work, "GITHUB_WORKSPACE": self.repo,
               "GITHUB_OUTPUT": self.output, "S3_PUBLIC_URL": "https://bucket.example", "FAKE_CURL_DIR": self.served}
        run = subprocess.run(["bash", "-e", "-c", marine_block() + 'wait "$MARINE_PID"\n'], env=env, capture_output=True,
                             text=True, timeout=60)
        return run, read(self.output)

    def test_the_pinned_file_is_downloaded_checked_and_given_to_the_build(self):
        self.pin("20261122", self.sha)
        run, output = self.run_block()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(read(os.path.join(self.served, "calls")).split(),
                         ["https://bucket.example/geocoder/marine/20261122/marine_areas.bin.zst"])
        self.assertEqual(output, f"marine_areas={self.work}/marine_areas.bin\n")
        with open(os.path.join(self.work, "marine_areas.bin"), "rb") as f:
            self.assertEqual(f.read(), self.raw)

    def test_a_file_of_another_sha256_fails_the_step(self):
        self.pin("20261122", "0" * 64)
        run, output = self.run_block()
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("::error::[marine]", run.stdout)
        self.assertEqual(output, "")
        self.assertFalse(os.path.exists(os.path.join(self.work, "marine_areas.bin")))

    def test_a_version_not_published_fails_the_step(self):
        self.pin("20261123", self.sha)
        run, output = self.run_block()
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("::error::[marine]", run.stdout)
        self.assertEqual(output, "")

    def test_no_pin_builds_without_marine_areas(self):
        self.pin(None)
        run, output = self.run_block()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(output, "")
        self.assertFalse(os.path.exists(os.path.join(self.served, "calls")))

    def test_the_build_takes_the_marine_areas_and_yesterdays_water_kinds(self):
        text = read(NIGHTLY)
        build = step(text, "Build geocoder index")
        self.assertIn("MARINE_FILE: ${{ steps.download.outputs.marine_areas }}", build)
        self.assertIn('BUILD_ARGS="$BUILD_ARGS --marine $MARINE_FILE"', script(text, "Build geocoder index"))
        # The type codes of water=* values come from yesterday's planet water_meta.json: fetched with the
        # sidecars, before the build reads them, not by the fetch beside it.
        download = script(text, "Download PBF and previous indexes")
        sidecars = download[download.index("# ── Background: previous strategy-2 sidecars ──"):download.index("IDX_PID=$!")]
        self.assertIn("planet/water/all/exact/water_meta.json", sidecars)


class MarineWorkflowTest(unittest.TestCase):
    def test_it_runs_on_request_and_publishes_a_new_version_under_geocoder_marine(self):
        text = read(MARINE_WORKFLOW)
        self.assertRegex(text, r"(?m)^on:\n  workflow_dispatch:")
        self.assertNotIn("schedule:", text)
        publish = script(text, "Upload to Tigris")
        self.assertIn('"s3://$S3_BUCKET/geocoder/marine/$VERSION/marine_areas.bin.zst"', publish)
        self.assertIn('"s3://$S3_BUCKET/geocoder/marine/$VERSION/manifest.json"', publish)
        # A version is never overwritten: one a pin names stays as it was.
        self.assertIn("already published", script(text, "Version"))

    def test_the_tool_is_tested_before_it_builds_with_the_geometry_tests_run(self):
        text = read(MARINE_WORKFLOW)
        tests = script(text, "Test the tool")
        self.assertIn("test_marine_import", tests)
        self.assertIn("skipped", tests)
        self.assertLess(text.index("- name: Test the tool"), text.index("- name: Build the marine areas"))


if __name__ == "__main__":
    unittest.main()
