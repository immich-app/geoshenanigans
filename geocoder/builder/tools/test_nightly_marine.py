#!/usr/bin/env python3
"""Tests for the marine areas' way into the nightly: Update Marine Areas publishes a run of marine_import.py
under geocoder/marine/<version>/, the repo pins one (builder/data/marine.json), and the nightly downloads
the pinned file, checks its sha256 and passes it to build-index --marine, with yesterday's water kinds
fetched before the build reads them (and the step failing when a day that wrote water dirs doesn't give
them).
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


# s5cmd as the blocks run it: `cp SRC DST` serves $FAKE_S3/<SRC past s3://> (a SRC ending in *.osm_ids copies
# every sidecar under its prefix, by its path past the prefix, into the DST dir) and fails for an object in
# $FAKE_S3_FAIL; `ls PATTERN` prints what $FAKE_S3_LS holds, or fails with $FAKE_S3_LS_ERROR.
FAKE_S5CMD = r"""#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    --endpoint-url|--retry-count|--numworkers|--concurrency) shift 2 ;;
    -*) shift ;;
    *) break ;;
  esac
done
cmd=$1; shift
while [ $# -gt 0 ]; do
  case "$1" in
    --concurrency) shift 2 ;;
    -*) shift ;;
    *) break ;;
  esac
done
case "$cmd" in
  ls)
    if [ -n "$FAKE_S3_LS_ERROR" ]; then echo "ERROR \"ls $1\": $FAKE_S3_LS_ERROR" >&2; exit 1; fi
    if [ -z "$FAKE_S3_LS" ]; then echo "ERROR \"ls $1\": no object found" >&2; exit 1; fi
    echo "$FAKE_S3_LS" ;;
  cp)
    src=${1#s3://}; dst=$2
    case "$src" in
      *'*.osm_ids')
        prefix=$FAKE_S3/${src%\*.osm_ids}
        [ -d "$prefix" ] || exit 1
        (cd "$prefix" && find . -name '*.osm_ids') | while read -r f; do
          mkdir -p "$(dirname "$dst/$f")" && cp "$prefix/$f" "$dst/$f"
        done ;;
      *)
        if [ -n "$FAKE_S3_FAIL" ] && [ "$src" = "$FAKE_S3_FAIL" ]; then echo "ERROR \"cp $1\": timeout" >&2; exit 1; fi
        [ -f "$FAKE_S3/$src" ] || { echo "ERROR \"cp $1\": NoSuchKey" >&2; exit 1; }
        mkdir -p "$(dirname "$dst")" && cp "$FAKE_S3/$src" "$dst" ;;
    esac ;;
  *) exit 2 ;;
esac
"""


def fake_s5cmd(bin_dir):
    path = os.path.join(bin_dir, "s5cmd")
    with open(path, "w") as f:
        f.write(FAKE_S5CMD)
    os.chmod(path, 0o755)


def sidecar_block():
    """The Download step's fetch of yesterday's sidecars, as bash, with its $S5 and a wait for it."""
    text = script(read(NIGHTLY), "Download PBF and previous indexes")
    s5 = re.search(r'^S5=".*"\n', text, re.M).group(0)
    block = re.search(r'^if \[ "\$HAS_PREVIOUS" = "true" \]; then\n  \(\n.*?^  IDX_PID=\$!\nfi\n', text, re.M | re.S)
    return s5 + block.group(0) + 'wait "$IDX_PID"\n' if block else None


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


class SidecarFetchTest(unittest.TestCase):
    """Yesterday's sidecars and its planet water_meta.json, which keeps the water=* values' codes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        t = self.tmp.name
        self.s3, self.bin, self.work = os.path.join(t, "s3"), os.path.join(t, "bin"), os.path.join(t, "work")
        for d in (self.s3, self.bin, os.path.join(self.work, "old-output")):
            os.makedirs(d)
        fake_s5cmd(self.bin)
        self.day = os.path.join(self.s3, "bucket", "geocoder", "builds", "2026-11-21")

    def tearDown(self):
        self.tmp.cleanup()

    def put(self, path, data=b"x"):
        full = os.path.join(self.day, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(data)

    def run_block(self, fail=""):
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "WORKDIR": self.work, "S3_ENDPOINT": "https://s3.example",
               "S3_BUCKET": "bucket", "S3_PREFIX": "geocoder", "YESTERDAY": "2026-11-21", "HAS_PREVIOUS": "true",
               "FAKE_S3": self.s3, "FAKE_S3_FAIL": fail}
        return subprocess.run(["bash", "-e", "-c", sidecar_block()], env=env, capture_output=True, text=True, timeout=60)

    def held(self, path):
        full = os.path.join(self.work, "old-output", path)
        if not os.path.exists(full):
            return None
        with open(full, "rb") as f:
            return f.read()

    def test_a_day_with_water_dirs_brings_its_water_kinds_down_with_its_sidecars(self):
        self.put("planet/full/street_ways.osm_ids")
        self.put("planet/water/all/exact/admin_polygons.osm_ids")
        self.put("planet/water/all/exact/water_meta.json", b'{"types": []}')
        run = self.run_block()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(self.held("planet/water/all/exact/water_meta.json"), b'{"types": []}')
        self.assertEqual(self.held("planet/full/street_ways.osm_ids"), b"x")

    def test_a_day_with_water_dirs_whose_water_kinds_do_not_come_down_fails_the_step(self):
        self.put("planet/full/street_ways.osm_ids")
        self.put("planet/water/all/exact/admin_polygons.osm_ids")
        self.put("planet/water/all/exact/water_meta.json", b'{"types": []}')
        run = self.run_block(fail="bucket/geocoder/builds/2026-11-21/planet/water/all/exact/water_meta.json")
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn("::error::", run.stdout)
        self.assertIn("water_meta.json", run.stdout)

    def test_a_day_before_the_water_dirs_starts_the_codes_afresh(self):
        self.put("planet/full/street_ways.osm_ids")
        run = self.run_block()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIsNone(self.held("planet/water/all/exact/water_meta.json"))
        self.assertIn("take new codes", run.stdout)


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
