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


def marine_wait():
    """The Download step's wait for the fetch of the marine areas, a line of the step's own after the fetch
    starts (not inside another block), which fails the step with the fetch."""
    text = script(read(NIGHTLY), "Download PBF and previous indexes")
    started = text.find("MARINE_PID=$!\n")
    wait = re.compile(r'^wait "?\$MARINE_PID"?\n', re.M).search(text, started) if started >= 0 else None
    return wait.group(0) if wait else None


# s5cmd as the blocks run it: `cp SRC DST` serves $FAKE_S3/<SRC past s3://> (a SRC ending in *.osm_ids copies
# every sidecar under its prefix, by its path past the prefix, into the DST dir) and fails for an object in
# $FAKE_S3_FAIL; `cp FILE DST`, of a local FILE, puts it at $FAKE_S3/<DST past s3://> and adds that path to
# $FAKE_S3/uploads; `cat SRC` prints $FAKE_S3/<SRC past s3://>; `ls PATTERN` prints what $FAKE_S3_LS holds, or
# fails with $FAKE_S3_LS_ERROR.
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
  cat)
    src=${1#s3://}
    [ -f "$FAKE_S3/$src" ] || { echo "ERROR \"cat $1\": NoSuchKey" >&2; exit 1; }
    cat "$FAKE_S3/$src" ;;
  cp)
    case "$1" in
      s3://*) ;;
      *)
        dst=${2#s3://}
        mkdir -p "$(dirname "$FAKE_S3/$dst")" && cp "$1" "$FAKE_S3/$dst" && echo "$dst" >> "$FAKE_S3/uploads"
        exit ;;
    esac
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


# The venv's python as the marine workflow's build runs it: `-I TOOL build ... -o OUT --stats STATS ...` writes
# OUT and copies $FAKE_STATS to STATS; `-I TOOL stats PATH` prints a total line.
FAKE_PYTHON = r"""#!/bin/sh
shift 2
cmd=$1; shift
case "$cmd" in
  build)
    while [ $# -gt 0 ]; do
      case "$1" in
        -o) out=$2; shift 2 ;;
        --stats) stats=$2; shift 2 ;;
        *) shift ;;
      esac
    done
    printf GCMARINE > "$out"
    cp "$FAKE_STATS" "$stats" ;;
  stats) printf 'total\t0 objects\n' ;;
  *) exit 2 ;;
esac
"""

# unzip as the Fetch step runs it, `-p ZIP PATTERN`: prints the land polygons README's date line.
FAKE_UNZIP = "#!/bin/sh\necho 'Date of the data used is 2026-11-20T00:00:00Z'\n"

REPO = os.path.join(HERE, "..", "..", "..")


def fake_s5cmd(bin_dir):
    path = os.path.join(bin_dir, "s5cmd")
    with open(path, "w") as f:
        f.write(FAKE_S5CMD)
    os.chmod(path, 0o755)


# build-index as the Build step runs it: writes each of its arguments on a line of $BUILD_INDEX_ARGS.
FAKE_BUILD_INDEX = '#!/bin/sh\nprintf "%s\\n" "$@" > "$BUILD_INDEX_ARGS"\n'


def build_run():
    """The Build step's build-index arguments and its run of build-index with them, as bash."""
    text = script(read(NIGHTLY), "Build geocoder index")
    args = re.search(r'^# Build args\n.*?(?=^echo "Building geocoder index)', text, re.M | re.S)
    run = re.search(r"^build-index .*\\\n(?: +.*\\\n)* +.*\n", text, re.M)
    return args.group(0) + run.group(0) if args and run else None


def sidecar_block():
    """The Download step's fetch of yesterday's sidecars, as bash, with its $S5, up to its pid."""
    text = script(read(NIGHTLY), "Download PBF and previous indexes")
    s5 = re.search(r'^S5=".*"\n', text, re.M).group(0)
    block = re.search(r'^if \[ "\$HAS_PREVIOUS" = "true" \]; then\n  \(\n.*?^  IDX_PID=\$!\nfi\n', text, re.M | re.S)
    return s5 + block.group(0) if block else None


def sidecar_wait():
    """The Download step's wait for yesterday's sidecars, a block of the step's own after the fetch starts
    (not inside another block), on a day with a previous build, which fails the step with the fetch."""
    text = script(read(NIGHTLY), "Download PBF and previous indexes")
    started = text.find("  IDX_PID=$!\n")
    wait = re.compile(r'^if \[ "\$HAS_PREVIOUS" = "true" \]; then\n(?:  .*\n)*?  wait "?\$IDX_PID"?\nfi\n',
                      re.M).search(text, started) if started >= 0 else None
    return wait.group(0) if wait else None


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
        block, wait = marine_block(), marine_wait()
        self.assertIsNotNone(block)
        self.assertIsNotNone(wait, "the Download step doesn't wait for the marine areas")
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "WORKDIR": self.work, "GITHUB_WORKSPACE": self.repo,
               "GITHUB_OUTPUT": self.output, "S3_PUBLIC_URL": "https://bucket.example", "FAKE_CURL_DIR": self.served}
        run = subprocess.run(["bash", "-e", "-c", block + wait], env=env, capture_output=True, text=True, timeout=60)
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

    def pin_text(self, text):
        with open(os.path.join(self.repo, "geocoder", "builder", "data", "marine.json"), "w") as f:
            f.write(text)

    def assert_fails(self, why):
        run, output = self.run_block()
        self.assertNotEqual(run.returncode, 0, why)
        self.assertIn("::error::[marine]", run.stdout, why)
        self.assertEqual(output, "", why)
        self.assertFalse(os.path.exists(os.path.join(self.work, "marine_areas.bin")), why)

    def test_a_pin_short_of_a_field_fails_the_step(self):
        path = "marine/20261122/marine_areas.bin.zst"
        for doc in ({"version": "", "path": path, "sha256": self.sha}, {"path": path, "sha256": self.sha},
                    {"version": None, "path": path, "sha256": self.sha}, {"version": "20261122", "path": None,
                                                                          "sha256": None},
                    {"version": None, "path": None}, {}):
            with self.subTest(pin=doc):
                self.pin_text(json.dumps(doc))
                self.assert_fails(json.dumps(doc))

    def test_a_pin_file_of_anything_but_one_json_object_fails_the_step(self):
        for text in ("", "\n", "null", "[]", "{", '{"version": null, "path": null, "sha256": null}\n{}'):
            with self.subTest(pin=text):
                self.pin_text(text)
                self.assert_fails(repr(text))

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

    def test_build_index_takes_the_marine_areas_when_the_download_gave_a_file(self):
        block = build_run()
        self.assertIsNotNone(block)
        path = os.path.join(self.bin, "build-index")
        with open(path, "w") as f:
            f.write(FAKE_BUILD_INDEX)
        os.chmod(path, 0o755)
        marine = os.path.join(self.work, "marine_areas.bin")
        with open(marine, "wb") as f:
            f.write(self.raw)
        recorded = os.path.join(self.tmp.name, "build-index-args")
        for given, want in ((marine, [["--marine", marine]]), ("", []), (os.path.join(self.work, "gone.bin"), [])):
            with self.subTest(marine=given):
                env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "WORKDIR": self.work, "MARINE_FILE": given,
                       "BUILD_INDEX_ARGS": recorded}
                run = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", block], env=env, capture_output=True,
                                     text=True, timeout=60)
                self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
                args = read(recorded).splitlines()
                self.assertEqual(args[:2], [f"{self.work}/output/", f"{self.work}/planet.osm.pbf"])
                self.assertEqual([args[i:i + 2] for i, a in enumerate(args) if a == "--marine"], want)


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
        block, wait = sidecar_block(), sidecar_wait()
        self.assertIsNotNone(block)
        self.assertIsNotNone(wait, "the Download step doesn't wait for yesterday's sidecars")
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "WORKDIR": self.work, "S3_ENDPOINT": "https://s3.example",
               "S3_BUCKET": "bucket", "S3_PREFIX": "geocoder", "YESTERDAY": "2026-11-21", "HAS_PREVIOUS": "true",
               "FAKE_S3": self.s3, "FAKE_S3_FAIL": fail}
        return subprocess.run(["bash", "-e", "-c", block + wait], env=env, capture_output=True, text=True, timeout=60)

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

    def test_runs_go_one_at_a_time_so_none_overwrites_a_version_another_published(self):
        text = read(MARINE_WORKFLOW)
        block = re.search(r"(?m)^concurrency:\n((?:  .*\n)+)", text)
        self.assertIsNotNone(block, "no workflow-level concurrency group")
        self.assertRegex(block.group(1), r"(?m)^  group: \S+$")
        self.assertRegex(block.group(1), r"(?m)^  cancel-in-progress: false$")

    def run_version(self, listed="", error=""):
        with tempfile.TemporaryDirectory() as t:
            bin_dir = os.path.join(t, "bin")
            os.makedirs(bin_dir)
            fake_s5cmd(bin_dir)
            env_file = os.path.join(t, "github_env")
            open(env_file, "w").close()
            env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "GITHUB_ENV": env_file, "REQUESTED": "20261122",
                   "S3_ENDPOINT": "https://s3.example", "S3_BUCKET": "bucket", "FAKE_S3_LS": listed,
                   "FAKE_S3_LS_ERROR": error}
            run = subprocess.run(["bash", "-e", "-c", script(read(MARINE_WORKFLOW), "Version")], env=env,
                                 capture_output=True, text=True, timeout=60)
            return run, read(env_file)

    def test_a_new_version_goes_ahead(self):
        run, env = self.run_version()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(env, "VERSION=20261122\n")

    def test_a_published_version_is_refused(self):
        for listed in ("2026/11/22 10:00:00   1234 marine_areas.bin.zst\n2026/11/22 10:00:01    567 manifest.json",
                       "2026/11/22 10:00:01    567 manifest.json", "2026/11/22 10:00:00     89 notes.txt"):
            with self.subTest(listed=listed):
                run, env = self.run_version(listed=listed)
                self.assertNotEqual(run.returncode, 0)
                self.assertIn("already published", run.stdout)
                self.assertEqual(env, "")

    def test_a_version_whose_file_went_up_without_its_manifest_goes_ahead_to_finish_it(self):
        run, env = self.run_version(listed="2026/11/22 10:00:00   1234 marine_areas.bin.zst")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(env, "UNFINISHED=true\nVERSION=20261122\n")

    def test_a_listing_that_fails_stops_the_run_as_it_cannot_tell(self):
        run, env = self.run_version(error="AccessDenied: Access Denied")
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("::error::", run.stdout)
        self.assertEqual(env, "")

    def run_tests_step(self, said, status=0):
        """The Test the tool step with a python that writes `said` and exits with `status`."""
        with tempfile.TemporaryDirectory() as t:
            os.makedirs(os.path.join(t, "venv", "bin"))
            with open(os.path.join(t, "venv", "bin", "python"), "w") as f:
                f.write(f"#!/bin/sh\nprintf '%s\\n' '{said}' >&2\nexit {status}\n")
            os.chmod(os.path.join(t, "venv", "bin", "python"), 0o755)
            return subprocess.run(["bash", "-e", "-c", script(read(MARINE_WORKFLOW), "Test the tool")], cwd=t,
                                  env={"PATH": os.environ["PATH"], "RUNNER_TEMP": t}, capture_output=True, text=True,
                                  timeout=60)

    def test_the_tool_is_tested_before_it_builds_with_the_geometry_tests_run(self):
        text = read(MARINE_WORKFLOW)
        self.assertIn("test_marine_import", script(text, "Test the tool"))
        self.assertLess(text.index("- name: Test the tool"), text.index("- name: Build the marine areas"))
        run = self.run_tests_step("OK")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        # Tests that pass with some skipped, as the geometry tests are without their modules, fail the step.
        run = self.run_tests_step("OK (skipped=3)")
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("::error::", run.stdout)
        run = self.run_tests_step("FAILED (failures=1)", status=1)
        self.assertNotEqual(run.returncode, 0)

    def run_fetch(self, layer):
        """The Fetch step with the WFS serving `layer`, a GeoJSON document: the run."""
        with tempfile.TemporaryDirectory() as t:
            served, bin_dir = os.path.join(t, "served"), os.path.join(t, "bin")
            for d in (served, bin_dir):
                os.makedirs(d)
            for name, text in [("curl", FAKE_CURL), ("unzip", FAKE_UNZIP)]:
                with open(os.path.join(bin_dir, name), "w") as f:
                    f.write(text)
                os.chmod(os.path.join(bin_dir, name), 0o755)
            with open(os.path.join(served, "eez_iho.json"), "w") as f:
                json.dump(layer, f)
            with open(os.path.join(served, "land.zip"), "wb") as f:
                f.write(b"PK")
            env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "RUNNER_TEMP": t, "GITHUB_ENV": os.path.join(t, "env"),
                   "EEZ_IHO_URL": "https://wfs.example/geocoder/eez_iho.json",
                   "LAND_URL": "https://land.example/geocoder/land.zip", "FAKE_CURL_DIR": served}
            return subprocess.run(["bash", "-e", "-c", script(read(MARINE_WORKFLOW), "Fetch the sources")], env=env,
                                  capture_output=True, text=True, timeout=60)

    def test_a_wfs_download_short_of_the_whole_layer_stops_the_run(self):
        features = [{"type": "Feature", "properties": {}, "geometry": None}] * 3
        whole = {"type": "FeatureCollection", "features": features, "numberMatched": 3, "numberReturned": 3}
        run = self.run_fetch(whole)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        # A server that caps a request's features says so only in these counts; without them nothing does.
        for why, layer in [("capped", {**whole, "numberMatched": 5}), ("returned less", {**whole, "numberReturned": 2}),
                           ("features short", {**whole, "features": features[:2]}),
                           ("matched unknown", {**whole, "numberMatched": "unknown"}),
                           ("no matched", {k: v for k, v in whole.items() if k != "numberMatched"}),
                           ("no returned", {k: v for k, v in whole.items() if k != "numberReturned"})]:
            with self.subTest(why):
                run = self.run_fetch(layer)
                self.assertNotEqual(run.returncode, 0)
                self.assertIn("::error::", run.stdout)

    def run_build(self, summary):
        """The Build step with the tool's summary `summary`: the run, and whether it compressed the file."""
        with tempfile.TemporaryDirectory() as t:
            os.makedirs(os.path.join(t, "venv", "bin"))
            with open(os.path.join(t, "venv", "bin", "python"), "w") as f:
                f.write(FAKE_PYTHON)
            os.chmod(os.path.join(t, "venv", "bin", "python"), 0o755)
            with open(os.path.join(t, "summary.json"), "w") as f:
                json.dump(summary, f)
            env = {"PATH": os.environ["PATH"], "RUNNER_TEMP": t, "FAKE_STATS": os.path.join(t, "summary.json")}
            run = subprocess.run(["bash", "-e", "-c", script(read(MARINE_WORKFLOW), "Build the marine areas")],
                                 cwd=t, env=env, capture_output=True, text=True, timeout=60)
            return run, os.path.exists(os.path.join(t, "marine-out", "marine_areas.bin.zst"))

    def test_a_build_of_no_seas_no_oceans_or_no_polygons_stops_the_run(self):
        good = {"objects": 101, "oceans": 7, "polygons": 54731}
        run, compressed = self.run_build(good)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertTrue(compressed)
        # Its oceans are some of its objects: objects no more than its oceans are oceans alone, without a sea.
        for summary in ({"objects": 0, "oceans": 0, "polygons": 0}, {**good, "objects": 0}, {**good, "objects": 7},
                        {**good, "oceans": 0}, {**good, "polygons": 0}):
            with self.subTest(summary=summary):
                run, compressed = self.run_build(summary)
                self.assertNotEqual(run.returncode, 0)
                self.assertIn("::error::", run.stdout)
                self.assertFalse(compressed)

    def run_upload(self, unfinished=False, held=None):
        """The Upload step of version 20261122, the bucket holding `held` as its file when not None: the run,
        the paths it put in the bucket, its step summary and the bucket's file after it."""
        with tempfile.TemporaryDirectory() as t:
            s3, bin_dir = os.path.join(t, "s3"), os.path.join(t, "bin")
            for d in (s3, bin_dir, os.path.join(t, "marine-src"), os.path.join(t, "marine-out")):
                os.makedirs(d)
            fake_s5cmd(bin_dir)
            for name, data in [("marine-src/eez_iho.geojson", b"{}"),
                               ("marine-src/land-polygons-split-4326.zip", b"PK"),
                               ("marine-out/marine_areas.bin", b"GCMARINE"),
                               ("marine-out/marine_areas.bin.zst", b"zst"),
                               ("marine-out/stats.json", b'{"objects": 101}')]:
                with open(os.path.join(t, name), "wb") as f:
                    f.write(data)
            published = os.path.join(s3, "bucket", "geocoder", "marine", "20261122", "marine_areas.bin.zst")
            if held is not None:
                os.makedirs(os.path.dirname(published))
                with open(published, "wb") as f:
                    f.write(held)
            summary = os.path.join(t, "summary.md")
            env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "RUNNER_TEMP": t, "VERSION": "20261122",
                   "S3_BUCKET": "bucket", "S3_ENDPOINT": "https://s3.example", "EEZ_IHO_URL": "https://eez.example",
                   "LAND_URL": "https://land.example", "LAND_DATE": "2026-11-20", "GITHUB_SHA": "0" * 40,
                   "GITHUB_STEP_SUMMARY": summary, "FAKE_S3": s3}
            if unfinished:
                env["UNFINISHED"] = "true"
            run = subprocess.run(["bash", "-e", "-c", script(read(MARINE_WORKFLOW), "Upload to Tigris")], cwd=REPO,
                                 env=env, capture_output=True, text=True, timeout=60)
            uploads = read(os.path.join(s3, "uploads")).split() if os.path.exists(os.path.join(s3, "uploads")) else []
            with open(published, "rb") as f:
                after = f.read()
            return run, uploads, read(summary) if os.path.exists(summary) else "", after

    def test_a_new_version_uploads_its_file_then_its_manifest_and_gives_its_pin(self):
        run, uploads, summary, after = self.run_upload()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(uploads, ["bucket/geocoder/marine/20261122/marine_areas.bin.zst",
                                   "bucket/geocoder/marine/20261122/manifest.json"])
        self.assertEqual(after, b"zst")
        self.assertIn(f'"sha256": "{hashlib.sha256(b"zst").hexdigest()}"', summary)

    def test_a_version_whose_file_went_up_alone_gets_its_manifest_when_this_run_built_the_same_file(self):
        run, uploads, summary, after = self.run_upload(unfinished=True, held=b"zst")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(uploads, ["bucket/geocoder/marine/20261122/manifest.json"])
        self.assertIn(f'"sha256": "{hashlib.sha256(b"zst").hexdigest()}"', summary)

    def test_a_version_whose_file_went_up_alone_is_refused_when_this_run_built_another(self):
        run, uploads, summary, after = self.run_upload(unfinished=True, held=b"another zst")
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("::error::", run.stdout)
        self.assertEqual(uploads, [])
        self.assertEqual(summary, "")
        self.assertEqual(after, b"another zst")


if __name__ == "__main__":
    unittest.main()
