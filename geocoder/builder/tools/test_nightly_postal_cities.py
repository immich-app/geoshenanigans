#!/usr/bin/env python3
"""Tests for the nightly's postal cities inputs: GeoNames' postal places come from our cache, which
Update GeoNames Postal Places fills and a missing or bad copy of fails the night; yesterday's
postal_cities.bin comes from the newest build latest.json names before today's, whatever its
version, so a fresh night keeps its names, and a rerun of a day from the day its first run took,
which its configurations.json records; a pointer or manifest that can't be read fails the night,
and only one the bucket holds no object for is no state; the build takes both.
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
DOWNLOAD = "Download PBF and previous indexes"
BUILD = "Build geocoder index"
FINALIZE = "Finalize upload and configurations.json"


def workflow(name):
    with open(os.path.join(WORKFLOWS, name)) as f:
        return f.read()


def step_script(text, name):
    """The `run:` script of the step `name` of a workflow's text, dedented."""
    block = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", text, re.M)
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", block.group(1), re.M) if block else None
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines()) if run else None


def postal_block():
    """The download's postal lines: the postal places, then yesterday's postal cities."""
    script = step_script(workflow("geocoder-build.yml"), DOWNLOAD)
    block = re.search(r"^# ── Postal places.*?(?=^\) &$)", script, re.M | re.S)
    return block.group(0) if block else None


def splice_block():
    """The finalize step's lines that splice the build's metadata into configurations.json."""
    script = step_script(workflow("geocoder-build.yml"), FINALIZE)
    block = re.search(r'^PREV_VAL=.*?^   *&& mv "\$WORKDIR/output/configurations.tmp".*?\n', script, re.M | re.S)
    return block.group(0) if block else None


def build_args_block():
    """The build step's postal lines, which add to BUILD_ARGS."""
    script = step_script(workflow("geocoder-build.yml"), BUILD)
    block = re.search(r"^# GeoNames' postal places.*?--prev-postal-cities.*?\nfi\n", script, re.M | re.S)
    return block.group(0) if block else None


# A curl that serves $SERVED/<url path> (after the host), and an s5cmd whose `cat` serves
# $SERVED/s3/<bucket path>: what the cache and the bucket hold. A missing object reads as s5cmd
# 2.3's "given object ... not found"; one beside a <path>.fail file as a failed request.
CURL = r"""#!/bin/bash
out=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in -o) out=$2; shift 2 ;; --connect-timeout|--max-time|--retry) shift 2 ;; -*) shift ;; *) url=$1; shift ;; esac
done
path="$SERVED/${url#*://*/}"
[ -f "$path" ] || exit 22
if [ -n "$out" ]; then cp "$path" "$out"; else cat "$path"; fi
"""
S5CMD = r"""#!/bin/bash
while [ $# -gt 0 ]; do case "$1" in cat) shift; break ;; *) shift ;; esac; done
path="$SERVED/s3/${1#s3://}"
if [ -f "$path.fail" ]; then echo "ERROR \"cat $1\": InternalError: We encountered an internal error. status code: 500" >&2; exit 1; fi
if ! [ -f "$path" ]; then echo "ERROR \"cat $1\": given object $1 not found" >&2; exit 1; fi
cat "$path"
"""


# An s5cmd whose `cat` reads as S5CMD's, and whose `cp SRC DST` and `pipe DST` write
# $SERVED/s3/<bucket path>, each write noted in $NOTES/uploads.
S5CMD_RW = r"""#!/bin/bash
while [ $# -gt 0 ]; do case "$1" in cat|cp|pipe) op=$1; shift; break ;; *) shift ;; esac; done
case "$op" in
  cat)
    path="$SERVED/s3/${1#s3://}"
    if [ -f "$path.fail" ]; then echo "ERROR \"cat $1\": InternalError: We encountered an internal error. status code: 500" >&2; exit 1; fi
    if ! [ -f "$path" ]; then echo "ERROR \"cat $1\": given object $1 not found" >&2; exit 1; fi
    cat "$path" ;;
  cp) mkdir -p "$(dirname "$SERVED/s3/${2#s3://}")"; cp "$1" "$SERVED/s3/${2#s3://}"; echo "$2" >> "$NOTES/uploads" ;;
  pipe) mkdir -p "$(dirname "$SERVED/s3/${1#s3://}")"; cat > "$SERVED/s3/${1#s3://}"; echo "$1" >> "$NOTES/uploads" ;;
esac
"""
# postal_places.py as the refresh runs it: OUT ZIP... --previous-rows N.
FAKE_POSTAL_PLACES = r"""#!/bin/bash
shift
echo "$@" > "$NOTES/args"
if [ -n "${REFUSE:-}" ]; then echo "postal_places: 1 rows, fewer than 99% of the cache's 41706" >&2; exit 1; fi
echo gz > "$1"
echo '{"rows": 41706, "size": 3, "sha256": "aa", "sources": {"US": 41529}}'
"""


class NightlyPostalCitiesTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = self.dir.name
        self.served = os.path.join(self.root, "served")
        self.work = os.path.join(self.root, "work")
        self.bin = os.path.join(self.root, "bin")
        for d in (self.served, self.work, self.bin):
            os.makedirs(d)
        for name, text in (("curl", CURL), ("s5cmd", S5CMD)):
            with open(os.path.join(self.bin, name), "w") as f:
                f.write(text)
            os.chmod(os.path.join(self.bin, name), 0o755)

    def serve(self, path, data):
        at = os.path.join(self.served, path)
        os.makedirs(os.path.dirname(at), exist_ok=True)
        with open(at, "wb") as f:
            f.write(data if isinstance(data, bytes) else data.encode())

    def cache(self, data=b"US\t19026\tDrexel Hill\tPA\n", sha=None, path="geonames/20261123/77-1/postal_places.tsv.gz"):
        self.serve(f"geocoder/{path}", data)
        self.serve("geocoder/geonames/latest.json", json.dumps(
            {"path": path, "size": len(data), "sha256": sha or hashlib.sha256(data).hexdigest(), "rows": 1}))

    def build(self, date, table=None, listed=True, built_from=None):
        """A published build of `date`: its configurations.json, and its postal table (zstd); its
        build.postal_cities_previous `built_from` where given ("" for null)."""
        files = {}
        if table is not None:
            self.serve(f"s3/bucket/geocoder/builds/{date}/planet/admin/postal_cities.bin.zst",
                       subprocess.run(["zstd", "-q", "-c"], input=table, capture_output=True, check=True).stdout)
            if listed:
                files["planet/admin/postal_cities.bin"] = {"sha256": hashlib.sha256(table).hexdigest()}
        manifest = {"files": files}
        if built_from is not None:
            manifest["build"] = {"date": date, "postal_cities_previous": built_from or None}
        self.serve(f"s3/bucket/geocoder/builds/{date}/configurations.json", json.dumps(manifest))

    def latest(self, latest, previous):
        self.serve("s3/bucket/geocoder/builds/latest.json", json.dumps({"latest": latest, "previous": previous}))

    def fail(self, path):
        """A read of the bucket's object `path` that fails."""
        self.serve(f"s3/bucket/geocoder/{path}.fail", "")

    def table_of(self, outputs):
        with open(outputs["prev_postal_cities"], "rb") as f:
            return f.read()

    def run_block(self, script):
        out = os.path.join(self.root, "github_output")
        open(out, "w").close()
        env = {"PATH": self.bin + os.pathsep + os.environ["PATH"], "SERVED": self.served, "WORKDIR": self.work,
               "GITHUB_OUTPUT": out, "S3_PUBLIC_URL": "https://cache.example", "S3_BUCKET": "bucket",
               "S3_PREFIX": "geocoder", "S5": "s5cmd", "TODAY": "2026-11-23"}
        r = subprocess.run(["bash", "-eo", "pipefail", "-c", script], env=env, capture_output=True, text=True)
        with open(out) as f:
            outputs = dict(line.rstrip("\n").split("=", 1) for line in f if "=" in line)
        return r, outputs

    def test_the_download_reads_the_postal_places_from_the_cache_and_fails_without_them(self):
        block = postal_block()
        self.assertIsNotNone(block)
        self.assertNotRegex(block, r"https?://[^\s\"']*geonames\.org")
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 1)
        self.assertIn("::error::[postal] No postal places", r.stdout)
        self.cache(sha="0" * 64)
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 1)
        self.assertIn("not the file latest.json names", r.stdout)
        self.cache(path="geonames/latest/postal_places.tsv.gz")
        self.assertEqual(self.run_block(block)[0].returncode, 1)
        self.cache()
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(outputs["postal_places"], os.path.join(self.work, "postal_places.tsv.gz"))
        with open(outputs["postal_places"], "rb") as f:
            self.assertEqual(f.read(), b"US\t19026\tDrexel Hill\tPA\n")

    def test_yesterdays_postal_cities_come_from_the_newest_build_before_today_whatever_its_version(self):
        block = postal_block()
        self.assertNotIn("HAS_PREVIOUS", block)
        self.cache()
        # No build yet: none, and the night goes on.
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertNotIn("prev_postal_cities", outputs)
        self.assertEqual(outputs["prev_postal_cities_date"], "null")
        # The newest build before today's: latest, or its previous where latest is today's.
        self.build("2026-11-21", b"day 21")
        self.build("2026-11-22", b"day 22")
        self.build("2026-11-23", b"day 23")
        for latest, previous, want in [("2026-11-22", "2026-11-21", b"day 22"), ("2026-11-23", "2026-11-22", b"day 22"),
                                       ("2026-11-21", None, b"day 21")]:
            self.serve("s3/bucket/geocoder/builds/latest.json", json.dumps({"latest": latest, "previous": previous}))
            r, outputs = self.run_block(block)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertEqual(self.table_of(outputs), want, latest)
            self.assertEqual(outputs["prev_postal_cities_date"], "2026-11-" + want.decode()[-2:])
        # A build that published none.
        self.build("2026-11-22", b"day 22", listed=False)
        self.serve("s3/bucket/geocoder/builds/latest.json", json.dumps({"latest": "2026-11-22", "previous": None}))
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertNotIn("prev_postal_cities", outputs)
        # One that did, whose table isn't the one it names, fails the night.
        self.build("2026-11-22", b"day 22")
        self.serve("s3/bucket/geocoder/builds/2026-11-22/planet/admin/postal_cities.bin.zst", b"not zstd")
        r, _ = self.run_block(block)
        self.assertEqual(r.returncode, 1)
        self.assertIn("::error::[postal] The postal cities of 2026-11-22", r.stdout)

    def test_a_rerun_of_a_day_takes_the_postal_cities_its_first_run_took(self):
        block = postal_block()
        self.cache()
        self.build("2026-11-20", b"day 20")
        self.build("2026-11-22", b"day 22")
        # Day 23's first run, fresh, takes day 22's; it publishes with no previous build.
        self.latest("2026-11-22", "2026-11-21")
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.table_of(outputs), b"day 22")
        self.assertEqual(outputs["prev_postal_cities_date"], "2026-11-22")
        self.build("2026-11-23", b"day 23", built_from="2026-11-22")
        self.latest("2026-11-23", None)
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.table_of(outputs), b"day 22")
        self.assertEqual(outputs["prev_postal_cities_date"], "2026-11-22")
        # One that took none takes none again, whatever latest.json names; one after a gap, the
        # day before the gap.
        self.build("2026-11-23", b"day 23", built_from="")
        self.latest("2026-11-22", None)
        r, outputs = self.run_block(block)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertNotIn("prev_postal_cities", outputs)
        self.assertEqual(outputs["prev_postal_cities_date"], "null")
        self.build("2026-11-23", b"day 23", built_from="2026-11-20")
        r, outputs = self.run_block(block)
        self.assertEqual(self.table_of(outputs), b"day 20")
        # A day of a build of before the record: latest.json's, as for a first run.
        self.build("2026-11-23", b"day 23")
        self.latest("2026-11-23", "2026-11-22")
        r, outputs = self.run_block(block)
        self.assertEqual(self.table_of(outputs), b"day 22")
        # A record of no day before today's fails the night.
        for bad in ("2026-11-23", "2026-11-24", "latest"):
            self.build("2026-11-23", b"day 23", built_from=bad)
            r, _ = self.run_block(block)
            self.assertEqual(r.returncode, 1, bad)
            self.assertIn("is no day before 2026-11-23", r.stdout)

    def test_a_pointer_or_manifest_that_cant_be_read_fails_the_night(self):
        block = postal_block()
        self.cache()
        self.build("2026-11-22", b"day 22")
        self.latest("2026-11-22", None)
        self.assertEqual(self.run_block(block)[0].returncode, 0)
        # latest.json, the named build's configurations.json, and today's (a rerun's) each fail
        # the night when their read fails, and when they hold no JSON object.
        for path in ("builds/latest.json", "builds/2026-11-22/configurations.json", "builds/2026-11-23/configurations.json"):
            self.fail(path)
            r, outputs = self.run_block(block)
            self.assertEqual(r.returncode, 1, path)
            self.assertIn("::error::[postal] Could not read s3://bucket/geocoder/" + path, r.stdout)
            self.assertIn("status code: 500", r.stdout)
            self.assertNotIn("prev_postal_cities", outputs)
            os.remove(os.path.join(self.served, f"s3/bucket/geocoder/{path}.fail"))
        self.assertEqual(self.run_block(block)[0].returncode, 0)
        for path, text in (("builds/latest.json", "<html>"), ("builds/latest.json", "[]"),
                           ("builds/2026-11-22/configurations.json", "{\"files\": "),
                           ("builds/2026-11-23/configurations.json", "")):
            self.serve(f"s3/bucket/geocoder/{path}", text)
            r, _ = self.run_block(block)
            self.assertEqual(r.returncode, 1, path)
            self.assertIn("holds no JSON object", r.stdout)
            if path == "builds/latest.json":
                self.latest("2026-11-22", None)
            elif path.startswith("builds/2026-11-22"):
                self.build("2026-11-22", b"day 22")
            else:
                os.remove(os.path.join(self.served, f"s3/bucket/geocoder/{path}"))
        self.assertEqual(self.run_block(block)[0].returncode, 0)
        # The build latest.json names has no configurations.json: no state to go on.
        os.remove(os.path.join(self.served, "s3/bucket/geocoder/builds/2026-11-22/configurations.json"))
        r, _ = self.run_block(block)
        self.assertEqual(r.returncode, 1)
        self.assertIn("The build of 2026-11-22 has no configurations.json", r.stdout)

    def test_the_build_records_the_day_whose_postal_cities_it_took(self):
        block = splice_block()
        self.assertIsNotNone(block)
        text = workflow("geocoder-build.yml")
        self.assertIn("POSTAL_CITIES_PREVIOUS: ${{ steps.download.outputs.prev_postal_cities_date || 'null' }}", text)
        out = os.path.join(self.work, "output")
        os.makedirs(out)

        def spliced(postal):
            with open(os.path.join(out, "configurations.json"), "w") as f:
                json.dump({"build": {"version": 24, "patch_version": 21}, "files": {}}, f)
            env = {"PATH": os.environ["PATH"], "WORKDIR": self.work, "NEW_DATE": "2026-11-23", "OLD_DATE": "2026-11-22",
                   "HAS_PREVIOUS": "false", "WIKIDATA_DATE": "null", "POSTAL_CITIES_PREVIOUS": postal}
            r = subprocess.run(["bash", "-eo", "pipefail", "-c", block], env=env, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            with open(os.path.join(out, "configurations.json")) as f:
                return json.load(f)["build"]

        build = spliced("2026-11-20")
        self.assertEqual(build["postal_cities_previous"], "2026-11-20")
        self.assertEqual((build["version"], build["date"], build["previous"]), (24, "2026-11-23", None))
        self.assertIsNone(spliced("null")["postal_cities_previous"])

    def test_the_build_takes_the_postal_places_and_yesterdays_postal_cities(self):
        block = build_args_block()
        self.assertIsNotNone(block)
        places, prev = os.path.join(self.root, "places.tsv.gz"), os.path.join(self.root, "prev.bin")
        for path in (places, prev):
            with open(path, "wb") as f:
                f.write(b"x")
        script = 'BUILD_ARGS="--continents"\n' + block + 'echo "ARGS $BUILD_ARGS"\n'

        def args(env):
            r = subprocess.run(["bash", "-eo", "pipefail", "-c", script], env={"PATH": os.environ["PATH"], **env},
                               capture_output=True, text=True)
            return r.returncode, re.search(r"^ARGS (.*)$", r.stdout, re.M).group(1) if r.returncode == 0 else r.stdout

        self.assertEqual(args({"POSTAL_PLACES_FILE": places, "PREV_POSTAL_CITIES": prev}),
                         (0, f"--continents --postal-places {places} --prev-postal-cities {prev}"))
        self.assertEqual(args({"POSTAL_PLACES_FILE": places, "PREV_POSTAL_CITIES": ""}), (0, f"--continents --postal-places {places}"))
        rc, stdout = args({"POSTAL_PLACES_FILE": "", "PREV_POSTAL_CITIES": prev})
        self.assertEqual(rc, 1)
        self.assertIn("::error::No GeoNames postal places", stdout)

    def test_the_cache_refreshes_weekly_from_geonames_and_never_overwrites_a_file(self):
        text = workflow("geocoder-geonames.yml")
        self.assertRegex(text, r"(?m)^  schedule:\n    - cron: '[^']+'")
        self.assertIn("workflow_dispatch:", text)
        script = step_script(text, "Build and upload postal_places.tsv.gz")
        self.assertIsNotNone(script)
        self.assertIn("postal_places.py", script)
        self.assertIn("--previous-rows", script)
        # A run's own key, as the nightly's check of the path expects.
        self.assertIn('KEY="geonames/$DATE/$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"', script)
        downloads = step_script(text, "Download GeoNames postal code files")
        self.assertIn("for CC in US PR VI GU AS MP; do", downloads)
        self.assertIn("readme.txt", downloads)

    def test_refreshes_of_the_cache_run_one_at_a_time(self):
        self.assertRegex(workflow("geocoder-geonames.yml"),
                         r"(?m)^concurrency:\n  group: geocoder-geonames\n  cancel-in-progress: false$")

    def test_a_refresh_checks_its_rows_against_the_buckets_latest_json_and_fails_without_it(self):
        script = step_script(workflow("geocoder-geonames.yml"), "Build and upload postal_places.tsv.gz")
        self.assertNotIn("S3_PUBLIC_URL", workflow("geocoder-geonames.yml"))
        with open(os.path.join(self.bin, "s5cmd"), "w") as f:
            f.write(S5CMD_RW)
        # postal_places.py as the step runs it: its arguments noted, its summary printed, unless
        # it refuses the files.
        with open(os.path.join(self.bin, "python3"), "w") as f:
            f.write(FAKE_POSTAL_PLACES)
        for name in ("s5cmd", "python3"):
            os.chmod(os.path.join(self.bin, name), 0o755)
        runner = os.path.join(self.root, "runner")
        os.makedirs(os.path.join(runner, "geonames"))
        with open(os.path.join(runner, "geonames", "readme.txt"), "w") as f:
            f.write("GeoNames\n")
        pointer = os.path.join(self.served, "s3", "bucket", "geocoder", "geonames", "latest.json")

        def refresh(**env):
            for name in ("args", "uploads"):
                if os.path.exists(os.path.join(self.root, name)):
                    os.remove(os.path.join(self.root, name))
            r = subprocess.run(["bash", "-eo", "pipefail", "-c", script], capture_output=True, text=True, env={
                "PATH": self.bin + os.pathsep + os.environ["PATH"], "SERVED": self.served, "NOTES": self.root,
                "S3_ENDPOINT": "https://bucket.example", "S3_BUCKET": "bucket", "RUNNER_TEMP": runner,
                "GITHUB_RUN_ID": "77", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_STEP_SUMMARY": os.path.join(self.root, "summary"),
                **env})
            args = open(os.path.join(self.root, "args")).read().split() if os.path.exists(os.path.join(self.root, "args")) else None
            uploads = open(os.path.join(self.root, "uploads")).read().split() if os.path.exists(os.path.join(self.root, "uploads")) else []
            return r, args, uploads

        # A first refresh: the bucket holds no latest.json.
        r, args, uploads = refresh()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(args[args.index("--previous-rows") + 1], "0")
        with open(pointer) as f:
            self.assertEqual(json.load(f)["rows"], 41706)
        self.assertEqual(len(uploads), 3)
        # The next one checks against its rows.
        r, args, _ = refresh()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(args[args.index("--previous-rows") + 1], "41706")
        # A refused refresh, a failed read and a latest.json of no row count leave it as it was.
        with open(pointer, "rb") as f:
            before = f.read()
        failing = [({"REFUSE": "1"}, None, "fewer than 99%"),
                   ({}, "fail", "Could not read geocoder/geonames/latest.json"),
                   ({}, '{"path": "geonames/x"}', "gives no row count"),
                   ({}, '{"rows": "41706"}', "gives no row count"),
                   ({}, "<html>", "gives no row count")]
        for env, served, why in failing:
            if served == "fail":
                open(pointer + ".fail", "w").close()
            elif served is not None:
                with open(pointer, "w") as f:
                    f.write(served)
            r, args, uploads = refresh(**env)
            self.assertNotEqual(r.returncode, 0, why)
            self.assertIn(why, r.stdout + r.stderr)
            self.assertEqual(uploads, [], why)
            if served is None:
                self.assertIsNotNone(args)
            else:
                self.assertIsNone(args, why)
            if os.path.exists(pointer + ".fail"):
                os.remove(pointer + ".fail")
            with open(pointer, "rb") as f:
                self.assertEqual(f.read(), before if served in (None, "fail") else served.encode(), why)
            with open(pointer, "wb") as f:
                f.write(before)

    def test_a_change_to_the_cache_workflow_runs_these_tests(self):
        paths = re.search(r"(?ms)^  pull_request:\n    paths:\n(.*?)^\S", workflow("geocoder-tests.yml")).group(1)
        self.assertIn('".github/workflows/geocoder-geonames.yml"', paths)


if __name__ == "__main__":
    unittest.main()
