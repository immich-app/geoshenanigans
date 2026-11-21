#!/usr/bin/env python3
"""Tests for the nightly's s3_prefix dispatch input: a test run from a branch publishes under a scratch prefix,
a scheduled night under the live one, and the Setup step refuses a prefix the cleanup could reach out of.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")


def nightly():
    with open(NIGHTLY) as f:
        return f.read()


def dispatch_input(name):
    """The lines of one workflow_dispatch input, as "key: value" pairs."""
    inputs = re.search(r"^  workflow_dispatch:\n    inputs:\n((?:      .*\n)*)", nightly(), re.M)
    block = re.search(rf"^      {name}:\n((?:        .*\n)*)", inputs.group(1), re.M)
    if not block:
        return None
    return dict(re.findall(r"^        (\w+): (.*)$", block.group(1), re.M))


def prefix_check():
    """The Setup step's test of S3_PREFIX, as a bash script."""
    check = re.search(r'^( *)if ! \[\[ "\$S3_PREFIX" =~ .*\n(?:\1  .*\n)*\1fi\n', nightly(), re.M)
    return check.group(0) if check else None


class S3PrefixTest(unittest.TestCase):
    def test_a_dispatch_names_its_prefix_defaulting_to_the_live_one(self):
        spec = dispatch_input("s3_prefix")
        self.assertIsNotNone(spec)
        self.assertEqual(spec.get("type"), "string")
        self.assertEqual(spec.get("default"), "'geocoder'")

    def test_a_scheduled_night_has_no_inputs_and_publishes_under_geocoder(self):
        self.assertRegex(nightly(), r"(?m)^  S3_PREFIX: \$\{\{ inputs\.s3_prefix \|\| 'geocoder' \}\}$")

    def test_every_bucket_path_goes_under_the_prefix(self):
        # The builds, latest.json and memwatch keys all sit under it, so a scratch run touches nothing live; the
        # one live path is the build a scratch run's seed copies from, which it only reads.
        paths = re.findall(r"s3://\$S3_BUCKET/([^\"\s]*)", nightly())
        self.assertTrue(paths)
        self.assertEqual([p for p in paths if not p.startswith("$S3_PREFIX/")], ["geocoder/builds/$PREV_DATE"])
        self.assertEqual(re.findall(r'(\w+) "\$SEED_FROM', nightly()), ["cp", "ls"])

    def test_the_setup_refuses_a_prefix_that_is_not_one_segment(self):
        script = prefix_check()
        self.assertIsNotNone(script)
        for prefix, ok in [("geocoder", True), ("geocoder-runner-check", True), ("arcs.pr3_check", True),
                           ("", False), ("a/b", False), ("../geocoder", False), ("/geocoder", False),
                           ("geocoder builds", False), ("Geocoder", False), ("-x", False), ("geocoder\n", False)]:
            r = subprocess.run(["bash", "-c", script], env={"S3_PREFIX": prefix, "PATH": os.environ["PATH"]},
                               capture_output=True, text=True)
            self.assertEqual(r.returncode == 0, ok, f"{prefix!r}: {r.stdout}{r.stderr}")


def step(name):
    """The lines of the nightly's step `name`, but for its `- name:` line."""
    block = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", nightly(), re.M)
    return block.group(1) if block else None


def step_script(name):
    """The `run:` script of the step `name`, unindented."""
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", step(name), re.M)
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines()) if run else None


def block_from(script, first_line):
    """The lines of `script` from the one that starts with `first_line` to the `fi` that closes it."""
    found = re.search(rf"^{re.escape(first_line)}.*\n(?:.*\n)*?fi\n", script, re.M)
    return found.group(0) if found else None


class FakeS5cmd:
    """An s5cmd on PATH that logs its arguments, a line a call, and does nothing else."""

    def __init__(self, test):
        self.root = tempfile.mkdtemp()
        test.addCleanup(shutil.rmtree, self.root)
        self.calls = os.path.join(self.root, "calls")
        for tool in ("s5cmd", "python3"):
            with open(os.path.join(self.root, tool), "w") as f:
                f.write(f'#!/bin/sh\necho "{tool} $*" >> "{self.calls}"\n')
            os.chmod(os.path.join(self.root, tool), 0o755)
        self.path = f"{self.root}:{os.environ['PATH']}"

    def logged(self):
        if not os.path.exists(self.calls):
            return []
        with open(self.calls) as f:
            return f.read().splitlines()


class ScratchPbfCacheTest(unittest.TestCase):
    """A scratch prefix's run keeps off the PBF cache the nightly reads: it never takes today's PBF from there (so
    it applies the diff, and can run after the nightly), writes its own PBF under a key of its own and deletes it
    afterwards, and prunes nothing shared."""

    def pbf_cache(self, prefix):
        script = block_from(step_script("Setup"), 'if [ "$S3_PREFIX" = geocoder ]; then')
        self.assertIsNotNone(script)
        return subprocess.run(["bash", "-c", script + 'echo "$PBF_CACHE"'], env={"S3_PREFIX": prefix, "PATH": os.environ["PATH"]},
                              check=True, capture_output=True, text=True).stdout.strip()

    def test_a_live_run_uses_the_shared_key_and_a_scratch_run_one_of_its_own(self):
        self.assertEqual(self.pbf_cache("geocoder"), "geocoder-cache")
        self.assertEqual(self.pbf_cache("nightly-speed-1121"), "geocoder-scratch/nightly-speed-1121")
        self.assertIn('echo "pbf_cache=$PBF_CACHE" >> "$GITHUB_OUTPUT"', step_script("Setup"))

    def test_only_yesterdays_pbf_is_read_from_the_shared_key_whatever_the_prefix(self):
        paths = re.findall(r"s3://\$CACHE_BUCKET/([^\"\s]*)", nightly())
        self.assertTrue(paths)
        self.assertEqual([p for p in paths if not p.startswith("$PBF_CACHE/")],
                         ["geocoder-cache/planet-${YESTERDAY}.osm.pbf"] * 2)
        for name in ("Download PBF and previous indexes", "Build geocoder index", "Cleanup old cache bucket"):
            self.assertIn("PBF_CACHE: ${{ steps.setup.outputs.pbf_cache }}", step(name), name)

    def run_cleanup(self, pbf_cache):
        s5 = FakeS5cmd(self)
        work = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, work)
        script = step_script("Cleanup old cache bucket")
        env = {"PATH": s5.path, "PBF_CACHE": pbf_cache, "CACHE_BUCKET": "cache", "CACHE_ENDPOINT": "http://cache.invalid",
               "CACHE_AWS_KEY": "k", "CACHE_AWS_SECRET": "s", "NEW_DATE": "2026-11-21", "KEEP_CACHE_PBF_DAYS": "2",
               "WORKDIR": work, "GITHUB_WORKSPACE": "/ws"}
        subprocess.run(["bash", "-e", "-c", script], env=env, check=True, capture_output=True, text=True)
        return s5.logged()

    def test_a_scratch_run_deletes_its_own_key_and_nothing_else(self):
        calls = self.run_cleanup("geocoder-scratch/nightly-speed-1121")

        self.assertEqual(calls, ["s5cmd --endpoint-url http://cache.invalid --retry-count 5 --numworkers 15 "
                                 "rm s3://cache/geocoder-scratch/nightly-speed-1121/*"])

    def test_a_live_run_prunes_the_shared_key_and_aborts_its_stale_uploads(self):
        calls = self.run_cleanup("geocoder-cache")

        self.assertIn("s5cmd --endpoint-url http://cache.invalid --retry-count 5 --numworkers 15 "
                      "ls s3://cache/geocoder-cache/planet-*.osm.pbf", calls)
        self.assertTrue([c for c in calls if c.startswith("python3 /ws/geocoder/builder/tools/abort_stale_uploads.py")])


# s5cmd over a bucket of files under $FAKE_BUCKET (s3://<bucket>/<key> is $FAKE_BUCKET/<bucket>/<key>): ls, cp and
# rm of a prefix's every object (<prefix>/*), each call logged to $FAKE_CALLS. A copy fails at its object number
# $FAKE_COPY_FAILS_AFTER, or passes over the key $FAKE_COPY_DROPS and says nothing.
FAKE_BUCKET_S5CMD = r"""#!{python}
import os, shutil, sys
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as f:
    f.write("s5cmd " + " ".join(args) + "\n")
verb = next(a for a in args if a in ("ls", "cp", "rm"))
urls = args[args.index(verb) + 1:]
def local(url):
    return os.path.join(os.environ["FAKE_BUCKET"], url[len("s3://"):].rstrip("*"))
def keys(url):
    base = local(url)
    found = sorted(os.path.relpath(os.path.join(d, n), base) for d, _, names in os.walk(base) for n in names)
    if not found:
        sys.exit("ERROR no object found")
    return found
if verb == "ls":
    for k in keys(urls[0]):
        print(f"2026/11/20 06:45:24 {{os.path.getsize(os.path.join(local(urls[0]), k)):>15}}  {{k}}")
elif verb == "rm":
    for k in keys(urls[0]):
        os.remove(os.path.join(local(urls[0]), k))
else:
    for i, k in enumerate(keys(urls[0])):
        if i == int(os.environ.get("FAKE_COPY_FAILS_AFTER", "-1")):
            sys.exit("ERROR copy failed")
        if k != os.environ.get("FAKE_COPY_DROPS"):
            os.makedirs(os.path.dirname(os.path.join(local(urls[1]), k)), exist_ok=True)
            shutil.copyfile(os.path.join(local(urls[0]), k), os.path.join(local(urls[1]), k))
"""

LIVE = {"configurations.json": b'{"build": {"date": "2026-11-20"}}', "planet/full/places.tt.zst": b"unit",
        "planet/full/geo_cells.bin.zst": b"cells", "planet/full/ways.osm_ids": b"ids" * 10}


class SeedPreviousTest(unittest.TestCase):
    """seed_previous makes a scratch prefix's date the live build of the day before, for a test run's patch night:
    that build's object set alone, whatever an earlier run left there."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.bucket = os.path.join(self.root, "bucket")
        self.bin = os.path.join(self.root, "bin")
        os.makedirs(self.bin)
        with open(os.path.join(self.bin, "s5cmd"), "w") as f:
            f.write(FAKE_BUCKET_S5CMD.format(python=sys.executable))
        os.chmod(os.path.join(self.bin, "s5cmd"), 0o755)
        self.calls = os.path.join(self.root, "calls")
        self.put("geocoder/builds/2026-11-20", LIVE)

    def put(self, prefix, objects):
        for key, data in objects.items():
            path = os.path.join(self.bucket, "bucket", prefix, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)

    def objects(self, prefix):
        base = os.path.join(self.bucket, "bucket", prefix)
        found = {}
        for d, _, names in os.walk(base):
            for n in names:
                with open(os.path.join(d, n), "rb") as f:
                    found[os.path.relpath(os.path.join(d, n), base)] = f.read()
        return found

    def run_seed(self, prefix, seed, **fake):
        script = block_from(step_script("Setup"), 'if [ "$SEED_PREVIOUS" = true ]; then')
        self.assertIsNotNone(script)
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "S3_PREFIX": prefix, "SEED_PREVIOUS": seed,
               "S3_BUCKET": "bucket", "S3_ENDPOINT": "https://s3.invalid", "PREV_DATE": "2026-11-20",
               "FAKE_BUCKET": self.bucket, "FAKE_CALLS": self.calls, **fake}
        done = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script], capture_output=True, text=True, env=env)
        calls = []
        if os.path.exists(self.calls):
            with open(self.calls) as f:
                calls = f.read().splitlines()
        return done, calls

    def test_a_dispatch_may_ask_for_it_and_a_scheduled_night_does_not(self):
        spec = dispatch_input("seed_previous")
        self.assertIsNotNone(spec)
        self.assertEqual((spec.get("type"), spec.get("default")), ("boolean", "false"))
        self.assertIn("SEED_PREVIOUS: ${{ inputs.seed_previous }}", step("Setup"))

    def test_it_copies_the_live_build_into_the_scratch_prefix(self):
        done, _ = self.run_seed("nightly-speed-1121", "true")

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(self.objects("nightly-speed-1121/builds/2026-11-20"), LIVE)
        self.assertEqual(self.objects("geocoder/builds/2026-11-20"), LIVE)

    # An earlier run's build of that date, its standalone .zst where the live build has a unit or a configuration
    # of its own, would otherwise stay beside the copy, and the run patch from a mix of the two.
    def test_what_an_earlier_run_left_at_that_date_goes(self):
        self.put("nightly-speed-1121/builds/2026-11-20", {"planet/full/place_nodes.bin.zst": b"standalone",
                                                          "configurations.json": b'{"build": {"date": "old"}}',
                                                          "planet/full/patch.gcpatch": b"old patch"})
        self.put("nightly-speed-1121/builds/2026-11-19", {"configurations.json": b"{}"})

        done, _ = self.run_seed("nightly-speed-1121", "true")

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(self.objects("nightly-speed-1121/builds/2026-11-20"), LIVE)
        self.assertEqual(self.objects("nightly-speed-1121/builds/2026-11-19"), {"configurations.json": b"{}"})

    def test_a_copy_that_failed_fails_the_setup(self):
        done, _ = self.run_seed("nightly-speed-1121", "true", FAKE_COPY_FAILS_AFTER="2")

        self.assertNotEqual(done.returncode, 0)

    # A copy that says it is done but left an object out is caught by the check of the copy against the live build.
    def test_a_copy_that_left_an_object_out_fails_the_setup(self):
        done, _ = self.run_seed("nightly-speed-1121", "true", FAKE_COPY_DROPS="planet/full/places.tt.zst")

        self.assertNotEqual(done.returncode, 0)
        self.assertIn("planet/full/places.tt.zst", done.stdout + done.stderr)

    def test_it_refuses_the_live_prefix_and_does_nothing_unasked(self):
        done, calls = self.run_seed("geocoder", "true")
        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(calls, [])
        for seed in ("false", ""):
            done, calls = self.run_seed("nightly-speed-1121", seed)
            self.assertEqual((done.returncode, calls), (0, []))
        self.assertEqual(self.objects("geocoder/builds/2026-11-20"), LIVE)

    # The seed lands before Setup looks for the previous build, which the run then patches from.
    def test_it_lands_before_the_previous_build_is_looked_for(self):
        script = step_script("Setup")
        self.assertLess(script.index('if [ "$SEED_PREVIOUS" = true ]; then'), script.index('PREV_BUILD="s3://'))


if __name__ == "__main__":
    unittest.main()
