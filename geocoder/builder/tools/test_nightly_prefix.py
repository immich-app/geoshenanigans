#!/usr/bin/env python3
"""Tests for the nightly's s3_prefix dispatch input: a test run from a branch publishes under a scratch prefix,
a scheduled night under the live one, and the Setup step refuses a prefix the cleanup could reach out of.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")
TEMPLATE = os.path.join(HERE, "..", "configurations.template.json")


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
        self.assertEqual(re.findall(r'(\w+) "\$SEED_FROM', nightly()), ["ls", "patch_base_versions", "cp", "ls"])
        self.assertRegex(nightly(), r'(?m)^ *PREV_CFG=\$\(s5cmd --endpoint-url \$S3_ENDPOINT cat "\$1/configurations\.json"')

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
    """An s5cmd on PATH that logs its arguments, a line a call, and does nothing else; with $UPLOAD_PID set (a pid
    file), each call says whether that process still ran (a zombie, which writes nothing, counts as ended)."""

    def __init__(self, test):
        self.root = tempfile.mkdtemp()
        test.addCleanup(shutil.rmtree, self.root)
        self.calls = os.path.join(self.root, "calls")
        for tool in ("s5cmd", "python3"):
            with open(os.path.join(self.root, tool), "w") as f:
                f.write(f'#!/bin/sh\necho "{tool} $*" >> "{self.calls}"\n'
                        f'[ -z "$UPLOAD_PID" ] || {{ case $(sed "s/.*) //" "/proc/$(cat "$UPLOAD_PID")/stat" 2> /dev/null '
                        f'| cut -c1) in ""|Z|X) echo "  upload ended" ;; *) echo "  upload running" ;; esac; }} '
                        f'>> "{self.calls}"\n')
            os.chmod(os.path.join(self.root, tool), 0o755)
        self.path = f"{self.root}:{os.environ['PATH']}"

    def logged(self):
        if not os.path.exists(self.calls):
            return []
        with open(self.calls) as f:
            return f.read().splitlines()


# The cache bucket's multipart uploads left over a day aborted, as Cleanup old cache bucket asks.
ABORT = "python3 /ws/geocoder/builder/tools/abort_stale_uploads.py http://cache.invalid cache us-east-1"


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
        # And the scratch keys, which the live run's cleanup alone prunes (a scratch run's deletes its own key only).
        self.assertEqual([p for p in paths if not p.startswith("$PBF_CACHE/")],
                         ["geocoder-cache/planet-${YESTERDAY}.osm.pbf"] * 2 + ["geocoder-scratch/*", "geocoder-scratch/$name"])
        for name in ("Download PBF and previous indexes", "Build geocoder index", "Cleanup old cache bucket"):
            self.assertIn("PBF_CACHE: ${{ steps.setup.outputs.pbf_cache }}", step(name), name)

    def run_cleanup(self, pbf_cache, upload_seconds=None):
        """Runs the cleanup step; with upload_seconds, after starting Build geocoder index's PBF upload as that step
        does (detached, in a session of its own) with a copy that takes that long. Its sleeps take 10 ms."""
        s5 = FakeS5cmd(self)
        with open(os.path.join(s5.root, "sleep"), "w") as f:
            f.write("#!/bin/sh\nexec /usr/bin/sleep 0.01\n")
        os.chmod(os.path.join(s5.root, "sleep"), 0o755)
        work = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, work)
        env = {"PATH": s5.path, "PBF_CACHE": pbf_cache, "CACHE_BUCKET": "cache", "CACHE_ENDPOINT": "http://cache.invalid",
               "CACHE_AWS_KEY": "k", "CACHE_AWS_SECRET": "s", "NEW_DATE": "2026-11-21", "KEEP_CACHE_PBF_DAYS": "2",
               "WORKDIR": work, "GITHUB_WORKSPACE": "/ws"}
        if upload_seconds is not None:
            upload = block_from(step_script("Build geocoder index"), 'if [ "$PBF_SOURCE" != "today" ]; then')
            self.assertIsNotNone(upload)
            upload, copies = re.subn(r"\$CACHE cp --concurrency 16 .* \\\n", f"/usr/bin/sleep {upload_seconds} \\\n", upload)
            self.assertEqual(copies, 1)
            subprocess.run(["bash", "-e", "-c", upload], env={**env, "PBF_SOURCE": "yesterday", "TODAY": "2026-11-21"},
                           check=True, capture_output=True, timeout=10)
            env["UPLOAD_PID"] = os.path.join(work, "pbf-cache.pid")
            with open(env["UPLOAD_PID"]) as f:
                self.addCleanup(subprocess.run, ["pkill", "-KILL", "-s", f.read().strip()])
        done = subprocess.run(["bash", "-e", "-c", step_script("Cleanup old cache bucket")], env=env, capture_output=True,
                              text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.output = done.stdout
        return s5.logged()

    # It deletes its own key and prunes no shared PBF. The bucket's multipart uploads past a day go (no run lasts
    # one), so that a stopped upload's parts are reclaimed though no live run cleans that bucket.
    def test_a_scratch_run_deletes_its_own_key_and_nothing_else(self):
        calls = self.run_cleanup("geocoder-scratch/nightly-speed-1121")

        self.assertEqual(calls, ["s5cmd --endpoint-url http://cache.invalid --retry-count 5 --numworkers 15 "
                                 "rm s3://cache/geocoder-scratch/nightly-speed-1121/*", ABORT])

    # Its PBF upload ends first: a copy that finished after the delete would put the PBF back.
    def test_a_scratch_run_deletes_its_own_key_once_its_upload_ended(self):
        calls = self.run_cleanup("geocoder-scratch/nightly-speed-1121", upload_seconds=0.5)

        self.assertEqual(calls, ["s5cmd --endpoint-url http://cache.invalid --retry-count 5 --numworkers 15 "
                                 "rm s3://cache/geocoder-scratch/nightly-speed-1121/*", "  upload ended",
                                 ABORT, "  upload ended"])

    # An upload past the wait is stopped, and gone, before the delete; its parts go with the stale uploads.
    def test_a_scratch_run_stops_an_upload_that_outlasts_the_wait_before_the_delete(self):
        calls = self.run_cleanup("geocoder-scratch/nightly-speed-1121", upload_seconds=300)

        self.assertEqual(calls, ["s5cmd --endpoint-url http://cache.invalid --retry-count 5 --numworkers 15 "
                                 "rm s3://cache/geocoder-scratch/nightly-speed-1121/*", "  upload ended",
                                 ABORT, "  upload ended"])
        self.assertIn("stopping it", self.output)

    # A scratch run deletes its own key afterwards, but not once cancelled, when its cleanup is skipped: the live run
    # deletes what scratch runs left there over a day ago (no run lasts one) and keeps what is newer, beside the
    # shared key's PBFs past the days it keeps.
    def test_a_live_run_deletes_scratch_pbfs_left_over_a_day_ago(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        bucket, bin_dir, work = (os.path.join(root, d) for d in ("bucket", "bin", "work"))
        objects = {"geocoder-cache/planet-2026-11-21.osm.pbf": 0, "geocoder-cache/planet-2026-11-20.osm.pbf": 0,
                   "geocoder-cache/planet-2026-11-19.osm.pbf": 0,
                   "geocoder-scratch/cancelled/planet-2026-11-19.osm.pbf": 2 * 86400,
                   "geocoder-scratch/recent/planet-2026-11-20.osm.pbf": 3600}
        for key, age in objects.items():
            path = os.path.join(bucket, "cache", key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(b"pbf")
            os.utime(path, (time.time() - age, time.time() - age))
        os.makedirs(bin_dir)
        os.makedirs(work)
        for tool, text in (("s5cmd", FAKE_BUCKET_S5CMD.format(python=sys.executable)), ("python3", "#!/bin/sh\nexit 0\n")):
            with open(os.path.join(bin_dir, tool), "w") as f:
                f.write(text)
            os.chmod(os.path.join(bin_dir, tool), 0o755)
        calls = os.path.join(root, "calls")
        open(calls, "w").close()
        env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "PBF_CACHE": "geocoder-cache", "CACHE_BUCKET": "cache",
               "CACHE_ENDPOINT": "http://cache.invalid", "CACHE_AWS_KEY": "k", "CACHE_AWS_SECRET": "s",
               "NEW_DATE": "2026-11-21", "KEEP_CACHE_PBF_DAYS": "2", "WORKDIR": work, "GITHUB_WORKSPACE": "/ws",
               "FAKE_BUCKET": bucket, "FAKE_CALLS": calls}

        done = subprocess.run(["bash", "-e", "-c", step_script("Cleanup old cache bucket")], env=env, capture_output=True,
                              text=True, timeout=60)

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        left = sorted(os.path.relpath(os.path.join(d, n), os.path.join(bucket, "cache"))
                      for d, _, names in os.walk(bucket) for n in names)
        self.assertEqual(left, ["geocoder-cache/planet-2026-11-20.osm.pbf", "geocoder-cache/planet-2026-11-21.osm.pbf",
                                "geocoder-scratch/recent/planet-2026-11-20.osm.pbf"])

    def test_a_live_run_prunes_the_shared_key_and_aborts_its_stale_uploads(self):
        calls = self.run_cleanup("geocoder-cache")

        self.assertIn("s5cmd --endpoint-url http://cache.invalid --retry-count 5 --numworkers 15 "
                      "ls s3://cache/geocoder-cache/planet-*.osm.pbf", calls)
        self.assertEqual(calls[-1], ABORT)


# s5cmd over a bucket of files under $FAKE_BUCKET (s3://<bucket>/<key> is $FAKE_BUCKET/<bucket>/<key>): ls and rm of
# an object or of the objects a pattern matches (* matches any run of characters, / included), as keys from the
# pattern's last / before its first *, each with its file's time (UTC) as its last modification; cp of a prefix's
# every object (<prefix>/*); and cat of an object. Each call is logged to $FAKE_CALLS. A copy fails at its object
# number $FAKE_COPY_FAILS_AFTER, or passes over the key $FAKE_COPY_DROPS and says nothing; with
# $FAKE_LIST_FAILS_AFTER_COPY set, every ls after a copy fails.
FAKE_BUCKET_S5CMD = r"""#!{python}
import os, re, shutil, sys, time
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"]) as f:
    copied = any(" cp " in line for line in f)
with open(os.environ["FAKE_CALLS"], "a") as f:
    f.write("s5cmd " + " ".join(args) + "\n")
verb = next(a for a in args if a in ("ls", "cp", "rm", "cat"))
urls = args[args.index(verb) + 1:]
def local(url):
    path = url[len("s3://"):]
    head = path.split("*", 1)[0]
    return os.path.join(os.environ["FAKE_BUCKET"], head[:head.rfind("/") + 1])
def keys(url):
    base = local(url)
    rest = url[len("s3://"):][len(os.path.relpath(base, os.environ["FAKE_BUCKET"])) + 1:]
    pattern = re.compile(".*".join(re.escape(part) for part in rest.split("*")) + "$")
    found = sorted(k for k in (os.path.relpath(os.path.join(d, n), base) for d, _, names in os.walk(base) for n in names)
                   if pattern.match(k))
    if not found:
        sys.exit("ERROR no object found")
    return found
if verb == "ls":
    if copied and os.environ.get("FAKE_LIST_FAILS_AFTER_COPY"):
        sys.exit("ERROR listing failed")
    for k in keys(urls[0]):
        path = os.path.join(local(urls[0]), k)
        print(f"{{time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(os.path.getmtime(path)))}} "
              f"{{os.path.getsize(path):>15}}  {{k}}")
elif verb == "rm":
    for k in keys(urls[0]):
        os.remove(os.path.join(local(urls[0]), k))
elif verb == "cat":
    with open(os.path.join(local(urls[0]), keys(urls[0])[0]), "rb") as f:
        sys.stdout.buffer.write(f.read())
else:
    for i, k in enumerate(keys(urls[0])):
        if i == int(os.environ.get("FAKE_COPY_FAILS_AFTER", "-1")):
            sys.exit("ERROR copy failed")
        if k != os.environ.get("FAKE_COPY_DROPS"):
            os.makedirs(os.path.dirname(os.path.join(local(urls[1]), k)), exist_ok=True)
            shutil.copyfile(os.path.join(local(urls[0]), k), os.path.join(local(urls[1]), k))
"""

def template_versions():
    """The build version and the transport version this run builds (configurations.template.json)."""
    with open(TEMPLATE) as f:
        template = json.load(f)
    return template["build"]["version"], template["transport"]["version"]


def live_config(build, transport):
    """A live build's configurations.json, of `build` version, its one unit of `transport` version (None: no unit)."""
    units = {} if transport is None else {"planet/full/places.tt.zst": {"kind": "places", "transport_version": transport}}
    return json.dumps({"build": {"date": "2026-11-20", "version": build}, "units": units}).encode()


LIVE = {"configurations.json": live_config(*template_versions()), "planet/full/places.tt.zst": b"unit",
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
        """Setup's seed, after its lines that read a patch base's versions, which it checks the seed's against."""
        versions = re.search(r"^EXPECTED_BUILD_VERSION=.*\n(?:.*\n)*?\}\n", step_script("Setup"), re.M)
        script = block_from(step_script("Setup"), 'if [ "$SEED_PREVIOUS" = true ]; then')
        self.assertIsNotNone(versions)
        self.assertIsNotNone(script)
        script = versions.group(0) + script
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "S3_PREFIX": prefix, "SEED_PREVIOUS": seed,
               "S3_BUCKET": "bucket", "S3_ENDPOINT": "https://s3.invalid", "PREV_DATE": "2026-11-20",
               "GITHUB_WORKSPACE": os.path.join(HERE, "..", "..", ".."),
               "FAKE_BUCKET": self.bucket, "FAKE_CALLS": self.calls, **fake}
        open(self.calls, "a").close()
        # As GitHub runs a step that names no shell: bash -e, without pipefail.
        done = subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True, env=env)
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

    # At the copy: the check of the listings after it would fail the setup too, but no listing comes.
    def test_a_copy_that_failed_fails_the_setup(self):
        done, calls = self.run_seed("nightly-speed-1121", "true", FAKE_COPY_FAILS_AFTER="2")

        self.assertNotEqual(done.returncode, 0)
        copy = next(i for i, c in enumerate(calls) if " cp " in c)
        self.assertEqual([c for c in calls[copy + 1:] if " ls " in c], [])

    # A copy that says it is done but left an object out is caught by the check of the copy against the live build.
    def test_a_copy_that_left_an_object_out_fails_the_setup(self):
        done, _ = self.run_seed("nightly-speed-1121", "true", FAKE_COPY_DROPS="planet/full/places.tt.zst")

        self.assertNotEqual(done.returncode, 0)
        self.assertIn("planet/full/places.tt.zst", done.stdout + done.stderr)

    # Listings that failed after the copy compare as two empty ones: the check fails instead.
    def test_a_listing_that_failed_fails_the_setup(self):
        done, _ = self.run_seed("nightly-speed-1121", "true", FAKE_LIST_FAILS_AFTER_COPY="1")

        self.assertNotEqual(done.returncode, 0)
        self.assertIn("::error::", done.stdout)

    # A live build past the full-index window keeps its patches and configurations.json alone: the test would
    # build fresh from it, not patch. Setup fails before it deletes what the scratch prefix holds at that date.
    def test_a_live_build_without_its_full_index_is_no_seed(self):
        shutil.rmtree(os.path.join(self.bucket, "bucket", "geocoder/builds/2026-11-20"))
        aged = {"configurations.json": b"{}", "planet/full/patch.gcpatch": b"patch"}
        self.put("geocoder/builds/2026-11-20", aged)
        earlier = {"planet/full/geo_cells.bin.zst": b"earlier", "configurations.json": b'{"build": {"date": "old"}}'}
        self.put("nightly-speed-1121/builds/2026-11-20", earlier)

        done, calls = self.run_seed("nightly-speed-1121", "true")

        self.assertNotEqual(done.returncode, 0)
        self.assertIn("no full index", done.stdout)
        self.assertEqual(self.objects("nightly-speed-1121/builds/2026-11-20"), earlier)
        self.assertEqual([c for c in calls if " rm " in c or " cp " in c], [])

    # Nor is one of other versions than this run's: a branch that bumps build.version, a unit of another transport
    # version (the fetch decodes the units with tonight's geocoder-transport), no unit, or no configurations.json to
    # say. The run would build fresh from it, not patch: Setup fails before it deletes the scratch prefix's date.
    def test_a_live_build_of_other_versions_is_no_seed(self):
        build, transport = template_versions()
        for name, config in (("build version", live_config(build + 1, transport)),
                             ("transport version", live_config(build, transport + 1)),
                             ("no unit", live_config(build, None)), ("no configurations.json", None)):
            with self.subTest(name):
                shutil.rmtree(os.path.join(self.bucket, "bucket"))
                self.put("geocoder/builds/2026-11-20", {k: v for k, v in dict(LIVE, **{"configurations.json": config}).items()
                                                        if v is not None})
                earlier = {"planet/full/geo_cells.bin.zst": b"earlier", "configurations.json": b'{"build": {"date": "old"}}'}
                self.put("nightly-speed-1121/builds/2026-11-20", earlier)
                if os.path.exists(self.calls):
                    os.remove(self.calls)

                done, calls = self.run_seed("nightly-speed-1121", "true")

                self.assertNotEqual(done.returncode, 0)
                self.assertIn("::error::", done.stdout)
                self.assertEqual(self.objects("nightly-speed-1121/builds/2026-11-20"), earlier)
                self.assertEqual([c for c in calls if " rm " in c or " cp " in c], [])

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
