#!/usr/bin/env python3
"""Tests for the order of the nightly's steps and what runs beside what: the day's diff and today's PBF in the
download, the Rust toolchain in the background, the place search beside compression and the patches from
build-index's exit, and the sidecars' upload beside them; and which runs the watchdog retries.
Run: python3 -m unittest discover -s geocoder/builder/tools (the tests that parse a workflow need PyYAML, Debian's
python3-yaml, which geocoder-tests.yml installs)."""
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")
WATCHDOG = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build-watchdog.yml")


def nightly():
    with open(NIGHTLY) as f:
        return f.read()


WORKFLOWS = os.path.dirname(NIGHTLY)


def workflow_yaml(path):
    """The workflow at path as GitHub reads it: parsed YAML."""
    import yaml
    with open(path) as f:
        return yaml.safe_load(f)


def steps():
    """The nightly's steps in order, each its name (or its action) and its lines."""
    found = re.findall(r"^      - (?:name: (.+)|uses: (\S+).*)\n((?:(?: {8}.*)?\n)*)", nightly(), re.M)
    return [(name or uses, body) for name, uses, body in found]


def step(name):
    return dict(steps())[name]


def step_script(name):
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", step(name), re.M)
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines())


def index_of(name):
    return [n for n, _ in steps()].index(name)


def loop_of(script, first):
    """The lines of `script` from the first that starts with `first` (indentation aside) to its loop's `done`."""
    lines = script.splitlines()
    at = next(i for i, line in enumerate(lines) if line.lstrip().startswith(first))
    end = next(i for i in range(at, len(lines)) if re.search(r"(^|; )done$", lines[i].strip()))
    return "\n".join(line.strip() for line in lines[at:end + 1]) + "\n"


def block_at(script, first):
    """The lines of `script` from the first that is `first` (indentation aside) to the `fi` that closes it."""
    lines = script.splitlines()
    at = next(i for i, line in enumerate(lines) if line.strip() == first)
    indent = lines[at][:len(lines[at]) - len(lines[at].lstrip())]
    end = next(i for i in range(at + 1, len(lines)) if lines[i] == indent + "fi")
    return "\n".join(line[len(indent):] for line in lines[at:end + 1]) + "\n"


class DownloadTest(unittest.TestCase):
    # On a normal night today's PBF isn't in the cache: one look, with no retries and their sleeps (15 s a night).
    def test_todays_pbf_is_looked_for_once(self):
        script = step_script("Download PBF and previous indexes")
        looks = re.findall(r".*planet-\$\{TODAY\}\.osm\.pbf.*", script)
        self.assertEqual(len([line for line in looks if " ls " in line]), 1, looks)
        self.assertNotIn("cache_exists \"s3://$CACHE_BUCKET/$PBF_CACHE/planet-${TODAY}", script)
        self.assertIn('cache_exists "s3://$CACHE_BUCKET/geocoder-cache/planet-${YESTERDAY}.osm.pbf"', script)

    # The day's diff comes down once the PBF is down, at most 10 minutes with its retries (fetch_day_diff.sh), and
    # the step waits for it last: beside the PBF it ended 20 to 25 s after it on the runner, alone it takes 12 to 14.
    def test_the_days_diff_comes_down_once_the_pbf_is_down(self):
        script = step_script("Download PBF and previous indexes")
        self.assertIn("TODAY_SEQ: ${{ steps.setup.outputs.today_seq }}", step("Download PBF and previous indexes"))
        fetch = script.index('fetch_day_diff.sh" "$TODAY_SEQ" "$WORKDIR/diff-today.osc.gz" "$DIFF_SECONDS" \\\n')
        self.assertGreater(fetch, script.index('echo "pbf_source=$PBF_SOURCE" >> "$GITHUB_OUTPUT"'))
        self.assertGreater(script.index('wait "$DIFF_PID"'), script.index("wait $EXTRAS_PID"))

    # The fetch's time comes from the step's timeout (DownloadStepTest), which the script can't read: it says it.
    def test_the_step_knows_its_timeout(self):
        name = "Download PBF and previous indexes"
        minutes = next(s["timeout-minutes"] for job in workflow_yaml(NIGHTLY)["jobs"].values()
                       for s in job.get("steps", []) if s.get("name") == name)
        self.assertIn(f"\nSTEP_SECONDS={minutes * 60}\n", step_script(name))

    def run_apply(self, diff, server="ok"):
        """Apply replication diffs from yesterday's PBF, with a diff from the download (bytes, or None), and a curl,
        pbf-apply, gzip -t and timeout that log their calls; curl gets the diff when `server` is "ok", and stalls when
        it is "stall", in which case the timeout gives it 1 s whatever it is asked. Returns the step's run and the
        calls."""
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        work, bin_dir = os.path.join(root, "work"), os.path.join(root, "bin")
        os.makedirs(work)
        os.makedirs(bin_dir)
        calls = os.path.join(root, "calls")
        fetch = 'echo "<osmChange/>" | gzip -c > "$out"\n' if server == "ok" else 'echo x > "$out"\nexec sleep 300\n'
        timeout = shutil.which("timeout")
        fakes = {"curl": f'echo "curl $*" >> {calls}\nout=""\n'
                         'while [ $# -gt 0 ]; do [ "$1" = -o ] && out=$2; shift; done\n' + fetch,
                 "pbf-apply": f'echo "pbf-apply $*" >> {calls}\n: > "{work}/planet-tmp.osm.pbf"\n',
                 "gzip": f'[ "$1" = -t ] && echo "gzip $*" >> {calls}\nexec {shutil.which("gzip")} "$@"\n',
                 "timeout": f'echo "timeout $*" >> {calls}\n'
                            + ('shift\nexec ' + timeout + ' 1 "$@"\n' if server == "stall" else f'exec {timeout} "$@"\n'),
                 "df": "exit 0\n"}
        for tool, body in fakes.items():
            with open(os.path.join(bin_dir, tool), "w") as f:
                f.write("#!/bin/sh\n" + body)
            os.chmod(os.path.join(bin_dir, tool), 0o755)
        open(os.path.join(work, "planet.osm.pbf"), "w").close()
        if diff is not None:
            with open(os.path.join(work, "diff-today.osc.gz"), "wb") as f:
                f.write(diff)
        env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "WORKDIR": work, "TODAY": "2026-11-21", "TODAY_SEQ": "4710",
               "PBF_SOURCE": "yesterday", "GITHUB_WORKSPACE": os.path.join(HERE, "..", "..", "..")}
        try:
            done = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step_script("Apply replication diffs")],
                                  env=env, capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            self.fail("the apply hung")
        with open(calls) as f:
            return done, f.read().splitlines()

    # fetch_day_diff.sh leaves the diff only once gzip -t has passed (FetchDayDiffTest): the apply takes it as it is.
    def test_the_apply_takes_the_downloads_diff_untested(self):
        done, calls = self.run_apply(gzip.compress(b"<osmChange/>"))

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual([c.split()[0] for c in calls], ["pbf-apply"])

    # Through fetch_day_diff.sh, within its 10 minutes, retries included.
    def test_the_apply_fetches_and_tests_the_diff_itself_when_the_download_left_none(self):
        done, calls = self.run_apply(None)

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual([c.split()[0] for c in calls], ["timeout", "curl", "gzip", "pbf-apply"])
        self.assertEqual(calls[0].split()[1], "600")
        self.assertIn(" curl ", calls[0])
        self.assertIn(" gzip -t ", calls[0])
        self.assertIn("/replication/day/000/004/710.osc.gz", calls[1])
        self.assertEqual(calls[2].split()[1], "-t")

    # A server that stalls fails the step at the fetch's limit, a failure the watchdog retries, where a fetch without
    # one would hold the step to its 120-minute timeout, a cancelled step it doesn't.
    def test_a_stalled_server_fails_the_apply_at_the_fetchs_limit(self):
        done, calls = self.run_apply(None, server="stall")

        self.assertNotEqual(done.returncode, 0)
        self.assertEqual([c.split()[0] for c in calls], ["timeout", "curl"])
        self.assertIn("::error::", done.stdout)


# The download step's tools as the step runs them. s5cmd lists the keys of $FAKE_CACHED (":"-separated, under the
# cache bucket) and copies any to a local path, the PBF in $FAKE_PBF_SECONDS by the step's clock (date); curl answers
# the wikidata and TIGER lookups and downloads, and the day's diff as $FAKE_DIFF says: "ok" (a gzip), or "stall" (it
# writes its pid to $FAKE_DIFF_PID and never ends); aria2c (the mirror) writes the PBF. Each call goes to $FAKE_CALLS.
FAKE_S5CMD = r"""#!{python}
import os, sys
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as f:
    f.write("s5cmd " + " ".join(args) + "\n")
verb = next(a for a in args if a in ("ls", "cp", "rm", "cat"))
rest = [a for a in args[args.index(verb) + 1:] if not a.startswith("--") and not a.isdigit()]
if verb == "ls":
    key = rest[0].split("/", 3)[3]
    if key not in os.environ.get("FAKE_CACHED", "").split(":"):
        sys.exit("ERROR no object found")
    print("2026/11/21 06:00:00          99 " + os.path.basename(key))
elif verb == "cp" and not rest[1].startswith("s3://"):
    with open(rest[1], "wb") as f:
        f.write(b"pbf")
    if rest[1].endswith("/planet.osm.pbf"):
        with open(os.environ["FAKE_CLOCK"], "w") as f:
            f.write(os.environ["FAKE_PBF_SECONDS"])
"""

# date +%s is the time plus the seconds in $FAKE_CLOCK, which the PBF's copy writes: the PBF took that long.
FAKE_DATE = """#!/bin/sh
if [ "$1" = +%s ]; then echo $(( $({date} +%s) + $(cat "$FAKE_CLOCK" 2> /dev/null || echo 0) )); else exec {date} "$@"; fi
"""

FAKE_TIMEOUT = """#!/bin/sh
echo "timeout $*" >> "$FAKE_CALLS"
exec {timeout} "$@"
"""

FAKE_CURL = r"""#!{python}
import gzip, json, os, sys, time
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as f:
    f.write("curl " + " ".join(args) + "\n")
url = next(a for a in args if a.startswith("http"))
out = args[args.index("-o") + 1] if "-o" in args else None
def put(data):
    with open(out, "wb") as f:
        f.write(data)
if url.endswith("/geocoder/wikidata/latest.json"):
    print(json.dumps({{"dump_date": "20261101", "size_bytes": 2000, "path": "wikidata/20261101/1-2/qid_sitelinks.bin"}}))
elif url.endswith("/qid_sitelinks.bin") or url.endswith(".csv.tar.gz"):
    put(b"x" * 2000)
elif url.endswith("/geocoder/tiger/latest.json"):
    print(json.dumps({{"year": "2025"}}))
elif "/replication/day/" in url and os.environ["FAKE_DIFF"] == "stall":
    with open(os.environ["FAKE_DIFF_PID"], "w") as f:
        f.write(str(os.getpid()))
    time.sleep(300)
elif "/replication/day/" in url:
    put(gzip.compress(b"<osmChange/>"))
else:
    sys.exit(22)
"""

FAKE_ARIA2C = r"""#!/bin/sh
echo "aria2c $*" >> "$FAKE_CALLS"
while [ $# -gt 0 ]; do case $1 in -d) d=$2 ;; -o) o=$2 ;; esac; shift; done
echo pbf > "$d/$o"
"""


def alive(pid):
    """Whether process `pid` runs (a zombie counts as ended)."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


class DownloadStepTest(unittest.TestCase):
    """The download step, run with the fakes above: only a run that applies the day's diff to yesterday's PBF
    fetches it, once the PBF is down, and waits for it."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.work, self.bin = os.path.join(self.root, "work"), os.path.join(self.root, "bin")
        os.makedirs(self.work)
        os.makedirs(self.bin)
        # python3 is the GeoNames download's, and sleep the retries' waits.
        fakes = {"s5cmd": FAKE_S5CMD, "curl": FAKE_CURL, "aria2c": FAKE_ARIA2C, "python3": "#!/bin/sh\nexit 0\n",
                 "sleep": "#!/bin/sh\nexit 0\n", "date": FAKE_DATE.format(date=shutil.which("date")),
                 "timeout": FAKE_TIMEOUT.format(timeout=shutil.which("timeout"))}
        for tool, body in fakes.items():
            with open(os.path.join(self.bin, tool), "w") as f:
                f.write(body.format(python=sys.executable) if body.startswith("#!{") else body)
            os.chmod(os.path.join(self.bin, tool), 0o755)
        self.calls = os.path.join(self.root, "calls")
        self.diff_pid = os.path.join(self.root, "diff.pid")
        self.addCleanup(self.end_stalled_fetch)

    def end_stalled_fetch(self):
        if os.path.exists(self.diff_pid):
            with open(self.diff_pid) as f:
                pid = int(f.read())
            if alive(pid):
                os.kill(pid, 9)

    def download(self, cached, diff, pbf_cache="geocoder-cache", pbf_seconds=0):
        """Runs the step with the cache holding `cached` (keys), the day's diff `diff` and a PBF that takes
        `pbf_seconds` by the step's clock; returns its outputs, its output lines and how long it took."""
        output = os.path.join(self.root, "output")
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "WORKDIR": self.work, "CACHE_ENDPOINT": "http://cache.invalid",
               "CACHE_BUCKET": "cache", "CACHE_AWS_KEY": "k", "CACHE_AWS_SECRET": "s", "YESTERDAY": "2026-11-20",
               "TODAY": "2026-11-21", "TODAY_SEQ": "4710", "HAS_PREVIOUS": "false", "PBF_CACHE": pbf_cache,
               "S3_ENDPOINT": "https://s3.invalid", "S3_BUCKET": "bucket", "S3_PREFIX": "geocoder",
               "S3_PUBLIC_URL": "https://public.invalid", "GITHUB_OUTPUT": output,
               "GITHUB_WORKSPACE": os.path.join(HERE, "..", "..", ".."), "FAKE_CALLS": self.calls,
               "FAKE_CACHED": ":".join(cached), "FAKE_DIFF": diff, "FAKE_DIFF_PID": self.diff_pid,
               "FAKE_CLOCK": os.path.join(self.root, "clock"), "FAKE_PBF_SECONDS": str(pbf_seconds)}
        start = time.time()
        try:
            done = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step_script("Download PBF and previous indexes")],
                                  env=env, capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            self.fail("the download step hung")
        took = time.time() - start
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        with open(output) as f:
            outputs = dict(line.split("=", 1) for line in f.read().splitlines())
        return outputs, done.stdout, took

    def diff_files(self):
        return sorted(n for n in os.listdir(self.work) if n.startswith("diff-today.osc"))

    def diff_fetches(self):
        with open(self.calls) as f:
            return [line for line in f.read().splitlines() if line.startswith("curl ") and "/replication/day/" in line]

    def diff_limits(self):
        """The seconds each fetch of the diff was given."""
        with open(self.calls) as f:
            return [int(line.split()[1]) for line in f.read().splitlines()
                    if line.startswith("timeout ") and "/replication/day/" in line]

    # A run that has today's PBF from the cache needs no diff, and a stalled server can't hold its step (up to its
    # 60-minute timeout, a cancelled step the watchdog doesn't retry): it never asks for one.
    def test_a_cached_pbf_of_today_fetches_no_diff(self):
        outputs, _, took = self.download(["geocoder-cache/planet-2026-11-21.osm.pbf"], "stall")

        self.assertEqual(outputs["pbf_source"], "today")
        self.assertLess(took, 30)
        self.assertEqual(self.diff_fetches(), [])
        self.assertEqual(self.diff_files(), [])

    # The mirror's PBF takes its diffs in Apply replication diffs.
    def test_a_mirror_pbf_fetches_no_diff(self):
        outputs, _, took = self.download([], "stall")

        self.assertEqual(outputs["pbf_source"], "mirror")
        self.assertLess(took, 30)
        self.assertEqual(self.diff_fetches(), [])
        self.assertEqual(self.diff_files(), [])

    # A scratch prefix's run applies the diff whatever the cache holds: its own key may keep an earlier test run's
    # PBF of the day (one whose upload outlived its cleanup), and the shared one is the nightly's.
    def test_a_scratch_run_never_takes_a_cached_pbf_of_today(self):
        cached = ["geocoder-scratch/nightly-speed-1121/planet-2026-11-21.osm.pbf", "geocoder-cache/planet-2026-11-21.osm.pbf",
                  "geocoder-cache/planet-2026-11-20.osm.pbf"]

        outputs, _, _ = self.download(cached, "ok", pbf_cache="geocoder-scratch/nightly-speed-1121")

        self.assertEqual(outputs["pbf_source"], "yesterday")
        with open(self.calls) as f:
            self.assertNotIn("planet-2026-11-21", f.read())

    def test_yesterdays_pbf_waits_for_the_diff_it_takes_once_it_is_down(self):
        outputs, out, _ = self.download(["geocoder-cache/planet-2026-11-20.osm.pbf"], "ok")

        self.assertEqual(outputs["pbf_source"], "yesterday")
        with open(self.calls) as f:
            calls = f.read().splitlines()
        pbf = next(i for i, line in enumerate(calls) if line.startswith("s5cmd") and " cp " in line
                   and "planet-2026-11-20.osm.pbf" in line)
        self.assertEqual(len(self.diff_fetches()), 1)
        self.assertGreater(calls.index(self.diff_fetches()[0]), pbf)
        self.assertEqual(self.diff_limits(), [600])
        self.assertEqual(self.diff_files(), ["diff-today.osc.gz"])
        with gzip.open(os.path.join(self.work, "diff-today.osc.gz")) as f:
            self.assertEqual(f.read(), b"<osmChange/>")
        self.assertIn("[diff] seq 4710", out)

    # The fetch has the time the step has left to its 60-minute timeout, less 2 minutes, and at most its 10: a PBF
    # that came down in 52 minutes leaves it 6, where 10 would hold the step past its timeout, a cancelled step the
    # watchdog doesn't retry.
    def test_the_diff_has_at_most_the_time_left_in_the_step(self):
        outputs, _, _ = self.download(["geocoder-cache/planet-2026-11-20.osm.pbf"], "ok", pbf_seconds=52 * 60)

        self.assertEqual(outputs["pbf_source"], "yesterday")
        [limit] = self.diff_limits()
        self.assertLessEqual(limit, 360)
        self.assertGreater(limit, 330)
        self.assertEqual(self.diff_files(), ["diff-today.osc.gz"])

    # With under 30 s of that left the step asks for no diff: Apply replication diffs fetches it.
    def test_a_pbf_that_took_the_step_leaves_the_diff_to_the_apply(self):
        outputs, out, took = self.download(["geocoder-cache/planet-2026-11-20.osm.pbf"], "stall",
                                           pbf_seconds=60 * 60 - 120 - 20)

        self.assertEqual(outputs["pbf_source"], "yesterday")
        self.assertLess(took, 30)
        self.assertEqual(self.diff_fetches(), [])
        self.assertEqual(self.diff_files(), [])
        self.assertIn("Apply replication diffs fetches it", out)


class FetchDayDiffTest(unittest.TestCase):
    """fetch_day_diff.sh: the day's diff, whole or not at all, within its time."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.bin = os.path.join(self.root, "bin")
        os.makedirs(self.bin)
        self.out = os.path.join(self.root, "diff-today.osc.gz")
        self.pid = os.path.join(self.root, "curl.pid")

    def fetch(self, curl, limit=60):
        with open(os.path.join(self.bin, "curl"), "w") as f:
            f.write(f"#!{sys.executable}\nimport gzip, os, sys, time\nargs = sys.argv[1:]\nout = args[args.index('-o') + 1]\n"
                    f"open({self.pid!r}, 'w').write(str(os.getpid()))\n" + curl)
        os.chmod(os.path.join(self.bin, "curl"), 0o755)
        start = time.time()
        done = subprocess.run(["bash", os.path.join(HERE, "fetch_day_diff.sh"), "4710", self.out, str(limit)],
                              env={"PATH": f"{self.bin}:{os.environ['PATH']}"}, capture_output=True, text=True, timeout=60)
        return done, time.time() - start

    def files(self):
        return sorted(n for n in os.listdir(self.root) if n.startswith("diff-today"))

    def test_writes_the_whole_diff(self):
        done, _ = self.fetch("assert args[-1].endswith('/replication/day/000/004/710.osc.gz'), args\n"
                             "open(out, 'wb').write(gzip.compress(b'<osmChange/>'))\n")

        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(self.files(), ["diff-today.osc.gz"])

    def test_a_broken_diff_leaves_nothing(self):
        done, _ = self.fetch("open(out, 'wb').write(b'<osmCha')\n")

        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(self.files(), [])
        self.assertIn("Fetching the diff of seq 4710 failed or took over 60s", done.stdout)

    # A server that accepts the connection and then sends nothing is given up on at the limit, curl ended with it.
    def test_a_stalled_server_is_given_up_on_at_the_limit(self):
        done, took = self.fetch("open(out, 'wb').write(b'\\x1f')\ntime.sleep(300)\n", limit=2)

        self.assertNotEqual(done.returncode, 0)
        self.assertLess(took, 30)
        self.assertEqual(self.files(), [])
        with open(self.pid) as f:
            self.assertFalse(alive(int(f.read())))

    # The check of the diff is within the limit too, ended with it.
    def test_a_check_that_hangs_is_given_up_on_at_the_limit(self):
        gzip_pid = os.path.join(self.root, "gzip.pid")
        with open(os.path.join(self.bin, "gzip"), "w") as f:
            f.write(f"#!/bin/sh\necho $$ > {gzip_pid}\nexec /usr/bin/sleep 300\n")
        os.chmod(os.path.join(self.bin, "gzip"), 0o755)

        try:
            done, took = self.fetch("open(out, 'wb').write(gzip.compress(b'<osmChange/>'))\n", limit=2)
        except subprocess.TimeoutExpired:
            self.fail("the check of the diff held the fetch past its limit")

        self.assertNotEqual(done.returncode, 0)
        self.assertLess(took, 30)
        self.assertEqual(self.files(), [])
        with open(gzip_pid) as f:
            self.assertFalse(alive(int(f.read())))

    # A diff that passed its check but can't take its name is no diff: the fetch fails and leaves nothing.
    def test_a_diff_that_cannot_be_moved_into_place_fails_the_fetch(self):
        os.makedirs(os.path.join(self.out, "x"))

        done, _ = self.fetch("open(out, 'wb').write(gzip.compress(b'<osmChange/>'))\n")

        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(self.files(), ["diff-today.osc.gz"])
        self.assertEqual(os.listdir(self.out), ["x"])
        self.assertIn("::warning::[diff] Could not move the diff of seq 4710 into place", done.stdout)


class RustToolchainTest(unittest.TestCase):
    """The stable toolchain installs inside the detached build of place-search (a fresh pod downloads it, ~15 s), and
    says how that went in a status of its own: a failed install, a release server down, is worth a retry and fails
    the run in a step the watchdog retries, where the place search index failure it leads to would not be retried."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.calls = os.path.join(self.root, "calls")

    def fakes(self, rustup_status):
        """A bin dir with a rustup that logs its calls (and, in self.copy_rename, its RUSTUP_PERMIT_COPY_RENAME) and
        exits `rustup_status`, a cargo that logs its own, and a sleep that doesn't (the install's retries wait 30 s)."""
        bin_dir = os.path.join(self.root, "cargo", "bin")
        os.makedirs(bin_dir, exist_ok=True)
        self.copy_rename = os.path.join(self.root, "copy_rename")
        for tool, body in {"rustup": f'echo "rustup $*" >> {self.calls}\n'
                                     f'echo "$*: ${{RUSTUP_PERMIT_COPY_RENAME-unset}}" >> {self.copy_rename}\n'
                                     f'exit {rustup_status}\n',
                           "cargo": f'echo "cargo $*" >> {self.calls}\n', "sleep": "exit 0\n"}.items():
            with open(os.path.join(bin_dir, tool), "w") as f:
                f.write("#!/bin/sh\n" + body)
            os.chmod(os.path.join(bin_dir, tool), 0o755)
        return bin_dir

    def logged(self):
        if not os.path.exists(self.calls):
            return []
        with open(self.calls) as f:
            return f.read().splitlines()

    def read(self, name):
        with open(os.path.join(self.root, name)) as f:
            return f.read().strip()

    def test_the_toolchain_script_installs_a_minimal_stable_toolchain_and_writes_its_status(self):
        bin_dir = self.fakes(0)
        status = os.path.join(self.root, "rust-toolchain.status")
        subprocess.run(["bash", os.path.join(HERE, "rust_toolchain.sh"), status], check=True, capture_output=True,
                       env={"PATH": f"{bin_dir}:{os.environ['PATH']}"})
        self.assertEqual(self.logged(), ["rustup toolchain install stable --profile minimal --no-self-update",
                                         "rustup default stable"])
        self.assertEqual(self.read("rust-toolchain.status"), "0")

    # A runner image that bakes a toolchain into a lower overlay layer can't rename across it (EXDEV): the install
    # may copy instead, as dtolnay/rust-toolchain lets it.
    def test_the_install_may_copy_where_it_cannot_rename(self):
        bin_dir = self.fakes(0)
        subprocess.run(["bash", os.path.join(HERE, "rust_toolchain.sh"), os.path.join(self.root, "rust-toolchain.status")],
                       check=True, capture_output=True, env={"PATH": f"{bin_dir}:{os.environ['PATH']}"})
        with open(self.copy_rename) as f:
            self.assertIn("toolchain install stable --profile minimal --no-self-update: 1", f.read().splitlines())

    def test_a_failed_install_is_retried_then_written_as_failed(self):
        bin_dir = self.fakes(1)
        status = os.path.join(self.root, "rust-toolchain.status")
        done = subprocess.run(["bash", os.path.join(HERE, "rust_toolchain.sh"), status], capture_output=True, text=True,
                              env={"PATH": f"{bin_dir}:{os.environ['PATH']}"})
        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(len(self.logged()), 5)
        self.assertEqual(self.read("rust-toolchain.status"), "1")

    def build_place_search(self, rustup_status):
        """Runs Build place search with the fakes; returns once both statuses are written."""
        env = {"PATH": os.environ["PATH"], "CARGO_HOME": os.path.dirname(self.fakes(rustup_status)),
               "RUNNER_TEMP": self.root, "HOME": self.root}
        start = time.time()
        subprocess.run(["bash", "-e", "-c", step_script("Build place search")], env=env, cwd=os.path.join(HERE, "..", "..", ".."),
                       check=True, capture_output=True, timeout=60)
        self.assertLess(time.time() - start, 10)
        deadline = time.time() + 30
        while time.time() < deadline and not all(os.path.exists(os.path.join(self.root, n)) and self.read(n)
                                                 for n in ("rust-toolchain.status", "search.status")):
            time.sleep(0.05)
        return self.read("rust-toolchain.status"), self.read("search.status")

    def test_the_toolchain_installs_in_the_background_before_the_place_search_build(self):
        self.assertNotIn("dtolnay/rust-toolchain", nightly())

        statuses = self.build_place_search(0)

        self.assertEqual(statuses, ("0", "0"))
        self.assertEqual([c.split()[0] for c in self.logged()], ["rustup", "rustup", "cargo"])
        self.assertEqual(self.logged()[-1], "cargo build --release --locked --manifest-path geocoder/search/Cargo.toml")

    def test_a_failed_install_builds_nothing_and_says_so_in_both_statuses(self):
        toolchain, search = self.build_place_search(1)

        self.assertNotEqual(toolchain, "0")
        self.assertNotEqual(search, "0")
        self.assertNotIn("cargo", [c.split()[0] for c in self.logged()])

    def check(self, status):
        """Check the Rust toolchain with the toolchain's status `status` (None: never written)."""
        if status is not None:
            with open(os.path.join(self.root, "rust-toolchain.status"), "w") as f:
                f.write(status + "\n")
        with open(os.path.join(self.root, "search.log"), "w") as f:
            f.write("error: could not download the toolchain\n")
        env = {"PATH": f"{self.fakes(0)}:{os.environ['PATH']}", "RUNNER_TEMP": self.root}
        return subprocess.run(["bash", "-e", "-c", step_script("Check the Rust toolchain")], env=env, capture_output=True,
                              text=True, timeout=60)

    def test_the_check_fails_the_run_on_a_failed_or_missing_install(self):
        self.assertEqual(self.check("0").returncode, 0)
        for status in ("1", None):
            with self.subTest(status=status):
                done = self.check(status)

                self.assertNotEqual(done.returncode, 0)
                self.assertIn("could not download the toolchain", done.stdout)

    # Early, before the hours of the build: the toolchain is long installed once the downloads are done.
    def test_the_check_follows_the_downloads(self):
        self.assertEqual(index_of("Check the Rust toolchain"), index_of("Download PBF and previous indexes") + 1)


class PostBuildTest(unittest.TestCase):
    # The place search starts in the background when build-index exits, and the steps after it start at once.
    def test_the_place_search_runs_in_the_background_from_build_index_s_exit(self):
        self.assertEqual(index_of("Build place search indexes"), index_of("Build geocoder index") + 1)
        self.assertEqual(index_of("Generate patches and verify"), index_of("Build place search indexes") + 1)
        self.assertEqual(index_of("Compress index files"), index_of("Generate patches and verify") + 1)
        script = step_script("Build place search indexes")
        self.assertRegex(script, r"setsid nohup bash -c '\"\$@\"; echo \$\? > \"\$0\"' \"\$WORKDIR/place-search\.status\" \\\n"
                                 r" +nice -n 5 \"\$GITHUB_WORKSPACE/geocoder/builder/tools/place_search_indexes\.sh\" "
                                 r"\"\$WORKDIR/output\" \\\n +\"\$GITHUB_WORKSPACE/geocoder/search/target/release/place-search\" "
                                 r"\"\$RUNNER_TEMP/search\.status\" 1800 \\\n")
        self.assertNotIn("build_search_indexes.py", script)

    # Both steps that compress start compression with geocoder-transport built, the sidecars' upload after the
    # prefix is cleared.
    def test_compression_waits_for_geocoder_transport_and_the_sidecars_go_up_after_the_prefix_is_cleared(self):
        for name in ("Generate patches and verify", "Compress index files"):
            script = step_script(name)
            clear = script.index("rm \"s3://$S3_BUCKET/$S3_PREFIX/builds/$NEW_DATE/*\"")
            sidecars = script.index("run \"$WORKDIR/sidecars.run\"")
            wait = script.index('[ -s "$RUNNER_TEMP/search.status" ]')
            compress = script.index("compress_index.py")
            # Its session, for a later step to end should it outlast the wait for it.
            pid = script.index('echo "$!" > "$WORKDIR/sidecars.pid"')
            self.assertLess(clear, sidecars, name)
            self.assertLess(sidecars, pid, name)
            self.assertLess(pid, compress, name)
            self.assertLess(wait, compress, name)
            self.assertIn("RUNNER_TEMP: ${{ runner.temp }}", step(name), name)
        retry = step_script("Compress index files")
        self.assertLess(retry.index('[ -s "$WORKDIR/sidecars.status" ]'), retry.index("rm \"s3://$S3_BUCKET/$S3_PREFIX"))

    def test_finalize_waits_for_the_sidecars_before_its_first_pass(self):
        script = step_script("Finalize upload and configurations.json")
        self.assertLess(script.index('[ -s "$WORKDIR/sidecars.status" ]'), script.index("cp --if-size-differ"))
        self.assertIn("Caching today's PBF ended at $(date -u -r \"$WORKDIR/pbf-cache.status\" +%H:%M:%S)", script)

    # On a live run Finalize lets today's PBF reach the cache (tomorrow's run starts from it) before it frees the
    # disk. A scratch run's, under its own key, it leaves to Cleanup old cache bucket, which waits for it a bounded
    # time and then stops it: a wait without end here would hold the job to its timeout, and skip that cleanup.
    def test_finalize_waits_for_a_live_runs_pbf_upload_alone(self):
        name = "Finalize upload and configurations.json"
        self.assertIn("PBF_CACHE: ${{ steps.setup.outputs.pbf_cache }}", step(name))
        block = block_at(step_script(name), 'if [ -f "$WORKDIR/pbf-cache.log" ] && [ "$PBF_CACHE" = geocoder-cache ]; then')
        for pbf_cache, waits in (("geocoder-cache", True), ("geocoder-scratch/nightly-speed-1121", False)):
            with self.subTest(pbf_cache=pbf_cache):
                took, out = self.wait_for(': > "$WORKDIR/pbf-cache.log"\n' + block, "pbf-cache.status",
                                          env={"PBF_CACHE": pbf_cache})

                if waits:
                    self.assertGreaterEqual(took, 0.9)
                    self.assertIn("Caching today's PBF ended at", out)
                else:
                    self.assertLess(took, 0.9)
                    self.assertNotIn("Caching today's PBF", out)

    # The step only starts the job: it returns while the job runs, leaves the job's session for Compress index
    # files, and the job's exit status comes once it ends.
    def test_the_place_search_step_returns_while_its_job_runs(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        tools = os.path.join(root, "ws", "geocoder", "builder", "tools")
        work = os.path.join(root, "work")
        os.makedirs(tools)
        os.makedirs(work)
        release = os.path.join(root, "release")
        self.addCleanup(lambda: open(release, "w").close())
        with open(os.path.join(tools, "place_search_indexes.sh"), "w") as f:
            f.write(f'#!/bin/sh\necho "$@" > {root}/args\nuntil [ -e {release} ]; do /usr/bin/sleep 0.05; done\nexit 3\n')
        os.chmod(os.path.join(tools, "place_search_indexes.sh"), 0o755)
        env = {"PATH": os.environ["PATH"], "WORKDIR": work, "RUNNER_TEMP": root, "GITHUB_WORKSPACE": os.path.join(root, "ws")}
        start = time.time()
        try:
            subprocess.run(["bash", "-e", "-c", step_script("Build place search indexes")], env=env, check=True,
                           capture_output=True, timeout=20)
        except subprocess.TimeoutExpired:
            self.fail("the step waited for the place search")
        self.assertLess(time.time() - start, 10)

        with open(os.path.join(work, "place-search.pid")) as f:
            session = f.read().strip()
        self.assertEqual(subprocess.run(["pgrep", "-s", session], capture_output=True).returncode, 0)
        self.assertFalse(os.path.exists(os.path.join(work, "place-search.status")))
        open(release, "w").close()
        deadline = time.time() + 10
        while time.time() < deadline and not os.path.exists(os.path.join(work, "place-search.status")):
            time.sleep(0.05)
        time.sleep(0.1)
        with open(os.path.join(work, "place-search.status")) as f:
            self.assertEqual(f.read().strip(), "3")
        with open(os.path.join(root, "args")) as f:
            self.assertEqual(f.read().split(), [os.path.join(work, "output"),
                                                os.path.join(root, "ws", "geocoder/search/target/release/place-search"),
                                                os.path.join(root, "search.status"), "1800"])

    def wait_for(self, script, status, env=None):
        """Runs `script` with a sleep of 10 ms; `status` (a file under the run's dir) is written "0" a second in.
        Returns how long it took and what it printed."""
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        bin_dir = os.path.join(root, "bin")
        os.makedirs(bin_dir)
        with open(os.path.join(bin_dir, "sleep"), "w") as f:
            f.write("#!/bin/sh\nexec /usr/bin/sleep 0.01\n")
        os.chmod(os.path.join(bin_dir, "sleep"), 0o755)
        open(os.path.join(root, "sidecars.run"), "w").close()

        def write_status():
            with open(os.path.join(root, status + ".part"), "w") as f:
                f.write("0\n")
            os.rename(os.path.join(root, status + ".part"), os.path.join(root, status))

        timer = threading.Timer(1.0, write_status)
        timer.start()
        self.addCleanup(timer.cancel)
        start = time.time()
        done = subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True, timeout=30,
                              env={"PATH": f"{bin_dir}:{os.environ['PATH']}", "WORKDIR": root, "RUNNER_TEMP": root,
                                   **(env or {})})
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        return time.time() - start, done.stdout

    # Both steps that compress wait, before they start it, for geocoder-transport's build (its status).
    def test_compression_waits_for_geocoder_transport(self):
        for name in ("Generate patches and verify", "Compress index files"):
            with self.subTest(name=name):
                loop = loop_of(step_script(name), 'until [ -s "$RUNNER_TEMP/search.status" ]')

                took, _ = self.wait_for("WAIT_START=$(date +%s)\nCOMPRESS_START=$WAIT_START\n" + loop, "search.status")

                self.assertGreaterEqual(took, 0.9)

    # The compress step after a failed background run lets the sidecars' upload that run started end before it
    # clears the prefix; Finalize lets it end before its first pass.
    def test_the_sidecars_upload_ends_before_the_prefix_is_cleared_again_and_before_finalize(self):
        for name in ("Compress index files", "Finalize upload and configurations.json"):
            with self.subTest(name=name):
                block = block_at(step_script(name), 'if [ -f "$WORKDIR/sidecars.run" ]; then')

                took, out = self.wait_for("COMPRESS_START=$(date +%s)\nUPLOAD_START=$COMPRESS_START\n" + block,
                                          "sidecars.status")

                self.assertGreaterEqual(took, 0.9)
                self.assertNotIn("::warning::", out)

    # An upload still running when the wait for it ends is stopped, its session and all, and gone, before the
    # prefix is cleared again (it would write the status of the upload started after it) and before Finalize's
    # first pass sends the same keys.
    def test_a_sidecars_upload_past_the_wait_is_stopped_before_the_prefix_is_cleared_again_and_before_finalize(self):
        for name in ("Compress index files", "Finalize upload and configurations.json"):
            with self.subTest(name=name):
                root = tempfile.mkdtemp()
                self.addCleanup(shutil.rmtree, root)
                open(os.path.join(root, "sidecars.run"), "w").close()
                upload = subprocess.Popen(["/usr/bin/sleep", "300"], start_new_session=True)
                self.addCleanup(upload.kill)
                with open(os.path.join(root, "sidecars.pid"), "w") as f:
                    f.write(f"{upload.pid}\n")
                block = block_at(step_script(name), 'if [ -f "$WORKDIR/sidecars.run" ]; then')
                start = time.time()

                done = subprocess.run(["bash", "-e", "-c", "COMPRESS_START=$(( $(date +%s) - 901 ))\n"
                                       "UPLOAD_START=$COMPRESS_START\n" + block],
                                      env={"PATH": os.environ["PATH"], "WORKDIR": root}, capture_output=True, text=True,
                                      timeout=90)

                self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
                self.assertLess(time.time() - start, 30)
                self.assertEqual(upload.wait(timeout=10), -9)
                self.assertIn("::warning::", done.stdout)

    # The watchdog tells a retry from a failure a rerun repeats by these steps' names.
    def test_every_step_the_watchdog_names_is_a_step_of_the_nightly(self):
        with open(WATCHDOG) as f:
            named = set(re.findall(r'\.name == "([^"]+)"', f.read()))
        self.assertEqual(named, {"Build tools", "Run builder tests", "Fail run on patch verify failure",
                                 "Fail run on place search index failure"})
        self.assertLessEqual(named, {n for n, _ in steps()})


class O11yReportStepTest(unittest.TestCase):
    # Report to o11y's `if:` compares without case, so a dispatch under Geocoder (which Setup refuses) passes it:
    # the step reads no token for it and reports nothing. The live prefix's run reports.
    def test_the_report_is_the_live_prefixs_alone(self):
        for prefix, reported in (("geocoder", True), ("Geocoder", False)):
            with self.subTest(prefix=prefix):
                root = tempfile.mkdtemp()
                self.addCleanup(shutil.rmtree, root)
                bin_dir = os.path.join(root, "bin")
                os.makedirs(bin_dir)
                calls = os.path.join(root, "calls")
                for tool, body in {"op": "echo s3cret\n", "python3": "exit 0\n"}.items():
                    with open(os.path.join(bin_dir, tool), "w") as f:
                        f.write(f'#!/bin/sh\necho "{tool}" >> {calls}\n{body}')
                    os.chmod(os.path.join(bin_dir, tool), 0o755)
                env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "RUNNER_TEMP": root, "WORKDIR": root,
                       "S3_PREFIX": prefix, "BUILD_DATE": "2026-11-21", "DATA_TIMESTAMP": "2026-11-21T00:00:00Z",
                       "O11Y_URL": "https://o11y.invalid", "GITHUB_RUN_ID": "1", "GITHUB_RUN_ATTEMPT": "1",
                       "OP_SERVICE_ACCOUNT_TOKEN": "t"}

                done = subprocess.run(["bash", "-e", "-c", step_script("Report to o11y")], env=env, cwd=root,
                                      capture_output=True, text=True, timeout=60)

                self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
                if reported:
                    deadline = time.time() + 10
                    while time.time() < deadline and not os.path.exists(os.path.join(root, "o11y.status")):
                        time.sleep(0.05)
                    with open(calls) as f:
                        self.assertEqual(f.read().split(), ["op", "python3"])
                else:
                    self.assertFalse(os.path.exists(calls))
                    self.assertFalse(os.path.exists(os.path.join(root, "o11y.log")))


def watchdog_script(name):
    """The `run:` script of the watchdog's step `name`, unindented."""
    with open(WATCHDOG) as f:
        text = f.read()
    body = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", text, re.M).group(1)
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", body, re.M)
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines())


# gh as the watchdog runs it, answering from the JSON in $FAKE_GH_DATA: the repo's default branch, the build's runs
# (`run list` newest first, on its --branch and from its --created date, at most its --limit, each with only the
# fields asked for), each run's jobs, and the REST API's runs (a page of per_page) and jobs; --jq filters go through
# jq. `workflow run` appends its arguments to $FAKE_GH_CALLS.
FAKE_GH = r'''#!{python}
import json, os, re, subprocess, sys
args = sys.argv[1:]
with open(os.environ["FAKE_GH_DATA"]) as f:
    data = json.load(f)
def opt(name):
    for i, arg in enumerate(args):
        if arg == name:
            return args[i + 1]
        if arg.startswith(name + "="):
            return arg[len(name) + 1:]
    return None
def answer(value):
    if opt("--jq") is None:
        print(json.dumps(value))
    else:
        sys.stdout.write(subprocess.run(["jq", "-r", opt("--jq")], input=json.dumps(value), capture_output=True,
                                        text=True, check=True).stdout)
if args[:2] == ["repo", "view"]:
    answer({{"defaultBranchRef": {{"name": data["default_branch"]}}}})
elif args[:2] == ["run", "list"]:
    fields = opt("--json").split(",")
    runs = sorted(data["runs"], key=lambda run: run["createdAt"], reverse=True)
    if opt("--branch"):
        runs = [run for run in runs if run["headBranch"] == opt("--branch")]
    created = opt("--created") or ""
    if created.startswith(">="):
        runs = [run for run in runs if run["createdAt"][:10] >= created[2:]]
    elif created:
        runs = [run for run in runs if run["createdAt"][:10] == created]
    runs = runs[:int(opt("--limit") or 20)]
    answer([{{k: run[k] for k in fields if k in run}} for run in runs])
elif args[:2] == ["run", "view"]:
    answer({{"jobs": data["jobs"].get(args[2], [])}})
elif args[:2] == ["workflow", "run"]:
    with open(os.environ["FAKE_GH_CALLS"], "a") as f:
        f.write(" ".join(args) + "\n")
elif args[0] == "api":
    page = re.search(r"[?&]per_page=([0-9]+)", args[1])
    per_page = int(page.group(1)) if page else 30
    answer({{"jobs": data["api_jobs"]}} if args[1].endswith("/jobs") else {{"workflow_runs": data["api_runs"][:per_page]}})
else:
    sys.exit("fake gh: " + " ".join(args))
'''

LIVE_TITLE = "Build Geocoder Index"
# A dispatch's under the live prefix: the watchdog's retries.
LIVE_DISPATCH_TITLE = "Build Geocoder Index [geocoder]"
SCRATCH_TITLE = "Build Geocoder Index [scratch: nightly-speed-1121]"
# A dispatch's under the live prefix in other case, which Setup refuses (an expression's == is blind to case).
MIXED_CASE_TITLE = "Build Geocoder Index [Geocoder]"


class WatchdogTest(unittest.TestCase):
    """The watchdog's retry of today's nightly, run with a gh that answers from fixtures: it counts the live
    nightly's runs alone (a dispatch under a scratch s3_prefix is a test, its title says so), and retries what a
    rerun may cure."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.bin = os.path.join(self.root, "bin")
        os.makedirs(self.bin)
        with open(os.path.join(self.bin, "gh"), "w") as f:
            f.write(FAKE_GH.format(python=sys.executable))
        os.chmod(os.path.join(self.bin, "gh"), 0o755)
        self.calls = os.path.join(self.root, "calls")
        self.today = time.strftime("%Y-%m-%d", time.gmtime())
        self.runs, self.jobs = [], {}

    def run_at(self, hhmm, title, conclusion, steps=(), run_id=None, branch="main"):
        """A run of the build on `branch` created today at hhmm, with its title, conclusion and steps
        ((name, conclusion)...)."""
        run_id = run_id or f"{len(self.runs) + 1}"
        self.runs.append({"databaseId": int(run_id), "conclusion": conclusion, "status": "completed",
                          "createdAt": f"{self.today}T{hhmm}:00Z", "event": "workflow_dispatch", "headBranch": branch,
                          "displayTitle": title})
        self.jobs[run_id] = [{"steps": [{"name": n, "conclusion": c} for n, c in steps]}]

    def watchdog(self):
        """Runs the retry step against self.runs and self.jobs; returns its output and the dispatches it made."""
        data = os.path.join(self.root, "data.json")
        with open(data, "w") as f:
            json.dump({"default_branch": "main", "runs": self.runs, "jobs": self.jobs}, f)
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "FAKE_GH_DATA": data, "FAKE_GH_CALLS": self.calls,
               "GH_TOKEN": "t", "GH_REPO": "immich-app/geoshenanigans", "MAX_RETRIES": "5"}
        done = subprocess.run(["bash", "-e", "-c", watchdog_script("Check and maybe retry today's build")], env=env,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        dispatched = []
        if os.path.exists(self.calls):
            with open(self.calls) as f:
                dispatched = f.read().splitlines()
            os.remove(self.calls)
        return done.stdout, dispatched

    def retry(self):
        return [f"workflow run geocoder-build.yml --ref main -f build_date={self.today}"]

    def test_a_live_run_lost_mid_build_is_retried(self):
        self.run_at("06:17", LIVE_TITLE, "failure", [("Setup", "success"), ("Build geocoder index", "failure")])

        self.assertEqual(self.watchdog()[1], self.retry())

    # A scratch dispatch on the default branch that failed is no reason to start the live build.
    def test_a_failed_scratch_run_starts_no_live_build(self):
        self.run_at("03:00", SCRATCH_TITLE, "failure", [("Setup", "failure")])

        out, dispatched = self.watchdog()

        self.assertEqual(dispatched, [])
        self.assertIn("total=0", out)

    # Nor is a scratch run that succeeded the night's success: the live run lost mid-build is retried.
    def test_a_scratch_run_that_succeeded_does_not_stand_for_the_live_one(self):
        self.run_at("06:17", LIVE_TITLE, "failure", [("Setup", "success"), ("Build geocoder index", "failure")])
        self.run_at("09:00", SCRATCH_TITLE, "success", [("Setup", "success")])

        out, dispatched = self.watchdog()

        self.assertEqual(dispatched, self.retry())
        self.assertIn("total=1  succeeded=0", out)

    # Nor is one under the live prefix in other case, which Setup refuses: it is no attempt of the live build's.
    def test_a_dispatch_under_the_live_prefix_in_other_case_is_no_live_run(self):
        self.run_at("03:00", MIXED_CASE_TITLE, "failure", [("Setup", "failure")])

        out, dispatched = self.watchdog()

        self.assertEqual(dispatched, [])
        self.assertIn("total=0", out)

    # The watchdog's own retries, dispatches under the live prefix, are attempts of the live build.
    def test_a_retry_counts_as_an_attempt_of_the_live_build(self):
        lost = [("Setup", "success"), ("Build geocoder index", "failure")]
        self.run_at("06:17", LIVE_TITLE, "failure", lost)
        self.run_at("08:15", LIVE_DISPATCH_TITLE, "failure", lost)

        out, dispatched = self.watchdog()

        self.assertEqual(dispatched, self.retry())
        self.assertIn("total=2", out)

    # Runs on other branches and scratch runs of the day, however many, don't push the live run out of the list.
    def test_the_days_other_runs_do_not_crowd_out_its_live_run(self):
        self.run_at("00:30", LIVE_TITLE, "failure", [("Setup", "success"), ("Build geocoder index", "failure")])
        for i in range(40):
            self.run_at(f"{1 + i // 60:02d}:{i % 60:02d}", SCRATCH_TITLE, "success", branch="perf/nightly-speed")
        for i in range(10):
            self.run_at(f"{3 + i // 60:02d}:{i % 60:02d}", SCRATCH_TITLE, "success")

        out, dispatched = self.watchdog()

        self.assertEqual(dispatched, self.retry())
        self.assertIn("total=1  succeeded=0", out)

    def test_a_live_run_that_succeeded_leaves_the_day_alone(self):
        self.run_at("06:17", LIVE_TITLE, "success", [("Setup", "success")])
        self.run_at("09:00", SCRATCH_TITLE, "failure", [("Setup", "failure")])

        self.assertEqual(self.watchdog()[1], [])

    # What a rerun repeats is not retried: a cancelled step (a timeout or a hand), or a failure of a step that fails
    # alike on every rerun. A failure elsewhere is, the steps of those names that passed notwithstanding.
    def test_retries_what_a_rerun_may_cure_and_nothing_else(self):
        passed = [("Build tools", "success"), ("Run builder tests", "success"),
                  ("Fail run on patch verify failure", "skipped"), ("Fail run on place search index failure", "skipped")]
        for steps, retried in [
            (passed + [("Build geocoder index", "failure")], True),
            ([("Build tools", "success"), ("Run builder tests", "failure")], False),
            ([("Build tools", "failure")], False),
            (passed[:2] + [("Compress index files", "cancelled")], False),
            (passed[:2] + [("Fail run on patch verify failure", "failure")], False),
            (passed[:2] + [("Fail run on place search index failure", "failure")], False),
            (passed[:2] + [("Check the Rust toolchain", "failure")], True),
            ([], True),
        ]:
            with self.subTest(steps=steps):
                self.runs, self.jobs = [], {}
                self.run_at("06:17", LIVE_TITLE, "failure", steps)

                self.assertEqual(self.watchdog()[1], self.retry() if retried else [])

    def report(self, api_runs):
        """Runs the report step against the REST API's runs `api_runs`, newest first; returns the run it took."""
        data = os.path.join(self.root, "data.json")
        with open(data, "w") as f:
            json.dump({"default_branch": "main", "runs": [], "jobs": {}, "api_runs": api_runs,
                       "api_jobs": [{"steps": []}]}, f)
        for tool, body in {"op": "echo token\n", "python3": "exit 0\n"}.items():
            with open(os.path.join(self.bin, tool), "w") as f:
                f.write("#!/bin/sh\n" + body)
            os.chmod(os.path.join(self.bin, tool), 0o755)
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "FAKE_GH_DATA": data, "FAKE_GH_CALLS": self.calls,
               "GITHUB_REPOSITORY": "immich-app/geoshenanigans", "RUNNER_TEMP": self.root, "O11Y_URL": "https://o.invalid",
               "LATEST_JSON_URL": "https://l.invalid/latest.json"}
        script = watchdog_script("Report the published build and the latest nightly")
        self.assertIn('--run "$RUN_JSON"', script)
        done = subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True, text=True, timeout=60)

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        with open(os.path.join(self.root, "o11y-run.json")) as f:
            return json.load(f)

    # The report's latest finished nightly is the live one's, a scratch run after it notwithstanding, or a dispatch
    # under the live prefix in other case.
    def test_the_report_takes_the_latest_live_run(self):
        live = {"id": 7, "run_attempt": 1, "display_title": LIVE_TITLE, "conclusion": "failure"}
        scratch = {"id": 8, "run_attempt": 1, "display_title": SCRATCH_TITLE, "conclusion": "success"}
        mixed = {"id": 9, "run_attempt": 1, "display_title": MIXED_CASE_TITLE, "conclusion": "failure"}

        self.assertEqual(self.report([mixed, scratch, live]), live)

    def test_the_report_takes_a_retry_as_a_live_run(self):
        retry = {"id": 8, "run_attempt": 1, "display_title": LIVE_DISPATCH_TITLE, "conclusion": "success"}
        live = {"id": 7, "run_attempt": 1, "display_title": LIVE_TITLE, "conclusion": "failure"}

        self.assertEqual(self.report([retry, live]), retry)

    # Past a page of the API's default size of scratch runs since it.
    def test_the_report_finds_the_live_run_past_a_page_of_scratch_runs(self):
        live = {"id": 7, "run_attempt": 1, "display_title": LIVE_TITLE, "conclusion": "failure"}
        scratch = [{"id": 100 + i, "run_attempt": 1, "display_title": SCRATCH_TITLE, "conclusion": "success"}
                   for i in range(40)]

        self.assertEqual(self.report(scratch + [live]), live)

    # The nightly's own title says which runs are tests, as the watchdog reads it: a scheduled run's is the workflow's
    # name, and a dispatch's names its prefix as given, which the watchdog matches byte for byte (an expression's ==
    # is blind to case).
    def test_the_nightly_names_a_scratch_run_in_its_title(self):
        self.assertEqual(workflow_yaml(NIGHTLY)["run-name"],
                         "${{ (inputs.s3_prefix || 'geocoder') != 'geocoder' "
                         "&& format('Build Geocoder Index [scratch: {0}]', inputs.s3_prefix) "
                         "|| inputs.s3_prefix && format('Build Geocoder Index [{0}]', inputs.s3_prefix) "
                         "|| 'Build Geocoder Index' }}")
        with open(WATCHDOG) as f:
            watchdog = f.read()
        self.assertEqual(len(re.findall(rf'== "{re.escape(LIVE_TITLE)}" or \.\w+ == "{re.escape(LIVE_DISPATCH_TITLE)}"',
                                        watchdog)), 2)

    # GitHub runs no workflow it can't parse: a scheduled nightly whose file
    # isn't YAML never starts, and a dispatch of it is refused.
    def test_every_workflow_is_yaml_github_can_read(self):
        names = sorted(n for n in os.listdir(WORKFLOWS) if n.endswith((".yml", ".yaml")))
        self.assertIn(os.path.basename(NIGHTLY), names)
        for name in names:
            with self.subTest(name):
                doc = workflow_yaml(os.path.join(WORKFLOWS, name))
                self.assertIsInstance(doc, dict)
                self.assertIn("jobs", doc)


if __name__ == "__main__":
    unittest.main()
