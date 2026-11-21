#!/usr/bin/env python3
"""Tests for the order of the nightly's steps and what runs beside what: the day's diff and today's PBF in the
download, the Rust toolchain in the background, the place search beside compression and the patches from
build-index's exit, and the sidecars' upload beside them; and which runs the watchdog retries.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import gzip
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
WATCHDOG = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build-watchdog.yml")


def nightly():
    with open(NIGHTLY) as f:
        return f.read()


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


class DownloadTest(unittest.TestCase):
    # On a normal night today's PBF isn't in the cache: one look, with no retries and their sleeps (15 s a night).
    def test_todays_pbf_is_looked_for_once(self):
        script = step_script("Download PBF and previous indexes")
        looks = re.findall(r".*planet-\$\{TODAY\}\.osm\.pbf.*", script)
        self.assertEqual(len([line for line in looks if " ls " in line]), 1, looks)
        self.assertNotIn("cache_exists \"s3://$CACHE_BUCKET/$PBF_CACHE/planet-${TODAY}", script)
        self.assertIn('cache_exists "s3://$CACHE_BUCKET/geocoder-cache/planet-${YESTERDAY}.osm.pbf"', script)

    # The day's diff comes down beside the PBF, at most 10 minutes with its retries (fetch_day_diff.sh).
    def test_the_days_diff_comes_down_beside_the_pbf(self):
        script = step_script("Download PBF and previous indexes")
        self.assertIn("TODAY_SEQ: ${{ steps.setup.outputs.today_seq }}", step("Download PBF and previous indexes"))
        fetch = script.index('fetch_day_diff.sh" "$TODAY_SEQ" "$WORKDIR/diff-today.osc.gz" 600 \\\n')
        self.assertLess(fetch, script.index("download_pbf_from_cache \\"))

    def run_apply(self, diff):
        """Apply replication diffs from yesterday's PBF, with a diff from the download (bytes, or None), and a curl,
        pbf-apply and df that log their calls; returns the calls."""
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        work, bin_dir = os.path.join(root, "work"), os.path.join(root, "bin")
        os.makedirs(work)
        os.makedirs(bin_dir)
        calls = os.path.join(root, "calls")
        fakes = {"curl": f'echo "curl $*" >> {calls}\nout=""\n'
                         'while [ $# -gt 0 ]; do [ "$1" = -o ] && out=$2; shift; done\n'
                         'echo "<osmChange/>" | gzip -c > "$out"\n',
                 "pbf-apply": f'echo "pbf-apply $*" >> {calls}\n: > "{work}/planet-tmp.osm.pbf"\n',
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
               "PBF_SOURCE": "yesterday"}
        done = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step_script("Apply replication diffs")], env=env,
                              capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        with open(calls) as f:
            return f.read().splitlines()

    def test_the_apply_takes_the_downloads_diff(self):
        calls = self.run_apply(gzip.compress(b"<osmChange/>"))

        self.assertEqual([c.split()[0] for c in calls], ["pbf-apply"])

    def test_the_apply_fetches_the_diff_itself_when_the_download_had_none_or_a_broken_one(self):
        for diff in (None, b"not gzip"):
            calls = self.run_apply(diff)

            self.assertEqual([c.split()[0] for c in calls], ["curl", "pbf-apply"], diff)
            self.assertIn("/replication/day/000/004/710.osc.gz", calls[0])


# The download step's tools as the step runs them. s5cmd lists the keys of $FAKE_CACHED (":"-separated, under the
# cache bucket) and copies any to a local path; curl answers the wikidata and TIGER lookups and downloads, and the
# day's diff as $FAKE_DIFF says: "ok" (a gzip), or "stall" (it writes its pid to $FAKE_DIFF_PID and never ends);
# aria2c (the mirror) writes the PBF. Each call goes to $FAKE_CALLS.
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
    """The download step, run with the fakes above: the day's diff comes down beside the PBF, and only a run that
    applies it to yesterday's PBF waits for it."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.work, self.bin = os.path.join(self.root, "work"), os.path.join(self.root, "bin")
        os.makedirs(self.work)
        os.makedirs(self.bin)
        # python3 is the GeoNames download's, and sleep the retries' waits.
        fakes = {"s5cmd": FAKE_S5CMD, "curl": FAKE_CURL, "aria2c": FAKE_ARIA2C, "python3": "#!/bin/sh\nexit 0\n",
                 "sleep": "#!/bin/sh\nexit 0\n"}
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

    def download(self, cached, diff, pbf_cache="geocoder-cache"):
        """Runs the step with the cache holding `cached` (keys) and the day's diff `diff`; returns its outputs, its
        output lines and how long it took."""
        output = os.path.join(self.root, "output")
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "WORKDIR": self.work, "CACHE_ENDPOINT": "http://cache.invalid",
               "CACHE_BUCKET": "cache", "CACHE_AWS_KEY": "k", "CACHE_AWS_SECRET": "s", "YESTERDAY": "2026-11-20",
               "TODAY": "2026-11-21", "TODAY_SEQ": "4710", "HAS_PREVIOUS": "false", "PBF_CACHE": pbf_cache,
               "S3_ENDPOINT": "https://s3.invalid", "S3_BUCKET": "bucket", "S3_PREFIX": "geocoder",
               "S3_PUBLIC_URL": "https://public.invalid", "GITHUB_OUTPUT": output,
               "GITHUB_WORKSPACE": os.path.join(HERE, "..", "..", ".."), "FAKE_CALLS": self.calls,
               "FAKE_CACHED": ":".join(cached), "FAKE_DIFF": diff, "FAKE_DIFF_PID": self.diff_pid}
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

    def stalled_fetch_ended(self):
        """Waits up to 5 s for the stalled fetch to end; whether it did."""
        deadline = time.time() + 5
        while not os.path.exists(self.diff_pid) and time.time() < deadline:
            time.sleep(0.05)
        with open(self.diff_pid) as f:
            pid = int(f.read())
        while alive(pid) and time.time() < deadline:
            time.sleep(0.05)
        return not alive(pid)

    def diff_files(self):
        return sorted(n for n in os.listdir(self.work) if n.startswith("diff-today.osc"))

    # A run that has today's PBF from the cache needs no diff: a stalled fetch of it must not hold the step (up to
    # its 60-minute timeout, a cancelled step the watchdog doesn't retry); it is ended, and leaves nothing.
    def test_a_cached_pbf_of_today_neither_waits_for_the_diff_nor_leaves_its_fetch_running(self):
        outputs, _, took = self.download(["geocoder-cache/planet-2026-11-21.osm.pbf"], "stall")

        self.assertEqual(outputs["pbf_source"], "today")
        self.assertLess(took, 30)
        self.assertTrue(self.stalled_fetch_ended())
        self.assertEqual(self.diff_files(), [])

    # The mirror's PBF takes its diffs in Apply replication diffs: the day's, stalled or not, is no concern here.
    def test_a_mirror_pbf_neither_waits_for_the_diff_nor_leaves_its_fetch_running(self):
        outputs, _, took = self.download([], "stall")

        self.assertEqual(outputs["pbf_source"], "mirror")
        self.assertLess(took, 30)
        self.assertTrue(self.stalled_fetch_ended())
        self.assertEqual(self.diff_files(), [])

    def test_yesterdays_pbf_waits_for_the_diff_it_takes(self):
        outputs, out, _ = self.download(["geocoder-cache/planet-2026-11-20.osm.pbf"], "ok")

        self.assertEqual(outputs["pbf_source"], "yesterday")
        self.assertEqual(self.diff_files(), ["diff-today.osc.gz"])
        with gzip.open(os.path.join(self.work, "diff-today.osc.gz")) as f:
            self.assertEqual(f.read(), b"<osmChange/>")
        self.assertIn("[diff] seq 4710", out)


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
        self.assertIn("Apply replication diffs fetches it", done.stdout)

    # A server that accepts the connection and then sends nothing is given up on at the limit, curl ended with it.
    def test_a_stalled_server_is_given_up_on_at_the_limit(self):
        done, took = self.fetch("open(out, 'wb').write(b'\\x1f')\ntime.sleep(300)\n", limit=2)

        self.assertNotEqual(done.returncode, 0)
        self.assertLess(took, 30)
        self.assertEqual(self.files(), [])
        with open(self.pid) as f:
            self.assertFalse(alive(int(f.read())))


class RustToolchainTest(unittest.TestCase):
    # A fresh pod downloads the toolchain (~15 s): inside the detached build of place-search, not a step of its own.
    def test_the_toolchain_installs_in_the_background_before_the_place_search_build(self):
        self.assertNotIn("dtolnay/rust-toolchain", nightly())
        script = step_script("Build place search")
        self.assertRegex(script, r"setsid nohup bash -c '\"\$1\" && shift && \"\$@\"; echo \$\? > \"\$0\"' "
                                 r"\"\$RUNNER_TEMP/search\.status\" \\\n +geocoder/builder/tools/rust_toolchain\.sh \\\n"
                                 r" +cargo build --release --locked --manifest-path geocoder/search/Cargo\.toml")
        self.assertIn('PATH="$CARGO_HOME/bin:$PATH"', script)

    def test_the_toolchain_script_installs_a_minimal_stable_toolchain(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        calls = os.path.join(root, "calls")
        with open(os.path.join(root, "rustup"), "w") as f:
            f.write(f'#!/bin/sh\necho "rustup $*" >> {calls}\n')
        os.chmod(os.path.join(root, "rustup"), 0o755)
        subprocess.run(["bash", os.path.join(HERE, "rust_toolchain.sh")], check=True, capture_output=True,
                       env={"PATH": f"{root}:{os.environ['PATH']}"})
        with open(calls) as f:
            self.assertEqual(f.read().splitlines(), ["rustup toolchain install stable --profile minimal --no-self-update",
                                                     "rustup default stable"])


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
            self.assertLess(clear, sidecars, name)
            self.assertLess(sidecars, compress, name)
            self.assertLess(wait, compress, name)
            self.assertIn("RUNNER_TEMP: ${{ runner.temp }}", step(name), name)
        retry = step_script("Compress index files")
        self.assertLess(retry.index('[ -s "$WORKDIR/sidecars.status" ]'), retry.index("rm \"s3://$S3_BUCKET/$S3_PREFIX"))

    def test_finalize_waits_for_the_sidecars_before_its_first_pass(self):
        script = step_script("Finalize upload and configurations.json")
        self.assertLess(script.index('[ -s "$WORKDIR/sidecars.status" ]'), script.index("cp --if-size-differ"))
        self.assertIn("Caching today's PBF ended at $(date -u -r \"$WORKDIR/pbf-cache.status\" +%H:%M:%S)", script)

    # The watchdog tells a retry from a failure a rerun repeats by these steps' names.
    def test_every_step_the_watchdog_names_is_a_step_of_the_nightly(self):
        with open(WATCHDOG) as f:
            named = set(re.findall(r'\.name == "([^"]+)"', f.read()))
        self.assertEqual(named, {"Build tools", "Run builder tests", "Fail run on patch verify failure",
                                 "Fail run on place search index failure"})
        self.assertLessEqual(named, {n for n, _ in steps()})


def watchdog_script(name):
    """The `run:` script of the watchdog's step `name`, unindented."""
    with open(WATCHDOG) as f:
        text = f.read()
    body = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", text, re.M).group(1)
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", body, re.M)
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines())


# gh as the watchdog runs it, answering from the JSON in $FAKE_GH_DATA: the repo's default branch, the build's runs
# (each `run list` one with only the fields asked for), each run's jobs, and the REST API's runs and jobs; --jq
# filters go through jq. `workflow run` appends its arguments to $FAKE_GH_CALLS.
FAKE_GH = r'''#!{python}
import json, os, subprocess, sys
args = sys.argv[1:]
with open(os.environ["FAKE_GH_DATA"]) as f:
    data = json.load(f)
def opt(name):
    return args[args.index(name) + 1] if name in args else None
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
    answer([{{k: run[k] for k in fields if k in run}} for run in data["runs"]])
elif args[:2] == ["run", "view"]:
    answer({{"jobs": data["jobs"].get(args[2], [])}})
elif args[:2] == ["workflow", "run"]:
    with open(os.environ["FAKE_GH_CALLS"], "a") as f:
        f.write(" ".join(args) + "\n")
elif args[0] == "api":
    answer({{"jobs": data["api_jobs"]}} if args[1].endswith("/jobs") else {{"workflow_runs": data["api_runs"]}})
else:
    sys.exit("fake gh: " + " ".join(args))
'''

LIVE_TITLE = "Build Geocoder Index"
SCRATCH_TITLE = "Build Geocoder Index [scratch: nightly-speed-1121]"


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

    def run_at(self, hhmm, title, conclusion, steps=(), run_id=None):
        """A run of the build created today at hhmm, with its title, conclusion and steps ((name, conclusion)...)."""
        run_id = run_id or f"{len(self.runs) + 1}"
        self.runs.append({"databaseId": int(run_id), "conclusion": conclusion, "status": "completed",
                          "createdAt": f"{self.today}T{hhmm}:00Z", "event": "workflow_dispatch", "headBranch": "main",
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
            ([], True),
        ]:
            with self.subTest(steps=steps):
                self.runs, self.jobs = [], {}
                self.run_at("06:17", LIVE_TITLE, "failure", steps)

                self.assertEqual(self.watchdog()[1], self.retry() if retried else [])

    # The report's latest finished nightly is the live one's, a scratch run after it notwithstanding.
    def test_the_report_takes_the_latest_live_run(self):
        live = {"id": 7, "run_attempt": 1, "display_title": LIVE_TITLE, "conclusion": "failure"}
        scratch = {"id": 8, "run_attempt": 1, "display_title": SCRATCH_TITLE, "conclusion": "success"}
        data = os.path.join(self.root, "data.json")
        with open(data, "w") as f:
            json.dump({"default_branch": "main", "runs": [], "jobs": {}, "api_runs": [scratch, live],
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
            self.assertEqual(json.load(f), live)

    # The nightly's own title says which runs are tests, as the watchdog reads it.
    def test_the_nightly_names_a_scratch_run_in_its_title(self):
        run_name = re.search(r"(?m)^run-name: (.*)$", nightly())
        self.assertIsNotNone(run_name)
        self.assertEqual(run_name.group(1), "${{ (inputs.s3_prefix || 'geocoder') == 'geocoder' && 'Build Geocoder Index' "
                                            "|| format('Build Geocoder Index [scratch: {0}]', inputs.s3_prefix) }}")


if __name__ == "__main__":
    unittest.main()
