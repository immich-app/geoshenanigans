#!/usr/bin/env python3
"""Tests for the nightly's transport units: geocoder-transport comes from the place search build,
every compress encodes the units with it, yesterday's fetch decodes them with it once it is built,
Setup patches only from a build whose units are of this transport version, and a night whose
unit failed (its files went as .zst) is published but neither named by latest.json nor followed
by a cleanup of the builds before it.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import contextlib
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")
TEMPLATE = os.path.join(HERE, "..", "configurations.template.json")
SEARCH_CARGO = os.path.join(HERE, "..", "..", "search", "Cargo.toml")
CLI = '"$GITHUB_WORKSPACE/geocoder/search/target/release/geocoder-transport"'


def nightly():
    with open(NIGHTLY) as f:
        return f.read()


def jq_program(var):
    """The jq program of the Setup line that sets `var` from a configurations.json."""
    line = re.search(rf"^ *{var}=\$\((?:echo \"\$PREV_CFG\" \| )?jq -r (?:'([^']*)'|(\S+))", nightly(), re.M)
    return (line.group(1) or line.group(2)) if line else None


def jq(program, doc):
    return subprocess.run(["jq", "-r", program], input=json.dumps(doc), capture_output=True, text=True,
                          check=True).stdout.strip()


def step(name):
    """The lines of the nightly's step `name`, but for its `- name:` line."""
    block = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", nightly(), re.M)
    return block.group(1) if block else None


def step_if(name):
    """The `if:` of the step `name`, as Python of `env` (a dict, an unset name ''); none without one."""
    cond = re.search(r"^        if: (.*)$", step(name), re.M)
    if not cond:
        return None
    expr = cond.group(1).strip()
    if expr.startswith("${{") and expr.endswith("}}"):
        expr = expr[3:-2]
    # The steps this reads test env alone: env.NAME, quoted strings, == and !=, && and ||.
    py = re.sub(r"env\.(\w+)", r"env.get('\1', '')", expr).replace("&&", " and ").replace("||", " or ")
    if re.sub(r"env\.get\('\w+', ''\)|'[^']*'|==|!=|\band\b|\bor\b|[\s()]", "", py):
        raise ValueError(f"an if: of more than env comparisons: {expr}")
    return py


def runs(name, env):
    """Whether the step `name` runs in a job of `env`."""
    py = step_if(name)
    return py is None or eval(py, {"__builtins__": {}}, {"env": env})


def step_script(name):
    """The `run:` script of the step `name`."""
    run = re.search(r"^        run: \|\n((?:(?: {10}.*)?\n)*)", step(name), re.M)
    return "".join(line[10:] + "\n" for line in run.group(1).splitlines()) if run else None


def latest_json_guard():
    """The test of Finalize's update of latest.json, as bash."""
    guard = re.search(r"# ── Update latest\.json ──\n(?: *#.*\n)* *if (.*); then\n", step_script(FINALIZE) or "")
    return guard.group(1) if guard else None


def advances_latest_json(env):
    """Whether Finalize updates latest.json in a job of `env`."""
    script = f"if {latest_json_guard()}; then echo update; else echo hold; fi"
    return subprocess.run(["bash", "-c", script], env={"PATH": os.environ["PATH"], **env}, capture_output=True,
                          text=True, check=True).stdout.strip() == "update"


PATCH_STEP = "Generate patches and verify"
COMPRESS_STEP = "Compress index files"
FINALIZE = "Finalize upload and configurations.json"
CLEANUP = "Cleanup old Tigris builds"

# compress_index.py for a step run: one line in its log as a failed unit's, the status it is given.
# What it took the place search's files on is the job's status, or none (none written, as past
# its wait); a status in $LATE_SEARCH_STATUS is then written as the job's, too late. Its .zst,
# one, is there once it is done (status 0 or 3).
FAKE_COMPRESS = """\
import os, sys
print("planet/full/places.tt.zst: verify: place_nodes.bin differs; its files go as .zst")
args = sys.argv[1:]
status = args[args.index("--late-search") + 1]
if "--late-search-taken" in args:
    with open(args[args.index("--late-search-taken") + 1], "w") as f:
        f.write((open(status).read().strip() if os.path.exists(status) else "none") + "\\n")
if os.environ.get("LATE_SEARCH_STATUS"):
    with open(status, "w") as f:
        f.write(os.environ["LATE_SEARCH_STATUS"])
code = int(os.environ["COMPRESS_STATUS"])
if code in (0, 3):
    open(os.path.join(args[0], "planet/full/geo_cells.bin.zst"), "w").close()
sys.exit(code)
"""

SEARCH_FILES = ("planet/search/full/simplified/all/places.fst", "planet/search/full/simplified/all/search_meta.json")

# checksums.sh zst for a step run: one line, for the step's output dir alone, once its .zst is there.
ZST_LINE = "0" * 64 + "  ./planet/full/geo_cells.bin.zst"
FAKE_CHECKSUMS = f"""#!/bin/sh
[ "$1" = zst ] && [ "$2" = "$WORKDIR/output" ] && [ -f "$2/planet/full/geo_cells.bin.zst" ] || exit 2
echo "{ZST_LINE}"
"""


def env_after(name, compress_status, search_status="0\n", late_status="", job=None, left=None):
    """What the step `name` (or each of a list of steps, one after the other in one workspace) puts in
    GITHUB_ENV (a dict) when compress_index.py exits `compress_status`,
    run as Actions runs it, on a night of no variants, with s5cmd, compress_index.py,
    generate_patches.sh and checksums.sh that do nothing else, place-search built and the place
    search job's status `search_status` (None: none written), then `late_status` written as its
    status once compression has taken it. With `job` (a dict) the place search job still runs, a
    process in a session of its own, and `job` gets whether it still runs after the step and the
    search files it left. With `left` (a dict), it gets the .zst checksums the step left for
    Finalize (zst.sha256, None for none) and whether their .part is left too."""
    with tempfile.TemporaryDirectory() as tmp:
        ws, work, bin_dir = (os.path.join(tmp, d) for d in ("ws", "work", "bin"))
        tools = os.path.join(ws, "geocoder", "builder", "tools")
        for d in (tools, os.path.join(work, "output", "planet", "full"), bin_dir):
            os.makedirs(d)
        for rel in SEARCH_FILES:
            os.makedirs(os.path.dirname(os.path.join(work, "output", rel)), exist_ok=True)
            with open(os.path.join(work, "output", rel), "w") as f:
                f.write("{}")
        if job is not None:
            # Its session's leader exits at once, as the job's process outlives it.
            pid = os.path.join(tmp, "job.pid")
            subprocess.run(["setsid", "sh", "-c", f"sleep 300 > /dev/null 2>&1 & echo $! > {pid}; "
                                                  f"echo $$ > {os.path.join(work, 'place-search.pid')}"], check=True)
            with open(pid) as f:
                job["pid"] = int(f.read())
        fakes = {os.path.join(tools, "compress_index.py"): FAKE_COMPRESS,
                 os.path.join(tools, "generate_patches.sh"): "#!/bin/sh\nexit 0\n",
                 os.path.join(tools, "checksums.sh"): FAKE_CHECKSUMS,
                 os.path.join(bin_dir, "s5cmd"): "#!/bin/sh\nexit 0\n",
                 os.path.join(work, "old-output.status"): "0\n",
                 os.path.join(work, "old-output.log"): "",
                 os.path.join(tmp, "search.status"): "0\n"}
        if search_status is not None:
            fakes[os.path.join(work, "place-search.status")] = search_status
        for path, text in fakes.items():
            with open(path, "w") as f:
                f.write(text)
            os.chmod(path, 0o755)
        os.symlink(sys.executable, os.path.join(bin_dir, "python3"))
        github_env = os.path.join(tmp, "github_env")
        open(github_env, "w").close()
        scripts = [(step_name, step_script(step_name)) for step_name in ([name] if isinstance(name, str) else name)]
        for step_name, script in scripts:
            assert "${{" not in script, f"{step_name}'s script takes an expression"
        env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "GITHUB_WORKSPACE": ws, "WORKDIR": work,
               "GITHUB_ENV": github_env, "GITHUB_STEP_SUMMARY": os.path.join(tmp, "summary"),
               "COMPRESS_STATUS": str(compress_status), "NEW_DATE": "2026-11-17", "OLD_DATE": "2026-11-16",
               "S3_ENDPOINT": "https://s3.invalid", "S3_BUCKET": "bucket", "S3_PREFIX": "geocoder",
               "TMPDIR": tmp, "RUNNER_TEMP": tmp, "LATE_SEARCH_STATUS": late_status}
        try:
            for step_name, script in scripts:
                r = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-c", script], env=env,
                                   capture_output=True, text=True, timeout=120)
                if r.returncode != 0:
                    break
            if job is not None:
                job["running"] = alive(job["pid"])
                job["search"] = sorted(rel for rel in SEARCH_FILES if os.path.exists(os.path.join(work, "output", rel)))
            if left is not None:
                zst = os.path.join(work, "zst.sha256")
                with contextlib.suppress(FileNotFoundError), open(zst) as f:
                    left["zst.sha256"] = f.read()
                left.setdefault("zst.sha256", None)
                left["part"] = os.path.exists(zst + ".part")
        finally:
            if job is not None and alive(job["pid"]):
                os.kill(job["pid"], 9)
        if r.returncode != 0:
            raise AssertionError(f"{step_name} exited {r.returncode}: {r.stdout}{r.stderr}")
        with open(github_env) as f:
            return dict(line.rstrip("\n").split("=", 1) for line in f if "=" in line)


def alive(pid):
    """Whether process `pid` runs (a zombie counts as ended)."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


class NightlyTransportTest(unittest.TestCase):
    def test_the_place_search_build_makes_geocoder_transport(self):
        with open(SEARCH_CARGO) as f:
            self.assertRegex(f.read(), r'(?m)^\[\[bin\]\]\nname = "geocoder-transport"$')
        self.assertIn("cargo build --release --locked --manifest-path geocoder/search/Cargo.toml", nightly())
        self.assertIn('"$RUNNER_TEMP/search.status"', nightly())

    def test_every_compress_encodes_the_units_with_it(self):
        calls = re.findall(r'compress_index\.py" "\$WORKDIR/output" \\\n((?: +.*\\\n)* +.*)', nightly())
        self.assertEqual(len(calls), 2)
        for call in calls:
            self.assertIn(f"--transport-bin {CLI}", call)

    def test_the_fetch_of_yesterday_decodes_with_it_once_its_build_is_done(self):
        fetch = re.search(r'fetch_previous_index\.sh" \\\n((?: +.*\\\n)* +.*)', nightly())
        self.assertIsNotNone(fetch)
        self.assertIn(f'{CLI} "$RUNNER_TEMP/search.status"', fetch.group(1))

    def test_setup_patches_only_from_units_of_this_transport_version(self):
        expected, previous = jq_program("EXPECTED_TRANSPORT_VERSION"), jq_program("PREV_TRANSPORT")
        self.assertIsNotNone(expected)
        self.assertIsNotNone(previous)
        with open(TEMPLATE) as f:
            version = jq(expected, json.load(f))
        self.assertEqual(version, "2")
        unit = lambda v: {"kind": "places", "transport_version": v, "size": 1, "files": []}  # noqa: E731
        for cfg, compatible in [
            ({"units": {"a/places.tt.zst": unit(2), "b/arcs.tt.zst": unit(2)}}, True),
            ({"units": {"a/places.tt.zst": unit(1)}}, False),
            ({"units": {"a/places.tt.zst": unit(2), "b/arcs.tt.zst": unit(1)}}, False),
            ({"units": {}}, False),
            ({"files": {}}, False),
        ]:
            self.assertEqual(jq(previous, cfg) == version, compatible, cfg)
        self.assertIn('[ "$PREV_TRANSPORT" = "$EXPECTED_TRANSPORT_VERSION" ]', nightly())

    def test_a_failed_unit_in_either_compress_keeps_latest_json_and_the_older_builds(self):
        # The test portal fetches a unit's files only as the unit: latest.json must not name a build
        # whose unit went as .zst, nor the cleanup drop the builds it does name.
        for name in (PATCH_STEP, COMPRESS_STEP):
            for status, failed in ((0, False), (3, True)):
                env = env_after(name, status)
                self.assertEqual(env.get("TRANSPORT_UNIT_FAILED") == "1", failed, f"{name}, {status}: {env}")
                self.assertEqual(advances_latest_json(env), not failed, f"{name}, {status}")
                self.assertEqual(runs(CLEANUP, env), not failed, f"{name}, {status}")
                self.assertEqual(runs("Fail run on transport unit failure", env), failed, f"{name}, {status}")

    # Compression done, a failed unit's files gone as .zst included, both steps leave the .zst checksums for
    # Finalize, which otherwise hashes the .zst again itself.
    def test_both_compress_steps_leave_the_zst_checksums_for_finalize(self):
        for name in (PATCH_STEP, COMPRESS_STEP):
            for status in (0, 3):
                with self.subTest(name=name, status=status):
                    left = {}
                    env_after(name, status, left=left)

                    self.assertEqual(left, {"zst.sha256": ZST_LINE + "\n", "part": False})

    # The place search runs in the background beside compression, which waits for its status: the compress
    # step, which always follows, fails the run at its end for a failed, stopped or missing one.
    def test_the_compress_step_fails_the_run_for_a_place_search_that_did_not_succeed(self):
        for status, failed in (("0\n", False), ("1\n", True), ("124\n", True), (None, True)):
            env = env_after(COMPRESS_STEP, 0, search_status=status)
            self.assertEqual(env.get("SEARCH_INDEX_FAILED") == "1", failed, f"{status!r}: {env}")
            self.assertEqual(runs("Fail run on place search index failure", env), failed, status)
            self.assertEqual(advances_latest_json(env), True, status)

    # Compression stopped waiting for a place search that then succeeded: its files are not in
    # configurations.json, so the run fails as for one that never ended, though its status now says
    # 0, and its indexes go with the job, which is ended first. One whose status compression took
    # keeps them, and its job is left alone.
    def test_a_place_search_compression_left_out_fails_the_run_whatever_its_status_says_since(self):
        for search_status, late_status, failed in ((None, "0\n", True), (None, "", True), ("0\n", "", False)):
            with self.subTest(search_status=search_status, late_status=late_status):
                job = {}
                env = env_after(COMPRESS_STEP, 0, search_status=search_status, late_status=late_status, job=job)

                self.assertEqual(env.get("SEARCH_INDEX_FAILED") == "1", failed, env)
                self.assertEqual(runs("Fail run on place search index failure", env), failed)
                self.assertEqual(job["running"], not failed)
                # The index files go once compressed, but for search_meta.json, which stays beside its .zst.
                self.assertEqual(job["search"], [] if failed else [SEARCH_FILES[1]])

    # On a patch night the patch step compresses and the compress step, which skips its own, reads the status
    # that compression took the place search's files on: one it left out fails the run as above.
    def test_a_place_search_the_patch_steps_compression_left_out_fails_the_run(self):
        job = {}
        env = env_after((PATCH_STEP, COMPRESS_STEP), 0, search_status=None, late_status="0\n", job=job)

        self.assertEqual(env.get("SEARCH_INDEX_FAILED"), "1", env)
        self.assertEqual(runs("Fail run on place search index failure", env), True)
        self.assertFalse(job["running"])
        self.assertEqual(job["search"], [])

    def test_latest_json_and_the_cleanup_wait_for_a_night_of_no_failed_patch_or_unit(self):
        self.assertIsNotNone(latest_json_guard())
        for env in ({}, {"PATCH_VERIFY_FAILED": "1"}, {"TRANSPORT_UNIT_FAILED": "1"},
                    {"PATCH_VERIFY_FAILED": "1", "TRANSPORT_UNIT_FAILED": "1"}):
            self.assertEqual(advances_latest_json(env), not env, env)
            self.assertEqual(runs(CLEANUP, env), not env, env)


if __name__ == "__main__":
    unittest.main()
