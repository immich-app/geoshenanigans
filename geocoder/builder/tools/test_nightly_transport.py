#!/usr/bin/env python3
"""Tests for the nightly's transport units: geocoder-transport comes from the place search build,
every compress encodes the units with it, yesterday's fetch decodes them with it once it is built,
Setup patches only from a build whose units are of this transport version, and a night whose
unit failed (its files went as .zst) is published but neither named by latest.json nor followed
by a cleanup of the builds before it.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
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
FAKE_COMPRESS = """\
import os, sys
print("planet/full/places.tt.zst: verify: place_nodes.bin differs; its files go as .zst")
sys.exit(int(os.environ["COMPRESS_STATUS"]))
"""


def env_after(name, compress_status):
    """What the step `name` puts in GITHUB_ENV (a dict) when compress_index.py exits `compress_status`,
    run as Actions runs it, on a night of no variants, with s5cmd, compress_index.py and
    generate_patches.sh that do nothing else."""
    with tempfile.TemporaryDirectory() as tmp:
        ws, work, bin_dir = (os.path.join(tmp, d) for d in ("ws", "work", "bin"))
        tools = os.path.join(ws, "geocoder", "builder", "tools")
        for d in (tools, os.path.join(work, "output"), bin_dir):
            os.makedirs(d)
        fakes = {os.path.join(tools, "compress_index.py"): FAKE_COMPRESS,
                 os.path.join(tools, "generate_patches.sh"): "#!/bin/sh\nexit 0\n",
                 os.path.join(bin_dir, "s5cmd"): "#!/bin/sh\nexit 0\n",
                 os.path.join(work, "old-output.status"): "0\n",
                 os.path.join(work, "old-output.log"): ""}
        for path, text in fakes.items():
            with open(path, "w") as f:
                f.write(text)
            os.chmod(path, 0o755)
        os.symlink(sys.executable, os.path.join(bin_dir, "python3"))
        github_env = os.path.join(tmp, "github_env")
        open(github_env, "w").close()
        script = step_script(name)
        assert "${{" not in script, f"{name}'s script takes an expression"
        env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "GITHUB_WORKSPACE": ws, "WORKDIR": work,
               "GITHUB_ENV": github_env, "GITHUB_STEP_SUMMARY": os.path.join(tmp, "summary"),
               "COMPRESS_STATUS": str(compress_status), "NEW_DATE": "2026-11-17", "OLD_DATE": "2026-11-16",
               "S3_ENDPOINT": "https://s3.invalid", "S3_BUCKET": "bucket", "S3_PREFIX": "geocoder",
               "TMPDIR": tmp}
        r = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-c", script], env=env, capture_output=True,
                           text=True)
        if r.returncode != 0:
            raise AssertionError(f"{name} exited {r.returncode}: {r.stdout}{r.stderr}")
        with open(github_env) as f:
            return dict(line.rstrip("\n").split("=", 1) for line in f if "=" in line)


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
        self.assertEqual(version, "1")
        unit = lambda v: {"kind": "places", "transport_version": v, "size": 1, "files": []}  # noqa: E731
        for cfg, compatible in [
            ({"units": {"a/places.tt.zst": unit(1), "b/arcs.tt.zst": unit(1)}}, True),
            ({"units": {"a/places.tt.zst": unit(2)}}, False),
            ({"units": {"a/places.tt.zst": unit(1), "b/arcs.tt.zst": unit(2)}}, False),
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

    def test_latest_json_and_the_cleanup_wait_for_a_night_of_no_failed_patch_or_unit(self):
        self.assertIsNotNone(latest_json_guard())
        for env in ({}, {"PATCH_VERIFY_FAILED": "1"}, {"TRANSPORT_UNIT_FAILED": "1"},
                    {"PATCH_VERIFY_FAILED": "1", "TRANSPORT_UNIT_FAILED": "1"}):
            self.assertEqual(advances_latest_json(env), not env, env)
            self.assertEqual(runs(CLEANUP, env), not env, env)


if __name__ == "__main__":
    unittest.main()
