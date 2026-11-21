#!/usr/bin/env python3
"""Tests for what runs beside what in the nightly's first steps: the day's diff and today's PBF in the
download, and the Rust toolchain in the background.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import gzip
import os
import re
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")


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


class DownloadTest(unittest.TestCase):
    # On a normal night today's PBF isn't in the cache: one look, with no retries and their sleeps (15 s a night).
    def test_todays_pbf_is_looked_for_once(self):
        script = step_script("Download PBF and previous indexes")
        looks = re.findall(r".*planet-\$\{TODAY\}\.osm\.pbf.*", script)
        self.assertEqual(len([line for line in looks if " ls " in line]), 1, looks)
        self.assertNotIn("cache_exists \"s3://$CACHE_BUCKET/$PBF_CACHE/planet-${TODAY}", script)
        self.assertIn('cache_exists "s3://$CACHE_BUCKET/geocoder-cache/planet-${YESTERDAY}.osm.pbf"', script)

    # The day's diff comes down beside the PBF, checked, and the step waits for it without failing on it.
    def test_the_days_diff_comes_down_beside_the_pbf(self):
        script = step_script("Download PBF and previous indexes")
        self.assertIn("TODAY_SEQ: ${{ steps.setup.outputs.today_seq }}", step("Download PBF and previous indexes"))
        fetch = script.index('-o "$WORKDIR/diff-today.osc.gz.part"')
        self.assertLess(fetch, script.index("download_pbf_from_cache \\"))
        self.assertIn('gzip -t "$WORKDIR/diff-today.osc.gz.part"', script)
        self.assertIn("--retry 5 --retry-delay 30 --retry-all-errors", script[fetch - 200:fetch])
        self.assertIn("wait $DIFF_PID || true", script)

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


if __name__ == "__main__":
    unittest.main()
