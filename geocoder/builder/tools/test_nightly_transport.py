#!/usr/bin/env python3
"""Tests for the nightly's transport units: geocoder-transport comes from the place search build,
every compress encodes the units with it, yesterday's fetch decodes them with it once it is built,
and Setup patches only from a build whose units are of this transport version.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import json
import os
import re
import subprocess
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


if __name__ == "__main__":
    unittest.main()
