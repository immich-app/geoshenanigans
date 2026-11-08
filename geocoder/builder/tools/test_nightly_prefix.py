#!/usr/bin/env python3
"""Tests for the nightly's s3_prefix dispatch input: a test run from a branch publishes under a scratch prefix,
a scheduled night under the live one, and the Setup step refuses a prefix the cleanup could reach out of.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import re
import subprocess
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
        # The builds, latest.json and memwatch keys all sit under it, so a scratch run touches nothing live.
        paths = re.findall(r"s3://\$S3_BUCKET/([^\"\s]*)", nightly())
        self.assertTrue(paths)
        self.assertEqual([p for p in paths if not p.startswith("$S3_PREFIX/")], [])

    def test_the_setup_refuses_a_prefix_that_is_not_one_segment(self):
        script = prefix_check()
        self.assertIsNotNone(script)
        for prefix, ok in [("geocoder", True), ("geocoder-runner-check", True), ("arcs.pr3_check", True),
                           ("", False), ("a/b", False), ("../geocoder", False), ("/geocoder", False),
                           ("geocoder builds", False), ("Geocoder", False), ("-x", False), ("geocoder\n", False)]:
            r = subprocess.run(["bash", "-c", script], env={"S3_PREFIX": prefix, "PATH": os.environ["PATH"]},
                               capture_output=True, text=True)
            self.assertEqual(r.returncode == 0, ok, f"{prefix!r}: {r.stdout}{r.stderr}")


if __name__ == "__main__":
    unittest.main()
