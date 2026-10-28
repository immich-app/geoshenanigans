#!/usr/bin/env python3
"""Tests for ci_build_tools.sh: its tools cache, and the packages the nightly builds the tools with.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
WORKFLOWS = os.path.join(HERE, "..", "..", "..", ".github", "workflows")


def cached_tools():
    with open(os.path.join(HERE, "ci_build_tools.sh")) as f:
        listed = re.search(r"^tools=\(([^)]*)\)$", f.read(), re.M)
    return set(listed.group(1).split())


# The tools geocoder-tests runs: the targets it depends on and the binaries
# its compile definitions name ($<TARGET_FILE:...>).
def tools_the_unit_tests_run():
    with open(os.path.join(HERE, "..", "CMakeLists.txt")) as f:
        cmake = f.read()
    depends = re.search(r"add_dependencies\(geocoder-tests ([^)]*)\)", cmake)
    return set(depends.group(1).split()) | set(re.findall(r"\$<TARGET_FILE:([\w-]+)>", cmake))


# The words of the nightly's apt-get install command, its continued lines too.
def nightly_packages():
    with open(os.path.join(WORKFLOWS, "geocoder-build.yml")) as f:
        install = re.search(r"apt-get install((?:.*\\\n)*.*)", f.read())
    return set(install.group(1).replace("\\\n", " ").split())


# The pull_request paths of geocoder-tests.yml, the workflow that runs these tests.
def paths_that_run_the_tests():
    with open(os.path.join(WORKFLOWS, "geocoder-tests.yml")) as f:
        paths = re.search(r"^  pull_request:\n    paths:\n((?:      .*\n)*)", f.read(), re.M)
    return set(re.findall(r'^      - "([^"]+)"$', paths.group(1), re.M))


class ToolsCacheTest(unittest.TestCase):
    # A cache hit restores only the cached binaries, so a tool the unit tests
    # run that the cache lacks fails them, and the night compiles afresh.
    def test_the_cache_holds_every_tool_the_unit_tests_run(self):
        self.assertEqual(tools_the_unit_tests_run() - cached_tools(), set())

    def test_the_cache_holds_the_unit_tests(self):
        self.assertIn("geocoder-tests", cached_tools())


class NightlyPackagesTest(unittest.TestCase):
    # ci_build_tools.sh configures the default build, whose patcher links
    # libzstd: without its headers and static library the night's tools don't
    # configure, which PR CI, installing its own packages, wouldn't show.
    def test_the_nightly_installs_the_libzstd_the_patcher_links(self):
        self.assertIn("libzstd-dev", nightly_packages())

    # The test above reads the nightly's workflow, so a pull request changing
    # only that workflow, dropping libzstd-dev say, must run it.
    def test_a_change_to_the_nightly_runs_these_tests(self):
        self.assertIn(".github/workflows/geocoder-build.yml", paths_that_run_the_tests())


if __name__ == "__main__":
    unittest.main()
