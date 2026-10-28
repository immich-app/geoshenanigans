#!/usr/bin/env python3
"""Tests for ci_build_tools.sh's tools cache. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


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


class ToolsCacheTest(unittest.TestCase):
    # A cache hit restores only the cached binaries, so a tool the unit tests
    # run that the cache lacks fails them, and the night compiles afresh.
    def test_the_cache_holds_every_tool_the_unit_tests_run(self):
        self.assertEqual(tools_the_unit_tests_run() - cached_tools(), set())

    def test_the_cache_holds_the_unit_tests(self):
        self.assertIn("geocoder-tests", cached_tools())


if __name__ == "__main__":
    unittest.main()
