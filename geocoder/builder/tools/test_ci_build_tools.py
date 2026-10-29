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


def without_sanitizer_block(cmake):
    """The CMake text the nightly configures: it builds without GEOCODER_SANITIZE,
    so nothing inside if(GEOCODER_SANITIZE) ... endif() is built or cached there."""
    kept, depth = [], 0
    for line in cmake.splitlines():
        stripped = line.strip()
        if depth == 0 and stripped.startswith("if(GEOCODER_SANITIZE)"):
            depth = 1
            continue
        if depth:
            if re.match(r"if\s*\(", stripped):
                depth += 1
            elif re.match(r"endif\s*\(", stripped):
                depth -= 1
            continue
        kept.append(line)
    return "\n".join(kept)


# The tools geocoder-tests runs in the nightly's build: the targets it depends
# on and the binaries its compile definitions name ($<TARGET_FILE:...>).
def tools_the_unit_tests_run(cmake=None):
    if cmake is None:
        with open(os.path.join(HERE, "..", "CMakeLists.txt")) as f:
            cmake = f.read()
    cmake = without_sanitizer_block(cmake)
    depends = re.search(r"add_dependencies\(geocoder-tests ([^)]*)\)", cmake)
    return set(depends.group(1).split()) | set(re.findall(r"\$<TARGET_FILE:([\w-]+)>", cmake))


class ToolsCacheTest(unittest.TestCase):
    # A cache hit restores only the cached binaries, so a tool the unit tests
    # run that the cache lacks fails them, and the night compiles afresh.
    def test_the_cache_holds_every_tool_the_unit_tests_run(self):
        self.assertEqual(tools_the_unit_tests_run() - cached_tools(), set())

    def test_the_cache_holds_the_unit_tests(self):
        self.assertIn("geocoder-tests", cached_tools())

    def test_a_sanitizer_only_tool_needs_no_cache_entry(self):
        cmake = "\n".join([
            "add_dependencies(geocoder-tests geocoder-diff)",
            'target_compile_definitions(geocoder-tests PRIVATE A="$<TARGET_FILE:geocoder-patch>")',
            "if(GEOCODER_SANITIZE)",
            "    if(UNIX)",
            "        set(X 1)",
            "    endif()",
            '    target_compile_definitions(geocoder-tests PRIVATE B="$<TARGET_FILE:sanitized-only>")',
            "endif()",
            'target_compile_definitions(geocoder-tests PRIVATE C="$<TARGET_FILE:after-the-block>")',
        ])
        self.assertEqual(tools_the_unit_tests_run(cmake), {"geocoder-diff", "geocoder-patch", "after-the-block"})


if __name__ == "__main__":
    unittest.main()
