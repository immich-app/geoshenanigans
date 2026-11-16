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


# The targets the nightly's build can make: its executables and libraries.
def targets_the_build_makes():
    with open(os.path.join(HERE, "..", "CMakeLists.txt")) as f:
        cmake = without_sanitizer_block(f.read())
    return set(re.findall(r"^\s*add_(?:executable|library)\(([\w-]+)", cmake, re.M))


# The words of the nightly's apt-get install command, its continued lines too.
def nightly_packages():
    with open(os.path.join(WORKFLOWS, "geocoder-build.yml")) as f:
        install = re.search(r"apt-get install((?:.*\\\n)*.*)", f.read())
    return set(install.group(1).replace("\\\n", " ").split())


# The steps of geocoder-tests.yml that run geocoder-tests, each (job, step
# name) with the variables its env sets.
def unit_test_steps():
    with open(os.path.join(WORKFLOWS, "geocoder-tests.yml")) as f:
        jobs = f.read().split("\njobs:\n", 1)[1]
    steps = {}
    for job in re.split(r"^(?=  [\w-]+:\n)", jobs, flags=re.M):
        name = re.match(r"  ([\w-]+):\n", job)
        for step in re.split(r"^(?=      - )", job, flags=re.M):
            if not name or "./geocoder-tests" not in step:
                continue
            env = re.search(r"^        env:\n((?:          .*\n)*)", step, re.M)
            steps[name[1], re.search(r"- name: (.+)", step)[1]] = dict(
                re.findall(r'^          (\w+): "([^"]*)"$', env[1] if env else "", re.M))
    return steps


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

    # ci_build_tools.sh hands make every cached tool by name, so one the build
    # has no target for fails the night's compile, which PR CI, building its
    # own targets, wouldn't show.
    def test_the_build_makes_every_cached_tool(self):
        self.assertEqual(cached_tools() - targets_the_build_makes(), set())

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


class Past4GibCasesTest(unittest.TestCase):
    VARIABLE = "GEOCODER_TESTS_PAST_4GIB"

    # The cases that put more than 4 GiB of address footprints through the
    # writer and the patcher run once a pull request, in the Release unit
    # tests of the default build.
    def test_the_pull_requests_release_unit_tests_run_the_cases_past_4_gib(self):
        steps = unit_test_steps()

        self.assertEqual(len(steps), 3)
        self.assertEqual({step for step, env in steps.items() if env.get(self.VARIABLE) == "1"},
                         {("builder-tests", "Build + run tests")})
        self.assertEqual({step for step, env in steps.items() if self.VARIABLE in env},
                         {("builder-tests", "Build + run tests")})

    # The nightly's unit tests gate the night on its runner, on cached tools
    # too: a case that is slow or fails there holds up the planet build.
    def test_the_nightly_runs_none_of_them(self):
        for path in (os.path.join(HERE, "ci_build_tools.sh"), os.path.join(WORKFLOWS, "geocoder-build.yml")):
            with open(path) as f:
                self.assertNotIn(self.VARIABLE, f.read(), path)


if __name__ == "__main__":
    unittest.main()
