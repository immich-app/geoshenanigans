#!/usr/bin/env python3
"""Tests for ci_build_tools.sh: its tools cache, its sharded unit tests, and the packages the nightly builds the
tools with.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
WORKFLOWS = os.path.join(HERE, "..", "..", "..", ".github", "workflows")
sys.path.insert(0, HERE)
from test_cpu_budget import fake_cpus  # noqa: E402

# geocoder-tests as the script runs it: a registry of $FAKE_TESTS tests (t0, t1, ...), --count printing
# $FAKE_COUNT (default: its size), GEOCODER_TEST_SHARD=i/n running those whose index is i mod n, as
# tests/test_main.cpp does, but every test when the shard is $FAKE_IGNORES_SHARD. The tests named in $FAKE_FAILS
# fail. Each run appends its shard to $FAKE_SHARD_LOG.
FAKE_TESTS = r'''#!{python}
import os, sys
n = int(os.environ["FAKE_TESTS"])
if sys.argv[1:] == ["--count"]:
    print(os.environ.get("FAKE_COUNT", n))
    sys.exit(0)
shard = os.environ.get("GEOCODER_TEST_SHARD", "0/1")
with open(os.environ["FAKE_SHARD_LOG"], "a") as f:
    f.write(shard + "\n")
i, count = map(int, shard.split("/"))
ran = [t for t in range(n) if t % count == i or shard == os.environ.get("FAKE_IGNORES_SHARD")]
fails = [t for t in ran if f"t{{t}}" in os.environ.get("FAKE_FAILS", "").split()]
for t in fails:
    print(f"    FAIL test_x.cpp:1  CHECK(t{{t}})")
    print(f"FAILED  t{{t}}")
print(f"\n{{len(ran) - len(fails)}}/{{len(ran)}} tests passed  ({{len(ran)}} checks, {{len(fails)}} failures)")
sys.exit(1 if fails else 0)
'''


class ShardedTestsRun:
    """One run of ci_build_tools.sh on cached tools whose geocoder-tests is the fake above, on `cpus` CPUs under
    the cgroup quotas `quotas` (test_cpu_budget.fake_cpus). A compile (the cached tools failing their tests) runs
    a cmake that only records it ran and fails."""

    def __init__(self, test, tests, cpus=88, quotas=None, **env):
        root = tempfile.mkdtemp()
        test.addCleanup(shutil.rmtree, root)
        cache, bin_dir = os.path.join(root, "cache"), os.path.join(root, "bin")
        os.makedirs(os.path.join(cache, "bin"))
        os.makedirs(bin_dir)
        for tool in cached_tools():
            with open(os.path.join(cache, "bin", tool), "w") as f:
                f.write(FAKE_TESTS.format(python=sys.executable) if tool == "geocoder-tests" else "#!/bin/sh\n")
            os.chmod(os.path.join(cache, "bin", tool), 0o755)
        with open(os.path.join(cache, "SHA256SUMS"), "w") as f:
            for tool in sorted(cached_tools()):
                with open(os.path.join(cache, "bin", tool), "rb") as b:
                    f.write(f"{hashlib.sha256(b.read()).hexdigest()}  bin/{tool}\n")
        self.compiles = os.path.join(root, "compiles")
        with open(os.path.join(bin_dir, "cmake"), "w") as f:
            f.write(f"#!/bin/sh\necho cmake >> {self.compiles}\nexit 1\n")
        os.chmod(os.path.join(bin_dir, "cmake"), 0o755)
        self.shard_log = os.path.join(root, "shards")
        budget = fake_cpus(test, cpus, "/job", quotas)
        run_env = dict(os.environ, CPU_BUDGET_ROOT=budget["CPU_BUDGET_ROOT"], PATH=f"{bin_dir}:{budget['PATH']}",
                       TMPDIR=root, FAKE_TESTS=str(tests), FAKE_SHARD_LOG=self.shard_log)
        run_env.update({k: str(v) for k, v in env.items()})
        done = subprocess.run(["bash", os.path.join(HERE, "ci_build_tools.sh"), os.path.join(root, "build"), cache,
                               os.path.join(root, "s2")], env=run_env, capture_output=True, text=True, timeout=120)
        self.status, self.out = done.returncode, done.stdout + done.stderr

    def shards(self):
        with open(self.shard_log) as f:
            return f.read().split()

    def compiled(self):
        """The fresh builds' cmake calls, a line each: none when the cached tools passed their tests."""
        if not os.path.exists(self.compiles):
            return []
        with open(self.compiles) as f:
            return f.read().splitlines()


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


# The words of the apt-get install command of the geocoder-tests.yml job that runs these tests.
def tool_test_packages():
    with open(os.path.join(WORKFLOWS, "geocoder-tests.yml")) as f:
        jobs = f.read().split("\njobs:\n", 1)[1]
    [job] = [job for job in re.split(r"^(?=  [\w-]+:\n)", jobs, flags=re.M) if "unittest discover" in job]
    install = re.search(r"apt-get install((?:.*\\\n)*.*)", job)
    return set(install.group(1).replace("\\\n", " ").split())


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


class ShardedUnitTestsTest(unittest.TestCase):
    SHARDS = [f"{i}/16" for i in range(16)]

    def test_the_unit_tests_run_as_sixteen_shards_whose_total_is_the_registrys(self):
        run = ShardedTestsRun(self, 100)

        self.assertEqual(run.status, 0, run.out)
        self.assertEqual(sorted(run.shards()), sorted(self.SHARDS))
        self.assertEqual(run.out.strip().splitlines()[-1], "100/100 tests passed")
        self.assertFalse(run.compiled())

    # Fewer CPUs in the budget than 16 (a cgroup's quota, or the CPUs the process may run on): a shard a CPU.
    def test_a_budget_of_fewer_than_sixteen_cpus_runs_a_shard_a_cpu(self):
        for cpus, quotas in ((88, {"/job": "400000 100000"}), (4, None)):
            run = ShardedTestsRun(self, 100, cpus, quotas)

            self.assertEqual(run.status, 0, run.out)
            self.assertEqual(sorted(run.shards()), [f"{i}/4" for i in range(4)])
            self.assertIn("unit tests: 100 in 4 shards", run.out)
            self.assertEqual(run.out.strip().splitlines()[-1], "100/100 tests passed")

    def test_a_failing_test_fails_its_shard_by_name_and_the_cached_tools_get_one_fresh_build(self):
        run = ShardedTestsRun(self, 100, FAKE_FAILS="t37")

        self.assertNotEqual(run.status, 0)
        self.assertIn("FAILED  t37", run.out)
        self.assertIn("Unit tests failed on the cached tools; building them afresh", run.out)
        self.assertEqual(run.compiled(), ["cmake"])

    # A shard index past the count, or a test_main that ignored the shard, would pass on fewer tests than the
    # registry holds: the run counts what each shard ran.
    def test_a_shard_that_runs_no_test_fails_the_run(self):
        run = ShardedTestsRun(self, 10)

        self.assertNotEqual(run.status, 0)
        self.assertRegex(run.out, r"shard 1[0-5]/16 ran no test")
        self.assertNotIn("10/10 tests passed", run.out)

    def test_shards_that_run_more_or_fewer_tests_than_the_registry_holds_fail_the_run(self):
        for env, ran in ((dict(FAKE_IGNORES_SHARD="3/16"), 193), (dict(FAKE_COUNT=101), 100)):
            run = ShardedTestsRun(self, 100, **env)

            self.assertNotEqual(run.status, 0, env)
            self.assertIn(f"the shards ran {ran} tests; the registry holds", run.out, env)
            self.assertNotRegex(run.out, r"(?m)^\d+/\d+ tests passed$", env)

    def test_each_phase_is_logged_with_its_time(self):
        run = ShardedTestsRun(self, 100)

        phases = re.findall(r"(?m)^\[tools \d\d:\d\d:\d\d\] (.*)$", run.out)
        self.assertEqual(len(phases), 3, run.out)
        self.assertEqual(phases[:2], ["Tools from the cache", "unit tests: 100 in 16 shards"])
        self.assertRegex(phases[2], r"^unit tests done in \d+s$")

    # The nightly greps the tools' log for the lines above: the phases, the total and a failure's lines.
    def test_the_nightly_shows_the_phases_the_total_and_a_failures_lines(self):
        with open(os.path.join(WORKFLOWS, "geocoder-build.yml")) as f:
            text = f.read()
        shown = re.search(r'grep -E "([^"]*)" "\$RUNNER_TEMP/tools\.log"$', text, re.M)
        self.assertIsNotNone(shown)
        for line in ("[tools 06:30:53] Tools from the cache", "[tools 06:30:53] unit tests: 1116 in 16 shards",
                     "Unit tests failed on the cached tools; building them afresh", "1116/1116 tests passed"):
            self.assertRegex(line, shown.group(1))

    # The tests' scratch (mkdtemp under TMPDIR) goes on the runner's work volume, not the container's /tmp, which
    # the tools job would otherwise use: it starts before Setup sets TMPDIR.
    def test_the_nightly_gives_the_tools_job_its_own_scratch_dir(self):
        with open(os.path.join(WORKFLOWS, "geocoder-build.yml")) as f:
            step = re.search(r"^      - name: Build tools\n((?: {8}.*\n|\n)*)", f.read(), re.M).group(1)
        self.assertIn('TMPDIR="$RUNNER_TEMP/tools-tmp"', step)
        self.assertIn('mkdir -p "$RUNNER_TEMP/tools-tmp"', step)


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


class ToolTestsJobTest(unittest.TestCase):
    # test_nightly_steps.py runs the watchdog's retry and report scripts and
    # reads its titles, and the nightly never runs these tests: a pull request
    # changing only the watchdog must.
    def test_a_change_to_the_watchdog_runs_these_tests(self):
        self.assertIn(".github/workflows/geocoder-build-watchdog.yml", paths_that_run_the_tests())

    # The tests that read the workflows as GitHub does parse them with PyYAML,
    # which the standard library lacks: the job installs it for the system
    # python3 that runs them, rather than count on the runner image.
    def test_the_job_that_runs_these_tests_installs_pyyaml(self):
        self.assertIn("python3-yaml", tool_test_packages())


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
