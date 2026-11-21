#!/usr/bin/env python3
"""Tests for cpu_budget.sh, the CPUs the nightly's tools size their threads and jobs by, and for the tools that do.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
BUDGET = os.path.join(HERE, "cpu_budget.sh")
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def fake_host(test, cpus, cgroups, files):
    """What a process's env sets for cpu_budget.sh to see it may run on `cpus` CPUs (nproc says so), with the lines
    `cgroups` in /proc/self/cgroup (None: no such file) and each of `files` (a path under /, to its line):
    CPU_BUDGET_ROOT, a dir of those files, and PATH, nproc first."""
    root = tempfile.mkdtemp()
    test.addCleanup(shutil.rmtree, root)
    if cgroups is not None:
        write(os.path.join(root, "proc/self/cgroup"), cgroups)
    for path, line in files.items():
        write(os.path.join(root, path), line + "\n")
    write(os.path.join(root, "bin/nproc"), f"#!/bin/sh\necho {cpus}\n")
    os.chmod(os.path.join(root, "bin/nproc"), 0o755)
    return {"CPU_BUDGET_ROOT": root, "PATH": f"{root}/bin:{os.environ['PATH']}"}


def fake_cpus(test, cpus, cgroup="/", quotas=None):
    """fake_host in cgroup v2's `cgroup` (None: no /proc/self/cgroup), with the cpu.max file of each cgroup `quotas`
    names (a path under /sys/fs/cgroup, "" for its root, to the file's line)."""
    return fake_host(test, cpus, None if cgroup is None else f"0::{cgroup}\n",
                     {f"sys/fs/cgroup{at}/cpu.max": line for at, line in (quotas or {}).items()})


def fake_v1_cpus(test, cpus, cgroups, quotas, controllers="cpu,cpuacct"):
    """fake_host with the lines `cgroups` in /proc/self/cgroup, under a cgroup v1 hierarchy mounted at
    /sys/fs/cgroup/<controllers>: the cpu.cfs_quota_us and cpu.cfs_period_us of each cgroup `quotas` names (a path
    under that dir, "" for its root, to the two files' values)."""
    files = {}
    for at, (quota, period) in quotas.items():
        files[f"sys/fs/cgroup/{controllers}{at}/cpu.cfs_quota_us"] = quota
        files[f"sys/fs/cgroup/{controllers}{at}/cpu.cfs_period_us"] = period
    return fake_host(test, cpus, cgroups, files)


def budget(env):
    done = subprocess.run(["bash", BUDGET], env=dict(os.environ, **env), capture_output=True, text=True, timeout=30)
    assert done.returncode == 0 and not done.stderr, done
    return int(done.stdout)


class CpuBudgetTest(unittest.TestCase):
    # The runner: 88 CPUs, a quota of 64 on the job's cgroup, the root of its namespace.
    def test_a_quota_under_the_cpu_count_is_the_budget(self):
        self.assertEqual(budget(fake_cpus(self, 88, "/", {"": "6400000 100000"})), 64)

    def test_without_a_quota_the_budget_is_the_cpus_the_process_may_run_on(self):
        self.assertEqual(budget(fake_cpus(self, 88, "/pod/job", {"": "max 100000", "/pod": "max 100000",
                                                                  "/pod/job": "max 100000"})), 88)
        self.assertEqual(budget(fake_cpus(self, 12, "/pod/job")), 12)
        self.assertEqual(budget(fake_cpus(self, 12, None)), 12)

    def test_the_tightest_quota_from_the_cgroup_up_holds(self):
        for quotas, expected in (({"/pod": "6400000 100000", "/pod/job": "1600000 100000"}, 16),
                                 ({"/pod": "800000 100000", "/pod/job": "3200000 100000"}, 8),
                                 ({"": "400000 100000", "/pod/job": "max 100000"}, 4)):
            self.assertEqual(budget(fake_cpus(self, 88, "/pod/job", quotas)), expected, quotas)

    def test_a_quota_over_the_cpu_count_leaves_the_cpu_count(self):
        self.assertEqual(budget(fake_cpus(self, 24, "/", {"": "6400000 100000"})), 24)

    def test_a_quota_counts_whole_cpus_and_at_least_one(self):
        for line, expected in (("250000 100000", 2), ("150000 100000", 1), ("50000 100000", 1), ("4000 1000", 4)):
            self.assertEqual(budget(fake_cpus(self, 88, "/", {"": line})), expected, line)

    def test_a_cpu_max_it_cannot_read_as_a_quota_is_left_out(self):
        for line in ("", "max", "64 cpus", "6400000 0", "-1 100000"):
            self.assertEqual(budget(fake_cpus(self, 88, "/job", {"": line, "/job": "3200000 100000"})), 32, line)

    # A host on cgroup v1, or a hybrid one, whose v2 hierarchy has no controllers: the quota is the CPU controller's,
    # cpu.cfs_quota_us over cpu.cfs_period_us, the tightest from the cgroup up, and -1 is none.
    def test_a_cgroup_v1_quota_under_the_cpu_count_is_the_budget(self):
        hybrid = "12:pids:/pod/job\n4:cpu,cpuacct:/pod/job\n1:name=systemd:/pod/job\n0::/pod/job\n"
        self.assertEqual(budget(fake_v1_cpus(self, 88, hybrid, {"/pod/job": ("6400000", "100000")})), 64)
        self.assertEqual(budget(fake_v1_cpus(self, 88, "3:cpu:/job\n", {"/job": ("200000", "100000")},
                                             controllers="cpu")), 2)

    def test_the_tightest_cgroup_v1_quota_from_the_cgroup_up_holds(self):
        for quotas, expected in (({"/pod": ("800000", "100000"), "/pod/job": ("-1", "100000")}, 8),
                                 ({"": ("400000", "100000"), "/pod": ("-1", "100000"),
                                   "/pod/job": ("1600000", "100000")}, 4),
                                 ({"": ("-1", "100000"), "/pod": ("-1", "100000"), "/pod/job": ("-1", "100000")}, 88)):
            self.assertEqual(budget(fake_v1_cpus(self, 88, "4:cpu,cpuacct:/pod/job\n", quotas)), expected, quotas)

    # A container without a cgroup namespace: /proc/self/cgroup names its cgroup on the host, and the hierarchy's
    # mount is that cgroup, its quota at the mount's root.
    def test_a_containers_cgroup_v1_quota_at_its_mounts_root_holds(self):
        self.assertEqual(budget(fake_v1_cpus(self, 88, "4:cpu,cpuacct:/docker/abc\n", {"": ("400000", "100000")})), 4)

    def test_a_cgroup_v1_quota_counts_whole_cpus_and_at_least_one(self):
        for quota, expected in (("250000", 2), ("50000", 1)):
            self.assertEqual(budget(fake_v1_cpus(self, 88, "4:cpu,cpuacct:/\n", {"": (quota, "100000")})), expected,
                             quota)

    def test_a_cgroup_v1_hierarchy_without_the_cpu_controller_sets_no_quota(self):
        self.assertEqual(budget(fake_v1_cpus(self, 88, "5:cpuset:/job\n6:cpuacct:/job\n", {"/job": ("200000", "100000")},
                                             controllers="cpuset")), 88)

    # This host's own /proc and /sys, quota or none.
    def test_on_this_host_the_budget_is_a_count_of_cpus(self):
        self.assertGreaterEqual(budget({}), 1)


# The tools the nightly sizes its threads and jobs in, and what in them may count CPUs without the budget:
# job_sampler.sh reports the count beside the quota, and build_s2.sh and build_zstd.sh compile their libraries
# only when their caches, keyed on the scripts' bytes, miss.
COUNTS_CPUS = re.compile(r"\bnproc\b|cpu_count\(|sched_getaffinity\(")
MAY_COUNT = {"cpu_budget.sh", "job_sampler.sh", "build_s2.sh", "build_zstd.sh"}


class BudgetUsersTest(unittest.TestCase):
    def test_the_nightlys_tools_size_their_threads_by_the_budget(self):
        counting = []
        for name in sorted(os.listdir(HERE)):
            if name.startswith("test_") or not name.endswith((".sh", ".py")) or name in MAY_COUNT:
                continue
            with open(os.path.join(HERE, name)) as f:
                for n, line in enumerate(f, 1):
                    if COUNTS_CPUS.search(line.split("#")[0]):
                        counting.append(f"{name}:{n}: {line.strip()}")
        with open(NIGHTLY) as f:
            for n, line in enumerate(f, 1):
                if COUNTS_CPUS.search(line.split("#")[0]):
                    counting.append(f"geocoder-build.yml:{n}: {line.strip()}")
        self.assertEqual(counting, [])

    def test_the_tools_that_size_by_it_name_it(self):
        for name in ("compress_index.py", "build_search_indexes.py", "generate_patches.sh", "checksums.sh",
                     "ci_build_tools.sh"):
            with open(os.path.join(HERE, name)) as f:
                self.assertIn("cpu_budget.sh", f.read(), name)


if __name__ == "__main__":
    unittest.main()
