#!/usr/bin/env python3
"""Tests for job_sampler.sh, the nightly's CPU and disk samples over the whole job, and how the nightly runs it.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import re
import shutil
import subprocess
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLER = os.path.join(HERE, "job_sampler.sh")
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")

PROC_STAT = "cpu  1000 20 300 40000 50 0 6 0 0 0\ncpu0 500 10 150 20000 25 0 3 0 0 0\nintr 1 2 3\n"
RBD_STAT = "  123 0 4567 89 1011 0 1213 14 0 15 16 0 0 0 0 0 0\n"


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


class JobSamplerTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        # A job in cgroup /pod/job: its memory limit there, its CPU quota a level up.
        write(os.path.join(self.root, "proc/self/cgroup"), "0::/pod/job\n")
        write(os.path.join(self.root, "proc/stat"), PROC_STAT)
        cgroup = os.path.join(self.root, "sys/fs/cgroup")
        write(os.path.join(cgroup, "cpu.max"), "max 100000\n")
        write(os.path.join(cgroup, "pod/cpu.max"), "6400000 100000\n")
        write(os.path.join(cgroup, "pod/memory.max"), "max\n")
        write(os.path.join(cgroup, "pod/job/cpu.max"), "max 100000\n")
        write(os.path.join(cgroup, "pod/job/memory.max"), str(96 << 30) + "\n")
        write(os.path.join(cgroup, "pod/job/memory.current"), str(5 << 30) + "\n")
        write(os.path.join(cgroup, "pod/job/cpu.stat"), "usage_usec 777000\nuser_usec 700000\nsystem_usec 77000\n")
        write(os.path.join(self.root, "sys/class/block/rbd4/stat"), RBD_STAT)
        # df names the work dir's device.
        self.bin = os.path.join(self.root, "bin")
        write(os.path.join(self.bin, "df"), "#!/bin/sh\necho Filesystem\necho /dev/rbd4\n")
        os.chmod(os.path.join(self.bin, "df"), 0o755)
        self.log = os.path.join(self.root, "jobstat.log")

    def sample(self, seconds=2.5):
        job = subprocess.Popen(["bash", SAMPLER, self.log, self.root, "1"],
                               env=dict(os.environ, JOB_SAMPLER_ROOT=self.root,
                                        PATH=f"{self.bin}:{os.environ['PATH']}"))
        time.sleep(seconds)
        job.kill()
        job.wait()
        with open(self.log) as f:
            return f.read().splitlines()

    def test_the_first_line_gives_the_cpus_and_the_jobs_cpu_quota(self):
        lines = self.sample()

        self.assertRegex(lines[0], r"^\[jobstat \d\d:\d\d:\d\d\] cpus=\d+ cgroup=/pod/job "
                                   r"cpu\.max=6400000 100000 \(/pod\) device=rbd4$")

    def test_each_sample_holds_the_raw_counters_of_cpu_memory_and_the_work_volume(self):
        lines = self.sample()

        samples = [line for line in lines[1:]]
        self.assertGreaterEqual(len(samples), 2, lines)
        for line in samples:
            self.assertRegex(line, r"^\[jobstat \d\d:\d\d:\d\d\] ")
            fields = dict(re.findall(r"(\w+)=(\S+(?: \S+)*?)(?= \w+=|$)", line.split("] ", 1)[1]))
            self.assertEqual(fields["usage_usec"], "777000")
            self.assertEqual(fields["memory"], str(5 << 30))
            self.assertEqual(fields["cpu"], "1000 20 300 40000 50 0 6 0 0 0")
            self.assertEqual(fields["rbd4"], " ".join(RBD_STAT.split()))

    def test_the_counters_are_read_again_for_each_sample(self):
        job = subprocess.Popen(["bash", SAMPLER, self.log, self.root, "1"],
                               env=dict(os.environ, JOB_SAMPLER_ROOT=self.root,
                                        PATH=f"{self.bin}:{os.environ['PATH']}"))
        time.sleep(1.5)
        write(os.path.join(self.root, "sys/fs/cgroup/pod/job/cpu.stat"), "usage_usec 999000\n")
        time.sleep(1.5)
        job.kill()
        job.wait()
        with open(self.log) as f:
            usage = re.findall(r"usage_usec=(\d+)", f.read())
        self.assertEqual(usage[0], "777000")
        self.assertEqual(usage[-1], "999000")


def step(name):
    with open(NIGHTLY) as f:
        text = f.read()
    block = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", text, re.M)
    return block.group(1) if block else None


def step_names():
    with open(NIGHTLY) as f:
        return re.findall(r"^      - (?:name: (.+)|uses: (\S+).*)$", f.read(), re.M)


class NightlySamplerTest(unittest.TestCase):
    # From the job's start, before apt, to its end, every 30 s, detached so it outlives its step; its log goes
    # with the run's other logs.
    def test_the_nightly_samples_the_whole_job_every_30_s(self):
        names = [name or uses for name, uses in step_names()]
        self.assertEqual(names[:2], ["actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803", "Start the job sampler"])
        script = step("Start the job sampler")
        self.assertIn('setsid nohup "$GITHUB_WORKSPACE/geocoder/builder/tools/job_sampler.sh" "$RUNNER_TEMP/jobstat.log" '
                      '"$RUNNER_TEMP" 30', script)
        logs = step("Upload job logs")
        self.assertIsNotNone(logs)
        self.assertIn("${{ runner.temp }}/jobstat.log", logs)
        self.assertIn("if: always()", logs)


if __name__ == "__main__":
    unittest.main()
