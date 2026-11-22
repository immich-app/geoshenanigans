#!/usr/bin/env python3
"""Tests for job_sampler.sh, the nightly's CPU and disk samples over the whole job, and how the nightly runs it.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import contextlib
import ctypes
import io
import mmap
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import page_cache  # noqa: E402

SAMPLER = os.path.join(HERE, "job_sampler.sh")
PAGE_CACHE = os.path.join(HERE, "page_cache.py")
NIGHTLY = os.path.join(HERE, "..", "..", "..", ".github", "workflows", "geocoder-build.yml")

PROC_STAT = "cpu  1000 20 300 40000 50 0 6 0 0 0\ncpu0 500 10 150 20000 25 0 3 0 0 0\nintr 1 2 3\n"
RBD_STAT = "  123 0 4567 89 1011 0 1213 14 0 15 16 0 0 0 0 0 0\n"
MEMORY_STAT = ("anon 4096\nfile 81920\nkernel 77\nfile_mapped 5\nfile_dirty 300\nfile_writeback 200\nactive_file 9\n"
               "pgscan 1000\npgsteal 900\nworkingset_refault_file 12\n")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


LIBC = ctypes.CDLL(None, use_errno=True)
LIBC.mmap.restype = ctypes.c_void_p
LIBC.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
LIBC.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
LIBC.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]


def resident_pages(path):
    """How many of the pages of the file at `path` are in the page cache (mincore over a mapping of it)."""
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        addr = LIBC.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, f.fileno(), 0)
        if addr in (None, ctypes.c_void_p(-1).value):
            raise OSError(ctypes.get_errno(), f"mmap {path}")
        try:
            vec = (ctypes.c_ubyte * ((size + mmap.PAGESIZE - 1) // mmap.PAGESIZE))()
            if LIBC.mincore(addr, size, vec) != 0:
                raise OSError(ctypes.get_errno(), f"mincore {path}")
        finally:
            LIBC.munmap(addr, size)
    return sum(page & 1 for page in vec)


def keeps_dropped_pages(path):
    """Whether the file system at `path` keeps a file's pages in memory whatever is dropped (tmpfs and ramfs, whose
    page cache is the file, or an overlay on one): a probe file there, synced and dropped, keeps a page cached."""
    probe = os.path.join(path, "drop-probe")
    with open(probe, "wb") as f:
        f.write(b"p" * (16 * mmap.PAGESIZE))
        f.flush()
        os.fsync(f.fileno())
        os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    try:
        return resident_pages(probe) != 0
    finally:
        os.remove(probe)


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
        write(os.path.join(cgroup, "pod/job/memory.stat"), MEMORY_STAT)
        write(os.path.join(self.root, "sys/class/block/rbd4/stat"), RBD_STAT)
        # df names the work dir's device.
        self.bin = os.path.join(self.root, "bin")
        write(os.path.join(self.bin, "df"), "#!/bin/sh\necho Filesystem\necho /dev/rbd4\n")
        os.chmod(os.path.join(self.bin, "df"), 0o755)
        self.log = os.path.join(self.root, "jobstat.log")

    def start(self, files=()):
        """The sampler, a sample a second, stopped when the test ends."""
        job = subprocess.Popen(["bash", SAMPLER, self.log, self.root, "1", *files],
                               env=dict(os.environ, JOB_SAMPLER_ROOT=self.root,
                                        PATH=f"{self.bin}:{os.environ['PATH']}"))
        self.addCleanup(job.wait)
        self.addCleanup(job.kill)
        return job

    def lines_once(self, done, seconds=30):
        """The log's whole lines once `done` holds of them (at most `seconds`)."""
        deadline = time.time() + seconds
        lines = []
        while time.time() < deadline:
            if os.path.exists(self.log):
                with open(self.log) as f:
                    text = f.read()
                lines = text[:text.rfind("\n") + 1].splitlines()
                if done(lines):
                    return lines
            time.sleep(0.05)
        self.fail(f"the sampler's log never got there: {lines}")

    def sample(self, lines=3, files=()):
        """The log's first `lines` lines: the header and samples."""
        job = self.start(files)
        try:
            return self.lines_once(lambda got: len(got) >= lines)[:lines]
        finally:
            job.kill()
            job.wait()

    def test_the_first_line_gives_the_cpus_and_the_jobs_cpu_quota(self):
        lines = self.sample()

        self.assertRegex(lines[0], r"^\[jobstat \d\d:\d\d:\d\d\] cpus=\d+ cgroup=/pod/job "
                                   r"cpu\.max=6400000 100000 \(/pod\) device=rbd4$")

    # The quota is the tightest from this process's cgroup up, as cpu_budget.sh takes it, the tools sizing by it: one
    # below the job's memory-limited cgroup, or one tighter above the first that is set, quota over period.
    def test_the_quota_is_the_tightest_from_the_process_up(self):
        cgroup = os.path.join(self.root, "sys/fs/cgroup")
        for at, quotas, expected in (
                ("/pod/job/step", {"pod/job/step": "200000 50000"}, r"200000 50000 \(/pod/job/step\)"),
                ("/pod/job", {"pod/job": "6400000 100000", "pod": "300000 100000", "": "800000 100000"},
                 r"300000 100000 \(/pod\)"),
                ("/pod/job", {"pod/job": "450000 150000", "pod": "400000 100000"}, r"450000 150000 \(/pod/job\)")):
            with self.subTest(at=at, quotas=quotas):
                write(os.path.join(self.root, "proc/self/cgroup"), f"0::{at}\n")
                for path in ("", "pod", "pod/job", "pod/job/step"):
                    write(os.path.join(cgroup, path, "cpu.max"), quotas.get(path, "max 100000") + "\n")
                if os.path.exists(self.log):
                    os.remove(self.log)

                lines = self.sample(lines=1)

                self.assertRegex(lines[0], rf" cgroup=/pod/job cpu\.max={expected} device=rbd4$")

    # In a container without a cgroup namespace /proc/self/cgroup names the cgroup on the host, of which the mount
    # holds a subtree (the 4th field of its /proc/self/mountinfo line): the path goes from the mount's root.
    def test_the_cgroup_path_goes_from_its_mounts_root(self):
        write(os.path.join(self.root, "proc/self/cgroup"), "0::/kubepods/pod/job\n")
        write(os.path.join(self.root, "proc/self/mountinfo"),
              "30 25 0:26 /kubepods /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw\n")

        lines = self.sample(lines=2)

        self.assertRegex(lines[0], r" cgroup=/pod/job cpu\.max=6400000 100000 \(/pod\) device=rbd4$")
        self.assertIn(" usage_usec=777000 ", lines[1])

    def test_each_sample_holds_the_raw_counters_of_cpu_memory_and_the_work_volume(self):
        lines = self.sample()

        samples = [line for line in lines[1:]]
        self.assertEqual(len(samples), 2, lines)
        for line in samples:
            self.assertRegex(line, r"^\[jobstat \d\d:\d\d:\d\d\] ")
            fields = dict(re.findall(r"(\w+)=(\S+(?: \S+)*?)(?= \w+=|$)", line.split("] ", 1)[1]))
            self.assertEqual(fields["usage_usec"], "777000")
            self.assertEqual(fields["memory"], str(5 << 30))
            self.assertEqual(fields["cpu"], "1000 20 300 40000 50 0 6 0 0 0")
            self.assertEqual(fields["rbd4"], " ".join(RBD_STAT.split()))
            # The page cache and anonymous memory, their dirty and writeback parts, and the reclaim counters.
            self.assertEqual({k: fields[k] for k in ("file", "anon", "file_dirty", "file_writeback", "pgscan", "pgsteal",
                                                     "workingset_refault_file")},
                             {"file": "81920", "anon": "4096", "file_dirty": "300", "file_writeback": "200",
                              "pgscan": "1000", "pgsteal": "900", "workingset_refault_file": "12"})
            self.assertNotIn("kernel", fields)
            self.assertNotIn("cached", fields)

    # The PBFs pbf-apply reads and writes, as each exists: how much of each is in the page cache.
    def test_each_sample_holds_the_page_cache_bytes_of_the_files_that_exist(self):
        files = [os.path.join(self.root, name) for name in ("planet.osm.pbf", "planet-tmp.osm.pbf")]
        write(files[0], "x" * 10000)

        lines = self.sample(files=files)

        self.assertEqual(len(lines), 3, lines)
        for line in lines[1:]:
            cached = re.search(r" cached=(\S+)$", line)
            self.assertIsNotNone(cached, line)
            self.assertRegex(cached.group(1), r"^planet\.osm\.pbf:\d+$")
            self.assertLessEqual(int(cached.group(1).split(":")[1]), 10000)

    # A dm or LVM volume's source, /dev/mapper/<name>, is a link to its kernel name (dm-N), which /sys/class/block
    # knows it by.
    def test_a_mapped_volumes_counters_are_its_kernel_names(self):
        write(os.path.join(self.bin, "df"), "#!/bin/sh\necho Filesystem\necho /dev/mapper/work\n")
        write(os.path.join(self.root, "dev/dm-3"), "")
        os.makedirs(os.path.join(self.root, "dev/mapper"))
        os.symlink("../dm-3", os.path.join(self.root, "dev/mapper/work"))
        write(os.path.join(self.root, "sys/class/block/dm-3/stat"), RBD_STAT)

        lines = self.sample(lines=2)

        self.assertTrue(lines[0].endswith(" device=dm-3"), lines[0])
        self.assertIn(f" dm-3={' '.join(RBD_STAT.split())}", lines[1])

    def test_the_counters_are_read_again_for_each_sample(self):
        def usage(lines):
            return re.findall(r"usage_usec=(\d+)", "\n".join(lines))

        self.start()
        self.lines_once(lambda lines: usage(lines))
        stat = os.path.join(self.root, "sys/fs/cgroup/pod/job/cpu.stat")
        write(stat + ".part", "usage_usec 999000\n")
        os.replace(stat + ".part", stat)
        lines = self.lines_once(lambda lines: "999000" in usage(lines))

        self.assertEqual(usage(lines)[0], "777000")
        self.assertEqual(usage(lines)[-1], "999000")


def step(name):
    with open(NIGHTLY) as f:
        text = f.read()
    block = re.search(rf"^      - name: {re.escape(name)}\n((?:(?: {{8}}.*)?\n)*)", text, re.M)
    return block.group(1) if block else None


def step_names():
    with open(NIGHTLY) as f:
        return re.findall(r"^      - (?:name: (.+)|uses: (\S+).*)$", f.read(), re.M)


class PageCacheTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def page_cache(self, *paths):
        done = subprocess.run([sys.executable, PAGE_CACHE, *paths], capture_output=True, text=True, timeout=30)
        self.assertEqual(done.returncode, 0, done.stderr)
        return done.stdout.strip()

    def test_a_file_is_cached_once_read_and_not_once_dropped(self):
        if keeps_dropped_pages(self.dir):
            self.skipTest("the page cache keeps what is dropped here")
        path = os.path.join(self.dir, "planet.osm.pbf")
        size = 5 * (1 << 20) + 123
        write(path, "y" * size)
        with open(path, "rb") as f:
            os.fsync(f.fileno())
            os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        self.assertEqual(self.page_cache(path), "planet.osm.pbf:0")
        with open(path, "rb") as f:
            f.read()

        self.assertEqual(self.page_cache(path), f"planet.osm.pbf:{size}")

    # A last page short of the file's end counts the file's bytes in it alone, whatever else is cached.
    def test_a_cached_last_page_counts_only_the_files_bytes_in_it(self):
        if keeps_dropped_pages(self.dir):
            self.skipTest("the page cache keeps what is dropped here")
        path = os.path.join(self.dir, "planet.osm.pbf")
        write(path, "y" * (mmap.PAGESIZE + 1))
        with open(path, "rb") as f:
            os.fsync(f.fileno())
            os.posix_fadvise(f.fileno(), 0, mmap.PAGESIZE, os.POSIX_FADV_DONTNEED)

        self.assertEqual(self.page_cache(path), "planet.osm.pbf:1")

    # Linux reports every page of a file's mapping cached unless the caller owns the file or could open it for
    # writing: such a file prints "?" rather than a count it can't know.
    def test_a_file_mincore_cannot_see_into_prints_a_question_mark(self):
        path = os.path.join(self.dir, "planet.osm.pbf")
        write(path, "y" * 10000)
        printed = io.StringIO()
        with mock.patch.object(os, "geteuid", return_value=os.stat(path).st_uid + 1), \
                mock.patch.object(os, "access", return_value=False), \
                mock.patch.object(sys, "argv", ["page_cache.py", path]), contextlib.redirect_stdout(printed):
            page_cache.main()

        self.assertEqual(printed.getvalue(), "planet.osm.pbf:?\n")

    def test_files_that_do_not_exist_are_left_out(self):
        path = os.path.join(self.dir, "a.pbf")
        write(path, "")

        self.assertEqual(self.page_cache(os.path.join(self.dir, "none.pbf"), path), "a.pbf:0")
        self.assertEqual(self.page_cache(os.path.join(self.dir, "none.pbf")), "")


class NightlySamplerTest(unittest.TestCase):
    # From the job's start, before apt, to its end, every 30 s, detached so it outlives its step; its log goes
    # with the run's other logs.
    def test_the_nightly_samples_the_whole_job_every_30_s(self):
        names = [name or uses for name, uses in step_names()]
        self.assertTrue(names[0].startswith("actions/checkout@"), names[0])
        self.assertEqual(names[1], "Start the job sampler")
        script = step("Start the job sampler")
        self.assertIn('setsid nohup "$GITHUB_WORKSPACE/geocoder/builder/tools/job_sampler.sh" "$RUNNER_TEMP/jobstat.log" '
                      '"$RUNNER_TEMP" 30 \\\n'
                      '            "$RUNNER_TEMP/geocoder/planet.osm.pbf" "$RUNNER_TEMP/geocoder/planet-tmp.osm.pbf" \\\n'
                      '            > /dev/null 2>&1 < /dev/null &\n',
                      script)
        # The PBFs are where Setup's work dir puts them.
        self.assertIn('WORKDIR="$RUNNER_TEMP/geocoder"', step("Setup"))
        logs = step("Upload job logs")
        self.assertIsNotNone(logs)
        self.assertIn("${{ runner.temp }}/jobstat.log", logs)
        self.assertIn("if: always()", logs)


if __name__ == "__main__":
    unittest.main()
