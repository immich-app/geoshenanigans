#!/usr/bin/env python3
"""Tests for generate_patches.sh: its diff command line, and the memory and scratch its jobs are started within.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
WORKFLOWS = os.path.join(HERE, "..", "..", "..", ".github", "workflows")
sys.path.insert(0, HERE)
import compress_index  # noqa: E402
from test_cpu_budget import fake_cpus  # noqa: E402

MIB = 1 << 10  # in KiB
GIB = 1 << 20
# What a verify holds besides the new variant's client files: generate_patches.sh's patcher_scratch_kib.
PATCHER_SCRATCH_KIB = GIB
# What a diff's estimate holds for each day besides its files: generate_patches.sh's 64 MiB.
DIFF_DAY_KIB = 64 * MIB
# A memory budget no test's diffs reach, and free scratch no test's jobs fill.
AMPLE_MEMORY_KIB = 1 << 40
AMPLE_SCRATCH_KIB = 1 << 40

# What both fake tools share. A job (a diff or a patch run) is "<stage>:<variant>". It marks itself started,
# waits for the job the scenario has it meet (if any) to be running too, holds for the scenario's time, records
# what ran beside it, and marks itself ended.
FAKE_JOB = """\
import json, os, shutil, sys, time
with open(os.environ["SCENARIO"]) as f:
    scenario = json.load(f)
def mark(kind, job):
    return os.path.join(scenario["marks"], kind, job.replace("/", "_"))
def running(me):
    started, ended = (set(os.listdir(os.path.join(scenario["marks"], kind))) for kind in ("started", "ended"))
    return started - ended - {me.replace("/", "_")}
def run_job(me, seen):
    open(mark("started", me), "w").close()
    beside = running(me)
    partner, met = scenario["meet"].get(me), False
    deadline = time.time() + scenario["patience"]
    while partner and time.time() < deadline and not os.path.exists(mark("ended", partner)):
        if os.path.exists(mark("started", partner)):
            met = True
            break
        time.sleep(0.01)
    time.sleep(scenario["hold"].get(me, 0))
    beside |= running(me)
    seen.update(job=me, beside=sorted(beside), met=met)
    with open(scenario["events"], "a") as f:
        f.write(json.dumps(seen) + "\\n")
    open(mark("ended", me), "w").close()
args = sys.argv[1:]
"""

# --logical-size DIR answers from the scenario's table (no row: it fails, with a line on stderr), once it has waited
# for the sizing of the dir the scenario has it meet (if any) to be running too and held for the scenario's size_hold;
# it records its start and end and whether it met that sizing. A diff records its command line and what the scratch
# dir holds as it starts, says a line on stderr, then writes an empty patch.
FAKE_DIFF = FAKE_JOB + """\
def sizing(kind, path):
    return os.path.join(scenario["marks"], "sizing", kind + path.replace("/", "_"))
if args[0] == "--logical-size":
    if args[1] not in scenario["logical"]:
        sys.exit("no footer in " + args[1])
    start = time.time()
    open(sizing("started", args[1]), "w").close()
    partner, met = scenario.get("size_meet", {}).get(args[1]), False
    deadline = time.time() + scenario["patience"]
    while partner and time.time() < deadline and not os.path.exists(sizing("ended", partner)):
        if os.path.exists(sizing("started", partner)):
            met = True
            break
        time.sleep(0.01)
    time.sleep(scenario.get("size_hold", 0))
    with open(scenario["sized"], "a") as f:
        f.write(json.dumps([args[1], start, time.time(), met]) + "\\n")
    open(sizing("ended", args[1]), "w").close()
    print(*scenario["logical"][args[1]])
    sys.exit(0)
variant = os.path.relpath(args[1], scenario["new_root"])
run_job("diff:" + variant, {"args": args, "scratch": os.listdir(os.environ["TMPDIR"])})
if variant in scenario["raw"]:
    print("Uncompressed patch: %d bytes (0 MiB) RSS=0 MiB" % scenario["raw"][variant], file=sys.stderr)
print("diffed", file=sys.stderr)
open(args[3], "wb").close()
"""

# Copies the new day's files beside the patch, so the verify passes.
FAKE_PATCH = FAKE_JOB + """\
src, out = os.path.dirname(args[1]), args[3]
run_job("verify:" + os.path.relpath(src, scenario["new_root"]), {})
for name in os.listdir(src):
    if not name.endswith(".gcpatch"):
        shutil.copy(os.path.join(src, name), out)
"""


def write_file(path, size):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"\0" * size)


def read(path):
    with open(path) as f:
        return f.read()


class Night:
    """Two days of variant dirs, the fake tools and one run of generate_patches.sh over them."""

    def __init__(self, test, variants):
        self.root = tempfile.mkdtemp()
        test.addCleanup(shutil.rmtree, self.root)
        self.old_root, self.new_root = os.path.join(self.root, "old"), os.path.join(self.root, "new")
        self.results, self.logs = os.path.join(self.root, "results"), os.path.join(self.root, "logs")
        self.scratch = os.path.join(self.root, "scratch")
        # What the fake tools read: where they leave their marks and events, what --logical-size answers for
        # each dir, which job waits to meet which, how long each holds, and how long one waits to meet another.
        self.scenario = {"marks": self.root, "events": os.path.join(self.root, "events.jsonl"),
                         "sized": os.path.join(self.root, "sized.jsonl"),
                         "new_root": self.new_root, "logical": {}, "raw": {}, "meet": {}, "hold": {}, "patience": 10}
        for d in ("started", "ended", "scratch", "sizing"):
            os.makedirs(os.path.join(self.root, d))
        # Two record files a day: 48 bytes of client files, which a verify counts as 1 KiB.
        for variant in variants:
            for day in (self.old_root, self.new_root):
                write_file(os.path.join(day, variant, "street_ways.bin"), 24)
                write_file(os.path.join(day, variant, "street_nodes.bin"), 24)
                self.scenario["logical"][os.path.join(day, variant)] = [0, 0]
        for name, source in (("diff", FAKE_DIFF), ("patch", FAKE_PATCH)):
            with open(os.path.join(self.root, name), "w") as f:
                f.write("#!%s\n%s" % (sys.executable, source))
            os.chmod(os.path.join(self.root, name), 0o755)

    def old(self, variant):
        return os.path.join(self.old_root, variant)

    def new(self, variant):
        return os.path.join(self.new_root, variant)

    def sizes(self, variant, mapped_kib, scratch_kib):
        """What --logical-size answers for each day's dir of the variant, in KiB."""
        for day in (self.old_root, self.new_root):
            self.scenario["logical"][os.path.join(day, variant)] = [mapped_kib * 1024, scratch_kib * 1024]

    def run(self, check=True, **env):
        """Runs the script with env set over this process's (None: unset); with check, it must exit 0. The free
        scratch is ample unless given, so no test depends on the space of the volume it runs on."""
        scenario = os.path.join(self.root, "scenario.json")
        with open(scenario, "w") as f:
            json.dump(self.scenario, f)
        given = dict(GEOCODER_PATCH_SCRATCH_FREE_KIB=AMPLE_SCRATCH_KIB)
        given.update(env)
        env = dict(os.environ, GEOCODER_DIFF=os.path.join(self.root, "diff"), GEOCODER_PATCH=os.path.join(self.root, "patch"),
                   SCENARIO=scenario, TMPDIR=self.scratch)
        for name, value in given.items():
            if value is None:
                env.pop(name, None)
            else:
                env[name] = str(value)
        done = subprocess.run(["bash", os.path.join(HERE, "generate_patches.sh"), self.old_root, self.new_root,
                               self.results, self.logs], env=env, check=check, capture_output=True, text=True)
        self.returncode, self.stdout, self.stderr = done.returncode, done.stdout, done.stderr
        self.events = {}
        if os.path.exists(self.scenario["events"]):
            for line in read(self.scenario["events"]).splitlines():
                event = json.loads(line)
                self.events[event["job"]] = event
        return self

    def result(self, variant):
        return read(os.path.join(self.results, variant.replace("/", "_"))).split()

    def mismatches(self, variant):
        return read(os.path.join(self.results, variant.replace("/", "_") + ".mismatches"))

    def beside(self, job):
        """The jobs that ran while `job` did, as it saw them."""
        return [other.replace("_", "/") for other in self.events[job]["beside"]]


class GeneratePatchesTest(unittest.TestCase):
    # The diff takes the keys from the records of both days' dirs: the script
    # passes it no sidecar dir and makes none in scratch.
    def test_diffs_a_variant_by_slot_identity_with_no_scratch_of_its_own(self):
        night = Night(self, ["r/no-addresses"]).run()

        old, new = night.old("r/no-addresses"), night.new("r/no-addresses")
        seen = night.events["diff:r/no-addresses"]
        self.assertEqual(seen["args"], [old, new, "-o", new + "/patch.gcpatch", "--identity-slots"])
        self.assertEqual(seen["scratch"], [])
        self.assertEqual(read(os.path.join(night.logs, "r_no-addresses.log")), "diffed\n")
        self.assertEqual(night.result("r/no-addresses")[:2], ["PASS", "2"])
        self.assertEqual(os.listdir(night.scratch), [])

    # The record files in the dirs are 48 bytes a day: the weight is 9/8 of
    # what the diff says it maps, 4 times the string tiers' files and 64 MiB,
    # a day each; the diff's scratch what it says it writes; the verify's the
    # new day's client files and the patcher's own.
    def test_a_variants_weight_and_scratch_need_are_the_diffs_two_logical_sizes(self):
        night = Night(self, ["r/full"])
        night.scenario["logical"][night.old("r/full")] = [3 * MIB * 1024, 5 * MIB * 1024 - 1023]
        night.scenario["logical"][night.new("r/full")] = [4 * MIB * 1024 + 1023, 7 * MIB * 1024]
        write_file(os.path.join(night.old("r/full"), "strings_core.bin"), 2 * 1024)
        write_file(os.path.join(night.new("r/full"), "strings_core.bin"), 4 * 1024)
        # Not client files: neither is linked for the verify.
        write_file(os.path.join(night.new("r/full"), "street_ways.bin.osm_ids"), 8 * 1024)
        write_file(os.path.join(night.new("r/full"), "street_ways.bin.zst"), 8 * 1024)

        night.run()

        memory = (3 * MIB + 4 * MIB) * 9 // 8 + (2 + 4) * 4 + 2 * DIFF_DAY_KIB
        verify = 5 + PATCHER_SCRATCH_KIB  # 48 + 4096 bytes of client files
        self.assertEqual(night.stdout, "r/full: diff %d KiB of memory and %d KiB of scratch, verify %d KiB of scratch\n"
                         % (memory, 5 * MIB + 7 * MIB, verify))
        self.assertEqual(night.result("r/full")[:2], ["PASS", "3"])

    def two_diffs_of_600_mib(self):
        night = Night(self, ["r/a", "r/b"])
        night.sizes("r/a", 1, 300 * MIB)
        night.sizes("r/b", 1, 300 * MIB)
        return night

    # Neither diff saw the other running, and both variants passed.
    def assert_the_diffs_took_turns(self, night):
        for job in ("diff:r/a", "diff:r/b"):
            self.assertEqual([other for other in night.beside(job) if other.startswith("diff:")], [])
        for variant in ("r/a", "r/b"):
            self.assertEqual(night.result(variant)[0], "PASS")

    # 600 MiB a diff in 1.1 GiB: the second starts when the first has ended,
    # and each verify (1 GiB) runs alone too. The budget has two CPUs, so
    # that the scratch, not a job a CPU, keeps them apart, as in the tests
    # below.
    def test_two_diffs_that_fit_the_memory_budget_and_not_the_scratch_run_one_after_the_other(self):
        night = self.two_diffs_of_600_mib()
        night.scenario["hold"] = {"diff:r/a": 0.3, "diff:r/b": 0.3}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=GIB + 100 * MIB,
                  **fake_cpus(self, 2))

        self.assert_the_diffs_took_turns(night)
        for job in ("diff:r/a", "diff:r/b", "verify:r/a", "verify:r/b"):
            self.assertEqual(night.beside(job), [])

    def test_two_diffs_that_fit_both_budgets_run_side_by_side(self):
        night = self.two_diffs_of_600_mib()
        night.scenario["meet"] = {"diff:r/a": "diff:r/b", "diff:r/b": "diff:r/a"}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=2 * GIB + 300 * MIB,
                  **fake_cpus(self, 2))

        self.assertTrue(night.events["diff:r/a"]["met"] or night.events["diff:r/b"]["met"])
        for variant in ("r/a", "r/b"):
            self.assertEqual(night.result(variant)[0], "PASS")

    # A job a CPU of the budget: on 8 CPUs the two diffs above run side by side, under a quota of one CPU they take
    # turns, and so do their verifies, though the scratch would fit a verify beside the other diff.
    def test_at_most_a_job_a_cpu_of_the_cgroups_quota_runs_at_once(self):
        night = self.two_diffs_of_600_mib()
        night.scenario["meet"] = {"diff:r/a": "diff:r/b", "diff:r/b": "diff:r/a"}
        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=2 * GIB + 300 * MIB,
                  **fake_cpus(self, 8, "/job", {"/job": "max 100000"}))
        self.assertTrue(night.events["diff:r/a"]["met"] or night.events["diff:r/b"]["met"])

        night = self.two_diffs_of_600_mib()
        night.scenario["hold"] = {"diff:r/a": 0.3, "diff:r/b": 0.3}
        night.scenario["size_hold"] = 0.2
        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=2 * GIB + 300 * MIB,
                  **fake_cpus(self, 8, "/job", {"/job": "100000 100000"}))
        self.assert_the_diffs_took_turns(night)
        for job in ("diff:r/a", "diff:r/b", "verify:r/a", "verify:r/b"):
            self.assertEqual(night.beside(job), [], job)
        sized = [json.loads(line) for line in read(night.scenario["sized"]).splitlines()]
        self.assertFalse(any(a[1] < b[2] and b[1] < a[2] for i, a in enumerate(sized) for b in sized[i + 1:]), sized)

    # The free space of the test above, where the two run side by side.
    def test_the_reserve_comes_off_the_free_space(self):
        night = self.two_diffs_of_600_mib()
        night.scenario["hold"] = {"diff:r/a": 0.3, "diff:r/b": 0.3}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=2 * GIB + 300 * MIB,
                  GEOCODER_PATCH_SCRATCH_RESERVE_KIB=GIB + 200 * MIB, **fake_cpus(self, 2))

        self.assert_the_diffs_took_turns(night)

    # Each diff's estimate is 129 MiB (512 KiB mapped a day): over a budget of
    # 200 MiB they take turns, and over one of 100 MiB each still runs, alone.
    def test_two_diffs_that_fit_the_scratch_and_not_the_memory_budget_run_one_after_the_other(self):
        for budget in (200 * MIB, 100 * MIB):
            with self.subTest(budget=budget):
                night = Night(self, ["r/a", "r/b"])
                night.sizes("r/a", 512, 1)
                night.sizes("r/b", 512, 1)
                night.scenario["hold"] = {"diff:r/a": 0.3, "diff:r/b": 0.3}

                night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=budget, GEOCODER_PATCH_SCRATCH_FREE_KIB=8 * GIB, **fake_cpus(self, 2))

                self.assert_the_diffs_took_turns(night)

    # The two diffs (100 and 200 MiB) run side by side in 1 GiB + 150 MiB.
    # r/a's ends first, and its verify (1 GiB) starts only when r/b's diff
    # has given its 200 MiB back.
    def test_a_verify_waits_for_scratch_a_running_diff_holds(self):
        night = Night(self, ["r/a", "r/b"])
        night.sizes("r/a", 1, 50 * MIB)
        night.sizes("r/b", 2, 100 * MIB)
        night.scenario["meet"] = {"diff:r/a": "diff:r/b", "diff:r/b": "diff:r/a"}
        night.scenario["hold"] = {"diff:r/b": 1.0}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=GIB + 150 * MIB,
                  **fake_cpus(self, 2))

        self.assertTrue(night.events["diff:r/a"]["met"] or night.events["diff:r/b"]["met"])
        self.assertEqual(night.beside("verify:r/a"), [])
        self.assertEqual(night.beside("verify:r/b"), [])
        for variant in ("r/a", "r/b"):
            self.assertEqual(night.result(variant)[0], "PASS")

    # Verifies that wait start only while a CPU of the budget is free, as diffs do. On two CPUs in 2 GiB + 100 MiB,
    # r/big's diff (1.5 GiB) keeps r/a's verify (1 GiB) back, and r/c's diff starts beside it. When r/big's diff ends
    # both verifies fit the scratch beside r/c's diff, which runs until r/a's verify has started: r/big's waits for
    # a CPU.
    def test_queued_verifies_start_only_while_a_cpu_is_free(self):
        night = Night(self, ["r/big", "r/a", "r/c"])
        night.sizes("r/big", 3, 768 * MIB)
        night.sizes("r/a", 2, 1)
        night.sizes("r/c", 1, 1)
        night.scenario["meet"] = {"diff:r/big": "diff:r/c", "diff:r/c": "verify:r/a"}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=2 * GIB + 100 * MIB,
                  **fake_cpus(self, 2))

        lines = re.findall(r"(?m)^\[[\d:]+\] ((?:start|end) (?:diff|verify) \S+?):? (.*)$", night.stderr)
        events = [event for event, _ in lines]
        self.assertLess(events.index("end diff r/big"), events.index("start verify r/a"), night.stderr)
        self.assertLess(events.index("start diff r/c"), events.index("end diff r/big"), night.stderr)
        verifies = [int(re.search(r"\(running: (\d+) jobs?", rest)[1]) for event, rest in lines
                    if event.startswith("start verify")]
        self.assertEqual(len(verifies), 3, night.stderr)
        self.assertTrue(all(n < 2 for n in verifies), night.stderr)
        for variant in ("r/big", "r/a", "r/c"):
            self.assertEqual(night.result(variant)[0], "PASS")

    # No diff of 2 GiB starts in 1.1 GiB, and no verify (1 GiB) in half a
    # GiB: the variant fails, its reason in .mismatches, and the others go on.
    def test_a_variant_that_cannot_fit_the_free_space_fails_by_name(self):
        night = Night(self, ["r/big", "r/small"])
        night.sizes("r/big", 2, GIB)
        night.sizes("r/small", 1, 1)
        free = GIB + 100 * MIB

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=free,
                  GEOCODER_PATCH_SCRATCH_RESERVE_KIB=7)

        self.assertEqual(night.result("r/big"), ["FAIL", "1", "0", "0"])
        self.assertEqual(night.mismatches("r/big"),
                         "the diff needs %d KiB of scratch, more than the %d KiB free less the reserve of 7 KiB\n" % (2 * GIB, free))
        self.assertNotIn("diff:r/big", night.events)
        self.assertFalse(os.path.exists(os.path.join(night.new("r/big"), "patch.gcpatch")))
        self.assertEqual(night.result("r/small")[0], "PASS")

        night = Night(self, ["r/a"])
        night.sizes("r/a", 1, 1)

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=GIB // 2)

        self.assertEqual(night.result("r/a"), ["FAIL", "1", "0", "0"])
        self.assertEqual(night.mismatches("r/a"),
                         "the verify needs %d KiB of scratch, more than the %d KiB free less the reserve of 0 KiB\n"
                         % (1 + PATCHER_SCRATCH_KIB, GIB // 2))
        self.assertIn("diff:r/a", night.events)
        self.assertNotIn("verify:r/a", night.events)

    # A variant whose client files pass 2 GiB (a sparse file of 3 GiB here) is
    # counted whole: its verify does not start in 3.5 GiB.
    def test_a_verifys_need_counts_client_files_past_two_gib_whole(self):
        night = Night(self, ["r/a"])
        night.sizes("r/a", 1, 1)
        with open(os.path.join(night.new("r/a"), "addr_points.bin"), "wb") as f:
            f.truncate(3 * GIB * 1024)
        free = 3 * GIB + GIB // 2

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=free)

        self.assertEqual(night.result("r/a"), ["FAIL", "1", "0", "0"])
        self.assertEqual(night.mismatches("r/a"),
                         "the verify needs %d KiB of scratch, more than the %d KiB free less the reserve of 0 KiB\n"
                         % (3 * GIB + 1 + PATCHER_SCRATCH_KIB, free))
        self.assertNotIn("verify:r/a", night.events)

    # A patch that decompresses to 3 GiB, which the patcher writes to scratch:
    # its verify does not start in 3.5 GiB.
    def test_a_verifys_need_counts_the_decompressed_patch(self):
        night = Night(self, ["r/a"])
        night.sizes("r/a", 1, 1)
        night.scenario["raw"]["r/a"] = 3 * GIB * 1024
        free = 3 * GIB + GIB // 2

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=free)

        self.assertEqual(night.result("r/a"), ["FAIL", "1", "0", "0"])
        self.assertEqual(night.mismatches("r/a"),
                         "the verify needs %d KiB of scratch, more than the %d KiB free less the reserve of 0 KiB\n"
                         % (3 * GIB + 1 + PATCHER_SCRATCH_KIB, free))
        self.assertIn("diff:r/a", night.events)
        self.assertNotIn("verify:r/a", night.events)

    # of the scratch dir's volume: half a GiB here, where no verify starts.
    def test_the_free_space_is_dfs_figure_for_the_scratch_dir(self):
        night = Night(self, ["r/a"])
        bin_dir, asked = os.path.join(night.root, "bin"), os.path.join(night.root, "df.args")
        os.makedirs(bin_dir)
        with open(os.path.join(bin_dir, "df"), "w") as f:
            f.write('#!/bin/sh\necho "$*" > "$DF_ASKED"\necho "    Avail"\necho " %d"\n' % (GIB // 2))
        os.chmod(os.path.join(bin_dir, "df"), 0o755)

        night.run(GEOCODER_PATCH_SCRATCH_FREE_KIB=None, DF_ASKED=asked, PATH=bin_dir + os.pathsep + os.environ["PATH"])

        self.assertEqual(read(asked), "--output=avail -k %s\n" % night.scratch)
        self.assertEqual(night.mismatches("r/a"),
                         "the verify needs %d KiB of scratch, more than the %d KiB free less the reserve of 0 KiB\n"
                         % (1 + PATCHER_SCRATCH_KIB, GIB // 2))

    # Without a dir in scratch for the estimates no variant can be sized:
    # the script fails, sizing and diffing nothing, rather than leave every
    # variant without a result.
    def test_no_dir_for_the_estimates_fails_the_script(self):
        night = Night(self, ["r/full"])
        os.rmdir(night.scratch)

        night.run(check=False)

        self.assertEqual(night.returncode, 1)
        self.assertIn("estimates-XXXXXX", night.stderr)
        self.assertFalse(os.path.exists(night.scenario["sized"]))
        self.assertEqual(night.events, {})
        self.assertEqual(os.listdir(night.results), [])

    # A dir the diff cannot size is not diffed: its variant fails with the
    # diff's last line, and the others go on.
    def test_a_variant_the_diff_cannot_size_fails_by_name(self):
        night = Night(self, ["r/bad", "r/good"])
        del night.scenario["logical"][night.new("r/bad")]

        night.run()

        self.assertEqual(night.result("r/bad"), ["FAIL", "1", "0", "0"])
        self.assertEqual(night.mismatches("r/bad"), "geocoder-diff --logical-size failed: no footer in %s\n" % night.new("r/bad"))
        self.assertNotIn("diff:r/bad", night.events)
        self.assertEqual(night.result("r/good")[0], "PASS")

    # Each job's start, with what it holds and what the running jobs hold, and its end, timestamped on stderr:
    # the order the last variants end in is in the step's log.
    def test_each_job_logs_its_start_and_end(self):
        night = Night(self, ["r/a", "r/b"])
        night.sizes("r/a", 1, 1)
        night.sizes("r/b", 2, 1)

        night.run()

        lines = re.findall(r"(?m)^\[\d\d:\d\d:\d\d\] (start|end) (diff|verify) (\S+?):? (.*)$", night.stderr)
        events = [(what, stage, variant) for what, stage, variant, _ in lines]
        for variant in ("r/a", "r/b"):
            for stage in ("diff", "verify"):
                self.assertEqual(events.count(("start", stage, variant)), 1, night.stderr)
                self.assertEqual(events.count(("end", stage, variant)), 1, night.stderr)
                self.assertLess(events.index(("start", stage, variant)), events.index(("end", stage, variant)))
            self.assertLess(events.index(("end", "diff", variant)), events.index(("start", "verify", variant)))
        weight = re.search(r"(?m)^r/b: diff (\d+) KiB of memory and (\d+) KiB of scratch", night.stdout)
        for what, stage, variant, rest in lines:
            if what == "end":
                self.assertRegex(rest, r"^after \d+s$")
            elif stage == "diff":
                self.assertRegex(rest, r"^\d+ KiB of memory and \d+ KiB of scratch \(running: \d+ jobs?, "
                                       r"\d+ KiB of memory, \d+ KiB of scratch\)$")
            else:
                self.assertRegex(rest, r"^\d+ KiB of scratch \(running: \d+ jobs?, \d+ KiB of memory, \d+ KiB of scratch\)$")
        start_b = next(rest for what, stage, variant, rest in lines if (what, stage, variant) == ("start", "diff", "r/b"))
        self.assertTrue(start_b.startswith(f"{weight[1]} KiB of memory and {weight[2]} KiB of scratch"), start_b)

    # The estimates (two geocoder-diff --logical-size a variant, ~0.1 s each on a planet night) run side by side,
    # and still print a line a variant, in order: on two CPUs, the first two variants' sizings of the old day meet.
    def test_the_variants_are_sized_side_by_side(self):
        night = Night(self, ["r/a", "r/b", "r/c", "r/d"])
        night.scenario["size_meet"] = {night.old("r/a"): night.old("r/b"), night.old("r/b"): night.old("r/a")}

        night.run(**fake_cpus(self, 2))

        sized = [json.loads(line) for line in read(night.scenario["sized"]).splitlines()]
        self.assertEqual(len(sized), 8)
        met = {path: met for path, _, _, met in sized}
        self.assertTrue(met[night.old("r/a")] or met[night.old("r/b")], sized)
        self.assertEqual([line.split(":")[0] for line in night.stdout.splitlines()], ["r/a", "r/b", "r/c", "r/d"])
        for variant in ("r/a", "r/b", "r/c", "r/d"):
            self.assertEqual(night.result(variant)[0], "PASS")

    # What the caller's other jobs hold beside the patches comes off the budget: two diffs of 129 MiB that run
    # side by side in 300 MiB take turns when 150 MiB of it is held elsewhere, each started with no diff's memory
    # running.
    def test_the_memory_reserve_comes_off_the_budget(self):
        night = Night(self, ["r/a", "r/b"])
        night.sizes("r/a", 512, 1)
        night.sizes("r/b", 512, 1)

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=300 * MIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=8 * GIB,
                  GEOCODER_PATCH_MEM_RESERVE_KIB=150 * MIB, **fake_cpus(self, 2))

        self.assert_the_diffs_took_turns(night)
        running = re.findall(r"(?m)^\[[\d:]+\] start diff \S+: .* \(running: \d+ jobs?, (\d+) KiB of memory, ", night.stderr)
        self.assertEqual(running, ["0", "0"], night.stderr)

    # On the same two CPUs.
    def test_without_the_memory_reserve_the_same_diffs_run_side_by_side(self):
        night = Night(self, ["r/a", "r/b"])
        night.sizes("r/a", 512, 1)
        night.sizes("r/b", 512, 1)
        night.scenario["meet"] = {"diff:r/a": "diff:r/b", "diff:r/b": "diff:r/a"}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=300 * MIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=8 * GIB, **fake_cpus(self, 2))

        self.assertTrue(night.events["diff:r/a"]["met"] or night.events["diff:r/b"]["met"])

    # A fresh build has no old day: nothing is sized, diffed or verified.
    def test_a_variant_without_an_old_day_is_skipped_unsized(self):
        night = Night(self, ["r/full"])
        shutil.rmtree(night.old_root)
        night.scenario["logical"] = {}

        night.run()

        self.assertEqual(night.result("r/full"), ["SKIP"])
        self.assertEqual(night.stdout, "")
        self.assertEqual(night.events, {})


# A water dir on three days (geocoder/testdata/water_dir, which the builder's
# s2 tests check is what the water layer writes): admin geometry, admin cells
# and a core of its own, no place files and no other string tier, slots of its
# own; day 2 moves one vertex of one of its four polygons, day 3 adds a pond of
# a water=* value of its own and a sea, so its water_meta.json and
# attribution.json change.
WATER_DIR_FIXTURE = os.path.join(HERE, "..", "..", "testdata", "water_dir")


BUILD_DIR = os.path.join(HERE, "..", "build")


def real_tool(name, env, build_dir=BUILD_DIR):
    """The tool generate_patches.sh would run: $env (a path, or a name on PATH, as the script runs it), else the one
    in the builder's build dir, else the one on PATH; None for none. The build dir's comes before PATH's, so a fresh
    build is what the tests run rather than a tool installed before it."""
    given = os.environ.get(env)
    if given:
        return shutil.which(given)
    built = os.path.join(build_dir, name)
    if os.path.isfile(built) and os.access(built, os.X_OK):
        return built
    return shutil.which(name)


HAS_REAL_TOOLS = real_tool("geocoder-diff", "GEOCODER_DIFF") and real_tool("geocoder-patch", "GEOCODER_PATCH")
NEEDS_REAL_TOOLS = ("needs geocoder-diff and geocoder-patch (GEOCODER_DIFF / GEOCODER_PATCH, builder/build, "
                    "or on PATH)")


class RealToolTest(unittest.TestCase):
    """Where the water dir tests find the tools generate_patches.sh runs."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.bin, self.build = os.path.join(self.root, "bin"), os.path.join(self.root, "build")
        os.makedirs(self.bin)
        os.makedirs(self.build)

    def tool(self, directory):
        path = os.path.join(directory, "geocoder-diff")
        with open(path, "w") as f:
            f.write("#!/bin/sh\n")
        os.chmod(path, 0o755)
        return path

    def found(self, given=None):
        env = {"PATH": self.bin}
        if given is not None:
            env["GEOCODER_DIFF"] = given
        with mock.patch.dict(os.environ, env):
            if given is None:
                os.environ.pop("GEOCODER_DIFF", None)
            return real_tool("geocoder-diff", "GEOCODER_DIFF", self.build)

    def test_a_tool_is_the_one_given_else_the_build_dirs_else_the_one_on_path(self):
        self.assertIsNone(self.found())
        on_path = self.tool(self.bin)
        self.assertEqual(self.found(), on_path)
        built = self.tool(self.build)
        self.assertEqual(self.found(), built)
        # Given by name, it is the one on PATH, as the script runs it; by path, that one; one not there, none.
        self.assertEqual(self.found("geocoder-diff"), on_path)
        self.assertEqual(self.found(built), built)
        self.assertIsNone(self.found(os.path.join(self.root, "gone")))
        self.assertIsNone(self.found("geocoder-gone"))


def patch_water_days(test, old_day, new_day):
    """Runs generate_patches.sh with the real tools from the fixture's old_day to its new_day, each at
    <root>/{old,new}/r/water/all/exact, beside a mode dir's street tier."""
    test.root = tempfile.mkdtemp()
    test.addCleanup(shutil.rmtree, test.root)
    test.old, test.new = os.path.join(test.root, "old"), os.path.join(test.root, "new")
    for day, src in ((test.old, old_day), (test.new, new_day)):
        shutil.copytree(os.path.join(WATER_DIR_FIXTURE, src), os.path.join(day, "r", "water", "all", "exact"))
        # A mode dir's street tier, which a variant one or two dirs down would borrow as its own (a dir of
        # one .bin file is no variant).
        write_file(os.path.join(day, "r", "full", "strings_street.bin"), 4 << 20)
    test.results, test.logs = os.path.join(test.root, "results"), os.path.join(test.root, "logs")
    os.makedirs(os.path.join(test.root, "scratch"))
    # Scratch space the scheduler takes as ample, whatever the volume holds and the environment reserves.
    env = dict(os.environ, GEOCODER_DIFF=real_tool("geocoder-diff", "GEOCODER_DIFF"),
               GEOCODER_PATCH=real_tool("geocoder-patch", "GEOCODER_PATCH"), TMPDIR=os.path.join(test.root, "scratch"),
               GEOCODER_PATCH_SCRATCH_FREE_KIB=str(AMPLE_SCRATCH_KIB), GEOCODER_PATCH_SCRATCH_RESERVE_KIB="0")
    test.run_ = subprocess.run(["bash", os.path.join(HERE, "generate_patches.sh"), test.old, test.new, test.results,
                                test.logs], env=env, capture_output=True, text=True, timeout=300)


@unittest.skipUnless(HAS_REAL_TOOLS, NEEDS_REAL_TOOLS)
class WaterDirPatchTest(unittest.TestCase):
    """The real diff and patcher on a water dir at <region>/water/all/exact: it patches on its own."""

    def setUp(self):
        patch_water_days(self, "day1", "day2")

    def test_a_water_dir_diffs_patches_and_verifies_on_its_own(self):
        self.assertEqual(self.run_.returncode, 0, self.run_.stdout + self.run_.stderr)
        result = read(os.path.join(self.results, "r_water_all_exact")).split()
        # All ten client files, the JSON ones among them, as the new day's.
        self.assertEqual(result[:2], ["PASS", "10"], result)
        self.assertTrue(os.path.getsize(os.path.join(self.new, "r", "water", "all", "exact", "patch.gcpatch")) > 0)
        self.assertEqual(read(os.path.join(self.results, "r_full")).split(), ["SKIP"])
        # Its strings are its core alone, three dirs down from the mode dirs: its estimate holds no street tier.
        weight = int(re.search(r"^r/water/all/exact: diff (\d+) KiB of memory", self.run_.stdout, re.M).group(1))
        self.assertLess(weight, 2 * DIFF_DAY_KIB + MIB)

    def test_a_moved_polygon_sends_its_own_slots_alone(self):
        log = read(os.path.join(self.logs, "r_water_all_exact.log"))
        # The pond keeps its slot (its way's) and its arc's: its record is replaced there and its arc's block
        # edited, a few bytes, the same length; the other three polygons, every ring block and the arc slots
        # stay as they were.
        self.assertRegex(log, r"admin_polygons\.bin by slot: match=3 replace=1 insert=0 delete=0")
        self.assertIn("admin_rings.bin: unchanged", log)
        self.assertIn("arc_index.bin: unchanged", log)
        arcs = re.search(r"arc_vertices\.bin: .* recs m/i/d/r=(\d+)/(\d+)/(\d+)/(\d+)", log)
        self.assertIsNotNone(arcs, log)
        kept, inserted, deleted, replaced = map(int, arcs.groups())
        self.assertEqual(inserted, deleted)
        self.assertTrue(0 < inserted <= 8, log)
        self.assertEqual(replaced, 0, log)
        self.assertGreater(kept, 100)


@unittest.skipUnless(HAS_REAL_TOOLS, NEEDS_REAL_TOOLS)
class WaterDirKindsPatchTest(unittest.TestCase):
    """The real diff and patcher on a day whose water dir lists another kind and another credit."""

    def setUp(self):
        patch_water_days(self, "day2", "day3")

    def test_new_kinds_and_credits_come_through_the_patch(self):
        exact = os.path.join("r", "water", "all", "exact")
        for name in ("water_meta.json", "attribution.json"):
            with open(os.path.join(self.old, exact, name), "rb") as f:
                before = f.read()
            with open(os.path.join(self.new, exact, name), "rb") as f:
                after = f.read()
            self.assertNotEqual(before, after, name)
        self.assertEqual(self.run_.returncode, 0, self.run_.stdout + self.run_.stderr)
        result = read(os.path.join(self.results, "r_water_all_exact")).split()
        # All ten client files as the new day's, the changed JSON ones among them.
        self.assertEqual(result[:2], ["PASS", "10"], result)
        log = read(os.path.join(self.logs, "r_water_all_exact.log"))
        self.assertRegex(log, r"admin_polygons\.bin by slot: match=\d+ replace=\d+ insert=[1-9]")


def sizing_loop():
    """generate_patches.sh's lines that start the estimates and wait for them, as bash: from the one after the
    estimates dir is made to the wait."""
    loop = re.search(r"^estimates=\$\(mktemp .*\n((?:.*\n)*?)^wait\n", read(os.path.join(HERE, "generate_patches.sh")), re.M)
    return loop.group(1) + "wait\n" if loop else None


class SizingLoopTest(unittest.TestCase):
    # An estimate a signal ended frees its slot. bash before 5.3 has its bare wait -n pass over a job it has
    # reported, which it does of one a signal ended once a wait -n returns another; the shell reaps what ends while
    # it is busy launching (a fork of a big shell takes a while), here slowed by a DEBUG trap. On three CPUs r/a's
    # estimate ends and r/b's is killed as r/c's starts: r/d's then takes a slot, and r/e's the other, while r/c's and
    # r/d's still run (3 s each).
    def test_an_estimate_a_signal_ended_frees_its_slot(self):
        loop = sizing_loop()
        self.assertIsNotNone(loop)
        with tempfile.TemporaryDirectory() as tmp:
            script = f"""set -uo pipefail
mkfifo {tmp}/slow
exec {{slow}}<> {tmp}/slow
max_jobs=3
variants=(r/a r/b r/c r/d r/e)
estimates={tmp}
estimate_variant() {{
  case $1 in
    r/b) kill -KILL $BASHPID ;;
    r/c) /usr/bin/sleep 3; echo "$EPOCHREALTIME" > {tmp}/c-ended ;;
    r/d) /usr/bin/sleep 3 ;;
    r/e) echo "$EPOCHREALTIME" > {tmp}/e-started ;;
  esac
}}
trap 'read -t 0.1 -u "$slow" || :' DEBUG
{loop}trap - DEBUG
"""
            done = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60)

            self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
            self.assertLess(float(read(os.path.join(tmp, "e-started"))), float(read(os.path.join(tmp, "c-ended"))),
                            "r/e's estimate started once r/c's had ended, not beside it")


# The nightly's patch step (geocoder-build.yml): its reserve, as a shell
# command over $WORKDIR/output, and how it starts generate_patches.sh.
def nightly_patch_step():
    text = read(os.path.join(WORKFLOWS, "geocoder-build.yml"))
    reserve = re.search(r"^ *(RESERVE_KIB=\$\((?:.*\\\n)*.*\))$", text, re.M)
    call = re.search(r'^ *((?:\S+=\S+ \\\n *)*)"\$GITHUB_WORKSPACE/geocoder/builder/tools/generate_patches\.sh" \\$', text, re.M)
    return reserve.group(1), call.group(1).replace("\\\n", " ").split()


def nightly_reserves():
    """The patch step's lines that set the reserves it gives generate_patches.sh (RESERVE_KIB and MEM_RESERVE_KIB),
    from what $WORKDIR holds, as bash."""
    text = read(os.path.join(WORKFLOWS, "geocoder-build.yml"))
    block = re.search(r"^ *VARIANTS=.*\n((?:.*\n)*?) *# Per variant: diff", text, re.M)
    return "".join(line.strip() + "\n" for line in block.group(1).splitlines()) if block else None


def reserves(workdir, path=None, compressing=True):
    """The reserves the patch step gives generate_patches.sh for what `workdir` holds, with `path` first on PATH,
    while compression runs or once it has ended."""
    env = dict(os.environ, WORKDIR=workdir)
    if path:
        env["PATH"] = f"{path}:{env['PATH']}"
    script = f"compressing() {{ {'true' if compressing else 'false'}; }}\n" + nightly_reserves()
    printed = subprocess.run(["bash", "-e", "-c", script + 'echo "$RESERVE_KIB $MEM_RESERVE_KIB"'], env=env,
                             check=True, capture_output=True, text=True).stdout
    return tuple(int(n) for n in printed.split())


class NightlyReserveTest(unittest.TestCase):
    # The compressor runs beside the patch jobs and writes a .zst beside each
    # file it compresses: at most those files' bytes, which the patch jobs
    # must leave free. Each kind of file it takes holds a KiB or more, so the
    # reserve in KiB changes without any one of them.
    def test_the_nightly_passes_the_new_trees_bytes_as_the_reserve(self):
        reserve, assignments = nightly_patch_step()
        with tempfile.TemporaryDirectory() as workdir:
            out = os.path.join(workdir, "output")
            sizes = {"planet/full/street_ways.bin": 300 * 1024, "planet/full/strings_layout.json": 1700,
                     "planet/full/poi_meta.json": 1900, "planet/full/attribution.json": 1500,
                     "planet/poi/all/poi_records.bin": 5000, "planet/poi/all/poi_attribution.json": 2048,
                     "planet/water/all/exact/water_meta.json": 1800, "planet/water/all/exact/attribution.json": 1600,
                     "planet/search/places.fst": 40 * 1024 + 1, "planet/search/packs/europe.pack": 3000,
                     # What the compressor leaves alone, or has written already.
                     "planet/full/street_ways.bin.osm_ids": 64 * 1024, "planet/full/street_ways.bin.zst": 100 * 1024,
                     "planet/search/places.fst.zst": 10 * 1024, "planet/full/patch.gcpatch": 2048,
                     "planet/full/configurations.json": 4096}
            for name, size in sizes.items():
                write_file(os.path.join(out, name), size)

            compressed = sum(os.path.getsize(path) for path in compress_index.index_files(out))
            printed = subprocess.run(["bash", "-c", reserve + '\necho "$RESERVE_KIB"'], env=dict(os.environ, WORKDIR=workdir),
                                     check=True, capture_output=True, text=True).stdout

        self.assertEqual(compressed, 300 * 1024 + 1700 + 1900 + 1500 + 5000 + 2048 + 1800 + 1600 + 40 * 1024 + 1 + 3000)
        self.assertEqual(printed, "%d\n" % -(-compressed // 1024))
        self.assertEqual(assignments, ['GEOCODER_PATCH_SCRATCH_RESERVE_KIB="$RESERVE_KIB"',
                                       'GEOCODER_PATCH_MEM_RESERVE_KIB="$MEM_RESERVE_KIB"'])

    # The patch step starts when build-index exits, beside the place search: until its status is written, the
    # indexes, their scratch and their .zst may still land on the volume (~16 GB of writes on a planet night,
    # scratch included), and its builds hold a few GiB of memory.
    def test_while_the_place_search_runs_the_reserves_hold_what_it_may_write_and_use(self):
        self.assertIsNotNone(nightly_reserves())
        for status, scratch, memory in (("", 16 * GIB, 6 * GIB), ("0\n", 0, 0), ("1\n", 0, 0)):
            with tempfile.TemporaryDirectory() as workdir:
                write_file(os.path.join(workdir, "output", "planet", "full", "street_ways.bin"), 7 * 1024)
                if status:
                    with open(os.path.join(workdir, "place-search.status"), "w") as f:
                        f.write(status)

                self.assertEqual(reserves(workdir), (7 + scratch, memory), status)

    # Compression starts before the step waits for yesterday's files, and may have ended by then: what it wrote has
    # left the free space already, and it writes no more, so the patch jobs hold back only what the place search may
    # still write.
    def test_a_compression_that_has_ended_holds_no_reserve(self):
        for status, scratch, memory in (("", 16 * GIB, 6 * GIB), ("0\n", 0, 0)):
            with tempfile.TemporaryDirectory() as workdir:
                write_file(os.path.join(workdir, "output", "planet", "full", "street_ways.bin"), 7 * 1024)
                if status:
                    with open(os.path.join(workdir, "place-search.status"), "w") as f:
                        f.write(status)

                self.assertEqual(reserves(workdir, compressing=False), (scratch, memory), status)

    # One that ends while the files are counted leaves files the count missed: its status is read before the count,
    # so that the reserves still hold what it may write and use.
    def test_a_place_search_that_ends_during_the_count_keeps_its_reserves(self):
        with tempfile.TemporaryDirectory() as workdir:
            write_file(os.path.join(workdir, "output", "planet", "full", "street_ways.bin"), 7 * 1024)
            bin_dir = os.path.join(workdir, "bin")
            os.makedirs(bin_dir)
            # find counts what is there; then the place search writes an index of 10 MiB and its status.
            with open(os.path.join(bin_dir, "find"), "w") as f:
                f.write(f"#!/bin/sh\n{shutil.which('find')} \"$@\"\nfound=$?\n"
                        'mkdir -p "$WORKDIR/output/planet/search"\n'
                        'head -c 10485760 /dev/zero > "$WORKDIR/output/planet/search/places.fst"\n'
                        'echo 0 > "$WORKDIR/place-search.status"\nexit $found\n')
            os.chmod(os.path.join(bin_dir, "find"), 0o755)

            self.assertEqual(reserves(workdir, path=bin_dir), (7 + 16 * GIB, 6 * GIB))


if __name__ == "__main__":
    unittest.main()
