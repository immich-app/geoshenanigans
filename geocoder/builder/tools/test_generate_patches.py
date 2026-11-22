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

HERE = os.path.dirname(os.path.abspath(__file__))
WORKFLOWS = os.path.join(HERE, "..", "..", "..", ".github", "workflows")
sys.path.insert(0, HERE)
import compress_index  # noqa: E402

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

# --logical-size DIR answers from the scenario's table (no row: it fails, with a line on stderr). A diff records its
# command line and what the scratch dir holds as it starts, says a line on stderr, then writes an empty patch.
FAKE_DIFF = FAKE_JOB + """\
if args[0] == "--logical-size":
    if args[1] not in scenario["logical"]:
        sys.exit("no footer in " + args[1])
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
                         "new_root": self.new_root, "logical": {}, "raw": {}, "meet": {}, "hold": {}, "patience": 10}
        for d in ("started", "ended", "scratch"):
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

    def run(self, **env):
        """Runs the script with env set over this process's (None: unset). The free scratch is ample unless
        given, so no test depends on the space of the volume it runs on."""
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
                               self.results, self.logs], env=env, check=True, capture_output=True, text=True)
        self.stdout = done.stdout
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


def at_least_two_jobs_at_once():
    return len(os.sched_getaffinity(0)) >= 2


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
    # and each verify (1 GiB) runs alone too.
    def test_two_diffs_that_fit_the_memory_budget_and_not_the_scratch_run_one_after_the_other(self):
        night = self.two_diffs_of_600_mib()
        night.scenario["hold"] = {"diff:r/a": 0.3, "diff:r/b": 0.3}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=GIB + 100 * MIB)

        self.assert_the_diffs_took_turns(night)
        for job in ("diff:r/a", "diff:r/b", "verify:r/a", "verify:r/b"):
            self.assertEqual(night.beside(job), [])

    @unittest.skipUnless(at_least_two_jobs_at_once(), "the script runs one job a core")
    def test_two_diffs_that_fit_both_budgets_run_side_by_side(self):
        night = self.two_diffs_of_600_mib()
        night.scenario["meet"] = {"diff:r/a": "diff:r/b", "diff:r/b": "diff:r/a"}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=2 * GIB + 300 * MIB)

        self.assertTrue(night.events["diff:r/a"]["met"] or night.events["diff:r/b"]["met"])
        for variant in ("r/a", "r/b"):
            self.assertEqual(night.result(variant)[0], "PASS")

    # The free space of the test above, where the two run side by side.
    def test_the_reserve_comes_off_the_free_space(self):
        night = self.two_diffs_of_600_mib()
        night.scenario["hold"] = {"diff:r/a": 0.3, "diff:r/b": 0.3}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=2 * GIB + 300 * MIB,
                  GEOCODER_PATCH_SCRATCH_RESERVE_KIB=GIB + 200 * MIB)

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

                night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=budget, GEOCODER_PATCH_SCRATCH_FREE_KIB=8 * GIB)

                self.assert_the_diffs_took_turns(night)

    # The two diffs (100 and 200 MiB) run side by side in 1 GiB + 150 MiB.
    # r/a's ends first, and its verify (1 GiB) starts only when r/b's diff
    # has given its 200 MiB back.
    @unittest.skipUnless(at_least_two_jobs_at_once(), "the script runs one job a core")
    def test_a_verify_waits_for_scratch_a_running_diff_holds(self):
        night = Night(self, ["r/a", "r/b"])
        night.sizes("r/a", 1, 50 * MIB)
        night.sizes("r/b", 2, 100 * MIB)
        night.scenario["meet"] = {"diff:r/a": "diff:r/b", "diff:r/b": "diff:r/a"}
        night.scenario["hold"] = {"diff:r/b": 1.0}

        night.run(GEOCODER_PATCH_MEM_BUDGET_KIB=AMPLE_MEMORY_KIB, GEOCODER_PATCH_SCRATCH_FREE_KIB=GIB + 150 * MIB)

        self.assertTrue(night.events["diff:r/a"]["met"] or night.events["diff:r/b"]["met"])
        self.assertEqual(night.beside("verify:r/a"), [])
        self.assertEqual(night.beside("verify:r/b"), [])
        for variant in ("r/a", "r/b"):
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

    # A fresh build has no old day: nothing is sized, diffed or verified.
    def test_a_variant_without_an_old_day_is_skipped_unsized(self):
        night = Night(self, ["r/full"])
        shutil.rmtree(night.old_root)
        night.scenario["logical"] = {}

        night.run()

        self.assertEqual(night.result("r/full"), ["SKIP"])
        self.assertEqual(night.stdout, "")
        self.assertEqual(night.events, {})


# The nightly's patch step (geocoder-build.yml): its reserve, as a shell
# command over $WORKDIR/output, and how it starts generate_patches.sh.
def nightly_patch_step():
    text = read(os.path.join(WORKFLOWS, "geocoder-build.yml"))
    reserve = re.search(r"^ *(RESERVE_KIB=\$\((?:.*\\\n)*.*\))$", text, re.M)
    call = re.search(r'^ *((?:\S+=\S+ \\\n *)*)"\$GITHUB_WORKSPACE/geocoder/builder/tools/generate_patches\.sh" \\$', text, re.M)
    return reserve.group(1), call.group(1).replace("\\\n", " ").split()


class NightlyReserveTest(unittest.TestCase):
    # The compressor runs beside the patch jobs and writes a .zst beside each
    # file it compresses: at most those files' bytes, which the patch jobs
    # must leave free.
    def test_the_nightly_passes_the_new_trees_bytes_as_the_reserve(self):
        reserve, assignments = nightly_patch_step()
        with tempfile.TemporaryDirectory() as workdir:
            out = os.path.join(workdir, "output")
            sizes = {"planet/full/street_ways.bin": 300 * 1024, "planet/full/strings_layout.json": 700,
                     "planet/full/poi_meta.json": 900, "planet/poi/all/poi_records.bin": 5000,
                     "planet/water/all/exact/water_meta.json": 800, "planet/water/all/exact/attribution.json": 600,
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

        self.assertEqual(compressed, 300 * 1024 + 700 + 900 + 5000 + 800 + 600 + 40 * 1024 + 1 + 3000)
        self.assertEqual(printed, "%d\n" % -(-compressed // 1024))
        self.assertEqual(assignments, ['GEOCODER_PATCH_SCRATCH_RESERVE_KIB="$RESERVE_KIB"'])


if __name__ == "__main__":
    unittest.main()
