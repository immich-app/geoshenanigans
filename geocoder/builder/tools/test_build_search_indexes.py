#!/usr/bin/env python3
"""Tests for build_search_indexes.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_search_indexes as sut  # noqa: E402

TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "configurations.template.json")

# Stands in for place-search: `build <set> <out> ...` records its arguments and what the set dir
# held (name -> link target) and writes the index files, then fails for a set path containing
# $FAIL_SET.
FAKE = """#!{python}
import json, os, sys
_, cmd, set_dir, out, *rest = sys.argv
os.makedirs(out)
held = {{n: os.readlink(os.path.join(set_dir, n)) for n in os.listdir(set_dir)}}
with open(os.path.join(out, "call.json"), "w") as f:
    json.dump({{"cmd": cmd, "rest": rest, "held": held}}, f)
for name in {files}:
    open(os.path.join(out, name), "w").close()
if os.environ.get("FAIL_SET") and os.environ["FAIL_SET"] in set_dir:
    sys.exit(3)
"""


class BuildSearchIndexesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = os.path.join(self.tmp.name, "output")
        with open(TEMPLATE) as f:
            self.cfg = json.load(f)
        self.binary = os.path.join(self.tmp.name, "place-search")
        with open(self.binary, "w") as f:
            f.write(FAKE.format(python=sys.executable, files=self.cfg["search_index"]["files"]))
        os.chmod(self.binary, 0o755)

    def write_output(self, regions):
        """Every file of every component in `regions`, as the builder writes them."""
        os.makedirs(self.out)
        with open(os.path.join(self.out, "configurations.json"), "w") as f:
            json.dump(self.cfg, f)
        for region in regions:
            for cid, component in self.cfg["components"].items():
                axis, value = cid.split(".", 1)
                sel = {"region": region, axis: value}
                for path in component.get("files", []):
                    path = os.path.join(self.out, sut.substitute(path, sel))
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w") as f:
                        f.write(path)

    def run_main(self, *args, env=None):
        argv = ["build_search_indexes.py", self.out, self.binary, "--jobs", "2", *args]
        with mock.patch.object(sys, "argv", argv), mock.patch.dict(os.environ, {"TMPDIR": self.tmp.name, **(env or {})}):
            with self.assertRaises(SystemExit) as exit_:
                sut.main()
        return exit_.exception.code

    def call(self, region, sel):
        index_dir = os.path.join(self.out, sut.substitute(self.cfg["search_index"]["dir"], dict(sel, region=region)))
        with open(os.path.join(index_dir, "call.json")) as f:
            return json.load(f)

    def test_builds_each_shipped_selection_in_every_region_from_its_files(self):
        self.write_output(["planet", "europe"])

        code = self.run_main("--threads", "3")

        self.assertEqual(code, 0)
        for region in ("planet", "europe"):
            for sel in self.cfg["search_index"]["shipped"]:
                call = self.call(region, sel)
                paths, optional = sut.resolve(self.cfg, dict(sel, region=region))
                self.assertEqual(call["cmd"], "build")
                self.assertEqual(call["rest"][:2], ["--threads", "3"])
                # The fixture writes no optional file.
                self.assertEqual(sorted(call["held"]), sorted({os.path.basename(p) for p in paths if p not in optional}))
                self.assertEqual(call["held"]["admin_vertices.bin"],
                                 os.path.join(self.out, region, "quality", sel["quality"], "admin_vertices.bin"))
                self.assertEqual(call["held"]["poi_records.bin"],
                                 os.path.join(self.out, region, "poi", sel["poi_tier"], "poi_records.bin"))
        self.assertFalse([d for d in os.listdir(self.tmp.name) if d.startswith("search-indexes-")])

    def test_a_failed_build_fails_the_run_and_the_others_still_build(self):
        self.write_output(["planet", "europe"])

        code = self.run_main(env={"FAIL_SET": "/europe/"})

        self.assertEqual(code, 1)
        for sel in self.cfg["search_index"]["shipped"]:
            self.assertEqual(self.call("planet", sel)["cmd"], "build")
            failed = sut.substitute(self.cfg["search_index"]["dir"], dict(sel, region="europe"))
            self.assertFalse(os.path.exists(os.path.join(self.out, failed)))

    def test_a_missing_file_of_the_selection_fails_its_build(self):
        self.write_output(["planet"])
        os.remove(os.path.join(self.out, "planet", "poi", "all", "strings_poi.bin"))

        self.assertEqual(self.run_main(), 1)


class ResolveTest(unittest.TestCase):
    CFG = {"components": {
        "mode.small": {"files": ["{region}/small/a.bin", "{region}/small/shared.json"],
                       "optional_files": ["{region}/small/opt.bin"],
                       "replaces": ["{region}/quality/{quality}/q.bin"]},
        "quality.q1": {"extends": "quality.base", "files": ["{region}/quality/q1/q.bin"]},
        "quality.base": {"files": ["{region}/quality/base.bin"]},
        "poi_tier.none": {"files": []},
    }}
    SEL = {"region": "planet", "mode": "small", "quality": "q1", "poi_tier": "none"}

    def test_takes_extended_and_optional_files_minus_the_replaced(self):
        paths, optional = sut.resolve(self.CFG, self.SEL)

        self.assertEqual(paths, ["planet/quality/base.bin", "planet/small/a.bin", "planet/small/opt.bin",
                                 "planet/small/shared.json"])
        self.assertEqual(optional, {"planet/small/opt.bin"})

    def test_links_a_set_without_a_missing_optional_file_but_not_without_a_required_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            for rel in ("planet/quality/base.bin", "planet/small/a.bin", "planet/small/shared.json"):
                os.makedirs(os.path.dirname(os.path.join(out, rel)), exist_ok=True)
                open(os.path.join(out, rel), "w").close()
            paths, optional = sut.resolve(self.CFG, self.SEL)

            sut.link_set(out, paths, optional, os.path.join(tmp, "set"))
            os.remove(os.path.join(out, "planet/small/a.bin"))

            self.assertEqual(sorted(os.listdir(os.path.join(tmp, "set"))), ["a.bin", "base.bin", "shared.json"])
            with self.assertRaises(FileNotFoundError):
                sut.link_set(out, paths, optional, os.path.join(tmp, "set2"))


if __name__ == "__main__":
    unittest.main()
