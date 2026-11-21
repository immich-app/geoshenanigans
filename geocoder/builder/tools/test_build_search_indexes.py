#!/usr/bin/env python3
"""Tests for build_search_indexes.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import itertools
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_search_indexes as sut  # noqa: E402

TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "configurations.template.json")
PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "test-portal")
SERVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "server")
# The files a dir's admin geometry is read from (server/src/admin_geometry.rs, ADMIN_GEOMETRY_FILES).
ADMIN_GEOMETRY = ["admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin"]

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
                self.assertEqual(call["held"]["arc_vertices.bin"],
                                 os.path.join(self.out, region, "quality", sel["quality"], "arc_vertices.bin"))
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


def load_template():
    with open(TEMPLATE) as f:
        return json.load(f)


def allowed(cfg, sel):
    """Whether `sel` keeps the template's constraints: a selection with every value a constraint's
    `when` names has every value its `requires` names."""
    def has(values):
        return all(sel.get(axis) == value for axis, value in values.items())
    return all(not has(c["when"]) or has(c["requires"]) for c in cfg["constraints"])


def selections(cfg, region="planet"):
    """Every mode × quality × poi_tier the template's constraints allow, in `region`."""
    axes = [list(cfg["axes"][axis]["values"]) for axis in sut.AXES]
    every = (dict(zip(sut.AXES, values), region=region) for values in itertools.product(*axes))
    return [sel for sel in every if allowed(cfg, sel)]


def flat(paths):
    """The dir a flat download takes each file name from: the last path of that name, as a client
    downloads in path order."""
    return {os.path.basename(p): os.path.dirname(p) for p in sorted(paths)}


class TemplateTest(unittest.TestCase):
    MINIMAL_FILES = ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_rings.bin",
                     "arc_index.bin", "arc_vertices.bin", "place_cells.bin", "place_hidden.bin", "place_nodes.bin",
                     "strings_core.bin", "strings_layout.json"]

    def test_admin_minimal_takes_its_poi_dir_beside_a_poi_tier(self):
        # admin-minimal/ holds strings of its own, which a POI tier's names and parent polygons don't
        # point into: beside a POI tier the mode takes admin-minimal-poi/, on the shared slots and strings.
        cfg = load_template()
        minimal = [sel for sel in selections(cfg) if sel["mode"] == "admin-minimal"]
        self.assertEqual(len(minimal), len(cfg["axes"]["poi_tier"]["values"]))
        for sel in minimal:
            paths, _ = sut.resolve(cfg, sel)
            own, poi = "planet/admin-minimal/", "planet/admin-minimal-poi/"
            dir_, other = (own, poi) if sel["poi_tier"] == "none" else (poi, own)

            self.assertEqual([p[len(dir_):] for p in paths if p.startswith(dir_)], self.MINIMAL_FILES, sel)
            self.assertEqual([p for p in paths if p.startswith(other) or p.startswith("planet/quality/")], [], sel)

    def test_every_selection_takes_each_admin_geometry_file_once_from_its_dir(self):
        # Downloads land flat by name: a second copy of an admin geometry file, or a .bin name twice,
        # would mix two sets of arcs.
        cfg = load_template()
        presets = [p["selections"] for p in cfg["presets"]]
        for sel in selections(cfg) + presets:
            paths, _ = sut.resolve(cfg, sel)
            if sel["mode"] == "admin-minimal":
                dir_ = "planet/admin-minimal" if sel["poi_tier"] == "none" else "planet/admin-minimal-poi"
            else:
                dir_ = f"planet/quality/{sel['quality']}"
            names = [os.path.basename(p) for p in paths]

            for name in ADMIN_GEOMETRY:
                self.assertEqual([p for p in paths if os.path.basename(p) == name], [f"{dir_}/{name}"], sel)
            bins = [n for n in names if n.endswith(".bin")]
            self.assertEqual(len(bins), len(set(bins)), sel)
            self.assertEqual([n for n in names if n == "admin_vertices.bin" or n.startswith("postal_")], [], sel)

    def test_presets_take_their_boundaries_from_the_tier_they_name(self):
        # smallest: admin-minimal's own; recommended and with-addresses: simplified; max: exact.
        cfg = load_template()
        expected = {"smallest": "planet/admin-minimal", "immich-recommended": "planet/quality/simplified",
                    "with-addresses": "planet/quality/simplified", "max-quality": "planet/quality/exact"}
        got = {p["id"]: flat(sut.resolve(cfg, p["selections"])[0])["arc_vertices.bin"] for p in cfg["presets"]}

        self.assertEqual(got, expected)


class TestPortalTest(unittest.TestCase):
    """The test portal's image runs query-portal (server/src/portal.rs), which syncs the whole planet
    build, decoding its transport units in-process, and serves every selection of it."""

    # The portal syncs and serves only builds of the version whose files it reads: the template's.
    def test_the_build_version_it_syncs_is_the_templates(self):
        with open(os.path.join(SERVER, "src", "portal.rs")) as f:
            versions = re.findall(r"^pub const BUILD_VERSION: u32 = (\d+);$", f.read(), re.M)
        self.assertEqual(versions, [str(load_template()["build"]["version"])])

    # The bucket holds a unit's files only in the unit, which the portal fetches and decodes itself:
    # the image builds and ships it, and starts its entrypoint, which runs it.
    def test_the_image_runs_query_portal(self):
        with open(os.path.join(PORTAL, "Dockerfile")) as f:
            image = f.read()
        self.assertIn("COPY server/ server/\nCOPY search/ search/\n", image)
        self.assertIn("RUN cargo build --release --locked --manifest-path search/Cargo.toml --features portal --bin query-portal\n", image)
        self.assertIn("COPY --from=builder /src/search/target/release/query-portal /usr/local/bin/\n", image)
        self.assertIn("COPY test-portal/entrypoint.sh /usr/local/bin/entrypoint.sh\n", image)
        self.assertEqual(re.findall(r"(?m)^(?:ENTRYPOINT|CMD)\b.*$", image), ['ENTRYPOINT ["entrypoint.sh"]'])
        with open(os.path.join(PORTAL, "entrypoint.sh")) as f:
            self.assertRegex(f.read(), r'(?m)^exec query-portal "\$DATA_DIR" 0\.0\.0\.0:3000$')

    # One service serves every dataset on :3002 (the minimal server of :3001 is a column of the page), init
    # passing query-portal the stop signal, set only by what query-portal and the entrypoint read: its
    # environment as a mapping (NAME: value) or a list (- NAME=value, - NAME).
    def test_compose_runs_one_portal_on_3002(self):
        with open(os.path.join(PORTAL, "docker-compose.yml")) as f:
            services = f.read().split("\nvolumes:\n")[0]
        self.assertEqual(re.findall(r"(?m)^  ([\w-]+):$", services), ["geocoder-portal"])
        self.assertEqual(re.findall(r'(?m)^      - "(\d+):3000"$', services), ["3002"])
        self.assertRegex(services, r"(?m)^    init: true$")
        mapped = re.findall(r"(?m)^      ([A-Z][A-Z_]*):", services)
        listed = re.findall(r'(?m)^      - "?([A-Z][A-Z_]*)(?:=|"?$)', services)
        self.assertLessEqual(set(mapped + listed), {"DATA_DIR", "PORTAL_BASE_URL", "PORTAL_SYNC_SECONDS"})


class ResolveTest(unittest.TestCase):
    CFG = {"components": {
        "mode.small": {"files": ["{region}/small/a.bin", "{region}/small/shared.json"],
                       "optional_files": ["{region}/small/opt.bin"],
                       "replaces": ["{region}/quality/{quality}/q.bin"]},
        "quality.simplified": {"extends": "quality.base", "files": ["{region}/quality/simplified/q.bin"]},
        "quality.base": {"files": ["{region}/quality/base.bin"]},
        "poi_tier.none": {"files": []},
    }}
    SEL = {"region": "planet", "mode": "small", "quality": "simplified", "poi_tier": "none"}

    def test_takes_extended_and_optional_files_minus_the_replaced(self):
        paths, optional = sut.resolve(self.CFG, self.SEL)

        self.assertEqual(paths, ["planet/quality/base.bin", "planet/small/a.bin", "planet/small/opt.bin",
                                 "planet/small/shared.json"])
        self.assertEqual(optional, {"planet/small/opt.bin"})

    def test_takes_the_first_variant_the_selection_matches(self):
        cfg = {"components": {
            "mode.small": {"files": ["{region}/small/a.bin"],
                           "variants": [{"when": {"mode": "small", "poi_tier": "none"}, "component": "small/alone"},
                                        {"component": "small/beside"},
                                        {"component": "small/never"}]},
            "small/alone": {"files": ["{region}/small/b.bin"]},
            "small/beside": {"files": ["{region}/small-poi/b.bin"], "replaces": ["{region}/small/a.bin"]},
            "small/never": {"files": ["{region}/never.bin"]},
            "poi_tier.major": {"files": ["{region}/poi/{poi_tier}/p.bin"]},
        }}

        alone, _ = sut.resolve(cfg, dict(self.SEL, poi_tier="none"))
        beside, _ = sut.resolve(cfg, dict(self.SEL, poi_tier="major"))

        self.assertEqual(alone, ["planet/small/a.bin", "planet/small/b.bin"])
        self.assertEqual(beside, ["planet/poi/major/p.bin", "planet/small-poi/b.bin"])

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
