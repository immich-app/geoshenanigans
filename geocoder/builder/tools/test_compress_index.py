#!/usr/bin/env python3
"""Tests for compress_index.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compress_index as sut  # noqa: E402

TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "configurations.template.json")

# geocoder-transport as the script runs it. encode writes the kind, the members' names and sizes
# (a JSON line), then the members' bytes; verify decodes that (through zstd) against the dir's
# files and fails for a unit whose path ends with one of $FAKE_VERIFY_FAILS (":"-separated). Each
# call is logged to $FAKE_TRANSPORT_LOG.
FAKE_TRANSPORT = r'''#!{python}
import json, os, subprocess, sys
MEMBERS = {{"places": ["place_cells.bin", "place_nodes.bin", "place_hidden.bin"],
           "pois": ["poi_records.bin", "poi_vertices.bin"], "arcs": ["arc_index.bin", "arc_vertices.bin"]}}
with open(os.environ["FAKE_TRANSPORT_LOG"], "a") as f:
    f.write(" ".join(sys.argv[1:]) + "\n")
if sys.argv[1] == "encode":
    kind, d, names = sys.argv[2], sys.argv[3], sys.argv[4:] or MEMBERS[sys.argv[2]]
    head = {{"kind": kind, "members": [[n, os.path.getsize(os.path.join(d, n))] for n in names]}}
    sys.stdout.buffer.write(json.dumps(head).encode() + b"\n")
    for n in names:
        with open(os.path.join(d, n), "rb") as f:
            sys.stdout.buffer.write(f.read())
    sys.exit(0)
unit, d = sys.argv[2], sys.argv[3]
if any(f and unit.endswith(f) for f in os.environ.get("FAKE_VERIFY_FAILS", "").split(":")):
    sys.exit("geocoder-transport: place_nodes.bin differs from the unit's at byte 7")
data = subprocess.run(["zstd", "-dc", unit], check=True, capture_output=True).stdout
line, body = data.split(b"\n", 1)
head = json.loads(line)
for name, size in head["members"]:
    with open(os.path.join(d, name), "rb") as f:
        if f.read() != body[:size]:
            sys.exit(f"geocoder-transport: {{name}} differs")
    body = body[size:]
print("verified " + head["kind"] + " unit")
'''


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def unzstd(path):
    return subprocess.run(["zstd", "-dc", path], check=True, capture_output=True).stdout


def unit_members(path):
    """The members a unit of the fake transport holds: {name: bytes}."""
    line, body = unzstd(path).split(b"\n", 1)
    members = {}
    for name, size in json.loads(line)["members"]:
        members[name], body = body[:size], body[size:]
    return members


def content(seed, n=200_000):
    return bytes((seed * 31 + i * 7 + i // 251) % 256 for i in range(n))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load_template():
    with open(TEMPLATE) as f:
        return json.load(f)


class OutputTest(unittest.TestCase):
    """An output dir of a few regions' files, its units, and the fake transport."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = os.path.join(self.tmp.name, "output")
        os.makedirs(self.out)
        with open(os.path.join(self.out, "configurations.json"), "w") as f:
            json.dump({"version": 1, "transport": load_template()["transport"]}, f)
        self.transport = os.path.join(self.tmp.name, "geocoder-transport")
        with open(self.transport, "w") as f:
            f.write(FAKE_TRANSPORT.format(python=sys.executable))
        os.chmod(self.transport, 0o755)
        self.log = os.path.join(self.tmp.name, "transport.log")
        self.env = mock.patch.dict(os.environ, {"FAKE_TRANSPORT_LOG": self.log, "FAKE_VERIFY_FAILS": ""})
        self.env.start()
        places = {"place_cells.bin": content(10, 3000), "place_nodes.bin": content(11), "place_hidden.bin": content(12, 500)}
        self.files = {
            "planet/full/geo_cells.bin": content(1),
            "planet/no-addresses/geo_cells.bin": content(1),
            "planet/full/strings_layout.json": b'{"tiers": []}',
            "planet/full/addr_points.bin": content(2),
            "planet/full/street_ways.bin": content(5),
            "planet/admin/postcode_centroids.bin": content(6, 1600),
            "planet/quality/simplified/admin_polygons.bin": content(7, 1600),
            "planet/quality/exact/admin_polygons.bin": content(7, 1600),
            "planet/quality/simplified/arc_index.bin": content(8, 400),
            "planet/quality/simplified/arc_vertices.bin": content(9),
            "planet/quality/exact/arc_index.bin": content(13, 400),
            "planet/quality/exact/arc_vertices.bin": content(14),
            "planet/poi/major/poi_records.bin": content(15),
            "planet/poi/major/poi_vertices.bin": content(16),
            "planet/poi/major/poi_meta.json": b'{"categories": []}',
            "planet/search/full/simplified/all/places.fst": content(4),
            "planet/search/full/simplified/all/search_meta.json": b'{"dataset": {}}',
        }
        for d in ("planet/admin", "planet/full"):
            self.files.update({f"{d}/{name}": data for name, data in places.items()})
        for rel, data in self.files.items():
            write(os.path.join(self.out, rel), data)
        write(os.path.join(self.out, "planet/full/ways.osm_ids"), content(3))
        # Each unit and the files it holds.
        self.units = {
            "planet/admin/places.tt.zst": ("places", [f"planet/admin/{n}" for n in places]),
            "planet/full/places.tt.zst": ("places", [f"planet/full/{n}" for n in places]),
            "planet/full/street_ways.tt.zst": ("cols", ["planet/full/street_ways.bin"]),
            "planet/quality/simplified/admin_polygons.tt.zst": ("cols", ["planet/quality/simplified/admin_polygons.bin"]),
            "planet/quality/exact/admin_polygons.tt.zst": ("cols", ["planet/quality/exact/admin_polygons.bin"]),
            "planet/quality/simplified/arcs.tt.zst": ("arcs", ["planet/quality/simplified/arc_index.bin",
                                                               "planet/quality/simplified/arc_vertices.bin"]),
            "planet/quality/exact/arcs.tt.zst": ("arcs", ["planet/quality/exact/arc_index.bin",
                                                          "planet/quality/exact/arc_vertices.bin"]),
            "planet/poi/major/pois.tt.zst": ("pois", ["planet/poi/major/poi_records.bin", "planet/poi/major/poi_vertices.bin"]),
        }
        self.members = {m: u for u, (_, members) in self.units.items() for m in members}

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def path(self, rel):
        return os.path.join(self.out, rel)

    def run_main(self, *args, transport=None):
        argv = ["compress_index.py", self.out, "--transport-bin", transport or self.transport, *args]
        with mock.patch.object(sys, "argv", argv):
            self.status = sut.main()
        with open(self.path("configurations.json")) as f:
            return json.load(f)

    def transport_calls(self, verb):
        with open(self.log) as f:
            return [line.split()[1:] for line in f.read().splitlines() if line.split()[0] == verb]


class CompressIndexTest(OutputTest):
    def test_compresses_each_content_once_and_links_its_copies(self):
        cfg = self.run_main()

        self.assertEqual(self.status, 0)
        self.assertEqual(sorted(cfg["files"]), sorted(self.files))
        for rel, data in self.files.items():
            if rel in self.members:
                continue
            self.assertEqual(unzstd(self.path(rel) + ".zst"), data, rel)
            self.assertEqual(cfg["files"][rel], {"size_zst": os.path.getsize(self.path(rel) + ".zst"), "size_raw": len(data),
                                                 "sha256": sha(data)})
        full = os.stat(self.path("planet/full/geo_cells.bin.zst"))
        copy = os.stat(self.path("planet/no-addresses/geo_cells.bin.zst"))
        self.assertEqual(full.st_ino, copy.st_ino)
        self.assertFalse(os.path.exists(self.path("planet/full/ways.osm_ids.zst")))
        self.assertEqual(cfg["version"], 1)

    def test_a_dir_holding_a_units_files_publishes_them_in_the_unit_alone(self):
        cfg = self.run_main()

        self.assertEqual(sorted(cfg["units"]), sorted(self.units))
        for unit, (kind, members) in self.units.items():
            self.assertEqual(cfg["units"][unit], {"kind": kind, "transport_version": 1, "size": os.path.getsize(self.path(unit)),
                                                  "files": members}, unit)
            self.assertEqual(unit_members(self.path(unit)), {os.path.basename(m): self.files[m] for m in members}, unit)
            for m in members:
                self.assertEqual(cfg["files"][m], {"size_raw": len(self.files[m]), "sha256": sha(self.files[m]), "unit": unit}, m)
                self.assertFalse(os.path.exists(self.path(m) + ".zst"), m)

    def test_each_unit_is_encoded_with_its_kind_and_verified_against_its_dir(self):
        self.run_main()

        encoded = sorted(self.transport_calls("encode"))
        self.assertEqual(encoded, sorted([
            ["places", self.path("planet/admin")],
            ["cols", self.path("planet/full"), "street_ways.bin"],
            ["cols", self.path("planet/quality/exact"), "admin_polygons.bin"],
            ["arcs", self.path("planet/quality/simplified")],
            ["arcs", self.path("planet/quality/exact")],
            ["pois", self.path("planet/poi/major")],
        ]))
        # A unit alike to one of a dir before it, in path order, is that one's.
        linked = {"planet/full/places.tt.zst", "planet/quality/simplified/admin_polygons.tt.zst"}
        self.assertEqual(sorted(self.transport_calls("verify")),
                         sorted([self.path(u), os.path.dirname(self.path(u))] for u in self.units if u not in linked))

    def test_a_unit_of_files_another_dir_sent_alike_is_linked_to_its_unit(self):
        self.run_main()

        for unit, copy in [("planet/admin/places.tt.zst", "planet/full/places.tt.zst"),
                           ("planet/quality/simplified/admin_polygons.tt.zst", "planet/quality/exact/admin_polygons.tt.zst")]:
            self.assertEqual(os.stat(self.path(unit)).st_ino, os.stat(self.path(copy)).st_ino, copy)

    def test_a_dir_lacking_one_of_a_units_files_publishes_the_others_as_zst(self):
        os.remove(self.path("planet/full/place_hidden.bin"))
        del self.files["planet/full/place_hidden.bin"]

        cfg = self.run_main()

        self.assertNotIn("planet/full/places.tt.zst", cfg["units"])
        self.assertFalse(os.path.exists(self.path("planet/full/places.tt.zst")))
        for name in ("place_cells.bin", "place_nodes.bin"):
            rel = f"planet/full/{name}"
            self.assertEqual(unzstd(self.path(rel) + ".zst"), self.files[rel])
            self.assertNotIn("unit", cfg["files"][rel])

    def test_a_unit_that_fails_its_verify_publishes_its_files_as_zst_and_fails_the_run(self):
        os.environ["FAKE_VERIFY_FAILS"] = "planet/admin/places.tt.zst:planet/poi/major/pois.tt.zst"

        cfg = self.run_main()

        self.assertEqual(self.status, sut.UNIT_FAILED)
        failed = ["planet/admin/places.tt.zst", "planet/full/places.tt.zst", "planet/poi/major/pois.tt.zst"]
        self.assertEqual(sorted(cfg["units"]), sorted(u for u in self.units if u not in failed))
        for unit in failed:
            self.assertFalse(os.path.exists(self.path(unit)), unit)
            for m in self.units[unit][1]:
                self.assertEqual(unzstd(self.path(m) + ".zst"), self.files[m], m)
                self.assertEqual(cfg["files"][m], {"size_zst": os.path.getsize(self.path(m) + ".zst"), "size_raw": len(self.files[m]),
                                                   "sha256": sha(self.files[m])}, m)
        # Files alike go as one .zst, as any others do.
        self.assertEqual(os.stat(self.path("planet/admin/place_nodes.bin.zst")).st_ino,
                         os.stat(self.path("planet/full/place_nodes.bin.zst")).st_ino)
        self.assertTrue(os.path.exists(self.path("planet/quality/exact/arcs.tt.zst")))

    def test_without_its_transport_cli_every_unit_falls_back_to_zst_and_fails_the_run(self):
        cfg = self.run_main(transport=os.path.join(self.tmp.name, "missing", "geocoder-transport"))

        self.assertEqual(self.status, sut.UNIT_FAILED)
        self.assertEqual(cfg["units"], {})
        for rel, data in self.files.items():
            self.assertEqual(unzstd(self.path(rel) + ".zst"), data, rel)
            self.assertIn("size_zst", cfg["files"][rel], rel)

    def test_a_unit_takes_the_place_of_its_files_zst_left_by_an_earlier_run(self):
        os.environ["FAKE_VERIFY_FAILS"] = "planet/poi/major/pois.tt.zst"
        self.run_main()
        os.environ["FAKE_VERIFY_FAILS"] = ""

        cfg = self.run_main()

        self.assertEqual(self.status, 0)
        self.assertIn("planet/poi/major/pois.tt.zst", cfg["units"])
        for m in self.units["planet/poi/major/pois.tt.zst"][1]:
            self.assertFalse(os.path.exists(self.path(m) + ".zst"), m)

    def test_streams_every_zst_and_unit_to_the_upload_command(self):
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = f"{sys.executable} -c \"import sys; open({log!r}, 'a').write(sys.stdin.read())\""

        self.run_main("--upload", "s3://bucket/day/", "--upload-cmd", recorder)

        with open(log) as f:
            sent = f.read().splitlines()
        sent_rel = [f"{rel}.zst" for rel in self.files if rel not in self.members] + list(self.units)
        expected = [f"cp {self.path(rel)} s3://bucket/day/{rel}" for rel in sent_rel]
        self.assertEqual(sorted(sent), sorted(expected))

    def test_a_water_dirs_json_files_are_published_with_its_index_files(self):
        water = {"planet/water/all/exact/water_meta.json": b'{"types": []}',
                 "planet/water/all/exact/attribution.json": b'[{"id": "osm"}]',
                 "planet/water/all/exact/strings_layout.json": b'{"tiers": []}',
                 "planet/water/all/exact/admin_cells.bin": content(20, 900)}
        for rel, data in water.items():
            write(self.path(rel), data)
        write(self.path("planet/water/all/exact/admin_polygons.fallback.osm_ids"), content(21, 64))
        log = os.path.join(self.tmp.name, "upload.log")
        recorder = f"{sys.executable} -c \"import sys; open({log!r}, 'a').write(sys.stdin.read())\""

        cfg = self.run_main("--upload", "s3://bucket/day/", "--upload-cmd", recorder)

        for rel, data in water.items():
            self.assertEqual(unzstd(self.path(rel) + ".zst"), data, rel)
            self.assertEqual(cfg["files"][rel]["sha256"], sha(data), rel)
        self.assertNotIn("planet/water/all/exact/admin_polygons.fallback.osm_ids", cfg["files"])
        # And each goes up with the rest.
        with open(log) as f:
            sent = f.read().splitlines()
        for rel in water:
            self.assertIn(f"cp {self.path(rel)}.zst s3://bucket/day/{rel}.zst", sent, rel)
        self.assertFalse([line for line in sent if "fallback.osm_ids" in line], sent)


class ComponentSizesTest(OutputTest):
    """configurations.json's component_sizes: each component's raw and wire bytes in each region."""

    COMPONENTS = {
        "mode.full": {"files": ["{region}/full/geo_cells.bin", "{region}/full/street_ways.bin", "{region}/full/place_cells.bin",
                                "{region}/full/place_nodes.bin", "{region}/full/place_hidden.bin",
                                "{region}/full/strings_layout.json"],
                      "optional_files": ["{region}/full/interp_postcodes.bin", "{region}/full/addr_postcodes.bin"]},
        "quality.simplified": {"files": ["{region}/quality/simplified/admin_polygons.bin",
                                         "{region}/quality/simplified/admin_rings.bin",
                                         "{region}/quality/simplified/arc_index.bin",
                                         "{region}/quality/simplified/arc_vertices.bin"]},
        "half": {"files": ["{region}/quality/exact/arc_index.bin"]},
        "poi_tier.none": {"files": []},
        "mode.admin-minimal": {"variants": [{"component": "half"}]},
    }

    def setUp(self):
        super().setUp()
        with open(self.path("configurations.json")) as f:
            cfg = json.load(f)
        cfg["axes"] = {"region": {"values": {"planet": {}, "europe": {}}}}
        cfg["components"] = self.COMPONENTS
        with open(self.path("configurations.json"), "w") as f:
            json.dump(cfg, f)
        # One of mode.full's optional files the build has; its other one it hasn't.
        write(self.path("planet/full/interp_postcodes.bin"), content(17))

    def test_each_component_counts_its_files_raw_and_each_unit_once_on_the_wire(self):
        cfg = self.run_main()
        sizes = cfg["component_sizes"]["planet"]
        files, units = cfg["files"], cfg["units"]

        full = [f"planet/full/{n}" for n in ("geo_cells.bin", "street_ways.bin", "place_cells.bin", "place_nodes.bin",
                                             "place_hidden.bin", "strings_layout.json", "interp_postcodes.bin")]
        self.assertNotIn("planet/full/addr_postcodes.bin", files)
        self.assertEqual(sizes["mode.full"], {
            "raw": sum(files[f]["size_raw"] for f in full),
            "wire": files["planet/full/geo_cells.bin"]["size_zst"] + files["planet/full/strings_layout.json"]["size_zst"]
            + files["planet/full/interp_postcodes.bin"]["size_zst"]
            + units["planet/full/street_ways.tt.zst"]["size"] + units["planet/full/places.tt.zst"]["size"]})
        # The files the build has (no admin_rings.bin here), the arcs unit once for its two files.
        simplified = [f"planet/quality/simplified/{n}" for n in ("admin_polygons.bin", "arc_index.bin", "arc_vertices.bin")]
        self.assertEqual(sizes["quality.simplified"], {
            "raw": sum(files[f]["size_raw"] for f in simplified),
            "wire": units["planet/quality/simplified/admin_polygons.tt.zst"]["size"]
            + units["planet/quality/simplified/arcs.tt.zst"]["size"]})
        # A component of some of a unit's files: its share of the unit, by their raw bytes.
        arcs = units["planet/quality/exact/arcs.tt.zst"]
        index = files["planet/quality/exact/arc_index.bin"]["size_raw"]
        vertices = files["planet/quality/exact/arc_vertices.bin"]["size_raw"]
        self.assertEqual(sizes["half"], {"raw": index, "wire": round(arcs["size"] * index / (index + vertices))})
        # None for a component of no files, or of none the region has.
        self.assertEqual(sorted(sizes), ["half", "mode.full", "quality.simplified"])
        self.assertNotIn("europe", cfg["component_sizes"])

    def test_a_unit_that_failed_counts_its_files_zst(self):
        os.environ["FAKE_VERIFY_FAILS"] = "planet/quality/simplified/arcs.tt.zst"

        cfg = self.run_main()

        files = cfg["files"]
        self.assertEqual(cfg["component_sizes"]["planet"]["quality.simplified"]["wire"],
                         cfg["units"]["planet/quality/simplified/admin_polygons.tt.zst"]["size"]
                         + files["planet/quality/simplified/arc_index.bin"]["size_zst"]
                         + files["planet/quality/simplified/arc_vertices.bin"]["size_zst"])


if __name__ == "__main__":
    unittest.main()
