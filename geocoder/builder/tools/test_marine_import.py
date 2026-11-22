#!/usr/bin/env python3
"""Tests for marine_import.py. Run: python3 -m unittest discover -s geocoder/builder/tools

The file format's tests need nothing past the standard library. The geometry's need shapely, numpy and
pyogrio, as `build` does: they run where those are (the marine workflow installs them before it builds),
and are skipped elsewhere."""
import importlib.util
import json
import os
import struct
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import marine_import  # noqa: E402

HAS_GEOMETRY = all(importlib.util.find_spec(m) for m in ("shapely", "numpy", "pyogrio"))

# Two areas, by id: the ocean 1902 of two polygons (a triangle; a square with a triangular hole) and the sea
# 2350 of one triangle. The builder's tests/test_marine.cpp reads the same bytes.
TWO_AREAS = [
    (1902, marine_import.KIND_OCEAN, "North Atlantic Ocean", [
        [[(0, -300000000), (0, -200000000), (100000000, -250000000)]],
        [[(-10000000, -10000000), (-10000000, 10000000), (10000000, 10000000), (10000000, -10000000)],
         [(-5000000, -5000000), (5000000, -5000000), (0, 5000000)]],
    ]),
    (2350, marine_import.KIND_SEA, "North Sea", [[[(530000000, 30000000), (530000000, 40000000), (540000000, 35000000)]]]),
]
TWO_AREAS_HEX = (
    "47434d4152494e4501000000020000006e070000000000000214004e6f7274682041746c616e746963204f6365616e020000000100000003"
    "00000000000000005d1eee0000000000e1f50500e1f505800f05fd0200000004000000806967ff806967ff00000000002d3101002d310100"
    "0000000000000000d3cefe03000000c0b4b3ffc0b4b3ff8096980000000000c0b4b3ff809698002e090000000000000109004e6f72746820"
    "5365610100000001000000030000008028971f80c3c901000000008096980080969800c0b4b3ff")


class FileTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "marine_areas.bin")

    def tearDown(self):
        self.dir.cleanup()

    def test_writes_the_bytes_the_builder_reads_by_id(self):
        marine_import.write_marine(self.path, list(reversed(TWO_AREAS)))
        with open(self.path, "rb") as f:
            self.assertEqual(f.read().hex(), TWO_AREAS_HEX)
        self.assertFalse(os.path.exists(self.path + ".part"))
        self.assertEqual(marine_import.read_marine(self.path), TWO_AREAS)

    def test_refuses_what_the_builder_would(self):
        sea = TWO_AREAS[1]
        cases = {
            "twice": [sea, sea],
            "a name of 0 bytes": [(1, 1, "", sea[3])],
            "kind 3": [(1, 3, "X", sea[3])],
            "a ring of 2 vertices": [(1, 1, "X", [[[(0, 0), (1, 1)]]])],
            "outside the world": [(1, 1, "X", [[[(0, 0), (1, 1), (900000001, 0)]]])],
            "a polygon of no rings": [(1, 1, "X", [[]])],
        }
        for why, objects in cases.items():
            with self.subTest(why), self.assertRaisesRegex(ValueError, why):
                marine_import.write_marine(self.path, objects)
            self.assertFalse(os.path.exists(self.path))

    def test_reads_refuse_a_file_cut_short_or_of_another_kind(self):
        marine_import.write_marine(self.path, TWO_AREAS)
        with open(self.path, "rb") as f:
            good = f.read()
        for why, data in [("cut short", good[:-1]), ("past its last object", good + b"x"),
                          ("not a marine areas file", b"GCMARINX" + good[8:])]:
            with open(self.path, "wb") as f:
                f.write(data)
            with self.subTest(why), self.assertRaisesRegex(ValueError, why):
                marine_import.read_marine(self.path)

    def test_the_oceans_are_the_areas_named_ocean(self):
        self.assertEqual(marine_import.kind_of("North Atlantic Ocean"), marine_import.KIND_OCEAN)
        self.assertEqual(marine_import.kind_of("Southern Ocean"), marine_import.KIND_OCEAN)
        self.assertEqual(marine_import.kind_of("North Sea"), marine_import.KIND_SEA)
        self.assertEqual(marine_import.kind_of("Mediterranean Sea - Eastern Basin"), marine_import.KIND_SEA)
        self.assertEqual(marine_import.kind_of("Bay of Biscay"), marine_import.KIND_SEA)


def feature(props, rings):
    return {"type": "Feature", "properties": props, "geometry": {"type": "Polygon", "coordinates": rings}}


def square(x0, y0, x1, y1):
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]


@unittest.skipUnless(HAS_GEOMETRY, "needs shapely, numpy and pyogrio")
class BuildTest(unittest.TestCase):
    """A world of land but for the water of 10-12 east by 50-50.8 north, two tiles of it, with an island; two
    sea areas over that water (the west one in two EEZs) whose shores stop short of the land's; and a lake the
    land holds far from any sea area."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        d = self.dir.name
        seas = [
            # The west sea in two EEZs, together 10.0-10.5 x 50.0-50.7: short of the coast at 50.8, and with a
            # hole round the island that's bigger than it.
            feature({"iho_mrgid": 2350, "iho_sea": "North Sea"},
                    [square(10.0, 50.0, 10.3, 50.7), square(10.1, 50.2, 10.25, 50.35)]),
            feature({"iho_mrgid": 2350, "iho_sea": "North Sea"}, [square(10.3, 50.0, 10.5, 50.7)]),
            # The east sea, 10.5-11.0 x 50.0-50.7, reaching into the next tile east.
            feature({"iho_mrgid": 1902, "iho_sea": "North Atlantic Ocean"}, [square(10.5, 50.0, 11.5, 50.7)]),
        ]
        land = [
            feature({}, [square(5.0, 45.0, 15.0, 55.0), square(10.0, 50.0, 12.0, 50.8)]),
            feature({}, [square(10.15, 50.25, 10.2, 50.3)]),
            # Water inside the coastline but in no sea area, far from them (the Caspian's case).
            feature({}, [square(30.0, 40.0, 32.0, 42.0), square(31.0, 41.0, 31.5, 41.5)]),
        ]
        self.seas = os.path.join(d, "eez_iho.geojson")
        self.land = os.path.join(d, "land.geojson")
        for path, features in [(self.seas, seas), (self.land, land)]:
            with open(path, "w") as f:
                json.dump({"type": "FeatureCollection", "features": features}, f)

    def tearDown(self):
        self.dir.cleanup()

    def build(self, name, workers=2):
        out = os.path.join(self.dir.name, name)
        stats = out + ".json"
        with open(os.devnull, "w") as quiet:
            saved, sys.stdout = sys.stdout, quiet
            try:
                rc = marine_import.main(["build", "--eez-iho", self.seas, "--land", self.land, "-o", out,
                                         "--stats", stats, "--workers", str(workers)])
            finally:
                sys.stdout = saved
        self.assertEqual(rc, 0)
        with open(stats) as f:
            return out, json.load(f)

    def test_the_seas_take_the_water_off_the_land_tile_by_tile(self):
        import shapely
        path, summary = self.build("marine_areas.bin")
        objects = {o[0]: o for o in marine_import.read_marine(path)}
        self.assertEqual(sorted(objects), [1902, 2350])
        self.assertEqual(objects[2350][1:3], (marine_import.KIND_SEA, "North Sea"))
        self.assertEqual(objects[1902][1:3], (marine_import.KIND_OCEAN, "North Atlantic Ocean"))

        def geometry(polygons):
            return shapely.union_all([shapely.Polygon([(lon / 1e7, lat / 1e7) for lat, lon in rings[0]],
                                                      [[(lon / 1e7, lat / 1e7) for lat, lon in h] for h in rings[1:]])
                                      for rings in polygons])

        west, east = geometry(objects[2350][3]), geometry(objects[1902][3])
        land = shapely.union_all([shapely.difference(shapely.box(5, 45, 15, 55), shapely.box(10, 50, 12, 50.8)),
                                  shapely.box(10.15, 50.25, 10.2, 50.3)])
        # Never on land, and every bit of the tiles' water is one sea's or the other's.
        self.assertAlmostEqual(shapely.intersection(west, land).area, 0, places=12)
        self.assertAlmostEqual(shapely.intersection(east, land).area, 0, places=12)
        self.assertAlmostEqual(shapely.intersection(west, east).area, 0, places=12)
        water = shapely.difference(shapely.box(10, 50, 12, 50.8), land)
        self.assertAlmostEqual(shapely.union_all([west, east]).area, water.area, places=9)
        # The strip the sea areas stop short of goes to the sea beside it, cut at the 0.1-degree grid: the
        # west sea's reaches the coast at 10.2, the east sea's at 10.8.
        self.assertTrue(west.contains(shapely.Point(10.2, 50.75)))
        self.assertTrue(east.contains(shapely.Point(10.8, 50.75)))
        # Round the island, the water the west sea's hole leaves is the west sea's.
        self.assertTrue(west.contains(shapely.Point(10.12, 50.22)))
        # The next tile east, which the east sea reaches into: its water nearer the east sea's than the west's.
        self.assertTrue(east.contains(shapely.Point(11.9, 50.75)))
        # No polygon spans two tiles; each outer ring counterclockwise, holes clockwise.
        for oid, kind, name, polygons in objects.values():
            for rings in polygons:
                lats = [lat for lat, lon in rings[0]]
                lons = [lon for lat, lon in rings[0]]
                self.assertLessEqual(max(lons) - min(lons), 10000000)
                self.assertLessEqual(max(lats) - min(lats), 10000000)
                for k, ring in enumerate(rings):
                    ccw = shapely.LinearRing([(lon, lat) for lat, lon in ring]).is_ccw
                    self.assertEqual(ccw, k == 0)
        # The lake far off is left out.
        self.assertGreater(summary["polygons"], 0)
        for polygons in (objects[1902][3], objects[2350][3]):
            for rings in polygons:
                self.assertTrue(all(lon < 200000000 for lat, lon in rings[0]))

    def test_the_file_is_the_same_on_one_worker_and_many(self):
        one, _ = self.build("one.bin", workers=1)
        many, _ = self.build("many.bin", workers=3)
        with open(one, "rb") as a, open(many, "rb") as b:
            self.assertEqual(a.read(), b.read())


if __name__ == "__main__":
    unittest.main()
