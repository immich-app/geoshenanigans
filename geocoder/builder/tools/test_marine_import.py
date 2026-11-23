#!/usr/bin/env python3
"""Tests for marine_import.py. Run: python3 -m unittest discover -s geocoder/builder/tools

The file format's tests need nothing past the standard library. The geometry's need shapely, numpy and
pyogrio, as `build` does: they run where those are (the marine workflow installs them before it builds),
and are skipped elsewhere."""
import contextlib
import importlib.util
import io
import json
import os
import struct
import sys
import tempfile
import time
import tracemalloc
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import marine_import  # noqa: E402

HAS_GEOMETRY = all(importlib.util.find_spec(m) for m in ("shapely", "numpy", "pyogrio"))

# Two areas, by id: the ocean 1902 of two polygons (a triangle; a square with a triangular hole) and the sea
# 2350 of one triangle. The builder's tests read the same bytes (tests/marine_test_file.h).
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
            for read in (marine_import.read_marine, marine_import.stats):
                with self.subTest(why, read=read.__name__), contextlib.redirect_stdout(io.StringIO()), \
                        self.assertRaisesRegex(ValueError, why):
                    read(self.path)

    def test_stats_counts_each_objects_polygons_rings_and_vertices_and_their_total(self):
        marine_import.write_marine(self.path, TWO_AREAS)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            marine_import.stats(self.path)
        self.assertEqual(out.getvalue().splitlines(), [
            "1902\tocean\tNorth Atlantic Ocean\t2 polygons\t3 rings\t10 vertices",
            "2350\tsea\tNorth Sea\t1 polygons\t1 rings\t3 vertices",
            "total\t2 objects\t3 polygons\t4 rings\t13 vertices",
        ])

    def test_stats_holds_a_ring_at_a_time_not_the_file(self):
        # 200 rings of 2,000 vertices, 3.2 MB of them: stats holds a ring's at most.
        ring = [(k, k) for k in range(2000)]
        marine_import.write_marine(self.path, [(1, marine_import.KIND_SEA, "North Sea", [[ring]] * 200)])
        tracemalloc.start()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                marine_import.stats(self.path)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, os.path.getsize(self.path) // 10)

    def test_the_oceans_are_the_areas_named_ocean(self):
        self.assertEqual(marine_import.kind_of("North Atlantic Ocean"), marine_import.KIND_OCEAN)
        self.assertEqual(marine_import.kind_of("Southern Ocean"), marine_import.KIND_OCEAN)
        self.assertEqual(marine_import.kind_of("North Sea"), marine_import.KIND_SEA)
        self.assertEqual(marine_import.kind_of("Mediterranean Sea - Eastern Basin"), marine_import.KIND_SEA)
        self.assertEqual(marine_import.kind_of("Bay of Biscay"), marine_import.KIND_SEA)


class BoundsTest(unittest.TestCase):
    def test_a_bbox_that_is_not_four_edges_in_order_is_refused_before_the_build(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "marine_areas.bin")
            for bbox, said in [("10,50,12", "four"), ("10,50,12,51,13", "four"), ("10,50,east,51", "four"),
                               ("10,50,nan,51", "four"), ("12,50,10,51", "antimeridian"),
                               ("170,50,-170,51", "antimeridian"), ("-181,50,12,51", "antimeridian"),
                               ("10,51,12,50", "south"), ("10,-91,12,50", "south")]:
                with self.subTest(bbox=bbox):
                    err = io.StringIO()
                    with contextlib.redirect_stderr(err), self.assertRaises(SystemExit):
                        marine_import.main(["build", "--eez-iho", os.path.join(d, "eez_iho.geojson"), "--land",
                                            os.path.join(d, "land.zip"), "-o", out, f"--bbox={bbox}"])
                    self.assertIn("--bbox", err.getvalue())
                    self.assertIn(said, err.getvalue())
                    self.assertFalse(os.path.exists(out))


def feature(props, rings):
    return {"type": "Feature", "properties": props, "geometry": {"type": "Polygon", "coordinates": rings}}


def square(x0, y0, x1, y1):
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]


def split_holed_parts_last(item):
    """marine_import's split of a sea area part into tiles, a part with a hole held back long enough that the
    workers finish the others first."""
    import shapely
    if shapely.get_num_interior_rings(shapely.from_wkb(item[1])):
        time.sleep(0.5)
    return SPLIT_INTO_TILES(item)


SPLIT_INTO_TILES = marine_import._split_into_tiles


def geometry(polygons):
    """An object's polygons, as the file holds them, as one shapely geometry in degrees."""
    import shapely
    return shapely.union_all([shapely.Polygon([(lon / 1e7, lat / 1e7) for lat, lon in rings[0]],
                                              [[(lon / 1e7, lat / 1e7) for lat, lon in h] for h in rings[1:]])
                              for rings in polygons])


@unittest.skipUnless(HAS_GEOMETRY, "needs shapely, numpy and pyogrio")
class BuildTest(unittest.TestCase):
    """A world of land but for the water of 10-12 east by 50-50.8 north, two tiles of it, with an island; two
    sea areas over that water (the west one in two EEZs) whose shores stop short of the land's; and a lake the
    land holds far from any sea area."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        d = self.dir.name
        # Each feature's fields in the other order from the WFS's (iho_mrgid, then iho_sea).
        seas = [
            # The west sea in two EEZs, together 10.0-10.5 x 50.0-50.7: short of the coast at 50.8, and with a
            # hole round the island that's bigger than it.
            feature({"iho_sea": "North Sea", "iho_mrgid": 2350},
                    [square(10.0, 50.0, 10.3, 50.7), square(10.1, 50.2, 10.25, 50.35)]),
            feature({"iho_sea": "North Sea", "iho_mrgid": 2350}, [square(10.3, 50.0, 10.5, 50.7)]),
            # The east sea, 10.5-11.0 x 50.0-50.7, reaching into the next tile east.
            feature({"iho_sea": "North Atlantic Ocean", "iho_mrgid": 1902}, [square(10.5, 50.0, 11.5, 50.7)]),
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

    def write(self, name, features):
        path = os.path.join(self.dir.name, name)
        with open(path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        return path

    def run_build(self, out, stats, workers=2, seas=None, land=None, more=()):
        with open(os.devnull, "w") as quiet:
            saved, sys.stdout = sys.stdout, quiet
            try:
                return marine_import.main(["build", "--eez-iho", seas or self.seas, "--land", land or self.land,
                                           "-o", out, "--stats", stats, "--workers", str(workers), *more])
            finally:
                sys.stdout = saved

    def build(self, name, workers=2, seas=None, land=None, more=()):
        out = os.path.join(self.dir.name, name)
        stats = out + ".json"
        self.assertEqual(self.run_build(out, stats, workers, seas, land, more), 0)
        with open(stats) as f:
            return out, json.load(f)

    def test_the_seas_take_the_water_off_the_land_tile_by_tile(self):
        import shapely
        path, summary = self.build("marine_areas.bin")
        objects = {o[0]: o for o in marine_import.read_marine(path)}
        self.assertEqual(sorted(objects), [1902, 2350])
        self.assertEqual(objects[2350][1:3], (marine_import.KIND_SEA, "North Sea"))
        self.assertEqual(objects[1902][1:3], (marine_import.KIND_OCEAN, "North Atlantic Ocean"))

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

    def test_each_tiles_pieces_keep_the_parts_order_whatever_order_the_workers_finish_in(self):
        import shapely

        def pieces():
            return [(key, mrgid, [shapely.to_wkb(g) for g in geoms])
                    for key, by_mrgid in marine_import._SEA_TILES.items() for mrgid, geoms in by_mrgid.items()]

        one, _ = self.build("one.bin", workers=1)
        in_order = pieces()
        # The west sea's holed part, the costliest, goes first and finishes last.
        with mock.patch.object(marine_import, "_split_into_tiles", split_holed_parts_last):
            many, _ = self.build("many.bin", workers=3)
        self.assertEqual(pieces(), in_order)
        with open(one, "rb") as a, open(many, "rb") as b:
            self.assertEqual(a.read(), b.read())

    def test_a_repaired_feature_keeps_the_polygons_its_repair_nests(self):
        import shapely
        # A bowtie with a spike: GEOS repairs it into a collection of the bowtie's two triangles, as one
        # multipolygon, and the spike's line.
        bowtie = shapely.from_wkt("POLYGON ((10 50, 10.2 50.2, 10.2 50, 10 50.2, 10 50, 9.9 49.9, 10 50))")
        self.assertEqual(shapely.get_type_id(shapely.get_parts(shapely.make_valid(bowtie))).tolist(), [6, 1])
        parts = marine_import._valid_parts((2350, shapely.to_wkb(bowtie)))
        self.assertEqual([m for m, _ in parts], [2350, 2350])
        polygons = [shapely.from_wkb(w) for _, w in parts]
        self.assertEqual([p.geom_type for p in polygons], ["Polygon", "Polygon"])
        self.assertAlmostEqual(sum(p.area for p in polygons), 0.02, places=12)

    def test_a_gap_bit_counts_the_border_it_shares_with_a_sea_area_once(self):
        import shapely
        # One tile of water. Sea 1 is two pieces that overlap along x = 10.42; sea 2 is the rest of the tile
        # but for a notch, 10.42-10.46 x 50.0-50.06, that no sea area covers. The notch shares 0.06 of its
        # border with sea 1 (once, though both its pieces run along it) and 0.10 with sea 2.
        seas = self.write("notch.geojson", [
            feature({"iho_mrgid": 1, "iho_sea": "West Sea"}, [square(10.0, 50.0, 10.42, 51.0)]),
            feature({"iho_mrgid": 1, "iho_sea": "West Sea"}, [square(10.3, 50.0, 10.42, 50.5)]),
            feature({"iho_mrgid": 2, "iho_sea": "East Sea"},
                    [[[10.42, 50.06], [10.46, 50.06], [10.46, 50.0], [11.0, 50.0], [11.0, 51.0], [10.42, 51.0],
                      [10.42, 50.06]]]),
        ])
        land = self.write("tile.geojson",
                          [feature({}, [square(5.0, 45.0, 15.0, 55.0), square(10.0, 50.0, 11.0, 51.0)])])
        path, _ = self.build("notch.bin", seas=seas, land=land)
        objects = {o[0]: geometry(o[3]) for o in marine_import.read_marine(path)}
        notch = shapely.Point(10.44, 50.03)
        self.assertTrue(objects[2].contains(notch))
        self.assertFalse(objects[1].intersects(notch))

    def test_a_bounded_run_gives_its_tiles_as_the_whole_run_does(self):
        # Water 10-12 east and 179 east to 179 west, 50-51 north; a sea area in each first tile, more than a
        # degree from the next tile's centre (and across the antimeridian from it, for the second).
        seas = self.write("near.geojson", [
            feature({"iho_mrgid": 10, "iho_sea": "West Sea"}, [square(10.0, 50.0, 10.45, 51.0)]),
            feature({"iho_mrgid": 20, "iho_sea": "Date Line Sea"}, [square(179.0, 50.0, 179.4, 51.0)]),
        ])
        land = self.write("bands.geojson", [
            feature({}, [square(5.0, 45.0, 15.0, 55.0), square(10.0, 50.0, 12.0, 51.0)]),
            feature({}, [square(170.0, 45.0, 180.0, 55.0), square(179.0, 50.0, 180.0, 51.0)]),
            feature({}, [square(-180.0, 45.0, -170.0, 55.0), square(-180.0, 50.0, -179.0, 51.0)]),
        ])
        whole, _ = self.build("whole.bin", seas=seas, land=land)
        for bbox in ("11.5,50.5,11.5,50.5", "-179.5,50.5,-179.5,50.5"):
            with self.subTest(bbox=bbox):
                west, south, east, north = (float(v) for v in bbox.split(","))

                def in_bbox(rings):
                    x = min(lon for lat, lon in rings[0]) // 10000000
                    y = min(lat for lat, lon in rings[0]) // 10000000
                    return west <= x + 0.5 <= east and south <= y + 0.5 <= north

                expected = [(oid, kind, name, [p for p in polygons if in_bbox(p)])
                            for oid, kind, name, polygons in marine_import.read_marine(whole)]
                expected = [o for o in expected if o[3]]
                self.assertEqual(len(expected), 1)
                bounded, _ = self.build("bounded.bin", seas=seas, land=land, more=[f"--bbox={bbox}"])
                self.assertEqual(marine_import.read_marine(bounded), expected)

    def test_a_run_of_the_world_without_land_is_refused_and_a_bounded_one_may_find_none(self):
        out = os.path.join(self.dir.name, "marine_areas.bin")
        line = {"type": "Feature", "properties": {},
                "geometry": {"type": "LineString", "coordinates": [[10.0, 50.0], [11.0, 51.0]]}}
        for why, features in [("no features", []), ("a line alone", [line])]:
            land = self.write("no-land.geojson", features)
            with self.subTest(why):
                with self.assertRaisesRegex(ValueError, "no land polygons"):
                    self.run_build(out, out + ".json", land=land)
                self.assertFalse(os.path.exists(out))
                # A bounded run's tiles can all lie offshore.
                _, summary = self.build("offshore.bin", land=land, more=["--bbox=10.5,50.5,10.5,50.5"])
                self.assertGreater(summary["polygons"], 0)

    def test_an_output_or_stats_path_that_is_a_source_is_refused(self):
        out = os.path.join(self.dir.name, "marine_areas.bin")
        os.symlink(self.dir.name, os.path.join(self.dir.name, "link"))
        held = {}
        for source in (self.seas, self.land):
            with open(source, "rb") as f:
                held[source] = f.read()
        for flag, source in (("--eez-iho", self.seas), ("--land", self.land)):
            alias = os.path.join(self.dir.name, "link", os.path.basename(source))
            for written, stats in ((alias, out + ".json"), (out, alias)):
                for path, data in held.items():
                    with open(path, "wb") as f:
                        f.write(data)
                with self.subTest(source=flag, output=written, stats=stats):
                    with self.assertRaisesRegex(ValueError, f"is a source, {flag} "):
                        self.run_build(written, stats)
                    with open(source, "rb") as f:
                        self.assertEqual(f.read(), held[source])
                    self.assertFalse(os.path.exists(out))

    def test_a_stats_path_that_is_the_output_is_refused(self):
        out = os.path.join(self.dir.name, "marine_areas.bin")
        os.symlink(self.dir.name, os.path.join(self.dir.name, "link"))
        for stats in (out, os.path.join(self.dir.name, ".", "marine_areas.bin"),
                      os.path.join(self.dir.name, "link", "marine_areas.bin")):
            with self.subTest(stats=stats), self.assertRaisesRegex(ValueError, "--stats"):
                self.run_build(out, stats)
            self.assertFalse(os.path.exists(out))


if __name__ == "__main__":
    unittest.main()
