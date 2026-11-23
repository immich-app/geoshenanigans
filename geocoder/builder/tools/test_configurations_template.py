#!/usr/bin/env python3
"""Tests of configurations.template.json: its water axis and components, and the credit of its data. Run:
python3 -m unittest discover -s geocoder/builder/tools"""
import fnmatch
import json
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, "..", "configurations.template.json")
# The major tier's thresholds (water_classify.h) and the client's guide to the water dirs.
WATER_CLASSIFY = os.path.join(HERE, "..", "src", "water_classify.h")
PATCHING = os.path.join(HERE, "..", "..", "PATCHING.md")
# A water dir holding a sea as the water layer writes it (the builder's s2 tests check it is).
WATER_DIR_WITH_A_SEA = os.path.join(HERE, "..", "..", "testdata", "water_dir", "day3")

# The client files of a water dir, as the builder writes them (water_layer.h).
WATER_DIR_FILES = ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_rings.bin", "arc_index.bin",
                   "arc_vertices.bin", "strings_core.bin", "strings_layout.json", "water_meta.json", "attribution.json"]
WATER_DIRS = {"major": "water/major/simplified", "all": "water/all/simplified", "all-exact": "water/all/exact"}


def load_template():
    with open(TEMPLATE) as f:
        return json.load(f)


class WaterAxisTest(unittest.TestCase):
    def test_water_is_an_axis_of_its_own_that_no_preset_takes(self):
        cfg = load_template()
        self.assertEqual(list(cfg["axes"]["water"]["values"]), ["none", "major", "all", "all-exact"])
        for preset in cfg["presets"]:
            self.assertEqual(sorted(preset["selections"]), sorted(cfg["axes"]), preset["id"])
            self.assertEqual(preset["selections"]["water"], "none", preset["id"])
            # Water adds files to any selection, so a preset without it is never the largest.
            self.assertNotIn("largest", preset["description"].lower(), preset["id"])
        # Any boundary precision with any water: no constraint names it.
        for c in cfg["constraints"]:
            self.assertNotIn("water", c["when"], c["id"])
            self.assertNotIn("water", c["requires"], c["id"])

    def test_each_water_value_takes_one_water_dir_into_the_datasets_water_dir(self):
        cfg = load_template()
        self.assertEqual(cfg["components"]["water.none"]["files"], [])
        for value, d in WATER_DIRS.items():
            component = cfg["components"][f"water.{value}"]
            self.assertEqual(component["dir"], "water", value)
            self.assertEqual(component["files"], [f"{{region}}/{d}/{name}" for name in WATER_DIR_FILES], value)
            self.assertNotIn("variants", component, value)
        # Only the water components land in a dir of their own.
        for cid, component in cfg["components"].items():
            if not cid.startswith("water."):
                self.assertNotIn("dir", component, cid)

    def test_the_water_axis_offers_its_dirs_files_answered_in_a_water_field(self):
        description = load_template()["axes"]["water"]["description"]
        self.assertIn("water/", description)
        self.assertIn("answered in a `water` field beside the address", description)
        self.assertNotIn("yet", description)

    def test_the_seas_and_oceans_are_offered_as_in_builds_that_pin_the_marine_areas(self):
        axis = load_template()["axes"]["water"]
        with open(PATCHING) as f:
            rows = [line for line in f if line.startswith(("| `water/major/", "| `water/all/simplified/"))]
        self.assertEqual(len(rows), 2)
        for text in [axis["description"], axis["values"]["major"]["description"],
                     axis["values"]["all"]["description"]] + rows:
            self.assertIn("seas and oceans", text.lower(), text)
            self.assertIn("in builds that pin the marine areas", text, text)

    def test_the_major_value_gives_the_thresholds_to_enter_it_and_to_stay_in_it(self):
        with open(WATER_CLASSIFY) as f:
            header = f.read()

        def constant(name):
            return float(re.search(rf"inline constexpr double {name} = ([0-9.]+);", header).group(1))

        keep = constant("kMajorKeepPercent") / 100
        km2 = [constant("kMajorAreaKm2"), constant("kMajorWikidataRiverKm2")]
        with open(PATCHING) as f:
            row = next(line for line in f if line.startswith("| `water/major/"))
        for text in (load_template()["axes"]["water"]["values"]["major"]["description"], row):
            for v in km2 + [k * keep for k in km2]:
                self.assertIn(f"{v:g} km²", text, text)

    def test_the_water_values_say_how_precise_their_shores_are(self):
        values = load_template()["axes"]["water"]["values"]
        self.assertEqual({v: values[v].get("precision_m") for v in WATER_DIRS}, {"major": 37.5, "all": 37.5, "all-exact": 0})


class AttributionTest(unittest.TestCase):
    def test_every_component_of_data_is_credited(self):
        cfg = load_template()
        entries = cfg["attribution"]
        for cid, component in cfg["components"].items():
            if not component.get("files") and not component.get("variants"):
                continue
            credited = [e["id"] for e in entries if any(fnmatch.fnmatchcase(cid, p) for p in e["applies_to"])]
            self.assertIn("osm", credited, cid)

    def test_marine_regions_is_credited_wherever_water_is_taken(self):
        entries = {e["id"]: e for e in load_template()["attribution"]}
        marine = entries["marine-regions"]
        self.assertEqual(marine["licence"], "CC BY 4.0")
        self.assertEqual(marine["licence_url"], "https://creativecommons.org/licenses/by/4.0/")
        self.assertIn("The intersect of the Exclusive Economic Zones and IHO sea areas, version 5", marine["text"])
        self.assertIn("https://doi.org/10.14284/699", marine["text"])
        self.assertTrue(any(n.startswith("Changes: ") for n in marine["notices"]))
        for value in WATER_DIRS:
            self.assertTrue(any(fnmatch.fnmatchcase(f"water.{value}", p) for p in marine["applies_to"]), value)
        self.assertFalse(any(fnmatch.fnmatchcase("water.none", p) for p in marine["applies_to"]))
        self.assertFalse(any(fnmatch.fnmatchcase("mode.full", p) for p in marine["applies_to"]))

    def test_the_marine_regions_changes_tell_where_the_water_they_leave_out_goes(self):
        marine = {e["id"]: e for e in load_template()["attribution"]}["marine-regions"]
        (changes,) = [n for n in marine["notices"] if n.startswith("Changes: ")]
        self.assertIn("coastal water the sea areas leave out given to the nearest sea area", changes)

    def test_a_water_dir_credits_marine_regions_as_the_template_does(self):
        with open(os.path.join(WATER_DIR_WITH_A_SEA, "attribution.json")) as f:
            written = {e["id"]: e for e in json.load(f)}
        marine = {e["id"]: e for e in load_template()["attribution"]}["marine-regions"]
        self.assertEqual(written["marine-regions"], {k: v for k, v in marine.items() if k != "applies_to"})

    def test_each_entry_has_what_a_credit_shows(self):
        for e in load_template()["attribution"]:
            for key in ("id", "text", "url", "licence", "notices", "applies_to"):
                self.assertIn(key, e, e.get("id"))


if __name__ == "__main__":
    unittest.main()
