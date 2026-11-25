#!/usr/bin/env python3
"""Tests for postal_places.py: GeoNames' US and territory postal code files become one sorted TSV
of country, ZIP, name and state, the same bytes for the same rows, and files GeoNames would not
publish, or a refresh that loses rows, are refused.
Run: python3 -m unittest discover -s geocoder/builder/tools"""
import contextlib
import gzip
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import postal_places as sut  # noqa: E402


def line(country, zip_, name, state_name="", state=""):
    return "\t".join([country, zip_, name, state_name, state, "", "", "", "", "39.9", "-75.3", "4"]) + "\n"


class PostalPlacesTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.mins = dict(sut.MIN_ROWS)
        self.addCleanup(lambda: sut.MIN_ROWS.update(self.mins))
        sut.MIN_ROWS.update({"US": 3, "PR": 1})

    def zip(self, country, text, member=None):
        path = os.path.join(self.dir.name, f"{country}.zip")
        with zipfile.ZipFile(path, "w") as z:
            z.writestr(member or f"{country}.txt", text)
            z.writestr("readme.txt", "This work is licensed under a Creative Commons Attribution 4.0 License.\n")
        return path

    def run_main(self, *args):
        out = os.path.join(self.dir.name, "postal_places.tsv.gz")
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            rc = sut.main([out, *args])
        return rc, out, stdout.getvalue(), stderr.getvalue()

    def test_the_files_rows_become_one_sorted_tsv_of_country_zip_name_and_state(self):
        us = self.zip("US", line("US", "19073", "Newtown Square", "Pennsylvania", "PA")
                      + line("US", "19026", "Drexel Hill", "Pennsylvania", "PA")
                      + line("US", "96860", "FPO AA") + line("US", "96860", "Jbphh", "Hawaii", "HI")
                      + line("US", "09001", "APO AA"))
        pr = self.zip("PR", line("PR", "00601", "Adjuntas", "Adjuntas", "001"))
        rc, out, stdout, _ = self.run_main(us, pr)
        self.assertEqual(rc, 0)
        with gzip.open(out, "rt", encoding="utf-8") as f:
            self.assertEqual(f.read(), "PR\t00601\tAdjuntas\tPR\nUS\t09001\tAPO AA\t\nUS\t19026\tDrexel Hill\tPA\n"
                                       "US\t19073\tNewtown Square\tPA\nUS\t96860\tJbphh\tHI\n")
        summary = json.loads(stdout)
        self.assertEqual(summary["rows"], 5)
        self.assertEqual(summary["sources"], {"US": 5, "PR": 1})
        with open(out, "rb") as f:
            data = f.read()
        self.assertEqual(summary["size"], len(data))
        # The same rows, in another order and on another day: the same bytes.
        us2 = self.zip("US", line("US", "09001", "APO AA") + line("US", "96860", "Jbphh", "Hawaii", "HI")
                       + line("US", "96860", "FPO AA") + line("US", "19026", "Drexel Hill", "Pennsylvania", "PA")
                       + line("US", "19073", "Newtown Square", "Pennsylvania", "PA"))
        rc, out, stdout2, _ = self.run_main(pr, us2)
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(stdout2)["sha256"], summary["sha256"])

    def test_a_file_geonames_would_not_publish_is_refused(self):
        good = line("US", "19026", "Drexel Hill", "Pennsylvania", "PA") + line("US", "19073", "Newtown Square") \
            + line("US", "19083", "Havertown")
        for text, why in [(good + "US\t19008\tBroomall\n", "fields, not 12"),
                          (good + line("PR", "19008", "Broomall"), "a row of 'PR'"),
                          (good + line("US", "1900", "Broomall"), "not five digits"),
                          (good + line("US", "19008-1234", "Broomall"), "not five digits"),
                          (good + line("US", "19008", " "), "no name"),
                          (line("US", "19026", "Drexel Hill"), "fewer than US's 3")]:
            rc, _, _, stderr = self.run_main(self.zip("US", text))
            self.assertEqual(rc, 1, why)
            self.assertIn(why, stderr)
        rc, _, _, stderr = self.run_main(self.zip("US", good, member="other.txt"))
        self.assertEqual(rc, 1)
        self.assertIn("holds no US.txt", stderr)
        rc, _, _, stderr = self.run_main(self.zip("CA", good))
        self.assertEqual(rc, 1)
        self.assertIn("not a file of", stderr)

    def test_a_row_the_builder_cant_take_is_neither_counted_nor_published(self):
        good = line("US", "19026", "Drexel Hill", "Pennsylvania", "PA") + line("US", "19073", "Newtown Square") \
            + line("US", "19083", "Havertown")
        for bad in (line("US", "00000", "Nowhere"), line("US", "19008", "B" * 49), line("US", "19008", "Broom\x01all"),
                    line("US", "19008", "Broom\x7fall")):
            rc, out, stdout, _ = self.run_main(self.zip("US", good + bad))
            self.assertEqual(rc, 0, bad)
            self.assertEqual(json.loads(stdout)["rows"], 3, bad)
            self.assertEqual(json.loads(stdout)["sources"], {"US": 3}, bad)
            with gzip.open(out, "rt", encoding="utf-8") as f:
                text = f.read()
            self.assertNotIn("19008", text)
            self.assertNotIn("00000", text)
        # A name of 48 bytes the builder takes, accents counted as their bytes.
        rc, out, stdout, _ = self.run_main(self.zip("US", good + line("US", "19008", "é" * 24)))
        self.assertEqual(json.loads(stdout)["rows"], 4)
        rc, out, stdout, _ = self.run_main(self.zip("US", good + line("US", "19008", "é" * 24 + "B")))
        self.assertEqual(json.loads(stdout)["rows"], 3)
        # Digits of another script are no ZIP's.
        rc, _, _, stderr = self.run_main(self.zip("US", good + line("US", "\uff11\uff19\uff10\uff10\uff18", "Broomall")))
        self.assertEqual(rc, 1)
        self.assertIn("not five digits", stderr)

    def test_a_zips_rows_of_one_name_keep_the_least_state_whatever_their_order(self):
        rows = [line("US", "19026", "Drexel Hill", "Pennsylvania", "PA"), line("US", "19026", "Drexel Hill", "Delaware", "DE"),
                line("US", "19073", "Newtown Square", "Pennsylvania", "PA"), line("US", "19083", "Havertown")]
        _, out, first, _ = self.run_main(self.zip("US", "".join(rows)))
        with gzip.open(out, "rt", encoding="utf-8") as f:
            self.assertIn("US\t19026\tDrexel Hill\tDE\n", f.read())
        _, _, second, _ = self.run_main(self.zip("US", "".join([rows[1], rows[0]] + rows[2:])))
        self.assertEqual(json.loads(first)["sha256"], json.loads(second)["sha256"])

    def test_a_refresh_keeps_99_percent_of_the_caches_rows(self):
        us = self.zip("US", "".join(line("US", f"{19000 + i:05d}", f"Town {i}") for i in range(99)))
        self.assertEqual(self.run_main(us, "--previous-rows", "100")[0], 0)
        rc, _, _, stderr = self.run_main(us, "--previous-rows", "101")
        self.assertEqual(rc, 1)
        self.assertIn("fewer than 99% of the cache's 101", stderr)


if __name__ == "__main__":
    unittest.main()
