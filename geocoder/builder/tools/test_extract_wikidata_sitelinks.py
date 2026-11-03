#!/usr/bin/env python3
"""Tests for extract_wikidata_sitelinks.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import gzip
import os
import struct
import subprocess
import sys
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "extract_wikidata_sitelinks.py")

# The dump's preamble and trailer, as wikidatawiki-20261001 writes them.
HEAD = b"""/*M!999999\\- enable the sandbox mode */
-- MariaDB dump 10.19  Distrib 10.11.18-MariaDB, for debian-linux-gnu (x86_64)
--
-- Table structure for table `wb_items_per_site`
--

DROP TABLE IF EXISTS `wb_items_per_site`;
CREATE TABLE `wb_items_per_site` (
  `ips_row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `ips_item_id` int(10) unsigned NOT NULL,
  `ips_site_id` varbinary(32) NOT NULL,
  `ips_site_page` varbinary(310) NOT NULL,
  PRIMARY KEY (`ips_row_id`),
  UNIQUE KEY `wb_ips_item_site_page` (`ips_site_id`,`ips_site_page`),
  KEY `wb_ips_item_id` (`ips_item_id`)
) ENGINE=InnoDB AUTO_INCREMENT=631412339 DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED;

--
-- Dumping data for table `wb_items_per_site`
--

/*!40000 ALTER TABLE `wb_items_per_site` DISABLE KEYS */;
"""
TAIL = b"""/*!40000 ALTER TABLE `wb_items_per_site` ENABLE KEYS */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

-- Dump completed on 2026-10-03 21:43:57
"""
INSERT = b"INSERT INTO `wb_items_per_site` VALUES"

# Rows from the 20261001 dump: Q3596065 x3, Q620894 x1, Q5 x2.
ROWS = [
    "(55,3596065,'abwiki','Џьгьарда')",
    "(76,620894,'abwiki','Агәы-Бедиа')",
    "(1106,5,'acewiki','Gunong Perkison')",
    "(5464,3596065,'afwiki','Abraham H. de Vries')",
    "(6531,3596065,'afwiki','Azanian People\\'s Liberation Army')",
    "(631412338,5,'commonswiki','Category:Views of structures in Baden-Württemberg')",
]
COUNTS = {5: 2, 620894: 1, 3596065: 3}

# Titles from the dump that hold a row's start, "(1,2,", or an escaped quote,
# and made-up ones that hold "'),(" behind escapes and end in a backslash.
TRICKY_ROWS = [
    "(11414622,1838431,'nlwiki','Indeen(1,2,3-cd)pyreen')",
    "(23875060,4113230,'enwiki','(2,2,3-Trimethyl-5-oxocyclopent-3-enyl)acetyl-CoA 1,5-monooxygenase')",
    "(35748305,4542150,'enwiki','(2,3,7) triangle group')",
    "(23117408,5165810,'enwiki','Control-\\\\')",
    "(6564657,2859513,'frwiki','Aranda\\\\Lasch')",
    "(32386,648715,'alswiki','Mustafa Sâ\\'î Çelebi')",
    "(900,7,'enwiki','x\\'),(901,8,\\'enwiki\\',\\'y')",
    "(902,7,'dewiki','\\\\')",
    "(903,7,'frwiki','(4,5,')",
]
TRICKY_COUNTS = {1838431: 1, 4113230: 1, 4542150: 1, 5165810: 1, 2859513: 1, 648715: 1, 7: 3}


def rows_per_line(*statements):
    """INSERT ... VALUES on its own line, then one row per line (20260901 on)."""
    body = b"".join(INSERT + b"\n" + ",\n".join(rows).encode() + b";\n" for rows in statements)
    return HEAD + body + TAIL


def rows_on_insert_line(*statements):
    """Every row of a statement on its INSERT line (dumps up to 20260801)."""
    body = b"".join(INSERT + b" " + ",".join(rows).encode() + b";\n" for rows in statements)
    return HEAD + body + TAIL


def decode(data):
    return {qid: count for qid, count in struct.iter_unpack("<IH", data)}


class ExtractSitelinksTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.out = os.path.join(self.dir.name, "qid_sitelinks.bin")

    def run_extract(self, dump, min_qids=1, gz=True):
        path = os.path.join(self.dir.name, "wb_items_per_site.sql" + (".gz" if gz else ""))
        with (gzip.open if gz else open)(path, "wb") as f:
            f.write(dump)
        args = [sys.executable, "-I", SCRIPT, path, "-o", self.out]
        if min_qids is not None:
            args += ["--min-qids", str(min_qids)]
        return subprocess.run(args, capture_output=True, text=True, timeout=120)

    def extract(self, dump, **kwargs):
        result = self.run_extract(dump, **kwargs)
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(self.out, "rb") as f:
            return f.read()

    def assert_fails(self, result, message):
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(message, result.stderr)
        self.assertFalse(os.path.exists(self.out), "a failed run must not leave an output file")
        self.assertFalse(os.path.exists(self.out + ".tmp"))

    def test_rows_per_line_layout(self):
        data = self.extract(rows_per_line(ROWS[:4], ROWS[4:]))
        self.assertEqual(decode(data), COUNTS)

    def test_rows_on_the_insert_line_layout(self):
        data = self.extract(rows_on_insert_line(ROWS[:4], ROWS[4:]))
        self.assertEqual(decode(data), COUNTS)
        self.assertEqual(data, self.extract(rows_per_line(ROWS), gz=False))

    def test_titles_holding_row_starts_and_escaped_quotes(self):
        for layout in (rows_per_line, rows_on_insert_line):
            with self.subTest(layout=layout.__name__):
                data = self.extract(layout(TRICKY_ROWS[:3], TRICKY_ROWS[3:]))
                self.assertEqual(decode(data), TRICKY_COUNTS)

    def test_output_bytes(self):
        # Q42 has 70000 sitelinks, past the uint16 count's 65535 cap.
        rows = ROWS + ["(%d,42,'site%d','T')" % (10**6 + i, i) for i in range(70000)]
        data = self.extract(rows_per_line(rows))
        self.assertEqual(data, bytes.fromhex(
            "05000000" "0200"    # Q5: 2
            "2a000000" "ffff"    # Q42: 70000, capped
            "5e790900" "0100"    # Q620894: 1
            "21df3600" "0300"))  # Q3596065: 3

    def test_no_rows_exits_nonzero(self):
        self.assert_fails(self.run_extract(HEAD + TAIL, min_qids=0), "no rows")

    def test_below_the_floor_exits_nonzero(self):
        self.assert_fails(self.run_extract(rows_per_line(ROWS), min_qids=4), "below the floor of 4")
        self.assertEqual(decode(self.extract(rows_per_line(ROWS), min_qids=3)), COUNTS)

    def test_default_floor_rejects_a_small_dump(self):
        self.assert_fails(self.run_extract(rows_per_line(ROWS), min_qids=None), "below the floor")

    def test_unreadable_row_exits_nonzero(self):
        broken = ROWS[:2] + ["(77,9,'abwiki','unterminated)"] + ROWS[2:]
        for layout in (rows_per_line, rows_on_insert_line):
            with self.subTest(layout=layout.__name__):
                self.assert_fails(self.run_extract(layout(broken)), "not a wb_items_per_site row")

    def test_truncated_statement_exits_nonzero(self):
        dump = HEAD + INSERT + b"\n" + ",\n".join(ROWS).encode() + b",\n"
        self.assert_fails(self.run_extract(dump), "ends inside an INSERT statement")

    def test_unknown_insert_exits_nonzero(self):
        dump = HEAD + b"INSERT INTO `wb_items_per_site` (`ips_row_id`) VALUES (1);\n" + TAIL
        self.assert_fails(self.run_extract(dump), "unrecognised INSERT")


if __name__ == "__main__":
    unittest.main()
