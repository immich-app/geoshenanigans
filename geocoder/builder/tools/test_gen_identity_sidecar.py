#!/usr/bin/env python3
"""Tests for gen_identity_sidecar.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import os
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_identity_sidecar as sut  # noqa: E402

NO = sut.NO_DATA


def write_words(path, rows):
    with open(path, "wb") as f:
        for row in rows:
            f.write(struct.pack("<%dI" % len(row), *row))


def read_words(path):
    with open(path, "rb") as f:
        data = f.read()
    return list(struct.unpack("<%dI" % (len(data) // 4), data))


class WriteSidecarTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def sidecar(self, rows, tombstone):
        src = os.path.join(self.dir.name, "records.bin")
        out = os.path.join(self.dir.name, "out.slot.u32")
        write_words(src, rows)
        result = sut.write_sidecar(src, tombstone, out)
        return result, read_words(out)

    def test_live_records_keep_their_index_and_tombstones_never_match(self):
        street = sut.TOMBSTONES["street"][1]
        rows = [(1, 2, 3), street, (5, 0, 0), (NO, 7, 8), street]
        # Records 2 and 3 together hold the tombstone bytes off the record
        # grid; that hit must not mark either of them.
        (n, dead), keys = self.sidecar(rows, street)
        self.assertEqual((n, dead), (5, 2))
        self.assertEqual(keys, [0, NO, 2, 3, NO])

    def test_keys_continue_across_chunks(self):
        addr = sut.TOMBSTONES["addr"][1]
        old_chunk = sut.CHUNK_RECORDS
        sut.CHUNK_RECORDS = 3
        self.addCleanup(setattr, sut, "CHUNK_RECORDS", old_chunk)
        rows = [(i + 1, 0, 0, 0, 0, 0, 0) for i in range(8)]
        rows[3] = addr
        rows[7] = addr
        (n, dead), keys = self.sidecar(rows, addr)
        self.assertEqual((n, dead), (8, 2))
        self.assertEqual(keys, [0, 1, 2, NO, 4, 5, 6, NO])

    def test_empty_file_writes_an_empty_sidecar(self):
        (n, dead), keys = self.sidecar([], sut.TOMBSTONES["interp"][1])
        self.assertEqual((n, dead), (0, 0))
        self.assertEqual(keys, [])

    def test_rejects_a_file_that_is_not_whole_records(self):
        src = os.path.join(self.dir.name, "records.bin")
        with open(src, "wb") as f:
            f.write(b"\x00" * 13)
        with self.assertRaises(ValueError):
            sut.write_sidecar(src, sut.TOMBSTONES["street"][1], os.path.join(self.dir.name, "out"))


class MainTest(unittest.TestCase):
    def test_writes_one_sidecar_per_family(self):
        with tempfile.TemporaryDirectory() as d:
            src, out = os.path.join(d, "full"), os.path.join(d, "meta")
            os.makedirs(src)
            for family, (name, tombstone) in sut.TOMBSTONES.items():
                write_words(os.path.join(src, name), [tombstone, tuple(range(1, len(tombstone) + 1))])
            self.assertEqual(sut.main(["gen_identity_sidecar.py", src, out]), 0)
            for family in sut.TOMBSTONES:
                self.assertEqual(read_words(os.path.join(out, family + ".slot.u32")), [NO, 1])

    def test_writes_no_sidecar_for_a_family_the_dir_lacks(self):
        with tempfile.TemporaryDirectory() as d:
            src, out = os.path.join(d, "no-addresses"), os.path.join(d, "meta")
            os.makedirs(src)
            write_words(os.path.join(src, "street_ways.bin"), [(1, 2, 3)])
            self.assertEqual(sut.main(["gen_identity_sidecar.py", src, out]), 0)
            self.assertEqual(os.listdir(out), ["street.slot.u32"])
            self.assertEqual(read_words(os.path.join(out, "street.slot.u32")), [0])


if __name__ == "__main__":
    unittest.main()
