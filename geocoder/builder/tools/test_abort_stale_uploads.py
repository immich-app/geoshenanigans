#!/usr/bin/env python3
"""Tests for abort_stale_uploads.py. Run: python3 -m unittest discover -s geocoder/builder/tools"""
import datetime
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import abort_stale_uploads as sut  # noqa: E402

UTC = datetime.timezone.utc


def page(uploads, truncated=False, next_key="", next_id=""):
    items = "".join(f"<Upload><Key>{k}</Key><UploadId>{i}</UploadId><Initiated>{t}</Initiated>"
                    "<StorageClass>STANDARD</StorageClass></Upload>" for k, i, t in uploads)
    more = f"<NextKeyMarker>{next_key}</NextKeyMarker><NextUploadIdMarker>{next_id}</NextUploadIdMarker>" if truncated else ""
    return (f'<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Bucket>b</Bucket><IsTruncated>{'true' if truncated else 'false'}</IsTruncated>{more}{items}"
            "</ListMultipartUploadsResult>").encode()


class ParseUploadsTest(unittest.TestCase):
    def test_reads_each_upload_and_ends_on_the_last_page(self):
        uploads, markers = sut.parse_uploads(page([
            ("geocoder-cache/builds/2026-04-05/full/admin_vertices.bin", "2~abc", "2026-04-06T16:16:56.759Z"),
            ("geocoder/speed-test/geo_cells.bin", "xyz", "2026-04-07T14:24:01.86810307Z"),
        ]))
        self.assertIsNone(markers)
        self.assertEqual(uploads, [
            ("geocoder-cache/builds/2026-04-05/full/admin_vertices.bin", "2~abc",
             datetime.datetime(2026, 4, 6, 16, 16, 56, tzinfo=UTC)),
            ("geocoder/speed-test/geo_cells.bin", "xyz", datetime.datetime(2026, 4, 7, 14, 24, 1, tzinfo=UTC)),
        ])

    def test_a_truncated_page_gives_the_next_markers(self):
        _, markers = sut.parse_uploads(page([("a", "1", "2026-04-06T16:16:56Z")], truncated=True,
                                            next_key="a", next_id="1"))
        self.assertEqual(markers, ("a", "1"))

    def test_an_empty_bucket_has_no_uploads(self):
        self.assertEqual(sut.parse_uploads(page([])), ([], None))


class StaleTest(unittest.TestCase):
    def test_takes_only_uploads_older_than_the_cutoff(self):
        now = datetime.datetime(2026, 10, 12, 12, tzinfo=UTC)
        old = ("old", "1", now - datetime.timedelta(hours=25))
        young = ("young", "2", now - datetime.timedelta(hours=23))
        self.assertEqual(sut.stale([old, young], now, datetime.timedelta(hours=24)), [old])


if __name__ == "__main__":
    unittest.main()
