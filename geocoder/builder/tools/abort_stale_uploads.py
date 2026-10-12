#!/usr/bin/env python3
"""Aborts a bucket's multipart uploads started more than --older-than hours
ago. An upload a killed run left behind keeps its parts' storage, and no
object listing shows it.

Usage: abort_stale_uploads.py <endpoint> <bucket> <region> [--older-than HOURS] [--dry-run]

Requests are signed by curl --aws-sigv4 with AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY, passed on its stdin rather than its command line.
"""
import argparse
import datetime
import os
import subprocess
import sys
import urllib.parse
import xml.etree.ElementTree as ET

# Uploads younger than this may belong to a run still uploading.
DEFAULT_OLDER_THAN_HOURS = 24


def tag(el):
    return el.tag.rsplit("}", 1)[-1]


def parse_time(text):
    """An S3 timestamp; Tigris gives nanoseconds, which fromisoformat can't take."""
    seconds = text.rstrip("Z").split(".")[0]
    return datetime.datetime.fromisoformat(seconds).replace(tzinfo=datetime.timezone.utc)


def parse_uploads(body):
    """A ListMultipartUploads page: its uploads as (key, upload id, initiated),
    and the markers of the next page, None on the last."""
    root = ET.fromstring(body)
    uploads, fields = [], {}
    for el in root:
        if tag(el) == "Upload":
            upload = {tag(c): c.text for c in el}
            uploads.append((upload["Key"], upload["UploadId"], parse_time(upload["Initiated"])))
        else:
            fields[tag(el)] = el.text
    if fields.get("IsTruncated") != "true":
        return uploads, None

    return uploads, (fields.get("NextKeyMarker") or "", fields.get("NextUploadIdMarker") or "")


def stale(uploads, now, older_than):
    return [u for u in uploads if now - u[2] > older_than]


def request(method, url, region):
    config = f'user = "{os.environ["AWS_ACCESS_KEY_ID"]}:{os.environ["AWS_SECRET_ACCESS_KEY"]}"\n'
    result = subprocess.run(
        ["curl", "-sS", "--fail-with-body", "--max-time", "60", "--aws-sigv4", f"aws:amz:{region}:s3",
         "-K", "-", "-X", method, url],
        input=config.encode(), capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"{method} {url.split('?')[0]}: curl exit {result.returncode}: "
                           f"{result.stdout[:300]!r} {result.stderr[:300]!r}")
    return result.stdout


def list_uploads(endpoint, bucket, region):
    markers = None
    while True:
        url = f"{endpoint}/{bucket}?uploads"
        if markers:
            url += "&" + urllib.parse.urlencode({"key-marker": markers[0], "upload-id-marker": markers[1]})
        uploads, markers = parse_uploads(request("GET", url, region))
        yield from uploads
        if markers is None:
            return


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("endpoint")
    p.add_argument("bucket")
    p.add_argument("region")
    p.add_argument("--older-than", type=float, default=DEFAULT_OLDER_THAN_HOURS, help="hours")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    uploads = list(list_uploads(args.endpoint, args.bucket, args.region))
    old = stale(uploads, now, datetime.timedelta(hours=args.older_than))
    failed = 0
    for key, upload_id, initiated in old:
        if args.dry_run:
            print(f"would abort {key} (started {initiated:%Y-%m-%d %H:%M})")
            continue
        url = (f"{args.endpoint}/{args.bucket}/{urllib.parse.quote(key, safe='/')}"
               f"?uploadId={urllib.parse.quote(upload_id, safe='')}")
        try:
            request("DELETE", url, args.region)
        except RuntimeError as e:
            failed += 1
            print(e, file=sys.stderr)
    verb = "would abort" if args.dry_run else "aborted"
    print(f"{args.bucket}: {verb} {len(old) - failed} of {len(uploads)} multipart uploads "
          f"(started over {args.older_than:g} h ago)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
