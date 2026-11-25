#!/usr/bin/env python3
"""GeoNames' postal places, the names postal_cities.bin falls back on (builder/src/postal_cities_build.h,
the builder's --postal-places): the rows of GeoNames' postal code files of the United States and its
territories (download.geonames.org/export/zip/{US,PR,VI,GU,AS,MP}.zip) as one TSV, a line per ZIP of
country, ZIP, place name and state (GeoNames' admin code of a US row, a territory's own code), sorted,
gzipped without a timestamp, so that the file's bytes are its rows'.

It refuses files GeoNames would not publish: a row of other than 12 fields, of another country than
its file's, of a ZIP of other than five ASCII digits or of no name, a country of fewer rows than
MIN_ROWS and, given the rows the cache holds now, fewer than 99% of them. A row the builder skips (a
ZIP of 00000, a name of more than NAME_BYTES bytes of UTF-8 or of a control character) is left out
and not counted. A ZIP of two rows (a Navy ZIP is an FPO's and its base's) keeps the one of a state,
else the least name, then the least state; a ZIP of two files is one row too, the least country's,
which the builder takes: it keys every row of these files as a US ZIP's and keeps a ZIP's first.

Usage: postal_places.py OUT.tsv.gz ZIP... [--previous-rows N]
Prints a JSON summary: {"rows": n, "size": bytes, "sha256": hex, "sources": {"US": n, ...}}."""
import argparse
import gzip
import hashlib
import io
import json
import os
import re
import sys
import zipfile

# The fewest rows each country's file holds (US 41,490 and PR 177 in 2026).
MIN_ROWS = {"US": 40000, "PR": 150, "VI": 1, "GU": 1, "AS": 1, "MP": 1}
# The share of the cache's rows a refresh must keep.
KEEP = 0.99
ZIP = re.compile(r"[0-9]{5}")
# The longest name the builder takes, in bytes of UTF-8 (postal_cities.h NAME_BYTES).
NAME_BYTES = 48


def builder_takes(zip_, name):
    """Whether the builder takes a row (postal_cities_build.h parse_places): a ZIP the grid's pattern
    takes, not 00000, and a name of at most NAME_BYTES bytes with no control character (U+0000 to
    U+001F, U+007F)."""
    return zip_ != "00000" and len(name.encode("utf-8")) <= NAME_BYTES and not any(
        ord(c) < 0x20 or ord(c) == 0x7F for c in name)


class Refused(Exception):
    pass


def rows_of(path):
    """(country, zip, name, state) of each row of a GeoNames postal code zip, <CC>.zip holding <CC>.txt."""
    country = os.path.splitext(os.path.basename(path))[0]
    if country not in MIN_ROWS:
        raise Refused(f"{path}: not a file of {', '.join(MIN_ROWS)}")
    with zipfile.ZipFile(path) as z:
        try:
            raw = z.read(f"{country}.txt")
        except KeyError:
            raise Refused(f"{path} holds no {country}.txt") from None
    out = []
    for n, line in enumerate(io.StringIO(raw.decode("utf-8")), 1):
        f = line.rstrip("\r\n").split("\t")
        if len(f) != 12:
            raise Refused(f"{path}:{n}: {len(f)} fields, not 12")
        if f[0] != country:
            raise Refused(f"{path}:{n}: a row of {f[0]!r} in {country}'s file")
        if not ZIP.fullmatch(f[1]):
            raise Refused(f"{path}:{n}: ZIP {f[1]!r} is not five digits")
        name = f[2].strip()
        if not name or "\t" in name:
            raise Refused(f"{path}:{n}: a row of no name")
        if builder_takes(f[1], name):
            out.append((country, f[1], name, f[4].strip() if country == "US" else country))
    if len(out) < MIN_ROWS[country]:
        raise Refused(f"{path}: {len(out)} rows, fewer than {country}'s {MIN_ROWS[country]}")
    return out


def table(paths):
    """The rows of every file, one a ZIP whatever its files, sorted, and the rows each file held."""
    best, sources = {}, {}

    def rank(r):
        return r[0], not r[3], r[2], r[3]

    for path in paths:
        rows = rows_of(path)
        sources[rows[0][0]] = len(rows)
        for r in rows:
            if r[1] not in best or rank(best[r[1]]) > rank(r):
                best[r[1]] = r
    return sorted(best.values(), key=lambda r: (r[0], r[1])), sources


def write(out, rows):
    """The TSV, gzipped without a name or a time, so the same rows give the same bytes."""
    text = "".join("\t".join(r) + "\n" for r in rows).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=9) as g:
        g.write(text)
    data = buf.getvalue()
    with open(out, "wb") as f:
        f.write(data)
    return data


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("out")
    p.add_argument("zips", nargs="+")
    p.add_argument("--previous-rows", type=int, default=0)
    a = p.parse_args(argv)
    try:
        rows, sources = table(a.zips)
        if len(rows) < KEEP * a.previous_rows:
            raise Refused(f"{len(rows)} rows, fewer than {KEEP:.0%} of the cache's {a.previous_rows}")
    except (Refused, zipfile.BadZipFile, UnicodeDecodeError) as e:
        print(f"postal_places: {e}", file=sys.stderr)
        return 1
    data = write(a.out, rows)
    print(json.dumps({"rows": len(rows), "size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                      "sources": sources}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
