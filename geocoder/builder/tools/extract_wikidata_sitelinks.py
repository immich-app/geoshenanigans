#!/usr/bin/env python3
"""Extract QID → sitelinks count from Wikidata wb_items_per_site SQL dump.

Input: wb_items_per_site.sql.gz (stdin or file argument, gzipped or plain)
Output: qid_sitelinks.bin — sorted array of (uint32_t qid, uint16_t count) pairs,
little-endian, the count capped at 65535

Each row of the table, (ips_row_id, ips_item_id, ips_site_id, ips_site_page),
is one sitelink of item Q<ips_item_id>. Dumps have written a statement's rows
in two layouts, and both are read:
  - every row on the INSERT line, as dumps did up to 20260801 (mariadb-dump 10.5):
      INSERT INTO `wb_items_per_site` VALUES (1,5,'enwiki','A'),(2,5,'dewiki','B');
  - INSERT ... VALUES on its own line, then one row per line, as from 20260901
    (mariadb-dump 10.11):
      INSERT INTO `wb_items_per_site` VALUES
      (1,5,'enwiki','A'),
      (2,5,'dewiki','B');

It exits non-zero and writes nothing when the dump yields no rows, fewer unique
QIDs than --min-qids, or a row it can't read, so a layout it doesn't know can
never be published as an empty or truncated file.

Usage:
  gunzip -c wb_items_per_site.sql.gz | python3 extract_wikidata_sitelinks.py -o qid_sitelinks.bin
  python3 extract_wikidata_sitelinks.py wb_items_per_site.sql.gz -o qid_sitelinks.bin
"""
import argparse
import gzip
import os
import re
import struct
import sys
import zlib
from collections import Counter

# Items with at least one sitelink: 34.0M in the 2026-06 dump. The floor sits
# well under that, so normal churn passes and a dump read only in part doesn't.
DEFAULT_MIN_QIDS = 20_000_000

MAX_COUNT = 0xFFFF
RECORD = struct.Struct('<IH')

INSERT = b'INSERT'
# The only statement the rows come from; any other INSERT is a layout change.
INSERT_HEADER = re.compile(rb"INSERT INTO `wb_items_per_site` VALUES\s*")
# A quoted string as the dump writes it: a backslash escapes the byte after
# it (\', \\, \n, ...), so only an unescaped quote ends the string.
QUOTED = rb"'[^'\\]*(?:\\.[^'\\]*)*'"
# One row and the ',' (the statement goes on) or ';' (it ends) after it.
# Group 1 is ips_item_id, read at the row's start; the values after it are
# quoted strings or bare tokens, comma-separated, up to the row's ')', so a
# page title holding "(1,2," or "'),(" is never taken for a row.
VALUE = rb"(?:" + QUOTED + rb"|[^,'()\s]+)"
ROW = re.compile(
    rb"\s*\(\s*\d+\s*,\s*(\d+)\s*(?:,\s*" + VALUE + rb"\s*)*\)\s*([,;])",
    re.DOTALL,
)
# The usual line of the rows-per-line layout: one row, ',' and the newline.
# It reads the same as through the split below, only faster.
ROW_LINE = re.compile(ROW.pattern + rb"\n", re.DOTALL)
# Item ids are counted in batches, so Counter.update runs per batch, not per row.
BATCH_ROWS = 1 << 20
PROGRESS_ROWS = 10_000_000


class DumpError(Exception):
    """The dump holds something the extractor can't read as rows."""


def _snippet(data, limit=160):
    text = data[:limit].decode('utf-8', errors='replace').rstrip('\n')
    return text + ('...' if len(data) > limit else '')


def count_sitelinks(lines, progress=None):
    """Count the rows per ips_item_id over the dump's lines (bytes).

    Returns (Counter of item id → rows, total rows). A raw newline never falls
    inside a row (the dump escapes it as \\n), so each line holds whole rows:
    all of a statement's, after the INSERT header, or one or more in the
    rows-per-line layout. Raises DumpError on anything else inside a statement.
    """
    counts = Counter()
    batch = []
    rows = 0
    next_progress = PROGRESS_ROWS

    def flush():
        nonlocal rows, next_progress
        counts.update(map(int, batch))
        rows += len(batch)
        batch.clear()
        if progress and rows >= next_progress:
            progress(rows, len(counts))
            next_progress = (rows // PROGRESS_ROWS + 1) * PROGRESS_ROWS

    in_statement = False
    lineno = 0
    for lineno, line in enumerate(lines, 1):
        if in_statement:
            row = ROW_LINE.fullmatch(line)
            if row is not None and row[2] == b',':
                batch.append(row[1])
                if len(batch) >= BATCH_ROWS:
                    flush()
                continue
            body = line
        elif line.startswith(INSERT):
            header = INSERT_HEADER.match(line)
            if not header:
                raise DumpError(f'line {lineno}: unrecognised INSERT statement: {_snippet(line)}')
            body = line[header.end():]
            in_statement = True
        else:
            continue

        # split() interleaves the text between rows with each row's groups:
        # [gap, item id, separator, gap, ..., gap]. The rows must follow one
        # another with nothing between them, so a row can only start where
        # the last one ended, never inside a page title.
        parts = ROW.split(body)
        gaps = parts[0:-1:3]
        if any(gaps) or parts[-1].strip():
            bad = next((g for g in gaps if g), parts[-1])
            raise DumpError(f'line {lineno}: not a wb_items_per_site row: {_snippet(bad)}')
        if len(parts) == 1:
            continue
        separators = parts[2::3]
        ends = separators.count(b';')
        if ends:
            if ends > 1 or separators[-1] != b';':
                raise DumpError(f'line {lineno}: rows after the end of an INSERT statement')
            in_statement = False

        batch += parts[1::3]
        if len(batch) >= BATCH_ROWS:
            flush()

    if in_statement:
        raise DumpError(f'the dump ends inside an INSERT statement (line {lineno}): truncated?')
    flush()
    return counts, rows


def encode_sitelinks(counts):
    """(uint32 qid, uint16 count) records sorted by qid, counts capped at 65535."""
    qids = sorted(counts)
    if qids and qids[-1] > 0xFFFFFFFF:
        raise DumpError(f'Q{qids[-1]} does not fit the uint32 qid field')
    out = bytearray(RECORD.size * len(qids))
    for i, qid in enumerate(qids):
        RECORD.pack_into(out, i * RECORD.size, qid, min(counts[qid], MAX_COUNT))
    return out


def open_input(path):
    if path is None:
        return sys.stdin.buffer
    if path.endswith('.gz'):
        return gzip.open(path, 'rb')
    return open(path, 'rb')


def main(argv=None):
    parser = argparse.ArgumentParser(description='Extract wikidata sitelinks counts')
    parser.add_argument('input', nargs='?', help='Input .sql.gz file (default: stdin)')
    parser.add_argument('-o', '--output', required=True, help='Output binary file')
    parser.add_argument(
        '--min-qids', type=int, default=DEFAULT_MIN_QIDS,
        help=f'Fail when fewer unique QIDs are found (default {DEFAULT_MIN_QIDS})')
    args = parser.parse_args(argv)

    def progress(rows, qids):
        print(f"  {rows // 1_000_000}M rows processed, {qids} unique QIDs...", file=sys.stderr)

    print("Counting sitelinks per QID...", file=sys.stderr)
    stream = open_input(args.input)
    try:
        counts, total_rows = count_sitelinks(stream, progress)
    except DumpError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except (EOFError, OSError, zlib.error) as e:
        print(f"error: reading {args.input or 'stdin'}: {e}", file=sys.stderr)
        return 1
    finally:
        if args.input:
            stream.close()

    print(f"Total: {total_rows} rows, {len(counts)} unique QIDs", file=sys.stderr)
    if total_rows == 0:
        print("error: no rows in any INSERT INTO `wb_items_per_site` statement; "
              "has the dump layout changed?", file=sys.stderr)
        return 1
    if len(counts) < args.min_qids:
        print(f"error: {len(counts)} unique QIDs, below the floor of {args.min_qids} "
              "(--min-qids): the dump looks truncated or partly unreadable", file=sys.stderr)
        return 1

    try:
        data = encode_sitelinks(counts)
    except DumpError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    # Written beside the target and renamed, so a failed write leaves no
    # partial output for the upload to pick up.
    tmp = args.output + '.tmp'
    with open(tmp, 'wb') as f:
        f.write(data)
    os.replace(tmp, args.output)

    entries = len(data) // RECORD.size
    size_mb = len(data) / 1024 / 1024
    print(f"Wrote {entries} entries ({size_mb:.1f} MiB) to {args.output}", file=sys.stderr)

    # Print some stats
    print("Top 10 by sitelinks:", file=sys.stderr)
    for qid, count in counts.most_common(10):
        print(f"  Q{qid}: {count} sitelinks", file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
