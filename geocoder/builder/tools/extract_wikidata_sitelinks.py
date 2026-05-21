#!/usr/bin/env python3
"""Extract QID → sitelinks count from Wikidata wb_items_per_site SQL dump.

Input: wb_items_per_site.sql.gz (stdin or file argument)
Output: qid_sitelinks.bin — sorted array of (uint32_t qid, uint16_t count) pairs

Usage:
  gunzip -c wb_items_per_site.sql.gz | python3 extract_wikidata_sitelinks.py -o qid_sitelinks.bin
  python3 extract_wikidata_sitelinks.py wb_items_per_site.sql.gz -o qid_sitelinks.bin
"""
import sys
import struct
import re
import gzip
import argparse
from collections import Counter

def parse_sql_inserts(stream):
    """Parse INSERT statements and yield item_id values."""
    # Pattern matches (row_id, item_id, 'site_id', 'page_name')
    # We only need item_id (second value in each tuple)
    value_pattern = re.compile(r'\((\d+),(\d+),')

    for line in stream:
        if isinstance(line, bytes):
            line = line.decode('utf-8', errors='replace')
        if not line.startswith('INSERT'):
            continue
        for match in value_pattern.finditer(line):
            yield int(match.group(2))

def main():
    parser = argparse.ArgumentParser(description='Extract wikidata sitelinks counts')
    parser.add_argument('input', nargs='?', help='Input .sql.gz file (default: stdin)')
    parser.add_argument('-o', '--output', required=True, help='Output binary file')
    args = parser.parse_args()

    print("Counting sitelinks per QID...", file=sys.stderr)
    counts = Counter()

    if args.input:
        if args.input.endswith('.gz'):
            stream = gzip.open(args.input, 'rt', encoding='utf-8', errors='replace')
        else:
            stream = open(args.input, 'r', encoding='utf-8', errors='replace')
    else:
        stream = sys.stdin

    total_rows = 0
    for item_id in parse_sql_inserts(stream):
        counts[item_id] += 1
        total_rows += 1
        if total_rows % 10_000_000 == 0:
            print(f"  {total_rows // 1_000_000}M rows processed, {len(counts)} unique QIDs...", file=sys.stderr)

    if args.input:
        stream.close()

    print(f"Total: {total_rows} rows, {len(counts)} unique QIDs", file=sys.stderr)

    # Write sorted binary: (uint32_t qid, uint16_t count) per entry
    # Cap count at 65535 (uint16_t max)
    sorted_items = sorted(counts.items())

    with open(args.output, 'wb') as f:
        for qid, count in sorted_items:
            if qid > 0xFFFFFFFF:
                continue  # skip if QID exceeds uint32
            f.write(struct.pack('<IH', qid, min(count, 65535)))

    size_mb = len(sorted_items) * 6 / 1024 / 1024
    print(f"Wrote {len(sorted_items)} entries ({size_mb:.1f} MiB) to {args.output}", file=sys.stderr)

    # Print some stats
    top_10 = counts.most_common(10)
    print(f"Top 10 by sitelinks:", file=sys.stderr)
    for qid, count in top_10:
        print(f"  Q{qid}: {count} sitelinks", file=sys.stderr)

if __name__ == '__main__':
    main()
