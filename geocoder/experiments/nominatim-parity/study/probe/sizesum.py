"""sizesum.py <sizes.tsv> : per preset, raw bytes before/after of all files and zstd -19 bytes of the
files that differ (identical files contribute the same compressed size to both sides)."""
import csv
import sys

rows = list(csv.DictReader(open(sys.argv[1]), delimiter="\t"))
for s in ("smallest", "recommended", "max"):
    R = [r for r in rows if r["set"] == s]
    rb = sum(int(r["raw_before"]) for r in R)
    ra = sum(int(r["raw_after"]) for r in R)
    diff = [r for r in R if r["zst_before"] != "same"]
    zb = sum(int(r["zst_before"]) for r in diff)
    za = sum(int(r["zst_after"]) for r in diff)
    print(f"{s}: files {len(R)}, differing {len(diff)}; raw {rb} -> {ra} ({ra - rb:+d}); "
          f"zst of differing files {zb} -> {za} ({za - zb:+d})")
    for r in diff:
        print(f"    {r['file']}: raw {int(r['raw_after']) - int(r['raw_before']):+d} "
              f"zst {int(r['zst_after']) - int(r['zst_before']):+d}")
