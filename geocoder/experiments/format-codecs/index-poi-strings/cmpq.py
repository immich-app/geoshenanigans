"""Compare two grid_query outputs line by line (exact bytes of every answer).
usage: cmpq.py <base.jsonl> <variant.jsonl>"""
import sys
from collections import Counter

a = open(sys.argv[1]).read().splitlines()
b = open(sys.argv[2]).read().splitlines()
assert len(a) == len(b), (len(a), len(b))
diff = [(x, y) for x, y in zip(a, b) if x != y]
print(f"{sys.argv[2]}: {len(a):,} answers, {len(a) - len(diff):,} byte-identical, {len(diff):,} different")
for x, y in diff[:5]:
    print("  base:", x[:300])
    print("  var :", y[:300])
