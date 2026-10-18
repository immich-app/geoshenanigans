#!/usr/bin/env python3
"""Test alternative state mappings for the geocoder address (A+B rows of comparison.jsonl), and
compare minimal vs recommended outputs (comparison_recommended.jsonl)."""
import json
import os
import unicodedata
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent


def norm(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.casefold().replace("-", " ").replace("'", "").replace("’", "").split())


def soft(a, b):
    a, b = norm(a), norm(b)
    return a == b or (a is not None and b is not None and len(a) >= 4 and len(b) >= 4 and (a in b or b in a))


rows = [json.loads(line) for line in open(HERE / os.environ.get("IN", "comparison.jsonl"), encoding="utf-8")]
ab = [r for r in rows if r["group"] in ("A_strat", "B_popw")]
n = len(ab)


def pct(a, b=n):
    return f"{a} ({100 * a / b:.1f}%)"


variants = {
    "V0 state ?? province (prototype)": ("state", "province"),
    "V1 + region": ("state", "province", "region"),
    "V2 + region + county": ("state", "province", "region", "county"),
    "V3 + county (no region)": ("state", "province", "county"),
}
print(f"## state mapping variants (A+B, n={n}): non-null / norm-equal to GeoNames / soft-equal")
for name, keys in variants.items():
    nn = eq = sf = 0
    for r in ab:
        v = next((r["addr"][k] for k in keys if r["addr"].get(k)), None)
        o = r["old"]["state"]
        nn += v is not None
        eq += norm(v) == norm(o)
        sf += soft(v, o)
    print(f"- {name}: non-null {pct(nn)}, equal {pct(eq)}, soft {pct(sf)}")
print(f"- GeoNames state non-null: {pct(sum(1 for r in ab if r['old']['state']))}")

print("\n## which keys Japanese rows carry (all groups)")
jp = [r for r in rows if r["new"]["cc"] == "JP"]
print(f"- JP rows {len(jp)}; with state {sum(1 for r in jp if r['new']['state'])}; key counts:",
      Counter(k for r in jp for k in r["addr"]).most_common())

rec_path = HERE / "comparison_recommended.jsonl"
if rec_path.exists():
    rec = [json.loads(line) for line in open(rec_path, encoding="utf-8")]
    print(f"\n## minimal vs recommended, mapped fields differ (all {len(rows)} rows)")
    for f in ("country", "state", "city"):
        d = [(a, b) for a, b in zip(rows, rec) if a["new"][f] != b["new"][f]]
        print(f"- {f}: {len(d)} rows differ; e.g. " + "; ".join(f"{a['label']}: {a['new'][f]!r} -> {b['new'][f]!r}" for a, b in d[:5]))
