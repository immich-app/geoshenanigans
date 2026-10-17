#!/usr/bin/env python3
"""Classify top-5 misses of one system on held-out categories.
  miss_classes.py <per_query.json> <system: ours|base> <category prefix>
  offset   a top-5 result has the query's name (folded) within 50 km of the expected point:
           same place, label points differ (GeoNames municipality point vs OSM node)
  namesake a top-5 result has the query's name but lies elsewhere: an equally named place
  other    no top-5 result carries the name (spelling/transliteration/coverage)"""
import collections
import json
import math
import sys
import unicodedata


def fold(s):
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).casefold().replace("-", " ").strip()


def km(a, b, c, d):
    p = math.pi / 180
    h = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(min(1.0, h)))


per = json.load(open(sys.argv[1]))
sysname, prefix = sys.argv[2], sys.argv[3]
key = "ours" if sysname == "ours" else "base"
cls = collections.Counter()
examples = collections.defaultdict(list)
for p in per:
    if not p["cat"].startswith(prefix):
        continue
    r = p[key + "_rank"]
    if r is not None and r <= 5:
        continue
    want = fold(p["q"].split(",")[0])
    lat, lon = p["targets"][0][:2]
    same = [x for x in p[key][:5] if fold(x["name"]) == want or fold(x["name"]).startswith(want)]
    if any(km(lat, lon, x["lat"], x["lon"]) < 50 for x in same):
        c = "offset"
    elif same:
        c = "namesake"
    else:
        c = "other"
    cls[c] += 1
    examples[c].append(p["q"])
print(dict(cls))
for c, qs in examples.items():
    print(c, qs[:12])
