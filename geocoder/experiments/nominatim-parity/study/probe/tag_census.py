"""tag_census.py <objects.opl> <summary.json> [<summary.json> ...] : the tags that decide Nominatim's
import (type; boundary, place, postal_code / postcode / addr:postcode) of each object the builds
disagree on, tallied per summary (dropped_ids.py output) and side."""
import collections
import json
import sys

tags = {}
for line in open(sys.argv[1], encoding="utf-8"):
    parts = line.split(" ")
    oid = parts[0]
    t = {}
    for p in parts:
        if p.startswith("T"):
            for kv in p[1:].split(","):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    t[k] = v
    key = ("r" if oid[0] == "r" else "w" if oid[0] == "w" else oid[0]) + oid[1:].split()[0]
    tags[key] = t


def kind(t):
    typ = t.get("type", "<none>")
    b = t.get("boundary")
    has_code = any(k in t for k in ("postal_code", "postcode", "addr:postcode"))
    return f"type={typ if typ in ('multipolygon', 'boundary') else ('<none>' if typ == '<none>' else 'other')}" \
           f" boundary={b or '-'}" + (" place" if "place" in t else "") + \
           ((" with-code" if has_code else " no-code") if b == "postal_code" else "")


for path in sys.argv[2:]:
    d = json.load(open(path))
    print(path)
    for side in ("only_a", "only_b"):
        c = collections.Counter()
        for k in d[side]:
            if k[0] in "rw":
                c[(k[0], kind(tags[k]) if k in tags else "not in planet")] += 1
        print(" ", side, dict(c.most_common(12)))
    for side in ("poi_only_a", "poi_only_b"):
        c = collections.Counter(kind(tags[k]).split(" ")[0] if k in tags else "not in planet" for k in d[side])
        print(" ", side, dict(c))
