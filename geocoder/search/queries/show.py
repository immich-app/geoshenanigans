#!/usr/bin/env python3
"""Pretty-print place-search JSONL: top N per query with score, text, importance, match."""
import json
import sys

n = int(sys.argv[2]) if len(sys.argv) > 2 else 6
for line in open(sys.argv[1], encoding="utf-8"):
    d = json.loads(line)
    print(f"## {d['q']}  ({d['us']:.0f} us)")
    for r in d["results"][:n]:
        print(f"   {r['name'][:40]:40} | {(r['admin1name'] or '')[:18]:18} | {r['country'][:14]:14} | {r['kind']} {r['type'][:12]:12} "
              f"s={r['score']:.3f} t={r['text']:.2f} imp={r['importance']:.2f} m={r.get('matched', '')[:24]}/{r.get('matched_class')} "
              f"{r['latitude']:.4f},{r['longitude']:.4f} {r['osm']}")
