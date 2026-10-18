"""Strings: are tiers sorted, exact duplicates within/across tiers, and the strings only dead
records reference."""
import json
import numpy as np
from lists import *

lay = json.load(open(f"{D}/strings_layout.json"))["tiers"]
allstr = {}
for t in lay:
    b = open(f"{D}/{t['file']}", "rb").read()
    parts = b.split(b"\0")[:-1]
    srt = all(parts[i] <= parts[i + 1] for i in range(len(parts) - 1))
    print(f"{t['name']:8s} strings {len(parts):,} sorted bytewise: {srt} distinct: {len(set(parts)) == len(parts)}")
    allstr[t["name"]] = parts
seen = {}
dups = 0
dup_bytes = 0
for name, parts in allstr.items():
    for s in parts:
        if s in seen:
            dups += 1
            dup_bytes += len(s) + 1
        else:
            seen[s] = name
print(f"exact duplicates across tiers: {dups:,} strings, {dup_bytes:,} bytes")
