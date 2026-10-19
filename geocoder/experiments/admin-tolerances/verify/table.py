#!/usr/bin/env python3
"""table.py <score json> <border pool> <photo pool>... : key rows from a v5score.py result."""
import json
import sys

d = json.load(open(sys.argv[1]))
bp = sys.argv[2]
photos = sys.argv[3:]
cols = [(f"{bp}:B2", "country"), (f"{bp}:B3", "any"), (f"{bp}:B4", "state"), (f"{bp}:B5", "any"), (f"{bp}:B6", "city"),
        (f"{bp}:B7", "city"), (f"{bp}:B8", "city"), (bp, "any")]
for p in photos:
    cols += [(p, "city"), (p, "state"), (p, "country"), (p, "any")]
print("label".ljust(26) + "".join(f"{g.split(':')[-1]}:{f}".rjust(15) for g, f in cols))
print("n".ljust(26) + "".join(str(next(iter(d.values()))[g]["n"]).rjust(15) for g, f in cols))
for lab, v in d.items():
    if lab == "pairs":
        continue
    print(lab.ljust(26) + "".join(f"{v[g][f]:.3f}".rjust(15) for g, f in cols))
for pr, v in d.get("pairs", {}).items():
    print("PAIR " + pr)
    for g, f in cols:
        x = v.get(g, {}).get(f)
        if x:
            print(f"   {g}:{f} only_a {x['only_a']} only_b {x['only_b']} p {x['p']}")
