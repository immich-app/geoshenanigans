"""Points where road / house_number / postcode / landmark differ but no admin-derived field differs
(admin = address minus those four keys), per configuration vs T, all pools together.
usage: nonadmin_only.py name ..."""
import json, sys
POOLS = ["places", "random", "L2", "L4", "L8", "p200k"]
NON = ("road", "house_number", "postcode", "landmark")
def ad(line):
    return json.loads(line.split(" ", 2)[2]).get("address", {})
for n in sys.argv[1:]:
    with_admin = only = 0
    per = {k: 0 for k in NON}
    for p in POOLS:
        for x, y in zip(open(f"/ssd/shrink/grid/q/T.{p}.jsonl"), open(f"/ssd/shrink/grid/q/{n}.{p}.jsonl")):
            if x == y:
                continue
            a, b = ad(x), ad(y)
            ch = [k for k in NON if a.get(k) != b.get(k)]
            if not ch:
                continue
            adm = {k: v for k, v in a.items() if k not in NON} != {k: v for k, v in b.items() if k not in NON}
            if adm:
                with_admin += 1
            else:
                only += 1
                for k in ch:
                    per[k] += 1
    print(f"{n:12s} points with a non-admin field change: {with_admin + only:,}; with an admin change too: {with_admin:,}; "
          f"non-admin only: {only:,} {per}")
