#!/usr/bin/env python3
"""Verifier comparator (written independently of grid/compare.py).

Per point: Immich triple = (city ?? town ?? village ?? hamlet ?? municipality, state ?? province, country_code);
admin = every address key except road, house_number, postcode, landmark (key AND value compared, key set too).
usage: vcompare.py <base.jsonl> <labels file or -> <name>=<jsonl> ...
Prints flip % per label group (and 'ALL'), Immich / admin, plus counts and country-only flips."""
import json, sys
from collections import defaultdict

NONADMIN = {"road", "house_number", "postcode", "landmark"}


def rows(path):
    out = []
    with open(path) as f:
        for line in f:
            la, lo, js = line.rstrip("\n").split(" ", 2)
            ad = json.loads(js).get("address", {}) or {}
            first = lambda *ks: next((ad[k] for k in ks if ad.get(k) is not None), None)
            imm = (first("city", "town", "village", "hamlet", "municipality"), first("state", "province"), ad.get("country_code"))
            adm = frozenset((k, json.dumps(v, sort_keys=True)) for k, v in ad.items() if k not in NONADMIN)
            non = tuple(ad.get(k) for k in sorted(NONADMIN))
            out.append(((la, lo), imm, adm, non))
    return out


base_path, lbl_path, cfgs = sys.argv[1], sys.argv[2], sys.argv[3:]
base = rows(base_path)
labels = [l.strip() for l in open(lbl_path)] if lbl_path != "-" else ["all"] * len(base)
assert len(labels) == len(base)
groups = defaultdict(list)
for i, l in enumerate(labels):
    groups[l].append(i)
    groups["ALL"].append(i)
    if l.startswith("photo"):
        groups["photo(all)"].append(i)
    if l.startswith("border"):
        groups["border(all)"].append(i)
        if l in ("border_L2", "border_L3", "border_L4", "border_L5", "border_L6", "border_L7", "border_L8"):
            groups["border(L2-8)"].append(i)
res = {"base": base_path}
for c in cfgs:
    n, p = c.split("=", 1)
    o = rows(p)
    assert len(o) == len(base) and all(a[0] == b[0] for a, b in zip(base, o)), n
    res[n] = {}
    for g, ix in sorted(groups.items()):
        imm = sum(base[i][1] != o[i][1] for i in ix)
        adm = sum(base[i][2] != o[i][2] for i in ix)
        cc = sum(base[i][1][2] != o[i][1][2] for i in ix)
        nonadmin_only = sum(base[i][3] != o[i][3] and base[i][2] == o[i][2] for i in ix)
        res[n][g] = {"n": len(ix), "immich": imm, "admin": adm, "country": cc, "nonadmin_without_admin": nonadmin_only,
                     "immich_pct": round(100 * imm / len(ix), 2), "admin_pct": round(100 * adm / len(ix), 2)}
print(json.dumps(res))
