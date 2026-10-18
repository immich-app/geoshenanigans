#!/usr/bin/env python3
"""Compare grid_query jsonl outputs of a variant against a baseline.

usage: compare.py <baseline.jsonl> <variant.jsonl> [--show N]

Reports: points with any non-postcode address field different, points where
the Immich prototype mapping differs (immich-geocoder
server/src/repositories/geocoding.repository.ts:67-78: city = city ?? town ??
village ?? hamlet ?? municipality, state = state ?? province, country from
country_code, landmark), per-field diff counts, places[] diffs, display_name
diffs beyond the postcode token, and examples.
"""
import json, sys
from collections import Counter


def load(p):
    out = {}
    for line in open(p):
        a, b, js = line.split(" ", 2)
        out[(a, b)] = json.loads(js)
    return out


def immich(a):
    return (a.get("city") or a.get("town") or a.get("village") or a.get("hamlet") or a.get("municipality"),
            a.get("state") or a.get("province"), a.get("country_code"), a.get("country"), a.get("landmark"))


def dn_wo_pc(r):
    pc = r.get("address", {}).get("postcode")
    parts = [p.strip() for p in r.get("display_name", "").split(",")]
    return [p for p in parts if p != pc]


def main():
    base, var = load(sys.argv[1]), load(sys.argv[2])
    show = int(sys.argv[sys.argv.index("--show") + 1]) if "--show" in sys.argv else 5
    field_diff = Counter()
    n = any_non_pc = immich_diff = places_diff = dn_diff = 0
    pc_base = pc_var = 0
    ex = []
    for k, b in base.items():
        v = var[k]
        n += 1
        ba, va = b.get("address", {}), v.get("address", {})
        pc_base += "postcode" in ba
        pc_var += "postcode" in va
        nonpc = False
        for f in set(ba) | set(va):
            if ba.get(f) != va.get(f):
                field_diff[f] += 1
                nonpc |= f != "postcode"
        any_non_pc += nonpc
        if immich(ba) != immich(va):
            immich_diff += 1
        if b.get("places") != v.get("places"):
            places_diff += 1
        if dn_wo_pc(b) != dn_wo_pc(v):
            dn_diff += 1
        if nonpc and len(ex) < show:
            ex.append((k, {f: ba.get(f) for f in set(ba) | set(va) if ba.get(f) != va.get(f)},
                       {f: va.get(f) for f in set(ba) | set(va) if ba.get(f) != va.get(f)}))
    print(f"points {n} | postcode present base {pc_base} var {pc_var} | any non-postcode address field differs {any_non_pc} | "
          f"Immich (city,state,country,landmark) differs {immich_diff} | places[] differs {places_diff} | display_name (minus postcode) differs {dn_diff}")
    print("  per-field diffs:", dict(field_diff.most_common()))
    for k, bb, vv in ex:
        print("   ", " ".join(k), "base", bb, "-> var", vv)


if __name__ == "__main__":
    main()
