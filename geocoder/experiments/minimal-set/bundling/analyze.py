#!/usr/bin/env python3
"""Summarise comparison.jsonl (from compare.py): per-field agreement and difference classes."""
import json
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent


def norm(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.casefold().replace("-", " ").replace("'", "").replace("’", "")
    return " ".join(s.split())


def soft_eq(a, b):
    """normalised equality, or one name contains the other (e.g. 'Dubai' vs 'Dubai Emirate')."""
    na, nb = norm(a), norm(b)
    if na is None or nb is None:
        return na == nb
    return na == nb or (len(na) >= 4 and na in nb) or (len(nb) >= 4 and nb in na)


import os
rows = [json.loads(l) for l in open(HERE / os.environ.get("IN", "comparison.jsonl"), encoding="utf-8")]
groups = defaultdict(list)
for r in rows:
    groups[r["group"]].append(r)
    groups["ALL_AB"].append(r) if r["group"] in ("A_strat", "B_popw") else None


def stats(rs):
    out = {"n": len(rs)}
    for f in ("country", "state", "city"):
        exact = sum(1 for r in rs if r["old"][f] == r["new"][f])
        nrm = sum(1 for r in rs if norm(r["old"][f]) == norm(r["new"][f]))
        soft = sum(1 for r in rs if soft_eq(r["old"][f], r["new"][f]))
        old_null = sum(1 for r in rs if r["old"][f] is None)
        new_null = sum(1 for r in rs if r["new"][f] is None)
        out[f] = dict(exact=exact, norm=nrm, soft=soft, old_null=old_null, new_null=new_null)
    out["country_raw_exact"] = sum(1 for r in rs if r["old"]["country"] == r["new"]["country_raw"])
    out["cc_eq"] = sum(1 for r in rs if r["old"].get("cc") and r["old"].get("cc") == r["new"]["cc"])
    out["old_cities500"] = sum(1 for r in rs if r["old"]["source"] == "cities500")
    out["all3_norm"] = sum(1 for r in rs if all(norm(r["old"][f]) == norm(r["new"][f]) for f in ("country", "state", "city")))
    # city name of GeoNames appears anywhere in the geocoder address
    out["city_in_addr"] = sum(1 for r in rs if r["old"]["city"] and any(norm(v) == norm(r["old"]["city"]) for k, v in r["addr"].items() if k not in ("country", "country_code")))
    return out


summary = {g: stats(rs) for g, rs in groups.items()}
json.dump(summary, open(HERE / os.environ.get("SUMMARY", "summary.json"), "w"), indent=1)


def pct(a, n):
    return f"{100*a/n:.1f}%" if n else "-"


print("| sample | n | country exact | state exact / norm / soft | city exact / norm / soft | all 3 (norm) | geocoder state null | geocoder city null | GeoNames city null |")
print("|---|---|---|---|---|---|---|---|---|")
for g in ("A_strat", "B_popw", "C_edge", "D_popw_exact", "ALL_AB"):
    s = summary[g]
    n = s["n"]
    print(f"| {g} | {n} | {pct(s['country']['exact'], n)} | {pct(s['state']['exact'], n)} / {pct(s['state']['norm'], n)} / {pct(s['state']['soft'], n)} | "
          f"{pct(s['city']['exact'], n)} / {pct(s['city']['norm'], n)} / {pct(s['city']['soft'], n)} | {pct(s['all3_norm'], n)} | "
          f"{pct(s['state']['new_null'], n)} | {pct(s['city']['new_null'], n)} | {pct(s['city']['old_null'], n)} |")
print()
for g in ("A_strat", "B_popw", "ALL_AB"):
    s = summary[g]
    print(g, "country using geocoder's own name:", pct(s["country_raw_exact"], s["n"]), " cc equal:", pct(s["cc_eq"], s["n"]),
          " GeoNames city found anywhere in geocoder address:", pct(s["city_in_addr"], s["n"]))

# ---- difference classes over A+B
ab = groups["ALL_AB"]
print("\n## country mismatches (A+B)")
for r in ab:
    if r["old"]["country"] != r["new"]["country"]:
        print(f"  {r['label']} ({r['lat']},{r['lon']}): GeoNames={r['old']['country']!r}[{r['old'].get('cc') or r['old'].get('a3')}] geocoder={r['new']['country']!r}[{r['new']['cc']}] raw={r['new']['country_raw']!r}")

print("\n## country_raw (geocoder's own country string) vs GeoNames countryInfo name, distinct pairs over all rows")
pairs = Counter((r["old"]["country"], r["new"]["country_raw"]) for r in rows if r["old"]["country"] != r["new"]["country_raw"])
for (a, b), n in pairs.most_common(60):
    print(f"  {n:3d}  {a!r} vs {b!r}")

print("\n## state null in geocoder, by country code (A+B)")
c = Counter(r["new"]["cc"] for r in ab if r["new"]["state"] is None)
tot = Counter(r["new"]["cc"] for r in ab)
print("  ", ", ".join(f"{k}:{v}/{tot[k]}" for k, v in c.most_common(80)))

print("\n## state mismatch (normalised, both non-null), by cc (A+B)")
c = Counter(r["new"]["cc"] for r in ab if r["new"]["state"] and r["old"]["state"] and norm(r["new"]["state"]) != norm(r["old"]["state"]))
print("  ", ", ".join(f"{k}:{v}/{tot[k]}" for k, v in c.most_common(80)))
print("  soft-equal share of those:", sum(1 for r in ab if r["new"]["state"] and r["old"]["state"] and norm(r["new"]["state"]) != norm(r["old"]["state"]) and soft_eq(r["new"]["state"], r["old"]["state"])))

print("\n## city null in geocoder, by cc (A+B)")
c = Counter(r["new"]["cc"] for r in ab if r["new"]["city"] is None)
print("  ", ", ".join(f"{k}:{v}/{tot[k]}" for k, v in c.most_common(80)))
print("  addr keys present when city null:", Counter(k for r in ab if r["new"]["city"] is None for k in r["addr"]).most_common())

print("\n## which address key supplied the geocoder city (A+B)")
def src_key(a):
    for k in ("city", "town", "village", "hamlet", "municipality"):
        if a.get(k):
            return k
    return None
print("  ", Counter(src_key(r["addr"]) for r in ab).most_common())

print("\n## GeoNames source (A+B):", Counter(r["old"]["source"] for r in ab).most_common())
print("## GeoNames fcode of matched place (A+B):", Counter(r["old"].get("fcode") for r in ab).most_common(12))

if "--examples" in sys.argv:
    print("\n## edge cases (C)")
    for r in groups["C_edge"]:
        o, n = r["old"], r["new"]
        print(f"  {r['label']}: GeoNames=({o['city']} | {o['state']} | {o['country']}) geocoder=({n['city']} | {n['state']} | {n['country']}) addr={json.dumps(r['addr'], ensure_ascii=False)}")
    print("\n## city mismatches sample (A+B, normalised, both non-null)")
    k = 0
    for r in ab:
        o, n = r["old"], r["new"]
        if o["city"] and n["city"] and norm(o["city"]) != norm(n["city"]):
            k += 1
            if k <= 80:
                print(f"  {r['label']} d={o.get('dist_m')}m: GeoNames={o['city']!r} [{o.get('fcode')}] geocoder={n['city']!r} addr={json.dumps(r['addr'], ensure_ascii=False)}")
    print("  total:", k)
