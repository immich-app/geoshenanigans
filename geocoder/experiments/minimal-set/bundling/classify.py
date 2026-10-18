#!/usr/bin/env python3
"""Classify the differences in comparison.jsonl (from compare.py) into improvement / regression /
neutral classes. Runs on the box; re-queries grid_query at the matched GeoNames points."""
import json
import re
import subprocess
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
GQ = "/ssd/shrink/target/release/examples/grid_query"
DS = "/ssd/shrink/idx/smallest"


def norm(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.casefold().replace("-", " ").replace("'", "").replace("’", "").split())


def latin(s):
    for ch in s:
        if ch.isalpha():
            try:
                if "LATIN" not in unicodedata.name(ch):
                    return False
            except ValueError:
                return False
    return True


def city_of(a):
    return a.get("city") or a.get("town") or a.get("village") or a.get("hamlet") or a.get("municipality")


ADMINISH = re.compile(r"\b(municipality|municipal|municipio|district|county|region|subdistrict|greater|kommun|commune|"
                      r"gemeinde|city of|shire|province|prefecture|borough|parish|township|ward)\b", re.I)

rows = [json.loads(line) for line in open(HERE / "comparison.jsonl", encoding="utf-8")]
ab = [r for r in rows if r["group"] in ("A_strat", "B_popw")]
n = len(ab)


def pct(a, b=n):
    return f"{a} ({100 * a / b:.1f}%)" if b else "-"


# ---------------- city
diff = [r for r in ab if r["old"]["city"] and r["new"]["city"] and norm(r["old"]["city"]) != norm(r["new"]["city"])]
inp = "".join(f"{r['old']['glat']} {r['old']['glon']}\n" for r in diff)
out = subprocess.run([GQ, DS], input=inp, capture_output=True, text=True, check=True).stdout.splitlines()
at_gn = {id(r): json.loads(line.split(" ", 2)[2]).get("address", {}) for r, line in zip(diff, out)}

cls = Counter()
ex = defaultdict(list)
for r in ab:
    o, g, addr = r["old"]["city"], r["new"]["city"], r["addr"]
    if norm(o) == norm(g):
        k = "same (normalised)"
    elif g is None:
        k = "REGRESSION geocoder city null"
    elif not latin(g):
        k = "geocoder name in non-Latin script"
    elif any(norm(v) == norm(o) for kk, v in addr.items() if kk not in ("country", "country_code")):
        k = "granularity: GeoNames place appears at another address level"
    elif norm(o) in norm(g) or norm(g) in norm(o):
        k = "name variant (one contains the other)"
    elif norm(city_of(at_gn[id(r)])) != norm(g):
        k = "location: photo lies in a different geocoder municipality than the GeoNames nearest point"
    elif ADMINISH.search(g):
        k = "granularity: geocoder city is an admin-unit name"
    else:
        k = "granularity: same geocoder municipality, GeoNames names a sub-place"
    cls[k] += 1
    if len(ex[k]) < 6:
        ex[k].append(f"{r['label']} ({r['lat']},{r['lon']}): GeoNames={o!r} [{r['old'].get('fcode')}, {r['old'].get('dist_m')} m] geocoder={g!r}")
print(f"## city classes, A+B (n={n})")
for k, v in cls.most_common():
    print(f"- {k}: {pct(v)}")
    for e in ex[k][:4]:
        print(f"    - {e}")

print("\n## non-Latin script share (A+B)")
for f in ("city", "state"):
    g_nl = sum(1 for r in ab if r["new"][f] and not latin(r["new"][f]))
    o_nl = sum(1 for r in ab if r["old"][f] and not latin(r["old"][f]))
    print(f"- {f}: geocoder {pct(g_nl)}, GeoNames {pct(o_nl)}")

# ---------------- state
print("\n## state classes, A+B")
sc = Counter()
sex = defaultdict(list)
bycc_null = Counter()
for r in ab:
    o, g = r["old"]["state"], r["new"]["state"]
    if norm(o) == norm(g):
        k = "same (normalised)"
    elif g is None and o is not None:
        k = "REGRESSION geocoder state null, GeoNames had one"
        bycc_null[r["new"]["cc"] or r["old"].get("cc")] += 1
    elif o is None and g is not None:
        k = "IMPROVEMENT geocoder has state, GeoNames null"
    elif g is None and o is None:
        k = "both null"
    elif len(norm(o)) >= 4 and len(norm(g)) >= 4 and (norm(o) in norm(g) or norm(g) in norm(o)):
        k = "name variant (one contains the other)"
    else:
        k = "different unit or naming"
    sc[k] += 1
    if len(sex[k]) < 6:
        sex[k].append(f"{r['label']}: GeoNames={o!r} geocoder={g!r} addr_keys={sorted(set(r['addr']) - {'country', 'country_code'})}")
for k, v in sc.most_common():
    print(f"- {k}: {pct(v)}")
    for e in sex[k][:4]:
        print(f"    - {e}")
print("- geocoder state null where GeoNames had one, by country:", ", ".join(f"{k}:{v}" for k, v in bycc_null.most_common(40)))

# ---------------- country
print("\n## country (A+B): GeoNames country vs geocoder country_code looked up in countryInfo.txt")
mm = [r for r in ab if r["old"]["country"] != r["new"]["country"]]
print(f"- mismatches {pct(len(mm))}")
for r in mm:
    print(f"    - {r['label']} ({r['lat']},{r['lon']}): GeoNames={r['old']['country']!r} [{r['old'].get('cc')}] geocoder={r['new']['country']!r} [{r['new']['cc']}] raw={r['new']['country_raw']!r}")

print("\n## country string if the geocoder's own `country` value were stored (all rows): distinct differing pairs")
pairs = Counter((r["old"]["country"], r["new"]["country_raw"]) for r in rows if r["old"]["country"] and r["old"]["country"] != r["new"]["country_raw"])
print(f"- rows differing: {sum(pairs.values())} of {len(rows)}")
for (a, b), c in pairs.most_common(40):
    print(f"    - {c:3d} {a!r} -> {b!r}")

# ---------------- spotlight
print("\n## spotlight: Japan / London / New Jersey / edge rows (all groups)")
for r in rows:
    if r["group"] == "C_edge" or (r["new"]["cc"] == "JP" and r["group"] == "B_popw"):
        o, g = r["old"], r["new"]
        print(f"- [{r['group']}] {r['label']} ({r['lat']},{r['lon']}): GeoNames=({o['city']} | {o['state']} | {o['country']}) "
              f"geocoder=({g['city']} | {g['state']} | {g['country']})")
