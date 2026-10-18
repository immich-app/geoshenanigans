"""Independent re-implementation of loc-to-city's matching rules (verify/src/evaluate.ts) on our answers.

usage: python -I l2c_eval.py <checks_v.json> <ne geojson> <answers dir> <sample ids json> <out json>
answers dir holds ans_<set>.txt ("lat lng <json>" per line, from grid_query) for sets smallest, recommended, max.

Tier mapping is "mapper B" as the report defines it:
  country = our country_code (compared as ISO code: their names -> NE NAME -> ISO_A2_EH)
  region  = first non-empty of state, province, region, county
  city    = first non-empty of city, town, village, hamlet, municipality
Rules (evaluate.ts): string/list expectation = exact equality with one of the names (we have one name);
namedFor = fold() then whole-word match; ANY = resolves; NONE = does not resolve; otherCityThan = both
resolve a city and the city ids differ (we have no ids: strict = names differ, lenient = names differ or
one of municipality/county/city_district/state_district differs).
"""
import json, re, sys, unicodedata
from collections import Counter

checks_path, ne_path, ans_dir, sample_path, out_path = sys.argv[1:6]
checks = json.load(open(checks_path, encoding="utf-8"))
SETS = ["smallest", "recommended", "max"]
REGION = ["state", "province", "region", "county"]
CITY = ["city", "town", "village", "hamlet", "municipality"]

# ---- NE name -> ISO_A2_EH, built from the geojson directly ----
ne = json.load(open(ne_path, encoding="utf-8"))
name_iso = {}
for ft in ne["features"]:
    p = ft["properties"]
    name_iso.setdefault(p["NAME"], set()).add(p["ISO_A2_EH"])
expected_countries = sorted({n for c in checks if c["country"] and c["country"]["t"] == "str" for n in c["country"]["v"]})
missing = [n for n in expected_countries if n not in name_iso]
ambig = {n: sorted(name_iso[n]) for n in expected_countries if n in name_iso and len(name_iso[n]) > 1}
bad = {n: sorted(name_iso[n]) for n in expected_countries if n in name_iso and any(not re.fullmatch(r"[A-Z]{2}", x) for x in name_iso[n])}
N2I = {n: next(iter(name_iso[n])) for n in expected_countries if n in name_iso}


def fold(s):
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "M").lower()
    s = re.sub("[‘’`]", "'", s)
    # JS: /(^|[^\p{L}\p{N}])(?:saint|st\.?)(?=$|[^\p{L}\p{N}])/gu -> "$1st"
    out, i = [], 0
    while i < len(s):
        prev_ok = i == 0 or not isLN(s[i - 1])
        m = re.match(r"(saint|st\.?)", s[i:]) if prev_ok else None
        if m:
            j = i + len(m.group(1))
            # JS regex is greedy with backtracking: "st." then lookahead; fall back to "st"
            for cand in ([m.group(1)] + (["st"] if m.group(1) == "st." else [])):
                j = i + len(cand)
                if j == len(s) or not isLN(s[j]):
                    out.append("st"); i = j
                    break
            else:
                out.append(s[i]); i += 1
            continue
        out.append(s[i]); i += 1
    return "".join(out)


def isLN(ch):
    return unicodedata.category(ch)[0] in "LN"


def whole_word(hay, needle):
    k = hay.find(needle)
    while k >= 0:
        a, b = k, k + len(needle)
        if (a == 0 or not isLN(hay[a - 1])) and (b == len(hay) or not isLN(hay[b])):
            return True
        k = hay.find(needle, k + 1)
    return False


def first(addr, keys):
    for k in keys:
        if addr.get(k):
            return addr[k]
    return None


def match(tier, exp, addr):
    cc = (addr.get("country_code") or "").upper() or None
    val = cc if tier == "country" else first(addr, REGION if tier == "region" else CITY)
    t = exp["t"]
    if t == "none":
        return val is None
    if val is None:
        return False
    if t == "any":
        return True
    if tier == "country":
        return cc in {N2I[n] for n in exp["v"]}
    if t == "namedFor":
        return any(whole_word(fold(val), fold(w)) for w in [exp["v"], *exp["aliases"]])
    return val in exp["v"]


def load(s):
    rows = {}
    for line in open(f"{ans_dir}/ans_{s}.txt", encoding="utf-8"):
        lat, lng, js = line.rstrip("\n").split(" ", 2)
        rows[(round(float(lat), 6), round(float(lng), 6))] = json.loads(js).get("address") or {}
    return rows


def key(pt):
    return (round(pt[0], 6), round(pt[1], 6))


ans = {s: load(s) for s in SETS}
res = {}
for s in SETS:
    for c in checks:
        addr = ans[s][key(c["at"])]
        tiers = {t: match(t, c[t], addr) for t in ("country", "region", "city") if c[t]}
        oc = None
        if c["otherCityThan"]:
            b = ans[s][key(c["otherCityThan"])]
            ca, cb = first(addr, CITY), first(b, CITY)
            strict = ca is not None and cb is not None and ca != cb
            lenient = ca is not None and cb is not None and (ca != cb or any(
                addr.get(k) != b.get(k) for k in ("municipality", "county", "city_district", "state_district")))
            oc = dict(strict=strict, lenient=lenient, a=ca, b=cb)
            tiers["otherCity"] = lenient
        res[f"{s}|{c['id']}"] = dict(pass_=all(tiers.values()), tiers=tiers, other=oc,
                                     strict_pass=all(v for k, v in tiers.items() if k != "otherCity") and (oc is None or oc["strict"]))

# compare with the report's results_variants.json (mapper B)
prev = {}
for r in json.load(open("/ssd/loc2city/out/results_variants.json", encoding="utf-8")):
    if r["mapper"] == "B":
        prev[f"{r['set']}|{r['id']}"] = r
sample = json.load(open(sample_path))
summary = {"ne": dict(expected_names=len(expected_countries), missing=missing, ambiguous=ambig, non_iso=bad)}
for s in SETS:
    allp = sum(res[f"{s}|{c['id']}"]["pass_"] for c in checks)
    strict = sum(res[f"{s}|{c['id']}"]["strict_pass"] for c in checks)
    per = Counter()
    tot = Counter()
    for c in checks:
        for t, ok in res[f"{s}|{c['id']}"]["tiers"].items():
            tot[t] += 1; per[t] += ok
    dis_all = [cid for cid in (c["id"] for c in checks) if res[f"{s}|{cid}"]["pass_"] != prev[f"{s}|{cid}"]["pass"]]
    dis_tier = [(cid, t) for cid in (c["id"] for c in checks) for t, ok in res[f"{s}|{cid}"]["tiers"].items()
                if prev[f"{s}|{cid}"]["tiers"].get(t) != ok]
    smp = [cid for cid in sample]
    smp_pass = sum(res[f"{s}|{cid}"]["pass_"] for cid in smp)
    smp_prev = sum(prev[f"{s}|{cid}"]["pass"] for cid in smp)
    smp_dis = [cid for cid in smp if res[f"{s}|{cid}"]["pass_"] != prev[f"{s}|{cid}"]["pass"]]
    summary[s] = dict(all_pass=allp, strict_pass=strict, per_tier={t: f"{per[t]}/{tot[t]}" for t in tot},
                      disagree_checks=dis_all, disagree_tiers=dis_tier,
                      sample=dict(n=len(smp), mine=smp_pass, report=smp_prev, disagree=smp_dis))
json.dump(dict(summary=summary, results=res), open(out_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(json.dumps(summary, ensure_ascii=False, indent=1))
