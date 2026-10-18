"""Evaluate our grid_query answers against loc-to-city's checks with THEIR rules.

usage: python -I evaluate.py <out dir>
reads  <out>/checks.json, <out>/ne_names.json, <out>/answers_<set>.txt
writes <out>/results.json (every check x set x mapper) and prints markdown tables.
"""
import collections
import json
import re
import sys
import unicodedata

OUT = sys.argv[1]
SETS = ["smallest", "recommended", "max"]
TIERS = ["country", "region", "city"]
STATUSES = ["fixed", "false-positive", "open", "unsolvable", "human-error"]


def first(addr, keys):
    for k in keys:
        v = addr.get(k)
        if v:
            return v
    return None


CITY_A = ["city", "town", "village", "hamlet", "municipality"]
MAPPERS = {
    "A": {"region": ["state", "province"], "city": CITY_A},
    "B": {"region": ["state", "province", "region", "county"], "city": CITY_A},
}


def add_mapper(name, spec):
    MAPPERS[name] = spec


NE_UNITS = []


def dependency_code(addr, cc):
    """Consumer-side: a region-level name that is a Natural Earth unit of the same sovereign."""
    if not cc:
        return cc
    sov = {u["sov"] for u in NE_UNITS if u["iso"] == cc}
    for key in ("state", "region", "province"):
        v = addr.get(key)
        if not v:
            continue
        fv = fold(v)
        for u in NE_UNITS:
            if u["sov"] in sov and u["iso"] != cc and fv in {fold(n) for n in u["names"]}:
                return u["iso"]
    return cc


CITY_PREFIX = re.compile(r"^City of (?=\S)")
CITY_SUFFIX = re.compile(r" (?:City District|Municipality)$")


def clean_city(v):
    if v is None:
        return v
    return CITY_SUFFIX.sub("", CITY_PREFIX.sub("", v))


# ---- their fold() and namedFor(), evaluate.ts ----
NONWORD = r"[\W_]"


def fold(s):
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.category(c).startswith("M"))
    s = s.lower()
    s = re.sub("[‘’`]", "'", s)
    return re.sub(r"(^|" + NONWORD + r")(?:saint|st\.?)(?=$|" + NONWORD + r")", r"\1st", s)


def named_for(names, wanted_list):
    wanted = [fold(w) for w in wanted_list]
    for name in names:
        n = fold(name)
        for w in wanted:
            if re.search(r"(^|" + NONWORD + r")" + re.escape(w) + r"($|" + NONWORD + r")", n):
                return True
    return False


def matches(actual, exp, tier, name_to_iso, country_code):
    """actual: our one name for the tier (or None). Country compares ISO codes."""
    t = exp["t"]
    if t == "none":
        return actual is None
    if actual is None:
        return False
    if t == "any":
        return True
    if tier == "country":
        if t == "str":
            return country_code in {name_to_iso[n] for n in exp["v"]}
        raise ValueError(exp)
    if t == "namedFor":
        return named_for([actual], [exp["v"], *exp["aliases"]])
    return actual in exp["v"]


def describe(exp):
    if exp is None:
        return "-"
    t = exp["t"]
    if t in ("any", "none"):
        return t
    if t == "namedFor":
        return "named for " + exp["v"]
    return " | ".join(exp["v"])


# ---- helpers for classifying failures ----
SUFFIXES = [
    "prefecture", "province", "region", "county", "district", "governorate", "oblast", "state",
    "municipality", "city", "metropolitan city", "metropolis", "capital", "capital district",
    "special city", "metropolitan", "urban", "rural", "parish", "department", "territory",
    "autonomous community", "autonomous region", "kray", "krai", "republic", "voivodeship",
    "canton", "shi", "ken", "to", "fu", "do", "gun", "si", "town", "village", "commune",
    "community", "borough", "of",
]
PREFIXES = ["city of", "municipality of", "province of", "state of", "district of", "republic of",
            "county of", "town of", "the", "canton of", "region of", "gemeente", "comune di", "provincia di"]


def norm(s):
    s = fold(s)
    s = re.sub(r"[\W_]+", " ", s).strip()
    changed = True
    while changed:
        changed = False
        for p in PREFIXES:
            if s.startswith(p + " ") and len(s) > len(p) + 1:
                s = s[len(p) + 1:]
                changed = True
        for x in SUFFIXES:
            if s.endswith(" " + x) and len(s) > len(x) + 1:
                s = s[: -len(x) - 1]
                changed = True
    return s


def naming_only(actual, exp):
    if actual is None or exp["t"] not in ("str", "namedFor"):
        return False
    wanted = exp["v"] if exp["t"] == "str" else [exp["v"], *exp["aliases"]]
    a = norm(actual)
    for w in wanted:
        b = norm(w)
        if a == b:
            return True
        if a and b and (re.search(r"(^| )" + re.escape(b) + r"( |$)", a) or re.search(r"(^| )" + re.escape(a) + r"( |$)", b)):
            return True
    return False


def keys_holding(addr, exp, strict=True):
    """Address keys whose value would satisfy exp (their rule), excluding country keys."""
    out = []
    if exp is None or exp["t"] not in ("str", "namedFor"):
        return out
    for k, v in addr.items():
        if k in ("country", "country_code", "postcode", "house_number", "road", "landmark"):
            continue
        ok = matches(v, exp, "city", {}, None) if strict else naming_only(v, exp)
        if ok:
            out.append(k)
    return out


def load_answers(s):
    rows = {}
    order = [tuple(map(float, l.split())) for l in open(f"{OUT}/points.txt")]
    lines = open(f"{OUT}/answers_{s}.txt", encoding="utf-8").read().splitlines()
    assert len(lines) == len(order), (s, len(lines), len(order))
    for pt, line in zip(order, lines):
        lat, lng, js = line.split(" ", 2)
        assert abs(float(lat) - pt[0]) < 1e-5 and abs(float(lng) - pt[1]) < 1e-5, (line, pt)
        rows[pt] = json.loads(js).get("address") or {}
    return rows


def other_city_distinct(a_addr, b_addr, mapper):
    """Strict: city strings differ. Lenient: same string but different parent (municipality/county)."""
    ca = first(a_addr, MAPPERS[mapper]["city"])
    cb = first(b_addr, MAPPERS[mapper]["city"])
    if ca is None or cb is None:
        return False, False, ca, cb
    strict = ca != cb
    parents = ["municipality", "county", "city_district", "state_district"]
    lenient = strict or any(a_addr.get(k) != b_addr.get(k) for k in parents)
    return strict, lenient, ca, cb


def evaluate_one(chk, addr, answers, mapper, name_to_iso):
    m = MAPPERS[mapper]
    cc = (addr.get("country_code") or "").upper() or None
    if m.get("dep"):
        cc = dependency_code(addr, cc)
    city = first(addr, m["city"])
    if m.get("clean"):
        city = clean_city(city)
    actual = {
        "country": addr.get("country") if cc else None,
        "region": first(addr, m["region"]),
        "city": city,
    }
    tiers = {}
    for t in TIERS:
        exp = chk[t]
        if exp is None:
            continue
        ok = matches(actual[t], exp, t, name_to_iso, cc)
        tiers[t] = ok
    other = None
    if chk["otherCityThan"]:
        b = answers[tuple(chk["otherCityThan"])]
        strict, lenient, ca, cb = other_city_distinct(addr, b, mapper)
        other = {"strict": strict, "lenient": lenient, "a": ca, "b": cb,
                 "a_parent": addr.get("municipality"), "b_parent": b.get("municipality")}
        tiers["otherCity"] = lenient
    return {
        "pass": all(tiers.values()),
        "tiers": tiers,
        "actual": actual,
        "cc": cc,
        "other": other,
    }


def classify(chk, addr, r, mapper):
    """One label per failing tier."""
    labels = {}
    for t, ok in r["tiers"].items():
        if ok or t == "otherCity":
            if t == "otherCity" and not ok:
                labels[t] = "same-city-both-points"
            continue
        exp = chk[t]
        act = r["actual"][t]
        if t == "country":
            if exp["t"] == "none":
                labels[t] = "country-where-none-expected"
            elif act is None:
                labels[t] = "missing-tier"
            else:
                labels[t] = "wrong-country-code"
            continue
        if exp["t"] == "none":
            labels[t] = "tier-filled-where-none-expected"
            continue
        holders = keys_holding(addr, exp)
        holders_loose = keys_holding(addr, exp, strict=False)
        if act is None:
            if holders:
                labels[t] = "missing-tier; value under " + "/".join(sorted(holders))
            elif holders_loose:
                labels[t] = "missing-tier; similar name under " + "/".join(sorted(holders_loose))
            else:
                labels[t] = "missing-tier"
            continue
        if naming_only(act, exp):
            labels[t] = "naming-only"
        elif holders:
            labels[t] = "wrong-key; value under " + "/".join(sorted(holders))
        elif holders_loose:
            labels[t] = "wrong-key; similar name under " + "/".join(sorted(holders_loose))
        else:
            labels[t] = "different-place"
    return labels


def main():
    checks = json.load(open(f"{OUT}/checks.json", encoding="utf-8"))
    ne = json.load(open(f"{OUT}/ne_names.json", encoding="utf-8"))
    name_to_iso = ne["name_to_iso"]
    answers = {s: load_answers(s) for s in SETS}
    NE_UNITS.extend(json.load(open(f"{OUT}/ne_units.json", encoding="utf-8")))
    if len(sys.argv) > 2:
        MAPPERS.clear()
        for name, spec in json.load(open(sys.argv[2])).items():
            add_mapper(name, spec)
    results = []
    for s in SETS:
        for mapper in MAPPERS:
            for chk in checks:
                addr = answers[s][tuple(chk["at"])]
                r = evaluate_one(chk, addr, answers[s], mapper, name_to_iso)
                r["labels"] = classify(chk, addr, r, mapper) if not r["pass"] else {}
                r.update({"set": s, "mapper": mapper, "id": chk["id"], "name": chk["name"],
                          "group": chk["group"], "status": chk["status"], "section": chk["section"],
                          "expected": {t: describe(chk[t]) for t in TIERS}, "addr": addr})
                results.append(r)
    tag = sys.argv[3] if len(sys.argv) > 3 else "AB"
    json.dump(results, open(f"{OUT}/results_{tag}.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    # ---- tables ----
    def table(rows, header):
        print("| " + " | ".join(header) + " |")
        print("|" + "---|" * len(header))
        for row in rows:
            print("| " + " | ".join(str(x) for x in row) + " |")
        print()

    groups = [("spot", "fixed"), ("spot", "false-positive"), ("spot", "open"),
              ("capital", "fixed"), ("capital", "open")]
    print("## pass counts (checks passing / checks), by group and status\n")
    rows = []
    for s in SETS:
        for mapper in MAPPERS:
            rr = [r for r in results if r["set"] == s and r["mapper"] == mapper]
            row = [s, mapper]
            for g, st in groups:
                sub = [r for r in rr if r["group"] == g and r["status"] == st]
                row.append(f"{sum(r['pass'] for r in sub)}/{len(sub)}")
            row.append(f"{sum(r['pass'] for r in rr)}/{len(rr)}")
            rows.append(row)
    table(rows, ["set", "mapper"] + [f"{g} {st}" for g, st in groups] + ["all"])

    print("## per tier (tier checks passing / tier checks)\n")
    rows = []
    for s in SETS:
        for mapper in MAPPERS:
            rr = [r for r in results if r["set"] == s and r["mapper"] == mapper]
            row = [s, mapper]
            for t in TIERS + ["otherCity"]:
                sub = [r["tiers"][t] for r in rr if t in r["tiers"]]
                row.append(f"{sum(sub)}/{len(sub)}")
            rows.append(row)
    table(rows, ["set", "mapper"] + TIERS + ["otherCity"])

    print("## failure labels per tier\n")
    for s in SETS:
        for mapper in MAPPERS:
            rr = [r for r in results if r["set"] == s and r["mapper"] == mapper]
            c = collections.Counter(f"{t}: {lab}" for r in rr for t, lab in r["labels"].items())
            print(f"### {s} / {mapper}")
            for k, v in sorted(c.items(), key=lambda kv: (-kv[1], kv[0])):
                print(f"- {v} {k}")
            print()


main()
