"""Hand categorisation of every failing (check, tier), judged by reading our answer.

usage: python -I categorise.py <out dir> <results json> <mapper> [<mapper> ...]
Keys are (check-name prefix, tier[, our value]) -> category; anything unmatched
prints as UNCLASSIFIED so the table can't silently drop a failure.
"""
import collections
import json
import sys

DEP = "country: dependency reported as its sovereign"
ENCL = "country: wrong at an enclave / border / disputed water"
NAME = "naming only (English vs local, suffix, punctuation)"
REGL = "region: their per-country level we do not emit"
DIST = "city: a district/part of the city a person would name"
BIG = "city: we answer the city or council district; their override wants the sub-unit"
UNIT = "city: an enclosing or neighbouring unit instead of the town"
MISS = "city: missing (the city exists only as a region, or no place at all)"
WRONG = "wrong place (data or hierarchy error)"
NONE = "their NONE expectation (artefact check of their pipeline)"
REGMISS = "region: missing (city-state / no first-level unit)"

RULES = [
    # (prefix, tier, value or None, category)
    ("Old Basing and Lychpit", "city", None, BIG),
    ("Salhouse", "city", None, BIG),
    ("Manhattan", "city", None, BIG),
    ("Brooklyn", "city", None, BIG),
    ("Taipei", "city", None, BIG),
    ("Taipei", "region", None, REGMISS),
    ("Abidjan", "city", None, MISS),
    ("Dubai", "city", None, MISS),
    ("Beirut", "city", None, MISS),
    ("Muscat", "city", None, MISS),
    ("Matruh", "city", None, MISS),
    ("Stanley", "city", None, MISS),
    ("Willemstad", "city", None, MISS),
    ("Kabul", "city", None, MISS),
    ("Luanda", "city", None, MISS),
    ("Moscow", "city", None, MISS),
    ("Okahandja", "city", None, MISS),
    ("Astana", "city", None, MISS),
    ("Abuja", "city", None, MISS),
    ("Kuwait City", "city", None, MISS),
    ("Bridgetown", "city", None, MISS),
    ("Al Ain", "city", None, DIST),
    ("Asunción", "city", None, NAME),
    ("Damascus", "city", None, DIST),
    ("Ganta", "city", None, DIST),
    ("Gbarnga", "city", None, DIST),
    ("Merzkirchen", "city", None, DIST),
    ("Algiers", "city", None, DIST),
    ("Ankara", "city", None, DIST),
    ("Beijing", "city", None, DIST),
    ("Brasília", "city", None, DIST),
    ("Lima", "city", None, DIST),
    ("London", "city", None, DIST),
    ("Panama City", "city", None, DIST),
    ("Phnom Penh", "city", None, DIST),
    ("Tokyo", "city", None, DIST),
    ("Saipan", "city", None, DIST),
    ("Brighton", "city", None, DIST),
    ("Saint Petersburg", "city", None, DIST),
    ("Sarajevo", "city", None, DIST),
    ("Nairobi", "city", None, DIST),
    ("Tórshavn", "city", None, DIST),
    ("Kinshasa", "city", None, DIST),
    ("Poland: namesake powiat tomaszowski", "city", None, DIST),
    ("Šentjur", "city", None, DIST),
    ("Mata-Utu", "city", None, UNIT),
    ("Nassau", "city", None, UNIT),
    ("Palikir", "city", None, UNIT),
    ("Ngerulmud", "city", None, UNIT),
    ("Kingston (Norfolk", "city", None, UNIT),
    ("Wormerveer", "city", None, UNIT),
    ("Europoort", "city", None, UNIT),
    ("Ter Heijde", "city", None, UNIT),
    ("Alteveer", "city", None, UNIT),
    ("Ansen", "city", None, WRONG),
    ("Washington", "city", None, WRONG),
    ("Lotte", "city", None, WRONG),
    ("Živinice", "city", None, WRONG),
    ("Macau", "city", None, WRONG),
    ("Plymouth", "city", None, WRONG),
    ("Vatican", "city", None, WRONG),
    ("Vatican", "region", None, WRONG),
    ("Dek'emhāre", "city", None, NONE),
    ("Tombouctou", "city", None, NONE),
    ("Belfast", "city", None, NAME),
    ("Edinburgh", "city", None, NAME),
    ("Mbale", "city", None, NAME),
    ("Nuremberg", "city", None, NAME),
    ("Papeete", "city", None, NAME),
    ("Paphos", "city", None, NAME),
    ("Rincon", "city", None, NAME),
    ("Riyadh", "city", None, NAME),
    ("Samarkand", "city", None, NAME),
    ("Port-Vila", "city", None, NAME),
    ("Pyongyang", "city", None, NAME),
    ("Sanaa", "city", None, NAME),
    ("Nukus", "city", None, NAME),
    ("Zabrat", "city", None, NAME),
    ("Poland:", "city", None, NAME),
    ("Campione", "region", None, NAME),
    ("Dollart", "region", None, NAME),
    ("Dordrecht", "region", None, NAME),
    ("Lipari", "region", None, NAME),
    ("Lotte", "region", None, NAME),
    ("Moncton", "region", None, NAME),
    ("Nukus", "region", None, NAME),
    ("Valparaíso", "region", None, NAME),
    ("Prague", "region", None, NAME),
    ("Poland:", "region", None, REGL),
    ("Central Ljubljana", "region", None, REGL),
    ("Kranj", "region", None, REGL),
    ("Maribor", "region", None, REGL),
    ("Šentjur", "region", None, REGL),
    ("La Massana", "region", None, REGL),
    ("Serravalle", "region", None, REGL),
    ("Victoria (Gozo)", "region", None, REGL),
    ("Kralendijk", "region", None, REGL),
    ("Rincon", "region", None, REGL),
    ("The Bottom", "region", None, REGL),
    ("Oranjestad is a town", "region", None, REGL),
    ("Zabrat", "region", None, REGMISS),
    ("Hamburg", "region", None, REGMISS),
    ("Daugavpils", "region", None, REGMISS),
    ("Monaco", "region", None, REGMISS),
    ("Baarle-Hertog", "country", None, ENCL),
    ("Campione", "country", None, ENCL),
    ("Dollart", "country", None, ENCL),
    ("Jungholz", "country", None, ENCL),
    ("Llívia", "country", None, ENCL),
    ("Kolkata", "country", None, ENCL),
    ("Nukus", "country", None, ENCL),
    ("Vatican", "country", None, ENCL),
    ("Marigot", "country", "NL", ENCL),
]
DEPENDENCIES = {"HK", "MO", "PR", "GU", "AS", "VI", "MP", "AW", "CW", "SX", "PM", "WF", "PF", "NC",
                "BL", "MF", "TF", "AX", "NF"}


def category(r, tier, checks_by_id):
    name = r["name"]
    value = r["cc"] if tier == "country" else r["actual"].get(tier)
    if tier == "otherCity":
        return "otherCity"
    for prefix, t, v, cat in RULES:
        if t == tier and name.startswith(prefix) and (v is None or v == value):
            return cat
    if tier == "country":
        exp = checks_by_id[r["id"]]["country"]
        if exp and exp["t"] == "str" and any(NAME_TO_ISO[n] in DEPENDENCIES for n in exp["v"]):
            return DEP
    return "UNCLASSIFIED"


out, path, *mappers = sys.argv[1:]
R = json.load(open(path, encoding="utf-8"))
checks_by_id = {c["id"]: c for c in json.load(open(f"{out}/checks.json", encoding="utf-8"))}
NAME_TO_ISO = json.load(open(f"{out}/ne_names.json", encoding="utf-8"))["name_to_iso"]
sets = ["smallest", "recommended", "max"]
table = collections.defaultdict(lambda: collections.Counter())
unclassified = set()
examples = collections.defaultdict(list)
for r in R:
    if r["mapper"] not in mappers or r["pass"]:
        continue
    for tier, ok in r["tiers"].items():
        if ok:
            continue
        cat = category(r, tier, checks_by_id)
        table[cat][(r["set"], r["mapper"])] += 1
        if cat == "UNCLASSIFIED":
            unclassified.add((r["set"], r["mapper"], r["name"], tier, str(r["actual"].get(tier)), r["cc"]))
        if r["set"] == "recommended" and r["mapper"] == mappers[0] and len(examples[cat]) < 6:
            got = r["cc"] if tier == "country" else r["actual"].get(tier)
            want = r["expected"].get(tier, "-") if tier != "otherCity" else "distinct"
            examples[cat].append(f"{want} -> {got}")
cols = [(s, m) for m in mappers for s in sets]
print("| category (failing tier checks) | " + " | ".join(f"{s} {m}" for s, m in cols) + " |")
print("|---|" + "---|" * len(cols))
for cat, c in sorted(table.items(), key=lambda kv: -sum(kv[1].values())):
    print(f"| {cat} | " + " | ".join(str(c[k]) for k in cols) + " |")
print("| total | " + " | ".join(str(sum(c[k] for c in table.values())) for k in cols) + " |")
print()
for cat, ex in examples.items():
    print(f"- {cat}: " + "; ".join(ex))
for u in sorted(unclassified):
    print("UNCLASSIFIED", u)

# How many failing checks fail only on naming (or only on naming / their-ledger-choice categories)?
print()
SOFT = {NAME}
SOFTER = {NAME, NONE, BIG, REGL}
for m in mappers:
    for s in sets:
        rows = [r for r in R if r["mapper"] == m and r["set"] == s]
        passing = sum(r["pass"] for r in rows)
        only_name = only_softer = 0
        for r in rows:
            if r["pass"]:
                continue
            cats = {category(r, t, checks_by_id) for t, ok in r["tiers"].items() if not ok}
            only_name += cats <= SOFT
            only_softer += cats <= SOFTER
        print(f"{s} {m}: pass {passing}/{len(rows)}; +naming-only {passing + only_name}; "
              f"+naming/NONE/override/region-level {passing + only_softer}")
