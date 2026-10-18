"""Recompute the report's 'if naming-only were accepted' estimates, per mapper, with and without two
recategorisations found in the spot check (Prague region under B = missing; Asuncion city = enclosing unit)."""
import json

src = open("/ssd/loc2city/categorise.py", encoding="utf-8").read()
ns = {}
exec(src[: src.index("def category")], ns)
RULES, NAME, NONE, BIG, REGL, REGMISS, UNIT = (ns[k] for k in ("RULES", "NAME", "NONE", "BIG", "REGL", "REGMISS", "UNIT"))
R = json.load(open("/ssd/loc2city/out/results_variants.json", encoding="utf-8"))


def cat(r, tier, fix):
    value = r["cc"] if tier == "country" else r["actual"].get(tier)
    if fix:
        if r["name"].startswith("Prague") and tier == "region" and value is None:
            return REGMISS
        if r["name"].startswith("Asunción") and tier == "city":
            return UNIT
    for prefix, t, v, c in RULES:
        if t == tier and r["name"].startswith(prefix) and (v is None or v == value):
            return c
    return "other"


for m in ("B", "B+dep+regfill", "B+dep+regfill+cityCounty"):
    rows = [r for r in R if r["set"] == "recommended" and r["mapper"] == m]
    if not rows:
        continue
    base = sum(r["pass"] for r in rows)
    for fix in (False, True):
        soft = softer = 0
        nname = 0
        for r in rows:
            if r["pass"]:
                continue
            cats = {cat(r, t, fix) for t, ok in r["tiers"].items() if not ok}
            nname += sum(1 for t, ok in r["tiers"].items() if not ok and cat(r, t, fix) == NAME)
            soft += cats <= {NAME}
            softer += cats <= {NAME, NONE, BIG, REGL}
        print(f"{m:28s} fix={fix!s:5s} pass={base} naming-only tier items={nname} +naming={base + soft} "
              f"+naming/NONE/override/region-level={base + softer}")
