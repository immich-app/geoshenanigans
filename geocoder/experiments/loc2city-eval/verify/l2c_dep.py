"""Independent re-implementation of the report's 'dep' and 'regfill' mapper changes; pass counts per set,
firing rate on the random-point probe, and the Kolkata / Vatican wrong-country probes.

usage: python -I l2c_dep.py   (reads /ssd/transport-verify/l2c/*, writes dep_v.json)
dep: if a state/region/province value folds to a name of an NE admin-0 unit that shares the answer's
     sovereign (SOV_A3) but has another ISO_A2_EH, the country becomes that unit's code.
regfill: region falls back to municipality, then city, after state/province/region/county.
"""
import json, re, unicodedata
from collections import Counter

L = "/ssd/transport-verify/l2c"
ne = json.load(open("/ssd/loc2city/ne/ne_10m_admin_0_countries.geojson", encoding="utf-8"))
FIELDS = ["NAME", "NAME_LONG", "ADMIN", "GEOUNIT", "SUBUNIT", "NAME_EN", "FORMAL_EN", "BRK_NAME"]


def isLN(ch):
    return unicodedata.category(ch)[0] in "LN"


def fold(s):
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "M").lower()
    s = re.sub("[‘’`]", "'", s)
    return re.sub(r"(^|[^\w]|_)(saint|st\.?)(?=$|[^\w]|_)", r"\1st", s)


def whole_word(hay, needle):
    k = hay.find(needle)
    while k >= 0:
        a, b = k, k + len(needle)
        if (a == 0 or not isLN(hay[a - 1])) and (b == len(hay) or not isLN(hay[b])):
            return True
        k = hay.find(needle, k + 1)
    return False


units = []
name_iso = {}
for ft in ne["features"]:
    p = ft["properties"]
    name_iso[p["NAME"]] = p["ISO_A2_EH"]
    units.append(dict(iso=p["ISO_A2_EH"], sov=p["SOV_A3"], names={fold(p[f]) for f in FIELDS if p.get(f)}))
sov_of = {}
for u in units:
    sov_of.setdefault(u["iso"], set()).add(u["sov"])


def dep(addr, cc):
    if not cc:
        return cc, None
    for k in ("state", "region", "province"):
        v = addr.get(k)
        if not v:
            continue
        fv = fold(v)
        for u in units:
            if u["sov"] in sov_of.get(cc, ()) and u["iso"] != cc and u["iso"] != "-99" and fv in u["names"]:
                return u["iso"], (k, v)
    return cc, None


REG_B = ["state", "province", "region", "county"]
REG_F = REG_B + ["municipality", "city"]
CITY = ["city", "town", "village", "hamlet", "municipality"]


def first(a, ks):
    return next((a[k] for k in ks if a.get(k)), None)


def match(tier, exp, addr, cc, reg):
    val = cc if tier == "country" else first(addr, reg if tier == "region" else CITY)
    t = exp["t"]
    if t == "none":
        return val is None
    if val is None:
        return False
    if t == "any":
        return True
    if tier == "country":
        return cc in {name_iso[n] for n in exp["v"]}
    if t == "namedFor":
        return any(whole_word(fold(val), fold(w)) for w in [exp["v"], *exp["aliases"]])
    return val in exp["v"]


def load(path):
    rows = {}
    for line in open(path, encoding="utf-8"):
        lat, lng, js = line.rstrip("\n").split(" ", 2)
        rows[(round(float(lat), 6), round(float(lng), 6))] = json.loads(js).get("address") or {}
    return rows


checks = json.load(open(f"{L}/checks_v.json", encoding="utf-8"))
k = lambda p: (round(p[0], 6), round(p[1], 6))
out = {}
for s in ("smallest", "recommended", "max"):
    ans = load(f"{L}/ans_{s}.txt")
    base = {}
    for m in ("B", "B+dep", "B+regfill", "B+dep+regfill"):
        npass, ctier = 0, 0
        per = {}
        for c in checks:
            a = ans[k(c["at"])]
            cc = (a.get("country_code") or "").upper() or None
            if "dep" in m:
                cc = dep(a, cc)[0]
            reg = REG_F if "regfill" in m else REG_B
            tiers = {t: match(t, c[t], a, cc, reg) for t in ("country", "region", "city") if c[t]}
            if c["otherCityThan"]:
                b = ans[k(c["otherCityThan"])]
                ca, cb = first(a, CITY), first(b, CITY)
                tiers["oc"] = ca is not None and cb is not None and (ca != cb or any(
                    a.get(x) != b.get(x) for x in ("municipality", "county", "city_district", "state_district")))
            ok = all(tiers.values())
            per[c["id"]] = ok
            npass += ok
            ctier += tiers.get("country", False)
        if m == "B":
            base = per
        gained = sum(per[i] and not base[i] for i in per)
        lost = sum(base[i] and not per[i] for i in per)
        out[f"{s}|{m}"] = dict(passing=npass, country_tiers=ctier, gained_vs_B=gained, lost_vs_B=lost)
        print(s, m, out[f"{s}|{m}"])

# firing rate of dep / regfill on random land points (re-queried here on recommended)
pr = load(f"{L}/probe_recommended.txt")
land = [a for a in pr.values() if a.get("country_code")]
fires = Counter()
for a in land:
    new, why = dep(a, a["country_code"].upper())
    if why:
        fires[new] += 1
reg_fire = sum(1 for a in land if first(a, REG_B) is None and first(a, REG_F) is not None)
out["probe"] = dict(points=len(pr), land=len(land), dep_fires=sum(fires.values()), dep_codes=dict(fires),
                    regfill_fires=reg_fire, regfill_pct=round(100 * reg_fire / len(land), 2))
print(out["probe"])
# Kolkata box and Vatican box on max (and recommended for contrast)
for s in ("recommended", "max"):
    kb = load(f"{L}/kolkata_{s}.txt")
    bd = sum(1 for a in kb.values() if a.get("country_code") == "BD")
    bd_wb = sum(1 for a in kb.values() if a.get("country_code") == "BD" and a.get("state") == "West Bengal")
    vb = load(f"{L}/vatican_{s}.txt")
    vc = Counter(a.get("country_code") for a in vb.values())
    out[f"probe_{s}"] = dict(kolkata_points=len(kb), kolkata_BD=bd, kolkata_BD_westbengal=bd_wb,
                             vatican_points=len(vb), vatican_cc=dict(vc))
    print(s, out[f"probe_{s}"])
json.dump(out, open(f"{L}/dep_v.json", "w"), indent=1)
