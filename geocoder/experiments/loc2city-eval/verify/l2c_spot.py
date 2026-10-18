"""List failing (check, tier) items the report labelled naming-only / wrong place, with our raw JSON answer.

usage: python -I l2c_spot.py <out.txt>
Category rules are read from the report's categorise.py (the RULES table only, up to `def category`).
Raw answers are this step's own grid_query output (l2c/ans_<set>.txt). Seeded sample of 15 naming-only
(recommended B); every distinct wrong-place item over the three sets (B), plus the max-set wrong-country
items, so at least 15 wrong-place/wrong-country items are shown.
"""
import json, random, re, sys

src = open("/ssd/loc2city/categorise.py", encoding="utf-8").read()
ns = {}
exec(src[: src.index("def category")], ns)
RULES, NAME, WRONG, ENCL = ns["RULES"], ns["NAME"], ns["WRONG"], ns["ENCL"]
checks = {c["id"]: c for c in json.load(open("/ssd/transport-verify/l2c/checks_v.json", encoding="utf-8"))}
REGION = ["state", "province", "region", "county"]
CITY = ["city", "town", "village", "hamlet", "municipality"]


def raw(s):
    rows = {}
    for line in open(f"/ssd/transport-verify/l2c/ans_{s}.txt", encoding="utf-8"):
        lat, lng, js = line.rstrip("\n").split(" ", 2)
        rows[(round(float(lat), 6), round(float(lng), 6))] = json.loads(js)
    return rows


ans = {s: raw(s) for s in ("smallest", "recommended", "max")}
R = [r for r in json.load(open("/ssd/loc2city/out/results_variants.json", encoding="utf-8")) if r["mapper"] == "B"]


def cat(r, tier):
    value = r["cc"] if tier == "country" else r["actual"].get(tier)
    for prefix, t, v, c in RULES:
        if t == tier and r["name"].startswith(prefix) and (v is None or v == value):
            return c
    return None


items = []
for r in R:
    for t, ok in r["tiers"].items():
        if not ok and t != "otherCity":
            items.append((r["set"], r["id"], r["name"], t, cat(r, t)))
naming = [i for i in items if i[0] == "recommended" and i[4] == NAME]
random.seed(7)
pick_n = random.sample(naming, 15)
wrong = [i for i in items if i[4] == WRONG]
encl_max = [i for i in items if i[4] == ENCL and i[0] == "max"]
out = open(sys.argv[1], "w", encoding="utf-8")


def show(i):
    s, cid, name, tier, c = i
    ch = checks[cid]
    a = ans[s][(round(ch["at"][0], 6), round(ch["at"][1], 6))]
    addr = a.get("address") or {}
    keys = REGION if tier == "region" else CITY
    mine = addr.get("country_code") if tier == "country" else next((addr[k] for k in keys if addr.get(k)), None)
    exp = ch[tier]
    out.write(f"[{s}] {name} | tier={tier} | expected={exp} | ours={mine!r}\n   display_name={a.get('display_name')!r}\n"
              f"   address={json.dumps(addr, ensure_ascii=False)}\n")


out.write(f"== naming-only, recommended B: {len(naming)} items, showing 15 (seed 7)\n")
for i in pick_n:
    show(i)
out.write(f"\n== wrong place, all sets B: {len(wrong)} items\n")
for i in wrong:
    show(i)
out.write(f"\n== wrong country at enclave/border, max B: {len(encl_max)} items\n")
for i in encl_max:
    show(i)
print(len(naming), len(wrong), len(encl_max))
