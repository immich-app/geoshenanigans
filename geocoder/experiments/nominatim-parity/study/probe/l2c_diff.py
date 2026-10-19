"""l2c_diff.py <eval A.json> <eval B.json> <answers root> <label A> <label B> : loc-to-city checks whose
pass state differs between two runs, with each run's answer at the check point (city/town/village/
suburb/state fields). Answers: <root>/<label>_<preset>/l2c.txt."""
import json
import sys

ea, eb = json.load(open(sys.argv[1]))["results"], json.load(open(sys.argv[2]))["results"]
root, la, lb = sys.argv[3:6]
FIELDS = ("city", "town", "village", "municipality", "hamlet", "suburb", "city_district", "county", "state", "country_code")


def answers(label, preset):
    out = {}
    for line in open(f"{root}/{label}_{preset}/l2c.txt", encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out[(round(float(a), 5), round(float(b), 5))] = json.loads(j).get("address") or {}
    return out


cache = {}
for key in sorted(set(ea) | set(eb)):
    ra, rb = ea.get(key), eb.get(key)
    pa, pb = (ra or {}).get("pass_"), (rb or {}).get("pass_")
    if pa == pb:
        continue
    preset, check = key.split("|", 1)
    lat, lng = (float(x) for x in check.split(":", 1)[1].split(","))
    for lab in (la, lb):
        cache.setdefault((lab, preset), answers(lab, preset))
    pt = (round(lat, 5), round(lng, 5))
    aa, ab = cache[(la, preset)].get(pt, {}), cache[(lb, preset)].get(pt, {})
    print(f"{preset} {check}: {la} {'pass' if pa else 'FAIL'} -> {lb} {'pass' if pb else 'FAIL'}")
    print(f"   tiers {la} {(ra or {}).get('tiers')} {lb} {(rb or {}).get('tiers')}")
    print(f"   {la}: {json.dumps({k: aa[k] for k in FIELDS if k in aa}, ensure_ascii=False)}")
    print(f"   {lb}: {json.dumps({k: ab[k] for k in FIELDS if k in ab}, ensure_ascii=False)}")
