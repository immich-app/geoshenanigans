"""All naming-only items (recommended B) with our mapped value; flags items where we gave no value at all."""
import io, json, sys

src = open("/ssd/transport-verify/scripts/l2c_spot.py").read()
src = src.replace('out = open(sys.argv[1], "w", encoding="utf-8")', "out = io.StringIO()")
sys.argv = ["x", "/dev/null"]
ns = {"__name__": "x", "io": io}
exec(src[: src.index('out.write(f"== naming-only')], ns)
naming, checks, ans = ns["naming"], ns["checks"], ns["ans"]
REG = ["state", "province", "region", "county"]
CITY = ["city", "town", "village", "hamlet", "municipality"]
for s, cid, name, tier, c in naming:
    ch = checks[cid]
    a = ans[s][(round(ch["at"][0], 6), round(ch["at"][1], 6))]["address"]
    mine = next((a[k] for k in (REG if tier == "region" else CITY) if a.get(k)), None)
    exp = ch[tier].get("v")
    flag = "  <-- NO VALUE (missing, not naming)" if mine is None else ""
    print(f"{name[:58]:58s} {tier:6s} exp={exp!r} ours={mine!r}{flag}")
