"""Second, independent extraction of loc-to-city's checks (verify/src/cases.ts) without running it.

Different method from /ssd/loc2city/parse_cases.py (tokenizer + recursive descent): cut each array
literal out by bracket matching, rewrite the TS literal into JSON text (drop comments, quote keys,
ANY/NONE -> marker strings, drop trailing commas) and json.loads it. Then build the checks exactly as
evaluate.ts allChecks() does: CASES (own status), CAPITALS (fixed), OPEN_CAPITALS (open).

usage: python -I l2c_extract.py <cases.ts> <out.json>
"""
import json, re, sys

src = open(sys.argv[1], encoding="utf-8").read()


def strip_comments(s):
    out, i, n = [], 0, len(s)
    while i < n:
        c = s[i]
        if c == '"':
            j = i + 1
            while s[j] != '"':
                j += 2 if s[j] == "\\" else 1
            out.append(s[i:j + 1]); i = j + 1
        elif s.startswith("//", i):
            j = s.find("\n", i); i = n if j < 0 else j
        elif s.startswith("/*", i):
            i = s.index("*/", i) + 2
        else:
            out.append(c); i += 1
    return "".join(out)


def array_after(marker):
    k = src.index(marker)
    start = src.index("= [", k) + 2
    depth, i, instr = 0, start, False
    while True:
        c = src[i]
        if instr:
            if c == "\\":
                i += 2; continue
            if c == '"':
                instr = False
        elif c == '"':
            instr = True
        elif c == "/" and src[i + 1] == "/":
            i = src.index("\n", i); continue
        elif c in "[{":
            depth += 1
        elif c in "]}":
            depth -= 1
            if depth == 0:
                return src[start:i + 1]
        i += 1


def ts_to_json(s):
    s = strip_comments(s)
    assert "'" not in re.sub(r'"(?:[^"\\]|\\.)*"', "", s), "single-quoted string present"
    assert "`" not in s, "template literal present"
    # protect strings, rewrite the rest
    parts = re.split(r'("(?:[^"\\]|\\.)*")', s)
    for k in range(0, len(parts), 2):
        p = parts[k]
        p = re.sub(r"\bANY\b", '"__ANY__"', p)
        p = re.sub(r"\bNONE\b", '"__NONE__"', p)
        p = re.sub(r"([{,]\s*)([A-Za-z_]\w*)(\s*:)", r'\1"\2"\3', p)
        p = re.sub(r",(\s*[\]}])", r"\1", p)
        parts[k] = p
    return json.loads("".join(parts))


def exp_of(v):
    if v is None:
        return None
    if v == "__ANY__":
        return {"t": "any"}
    if v == "__NONE__":
        return {"t": "none"}
    if isinstance(v, str):
        return {"t": "str", "v": [v]}
    if isinstance(v, list):
        return {"t": "str", "v": v}
    if isinstance(v, dict) and "namedFor" in v:
        return {"t": "namedFor", "v": v["namedFor"], "aliases": v.get("aliases", [])}
    raise ValueError(v)


cases = ts_to_json(array_after("export const CASES: Case[]"))
caps = ts_to_json(array_after("export const CAPITALS: Capital[]"))
opens = ts_to_json(array_after("export const OPEN_CAPITALS: Capital[]"))
checks = []
for c in cases:
    unknown = set(c) - {"name", "at", "country", "region", "city", "otherCityThan", "status", "section", "entry"}
    assert not unknown, unknown
    checks.append(dict(id=f"spot:{c['at'][0]},{c['at'][1]}", group="spot", name=c["name"], at=c["at"],
                       country=exp_of(c.get("country")), region=exp_of(c.get("region")),
                       city=exp_of(c.get("city")), otherCityThan=c.get("otherCityThan"), status=c["status"]))
for lst, st in ((caps, "fixed"), (opens, "open")):
    for row in lst:
        country, capital, lat, lon = row[:4]
        aliases = row[4] if len(row) > 4 else []
        checks.append(dict(id=f"capital:{lat},{lon}", group="capital", name=f"{capital} ({country})", at=[lat, lon],
                           country={"t": "str", "v": [country]}, region=None,
                           city={"t": "namedFor", "v": capital, "aliases": aliases}, otherCityThan=None, status=st))
json.dump(checks, open(sys.argv[2], "w", encoding="utf-8"), ensure_ascii=False, indent=1)
from collections import Counter
print("cases", len(cases), "capitals", len(caps), "open_capitals", len(opens), "checks", len(checks))
print("spot status", dict(Counter(c["status"] for c in cases)))
print("expectation types", dict(Counter((t, c[t]["t"]) for c in checks for t in ("country", "region", "city") if c[t])))
print("otherCityThan", sum(1 for c in checks if c["otherCityThan"]))
pts = {tuple(c["at"]) for c in checks} | {tuple(c["otherCityThan"]) for c in checks if c["otherCityThan"]}
print("distinct points", len(pts), "distinct ids", len({c["id"] for c in checks}))
