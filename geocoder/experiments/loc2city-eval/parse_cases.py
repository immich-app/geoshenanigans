"""Extract every check from loc-to-city's verify/src/cases.ts without running it.

A tiny tokenizer + recursive-descent parser for the literal subset the file
uses (objects, arrays, strings, numbers, the ANY/NONE symbols). Mirrors
evaluate.ts allChecks(): CASES, then CAPITALS (fixed), then OPEN_CAPITALS (open).

usage: python -I parse_cases.py <cases.ts> <out.json>
"""
import json
import re
import sys

TOKEN = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<line>//[^\n]*)
  | (?P<block>/\*.*?\*/)
  | (?P<dstr>"(?:[^"\\\n]|\\.)*")
  | (?P<sstr>'(?:[^'\\\n]|\\.)*')
  | (?P<tstr>`(?:[^`\\$]|\\.)*`)
  | (?P<num>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)
  | (?P<ident>[A-Za-z_$][\w$]*)
  | (?P<punct>[{}\[\],:])
  | (?P<other>[;=?()|<>.&*!+/-])
    """,
    re.S | re.X,
)


def tokenize(src):
    pos = 0
    out = []
    while pos < len(src):
        m = TOKEN.match(src, pos)
        if not m:
            raise SyntaxError(f"unexpected {src[pos:pos+40]!r} at {pos}")
        pos = m.end()
        kind = m.lastgroup
        if kind in ("ws", "line", "block"):
            continue
        text = m.group()
        if kind == "dstr":
            out.append(("str", json.loads(text)))
        elif kind == "sstr":
            inner = text[1:-1].replace('\\"', '"').replace('"', '\\"').replace("\\'", "'")
            out.append(("str", json.loads('"' + inner + '"')))
        elif kind == "tstr":
            out.append(("str", text[1:-1]))
        elif kind == "num":
            out.append(("num", float(text)))
        else:
            out.append((kind, text))
    return out


class Parser:
    def __init__(self, toks):
        self.t = toks
        self.i = 0

    def peek(self):
        return self.t[self.i]

    def take(self, kind=None, text=None):
        tok = self.t[self.i]
        if kind and tok[0] != kind or text and tok[1] != text:
            raise SyntaxError(f"expected {kind} {text}, got {tok} at token {self.i}")
        self.i += 1
        return tok

    def value(self):
        kind, text = self.peek()
        if kind == "punct" and text == "{":
            return self.obj()
        if kind == "punct" and text == "[":
            return self.arr()
        self.i += 1
        if kind in ("str", "num"):
            return text
        if kind == "ident":
            if text in ("ANY", "NONE"):
                return {"$sym": text}
            if text in ("true", "false"):
                return text == "true"
        raise SyntaxError(f"unsupported value {kind} {text} at token {self.i}")

    def obj(self):
        self.take("punct", "{")
        out = {}
        while not (self.peek() == ("punct", "}")):
            kind, key = self.take()
            if kind not in ("ident", "str"):
                raise SyntaxError(f"bad key {key}")
            self.take("punct", ":")
            out[key] = self.value()
            if self.peek() == ("punct", ","):
                self.i += 1
        self.take("punct", "}")
        return out

    def arr(self):
        self.take("punct", "[")
        out = []
        while not (self.peek() == ("punct", "]")):
            out.append(self.value())
            if self.peek() == ("punct", ","):
                self.i += 1
        self.take("punct", "]")
        return out


def array_literal(src, decl):
    m = re.search(r"export const " + decl + r"\s*:\s*\w+\[\]\s*=\s*", src)
    if not m:
        raise SystemExit(f"no {decl}")
    toks = tokenize(src[m.end():])
    p = Parser(toks)
    val = p.arr()
    return val


def expected(v):
    """Normalise an Expected to {"t": str|any|none|namedFor, ...}."""
    if v is None:
        return None
    if isinstance(v, str):
        return {"t": "str", "v": [v]}
    if isinstance(v, list):
        return {"t": "str", "v": v}
    if isinstance(v, dict) and "$sym" in v:
        return {"t": v["$sym"].lower()}
    if isinstance(v, dict) and "namedFor" in v:
        return {"t": "namedFor", "v": v["namedFor"], "aliases": v.get("aliases", [])}
    raise ValueError(v)


def main():
    src = open(sys.argv[1], encoding="utf-8").read()
    cases = array_literal(src, "CASES")
    caps = array_literal(src, "CAPITALS")
    open_caps = array_literal(src, "OPEN_CAPITALS")
    checks = []
    for c in cases:
        unknown = set(c) - {"name", "at", "country", "region", "city", "otherCityThan", "status", "section", "entry"}
        if unknown:
            raise SystemExit(f"unknown keys {unknown} in {c['name']}")
        checks.append({
            "id": f"spot:{c['at'][0]},{c['at'][1]}",
            "group": "spot",
            "name": c["name"],
            "at": c["at"],
            "country": expected(c.get("country")),
            "region": expected(c.get("region")),
            "city": expected(c.get("city")),
            "otherCityThan": c.get("otherCityThan"),
            "status": c["status"],
            "section": c["section"],
            "entry": c.get("entry"),
        })
    for rows, status in ((caps, "fixed"), (open_caps, "open")):
        for row in rows:
            country, capital, lat, lon = row[:4]
            aliases = row[4] if len(row) > 4 else []
            checks.append({
                "id": f"capital:{lat},{lon}",
                "group": "capital",
                "name": f"{capital} ({country})",
                "at": [lat, lon],
                "country": expected(country),
                "region": None,
                "city": {"t": "namedFor", "v": capital, "aliases": aliases},
                "otherCityThan": None,
                "status": status,
                "section": "What each tier is for" if status == "fixed" else "Cross-cutting patterns",
                "entry": None if status == "fixed" else "Capitals.",
            })
    json.dump(checks, open(sys.argv[2], "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"cases={len(cases)} capitals={len(caps)} open_capitals={len(open_caps)} total={len(checks)}")


main()
