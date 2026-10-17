#!/usr/bin/env python3
"""Search corpus, step 2: context by reverse geocoding every label point with grid_query on the
recommended set (2026-10-13), plus the context alias table.

  corpus_noctx.tsv -> corpus.tsv: the same columns + cc country state county city
  iso3166.opl (osmium tags-filter r/ISO3166-1 r/ISO3166-1:alpha2 r/ISO3166-2 of planet-261006)
    -> context_aliases.tsv: cc  level(country|state)  aliases joined by \x1f
       aliases = every searchable name tag of one relation and its ISO code (alpha-2/alpha-3
       for countries, the subdivision part of ISO3166-2 for states); a corpus row's printed
       country/state joins the group of its cc and level that lists the printed name."""
import json
import os
import re
import subprocess
import sys
import time

D = "/ssd/search/data/"
TMP = "/ssd/search/tmp/ctx/"
GQ = "/ssd/shrink/target/release/examples/grid_query"
IDX = "/ssd/shrink/idx/recommended"
SHARDS = 20
T0 = time.time()
os.makedirs(TMP, exist_ok=True)


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, file=sys.stderr, flush=True)


coords = []
with open(D + "corpus_noctx.tsv", encoding="utf-8", errors="surrogateescape") as f:
    for line in f:
        p = line.split("\t", 5)
        coords.append(f"{p[3]} {p[4]}\n")
n = len(coords)
per = -(-n // SHARDS)
same = all(os.path.exists(TMP + f"in_{s}.txt") and open(TMP + f"in_{s}.txt").readlines() == coords[s * per:(s + 1) * per]
           for s in range(SHARDS))
if not same:
    procs = []
    for s in range(SHARDS):
        with open(TMP + f"in_{s}.txt", "w") as o:
            o.writelines(coords[s * per:(s + 1) * per])
        procs.append(subprocess.Popen(f"{GQ} {IDX} < {TMP}in_{s}.txt > {TMP}out_{s}.txt", shell=True))
    for p in procs:
        assert p.wait() == 0
del coords
log("grid_query", "reused (same coordinates)" if same else "done", n)

CITY_KEYS = ("city", "town", "village", "municipality", "hamlet")


def ctx_of(js):
    a = json.loads(js).get("address", {})
    city = next((a[k] for k in CITY_KEYS if a.get(k)), "")
    return [a.get("country_code", ""), a.get("country", ""), a.get("state", ""), a.get("county", ""), city]


def clean(s):
    return s.replace("\t", " ").replace("\n", " ")


with open(D + "corpus_noctx.tsv", encoding="utf-8", errors="surrogateescape") as f, \
        open(D + "corpus.tsv", "w", encoding="utf-8", errors="surrogateescape") as o:
    for s in range(SHARDS):
        with open(TMP + f"out_{s}.txt", encoding="utf-8") as g:
            for res in g:
                line = f.readline().rstrip("\n")
                js = res.split(" ", 2)[2]
                o.write(line + "\t" + "\t".join(clean(x) for x in ctx_of(js)) + "\n")
    assert not f.readline(), "grid_query dropped lines"
log("corpus.tsv")

# ---------------------------------------------------------------- context aliases
ESC = re.compile(r"%([0-9a-f]+)%")
LANG_RE = re.compile(r"^[a-z]{2,3}(?:[-_][A-Za-z0-9]+)*$")
ALT = {"int_name", "alt_name", "official_name", "short_name", "old_name"}


def unesc(s):
    return ESC.sub(lambda m: chr(int(m.group(1), 16)), s) if "%" in s else s


def name_tag(k):
    if k == "name":
        return True
    b, _, suf = k.partition(":")
    return (b == "name" and LANG_RE.match(suf)) or (b in ALT and (suf == "" or LANG_RE.match(suf)))


rows = set()
for line in open(D + "iso3166.opl", encoding="utf-8"):
    parts = line.rstrip("\n").split(" ")
    t = next((p for p in parts if p.startswith("T")), "T")
    tags = {}
    if len(t) > 1:
        for kv in t[1:].split(","):
            k, _, v = kv.partition("=")
            tags[unesc(k)] = unesc(v)
    if tags.get("boundary") not in ("administrative", None) and "admin_level" not in tags:
        continue
    names = {v.strip() for k, v in tags.items() if name_tag(k) for v in v.split(";") if v.strip()}
    if not names:
        continue
    iso2 = tags.get("ISO3166-2", "")
    iso1 = tags.get("ISO3166-1:alpha2") or tags.get("ISO3166-1", "")
    if iso1 and tags.get("admin_level") == "2" and len(iso1) == 2:
        cc, level, codes = iso1.upper(), "country", {iso1.lower()}
        if tags.get("ISO3166-1:alpha3"):
            codes.add(tags["ISO3166-1:alpha3"].lower())
    elif "-" in iso2:
        cc, sub = iso2.split("-", 1)
        level = "state"
        codes = {sub.lower()} if sub.isalpha() and len(sub) >= 2 else set()
    else:
        continue
    rows.add((cc.upper(), level, "\x1f".join(sorted(clean(x) for x in names | codes))))
with open(D + "context_aliases.tsv", "w", encoding="utf-8") as o:
    for r in sorted(rows):
        o.write("\t".join(r) + "\n")
log("context_aliases.tsv", len(rows))
