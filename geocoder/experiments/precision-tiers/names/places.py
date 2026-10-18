#!/usr/bin/env python3
"""Step "names": place-search data facts (coverage, languages, ranking signals)
and the per-feature string files the size measurement compresses.

Inputs (box):
  /ssd/precision/build/names/names.tsv        builder --name-dumps (place + admin rows used here)
  /ssd/precision/names/run-links/links.tsv    builder --link-dumps: boundary claims place node
  /ssd/precision/names/osm/nodes.tsv, bnd.tsv prep_osm.py: exact coords, population, wikidata, capital
  /ssd/precision/build/out-nocap/planet/full/place_nodes.bin + .osm_ids   shipped place nodes
  /ssd/precision/build/dumps/admin_meta.tsv   shipped admin polygons (uncapped quality)
  /ssd/work/qid_sitelinks.bin                 the builder's --wikidata-sitelinks input (u32 qid, u16 n)
Outputs: out/*.tsv, files/{fm,dict}_<set>_<level>.txt, places.pkl, top10.txt
Search places = visible shipped place nodes city/town/village/hamlet + shipped
boundaries that claim (hide) a place node; a boundary's names are its own plus
its claimed nodes' (Nominatim adds deviating node names as _place_* names).
"""
import collections
import os
import pickle
import re
import sys
import time

import numpy as np

B = "/ssd/precision/build/"
N = "/ssd/precision/names/"
OUT = N + "out/"
FILES = N + "files/"
os.makedirs(OUT, exist_ok=True)
os.makedirs(FILES, exist_ok=True)
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, file=sys.stderr, flush=True)


S4 = ("city", "town", "village", "hamlet")
MINOR = ("suburb", "quarter", "neighbourhood")
PT_NAMES = {0: "city", 1: "town", 2: "village", 3: "suburb", 4: "hamlet", 5: "neighbourhood", 6: "quarter"}
ALT_BASES = {"int_name", "alt_name", "old_name", "official_name", "short_name", "loc_name", "reg_name",
             "nat_name", "sorting_name"}
BASE_SET = ("name", "name:en", "int_name", "alt_name", "official_name", "old_name")
LANG_RE = re.compile(r"^[a-z]{2,3}(?:[-_][A-Za-z0-9]+)*$")
NO_DATA = 0xFFFFFFFF


def is_lang(s):
    return bool(LANG_RE.match(s))


def lang_of(k):
    return k[5:] if k.startswith("name:") and is_lang(k[5:]) else None


def in_all(k):
    if k == "name":
        return True
    if k.startswith("name:"):
        return is_lang(k[5:])
    base, _, suf = k.partition(":")
    return base in ALT_BASES and (suf == "" or is_lang(suf))


def unescape(s):
    if "\\" not in s:
        return s
    out, i = [], 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            d = s[i + 1]
            out.append({"t": "\t", "n": "\n", "r": "\r"}.get(d, d))
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def split_values(v):
    return [p.strip(" ") for p in v.split(";") if p.strip(" ")]


def clean(s):
    return s.replace("\n", " ").replace("\r", " ").replace("\t", " ").replace("\x1f", " ")


def distinct(tags, keep):
    seen, out = set(), []
    for k, v in tags:
        if not keep(k):
            continue
        for p in split_values(v):
            if p not in seen:
                seen.add(p)
                out.append(p)
    return out


POP_RE = re.compile(r"^~?\s*([0-9][0-9 ,.'  ]*)$")


def parse_pop(v):
    if not v:
        return None
    m = POP_RE.match(v.strip())
    if not m:
        return None
    s = re.sub(r"[ ,'  ]", "", m.group(1))
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        s = s.replace(".", "")
    try:
        return int(float(s))
    except ValueError:
        return None


# ---------------------------------------------------------------- inputs
log("links")
claims = collections.defaultdict(dict)  # (t, id) -> {node: method}
for line in open(N + "run-links/links.tsv"):
    m, t, bid, nid, _pto = line.rstrip("\n").split("\t")
    claims[(t, int(bid))].setdefault(int(nid), m)
claimed_nodes = {n for d in claims.values() for n in d}
log("claims", len(claims), "boundaries,", len(claimed_nodes), "nodes")

shipped_admin = {}
with open(B + "dumps/admin_meta.tsv") as f:
    next(f)
    for line in f:
        p = line.split("\t")
        shipped_admin[("r" if p[2] == "relation" else "w", int(p[3]))] = int(p[5])
log("shipped admin boundaries", len(shipped_admin))

pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"),
                  ("p2", "<u2"), ("parent", "<u4")])
slot_dt = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])
pn = np.fromfile(B + "out-nocap/planet/full/place_nodes.bin", dtype=pn_dt)
raw = np.fromfile(B + "out-nocap/planet/full/place_nodes.osm_ids", dtype=np.uint8)
slots = np.frombuffer(raw[12:].tobytes(), dtype=slot_dt)
assert len(slots) == len(pn), (len(slots), len(pn))
assert (slots["type"] == 1).all()
shipped_pn = {}  # node id -> (place type name, hidden)
for oid, pt, nid in zip(slots["id"].tolist(), pn["pt"].tolist(), pn["name_id"].tolist()):
    shipped_pn[oid] = (PT_NAMES[pt], nid == NO_DATA)
hidden_bin = {k for k, v in shipped_pn.items() if v[1]}
log("shipped place nodes", len(shipped_pn), "hidden", len(hidden_bin),
    "hidden & claimed", len(hidden_bin & claimed_nodes),
    "shipped & claimed but visible", len({n for n in claimed_nodes if n in shipped_pn} - hidden_bin))

log("osm nodes")
nodes = {}
for line in open(N + "osm/nodes.tsv", encoding="utf-8"):
    p = line.rstrip("\n").split("\t")
    nodes[int(p[0])] = (float(p[1]), float(p[2]), p[3], p[4], p[5], p[6] == "1", p[7])
log("osm nodes", len(nodes))
bnd = {}
for line in open(N + "osm/bnd.tsv", encoding="utf-8"):
    p = line.rstrip("\n").split("\t")
    bnd[(p[0], int(p[1]))] = p[2:]
log("osm boundaries", len(bnd))

log("names.tsv (place + admin rows)")
place_names, admin_names = {}, {}
dup = collections.Counter()
with open(B + "names/names.tsv", encoding="utf-8", errors="surrogateescape") as f:
    for line in f:
        if line.startswith("poi\t"):
            continue
        p = line.rstrip("\n").split("\t")
        cls, t, oid, sub, shp = p[:5]
        kv = p[5:]
        tags = [(kv[i], unescape(kv[i + 1])) for i in range(0, len(kv) - 1, 2)]
        d = place_names if cls == "place" else admin_names
        key = int(oid) if cls == "place" else (t, int(oid))
        if key in d:
            dup[cls] += 1
            continue
        d[key] = (sub, shp == "1", tags)
log("place rows", len(place_names), "admin rows", len(admin_names), "dups", dict(dup))

# Sitelinks: sorted qids for lookups.
sl = np.fromfile("/ssd/work/qid_sitelinks.bin", dtype=np.dtype([("qid", "<u4"), ("n", "<u2")]))
order = np.argsort(sl["qid"], kind="stable")
sl_q, sl_n = sl["qid"][order].astype(np.int64), sl["n"][order]  # int64: a Python-int search key would otherwise promote (copy) the array per call
log("sitelinks records", len(sl), "qids with >=1 sitelink", int((sl_n > 0).sum()))


def sitelinks(qid):
    if not qid or not re.fullmatch(r"Q\d+", qid):
        return None
    q = int(qid[1:])
    i = int(np.searchsorted(sl_q, q))
    return int(sl_n[i]) if i < len(sl_q) and sl_q[i] == q else None


# ---------------------------------------------------------------- place records
METHOD_RANK = {"rel-label": 0, "rel-wikidata": 1, "way-wikidata": 1, "name": 2}
records = []  # dicts
for oid, (ptype, hidden) in shipped_pn.items():
    if hidden:
        continue
    if ptype not in S4 and ptype not in MINOR:
        continue
    nm = place_names.get(oid)
    nd = nodes.get(oid)
    records.append(dict(kind="node", t="n", id=oid, ptype=ptype, group=("node:" + ptype),
                        scope="places" if ptype in S4 else "minor",
                        lat=nd[0] if nd else None, lon=nd[1] if nd else None,
                        tags=nm[2] if nm else [], pop=nd[3] if nd else "", wd=nd[4] if nd else "",
                        wp=nd[5] if nd else False, capital=nd[6] if nd else "", level=None))
log("visible place-node records", len(records))

claims_unshipped = 0
for key, cl in claims.items():
    if key not in shipped_admin:
        claims_unshipped += 1
        continue
    an = admin_names.get(key)
    btags = list(an[2]) if an else []
    have = set(btags)
    nlist = sorted(cl.items(), key=lambda kv: (METHOD_RANK.get(kv[1], 9), kv[0]))
    first = nlist[0][0]
    for nid, _m in nlist:
        nm = place_names.get(nid)
        if nm:
            for kv in nm[2]:
                if kv not in have:
                    have.add(kv)
                    btags.append(kv)
    nd = nodes.get(first)
    ptype = nd[2] if nd else "?"
    b = bnd.get(key)
    bpop, bwd, bwp = (b[6], b[4], b[5] == "1") if b else ("", "", False)
    grp = "bnd:" + (ptype if ptype in S4 else "minor" if ptype in MINOR else "higher")
    records.append(dict(kind="bnd", t=key[0], id=key[1], ptype=ptype, group=grp, scope="places",
                        lat=nd[0] if nd else None, lon=nd[1] if nd else None, tags=btags,
                        pop=bpop or (nd[3] if nd else ""), wd=bwd or (nd[4] if nd else ""),
                        wp=bwp or (nd[5] if nd else False), capital=nd[6] if nd else "",
                        level=shipped_admin[key], method=nlist[0][1], nclaimed=len(cl),
                        bnd_pop=bpop, bnd_wd=bwd, node_pop=nd[3] if nd else "", node_wd=nd[4] if nd else ""))
log("records incl. boundaries", len(records), "claiming boundaries that did not ship", claims_unshipped)

# Other shipped named admin boundaries (no claimed node): an optional add-on scope.
n_other_admin = 0
for key, lvl in shipped_admin.items():
    if key in claims:
        continue
    an = admin_names.get(key)
    if not an:
        continue
    b = bnd.get(key)
    records.append(dict(kind="bnd", t=key[0], id=key[1], ptype="admin%d" % lvl, group="bnd-unlinked",
                        scope="admin", lat=None, lon=None, tags=an[2], pop=b[6] if b else "",
                        wd=b[4] if b else "", wp=(b[5] == "1") if b else False, capital="", level=lvl))
    n_other_admin += 1
log("unlinked shipped named admin boundaries", n_other_admin)

for r in records:
    r["base"] = distinct(r["tags"], lambda k: k in BASE_SET)
    r["all"] = distinct(r["tags"], in_all)
    r["any"] = distinct(r["tags"], lambda k: True)
    keys = {k for k, _ in r["tags"]}
    r["keys"] = keys
    r["langs"] = {lang_of(k) for k in keys} - {None}
    r["sl"] = sitelinks(r["wd"])
    r["popv"] = parse_pop(r["pop"])

places = [r for r in records if r["scope"] == "places" and r["all"]]
unnamed = sum(1 for r in records if r["scope"] == "places" and not r["all"])
log("search places with >=1 name", len(places), "without a name-like tag (ref only)", unnamed)

# ---------------------------------------------------------------- 1. coverage
GROUPS = [("node:city", "nodes city"), ("node:town", "nodes town"), ("node:village", "nodes village"),
          ("node:hamlet", "nodes hamlet"), ("bnd:city", "boundaries hiding city"),
          ("bnd:town", "boundaries hiding town"), ("bnd:village", "boundaries hiding village"),
          ("bnd:hamlet", "boundaries hiding hamlet"), ("bnd:minor", "boundaries hiding suburb/quarter/neighbourhood"),
          ("bnd:higher", "boundaries hiding other place types (state, county, island, ...)")]


def by_group(recs):
    g = collections.defaultdict(list)
    for r in recs:
        g[r["group"]].append(r)
        if r["kind"] == "node" or r["group"] in ("bnd:city", "bnd:town", "bnd:village", "bnd:hamlet"):
            g["type:" + r["ptype"]].append(r)
        g["all"].append(r)
    return g


def pct(a, b):
    return f"{100.0 * a / b:.1f}" if b else "-"


def q(vals, p):
    return float(np.percentile(vals, p)) if len(vals) else 0.0


rows_def = GROUPS + [("type:city", "city (node or boundary)"), ("type:town", "town (node or boundary)"),
                     ("type:village", "village (node or boundary)"), ("type:hamlet", "hamlet (node or boundary)"),
                     ("all", "ALL search places")]
g = by_group(places)
with open(OUT + "coverage.tsv", "w") as f:
    f.write("group\tplaces\tname:en%\tname:en!=name%\tint_name%\talt_name%\tofficial_name%\told_name%\t"
            "lang_not_en%\tlangs>=2%\tlangs>=5%\tlangs>=10%\tlangs>=50%\t"
            "base_mean\tbase_p50\tbase_p90\tall_mean\tall_p50\tall_p90\tall_p99\tall_max\tany_mean\n")
    for key, label in rows_def:
        rs = g.get(key, [])
        n = len(rs)
        if not n:
            continue

        def c(pred):
            return pct(sum(1 for r in rs if pred(r)), n)

        name_of = lambda r: next((v for k, v in r["tags"] if k == "name"), None)
        en_diff = lambda r: any(k == "name:en" and v != name_of(r) for k, v in r["tags"])
        nb = [len(r["base"]) for r in rs]
        na = [len(r["all"]) for r in rs]
        ny = [len(r["any"]) for r in rs]
        f.write("\t".join([label, str(n), c(lambda r: "name:en" in r["keys"]), c(en_diff),
                           c(lambda r: "int_name" in r["keys"]), c(lambda r: "alt_name" in r["keys"]),
                           c(lambda r: "official_name" in r["keys"]), c(lambda r: "old_name" in r["keys"]),
                           c(lambda r: len(r["langs"] - {"en"}) >= 1), c(lambda r: len(r["langs"]) >= 2),
                           c(lambda r: len(r["langs"]) >= 5), c(lambda r: len(r["langs"]) >= 10),
                           c(lambda r: len(r["langs"]) >= 50),
                           f"{np.mean(nb):.2f}", f"{q(nb, 50):.0f}", f"{q(nb, 90):.0f}",
                           f"{np.mean(na):.2f}", f"{q(na, 50):.0f}", f"{q(na, 90):.0f}", f"{q(na, 99):.0f}",
                           str(max(na)), f"{np.mean(ny):.2f}"]) + "\n")
log("coverage.tsv")

# Languages over the search places: places tagged, bytes, and what each adds beyond the base set.
lang_places = collections.Counter()
lang_bytes = collections.Counter()
lang_new = collections.Counter()
lang_new_bytes = collections.Counter()
for r in places:
    base = set(r["base"])
    seen_new = set()
    for k, v in r["tags"]:
        l = lang_of(k)
        if not l:
            continue
        lang_places[l] += 1
        lang_bytes[l] += len(v.encode("utf-8", "surrogateescape"))
        new = [p for p in split_values(v) if p not in base]
        if new:
            lang_new[l] += 1
            for p in new:
                if (l, p) not in seen_new:
                    seen_new.add((l, p))
                    lang_new_bytes[l] += len(p.encode("utf-8", "surrogateescape"))
with open(OUT + "languages.tsv", "w") as f:
    f.write("lang\tplaces\tplaces%\tvalue_bytes\tplaces_where_new_vs_base\tnew_string_bytes\n")
    for l, n in lang_places.most_common():
        f.write(f"{l}\t{n}\t{100.0 * n / len(places):.2f}\t{lang_bytes[l]}\t{lang_new[l]}\t{lang_new_bytes[l]}\n")
top10 = [l for l, _ in lang_places.most_common() if l != "en"][:10]
open(N + "top10.txt", "w").write("\n".join(top10) + "\n")
log("top10", top10)
TOP10_KEYS = set(BASE_SET) | {"name:" + l for l in top10}
for r in records:
    r["top10"] = distinct(r["tags"], lambda k: k in TOP10_KEYS)

# ---------------------------------------------------------------- 2. string files
# Levels: L0 search places, Lminor visible suburb/quarter/neighbourhood nodes,
# Ladmin unlinked shipped admin boundaries (POI levels L1-L3 come from poi_strings.py).
SETS = ("base", "top10", "all")
level_of = {"places": "L0", "minor": "Lminor", "admin": "Ladmin"}
stats = collections.defaultdict(lambda: [0, 0, 0])  # (set, level) -> features, postings, string bytes
recs_sorted = sorted((r for r in records if r["all"]), key=lambda r: (r["kind"], r["t"], r["id"]))
fh = {}
for s in SETS:
    for lv in level_of.values():
        fh[("fm", s, lv)] = open(FILES + f"fm_{s}_{lv}.txt", "w", encoding="utf-8", errors="surrogateescape")
        fh[("dict", s, lv)] = open(FILES + f"dict_{s}_{lv}.txt", "w", encoding="utf-8", errors="surrogateescape")
for r in recs_sorted:
    lv = level_of[r["scope"]]
    for s in SETS:
        strs = [clean(x) for x in r[s]]
        if not strs:
            continue
        fh[("fm", s, lv)].write("\x1f".join(strs) + "\n")
        fh[("dict", s, lv)].write("\n".join(strs) + "\n")
        st = stats[(s, lv)]
        st[0] += 1
        st[1] += len(strs)
        st[2] += sum(len(x.encode("utf-8", "surrogateescape")) for x in strs)
for h in fh.values():
    h.close()
with open(OUT + "levels_places.tsv", "w") as f:
    f.write("set\tlevel\tfeatures\tpostings\tstring_bytes\n")
    for (s, lv), (a, b_, c_) in sorted(stats.items()):
        f.write(f"{s}\t{lv}\t{a}\t{b_}\t{c_}\n")
log("string files")

# ---------------------------------------------------------------- 4. ranking signals
with open(OUT + "signals.tsv", "w") as f:
    f.write("group\tplaces\tpopulation_tag%\tpopulation_parsed%\tpop_median\twikidata%\twikidata_in_sitelinks%\t"
            "sitelinks>=1%\tsitelinks>=10%\tsitelinks_median(with qid)\tsitelinks_p90\twikipedia%\tcapital%\n")
    allg = by_group([r for r in records if r["all"]])
    for key, label in rows_def + [("node:suburb", "nodes suburb (add-on)"), ("node:quarter", "nodes quarter (add-on)"),
                                  ("node:neighbourhood", "nodes neighbourhood (add-on)"),
                                  ("bnd-unlinked", "unlinked admin boundaries (add-on)")]:
        rs = [r for r in allg.get(key, []) if key == "bnd-unlinked" or r["scope"] == "places" or key.startswith("node:")]
        n = len(rs)
        if not n:
            continue
        pops = [r["popv"] for r in rs if r["popv"] is not None]
        sls = [r["sl"] for r in rs if r["sl"] is not None]
        f.write("\t".join([label, str(n), pct(sum(1 for r in rs if r["pop"]), n), pct(len(pops), n),
                           f"{q(pops, 50):.0f}" if pops else "-",
                           pct(sum(1 for r in rs if r["wd"]), n), pct(len(sls), n),
                           pct(sum(1 for x in sls if x >= 1), n), pct(sum(1 for x in sls if x >= 10), n),
                           f"{q(sls, 50):.0f}" if sls else "-", f"{q(sls, 90):.0f}" if sls else "-",
                           pct(sum(1 for r in rs if r["wp"]), n),
                           pct(sum(1 for r in rs if r["capital"]), n)]) + "\n")
log("signals.tsv")

# Boundary places: where the population / wikidata signal comes from.
with open(OUT + "boundary_places.tsv", "w") as f:
    bs = [r for r in places if r["kind"] == "bnd"]
    f.write("measure\tvalue\n")
    f.write(f"boundary places\t{len(bs)}\n")
    for m in ("rel-label", "rel-wikidata", "way-wikidata", "name"):
        f.write(f"linked by {m}\t{sum(1 for r in bs if r['method'] == m)}\n")
    f.write(f"claim >1 node\t{sum(1 for r in bs if r['nclaimed'] > 1)}\n")
    f.write(f"population on boundary\t{sum(1 for r in bs if r['bnd_pop'])}\n")
    f.write(f"population on node only\t{sum(1 for r in bs if not r['bnd_pop'] and r['node_pop'])}\n")
    f.write(f"wikidata on boundary\t{sum(1 for r in bs if r['bnd_wd'])}\n")
    f.write(f"wikidata on node only\t{sum(1 for r in bs if not r['bnd_wd'] and r['node_wd'])}\n")
    f.write(f"claiming boundaries that did not ship\t{claims_unshipped}\n")
    f.write(f"shipped place nodes hidden (place_nodes.bin)\t{len(hidden_bin)}\n")
    f.write(f"hidden and in links.tsv\t{len(hidden_bin & claimed_nodes)}\n")
    f.write(f"search-place candidates without a name-like tag (visible nodes with only ref)\t{unnamed}\n")

# ---------------------------------------------------------------- pickle for geonames.py
# Search places + every other named place node (loose candidates, any place value).
in_places = {(r["t"], r["id"]) for r in places}
cand = []
for r in places:
    if r["lat"] is None:
        continue
    cand.append(("places", r["kind"], r["t"], r["id"], r["ptype"], r["lat"], r["lon"], r["wd"], r["popv"],
                 r["base"], r["top10"], r["all"], r["any"]))
for oid, nm in place_names.items():
    if ("n", oid) in in_places:
        continue
    nd = nodes.get(oid)
    if not nd:
        continue
    tags = nm[2]
    cand.append(("loose", "node", "n", oid, nd[2], nd[0], nd[1], nd[4], parse_pop(nd[3]),
                 distinct(tags, lambda k: k in BASE_SET), distinct(tags, lambda k: k in TOP10_KEYS),
                 distinct(tags, in_all), distinct(tags, lambda k: True)))
with open(N + "places.pkl", "wb") as f:
    pickle.dump(dict(cand=cand, top10=top10), f, protocol=pickle.HIGHEST_PROTOCOL)
log("places.pkl", len(cand))
