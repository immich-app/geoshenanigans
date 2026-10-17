#!/usr/bin/env python3
"""Search corpus, step 1 (no context yet): one row per searchable entity with its label point,
every name-like tag and the ranking signals.

Entities (kind column):
  P  visible shipped place node, place = city/town/village/hamlet
  B  shipped boundary that claims (hides) a place node; label point = the claimed node
     (rel-label > rel-wikidata > way-wikidata > name); names = boundary's + claimed nodes'
  M  visible shipped place node, place = suburb/quarter/neighbourhood
  A  shipped named admin boundary (admin_level 2-8) that claims no node; label point =
     area centroid of its largest ring (admin_dp.e7)
  O  POI of the major tier (poi/major); point = the shipped record (centroid for polygons)

Inputs: the instrumented cold planet build (planet-261006) and the names study, see
/ssd/precision/build/{dumps,names}_README.md and /ssd/precision/names/places.py.
Output: /ssd/search/data/corpus_noctx.tsv
  kind osm type lat lon popv sitelinks capital admin_level poi_importance poi_flags names
  names = key=value pairs joined by \\x1f (values split on ';', deduplicated per entity).
POI sitelinks come from osm_wikidata.tsv (osmium tags-filter -R nwr/wikidata of planet-261006)."""
import collections
import re
import sys
import time

import numpy as np

B = "/ssd/precision/build/"
N = "/ssd/precision/names/"
OUT = "/ssd/search/data/corpus_noctx.tsv"
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, file=sys.stderr, flush=True)


S4 = ("city", "town", "village", "hamlet")
MINOR = ("suburb", "quarter", "neighbourhood")
PT_NAMES = {0: "city", 1: "town", 2: "village", 3: "suburb", 4: "hamlet", 5: "neighbourhood", 6: "quarter"}
ALT_BASES = {"int_name", "alt_name", "old_name", "official_name", "short_name", "loc_name", "reg_name",
             "nat_name"}
LANG_RE = re.compile(r"^[a-z]{2,3}(?:[-_][A-Za-z0-9]+)*$")
NO_DATA = 0xFFFFFFFF
METHOD_RANK = {"rel-label": 0, "rel-wikidata": 1, "way-wikidata": 1, "name": 2}
TYPE = {"n": 1, "w": 2, "r": 3}


def is_lang(s):
    return bool(LANG_RE.match(s))


def searchable(k):
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


def clean(s):
    return s.replace("\n", " ").replace("\r", " ").replace("\t", " ").replace("\x1f", " ")


def name_pairs(tags):
    seen, out = set(), []
    for k, v in tags:
        if not searchable(k):
            continue
        for p in v.split(";"):
            p = clean(p.strip(" "))
            if p and (k, p) not in seen:
                seen.add((k, p))
                out.append(f"{k}={p}")
    return out


POP_RE = re.compile(r"^~?\s*([0-9][0-9 ,.'  ]*)$")


def parse_pop(v):
    if not v:
        return None
    m = POP_RE.match(v.strip())
    if not m:
        return None
    s = re.sub(r"[ ,'  ]", "", m.group(1))
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        s = s.replace(".", "")
    try:
        return int(float(s))
    except ValueError:
        return None


# ---------------------------------------------------------------- inputs
claims = collections.defaultdict(dict)  # (t, id) -> {node: method}
for line in open(N + "run-links/links.tsv"):
    m, t, bid, nid, _pto = line.rstrip("\n").split("\t")
    claims[(t, int(bid))].setdefault(int(nid), m)
log("claims", len(claims))

shipped_admin = {}  # (t, id) -> admin_level
best_ring = {}  # (t, id) -> (n_dp, polygon index)
with open(B + "dumps/admin_meta.tsv") as f:
    next(f)
    for line in f:
        p = line.split("\t")
        key = ("r" if p[2] == "relation" else "w", int(p[3]))
        shipped_admin[key] = int(p[5])
        ndp = int(p[7])
        if key not in best_ring or ndp > best_ring[key][0]:
            best_ring[key] = (ndp, int(p[0]))
log("shipped admin boundaries", len(shipped_admin))

pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"),
                  ("p2", "<u2"), ("parent", "<u4")])
slot_dt = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])
pn = np.fromfile(B + "out-nocap/planet/full/place_nodes.bin", dtype=pn_dt)
raw = np.fromfile(B + "out-nocap/planet/full/place_nodes.osm_ids", dtype=np.uint8)
slots = np.frombuffer(raw[12:].tobytes(), dtype=slot_dt)
assert len(slots) == len(pn)
shipped_pn = {}
for oid, pt, nid in zip(slots["id"].tolist(), pn["pt"].tolist(), pn["name_id"].tolist()):
    shipped_pn[oid] = (PT_NAMES[pt], nid == NO_DATA)
log("shipped place nodes", len(shipped_pn))

nodes = {}
for line in open(N + "osm/nodes.tsv", encoding="utf-8"):
    p = line.rstrip("\n").split("\t")
    nodes[int(p[0])] = (float(p[1]), float(p[2]), p[3], p[4], p[5], p[7])  # lat lon place pop wd capital
bnd = {}
for line in open(N + "osm/bnd.tsv", encoding="utf-8"):
    p = line.rstrip("\n").split("\t")
    bnd[(p[0], int(p[1]))] = p[2:]  # boundary admin_level place linked_place wikidata wikipedia population ...
log("osm nodes", len(nodes), "boundaries", len(bnd))

# POI major tier: record i is the i-th sidecar slot with tier <= 1 (the tier writer filters the
# full record array in sidecar order); relation slots carry (relation id << 16 | ring).
rec_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("voff", "<u4"), ("vcnt", "<u4"), ("name", "<u4"),
                   ("cat", "u1"), ("tier", "u1"), ("flags", "u1"), ("imp", "u1"), ("ps", "<u4"),
                   ("pp", "<u4"), ("ppoly", "<u4")])
praw = np.fromfile(B + "out-nocap/planet/poi/all/poi_records.osm_ids", dtype=np.uint8)
pslots = np.frombuffer(praw[12:].tobytes(), dtype=slot_dt)
del praw
sel = np.nonzero(pslots["tier"] <= 1)[0]
maj = np.fromfile(B + "out-nocap/planet/poi/major/poi_records.bin", dtype=rec_dt)
assert len(sel) == len(maj)
import json
cat_name = {int(k): v["name"] for k, v in json.load(open(B + "out-nocap/planet/poi/major/poi_meta.json")).items()}
poi_best = {}  # (t, id) -> record index (largest ring for relations)
for i, j in enumerate(sel.tolist()):
    t = int(pslots["type"][j])
    oid = int(pslots["id"][j])
    if t == 3:
        oid >>= 16
    key = ("nwr"[t - 1], oid)
    if key not in poi_best or maj["vcnt"][i] > maj["vcnt"][poi_best[key]]:
        poi_best[key] = i
del pslots
log("major POI records", len(maj), "distinct features", len(poi_best))

place_names, admin_names, poi_names = {}, {}, {}
with open(B + "names/names.tsv", encoding="utf-8", errors="surrogateescape") as f:
    for line in f:
        p = line.rstrip("\n").split("\t")
        cls, t, oid = p[0], p[1], int(p[2])
        if cls == "poi":
            if (t, oid) not in poi_best or (t, oid) in poi_names:
                continue
            d, key = poi_names, (t, oid)
        elif cls == "place":
            d, key = place_names, oid
        else:
            d, key = admin_names, (t, oid)
        if key in d:
            continue
        kv = p[5:]
        d[key] = [(kv[i], unescape(kv[i + 1])) for i in range(0, len(kv) - 1, 2)]
log("names: place", len(place_names), "admin", len(admin_names), "poi", len(poi_names))

sl = np.fromfile("/ssd/work/qid_sitelinks.bin", dtype=np.dtype([("qid", "<u4"), ("n", "<u2")]))
order = np.argsort(sl["qid"], kind="stable")
sl_q, sl_n = sl["qid"][order].astype(np.int64), sl["n"][order]


poi_wd = {}
for line in open("/ssd/search/data/osm_wikidata.tsv", encoding="utf-8"):
    t, _, q = line.rstrip("\n").partition("\t")
    key = (t[0], int(t[1:]))
    if key in poi_best:
        poi_wd[key] = q
log("major POIs with wikidata", len(poi_wd))


def sitelinks(qid):
    if not qid or not re.fullmatch(r"Q\d+", qid):
        return 0
    q = int(qid[1:])
    i = int(np.searchsorted(sl_q, q))
    return int(sl_n[i]) if i < len(sl_q) and sl_q[i] == q else 0


# ---------------------------------------------------------------- label points for unlinked admin
dp_idx = np.fromfile(B + "dumps/admin_dp.e7.idx", dtype="<u8")
dp = np.memmap(B + "dumps/admin_dp.e7", dtype="<i4", mode="r").reshape(-1, 2)


def ring_centroid(poly):
    a, b = int(dp_idx[poly]), int(dp_idx[poly + 1])
    r = dp[a:b].astype(np.float64) / 1e7
    if len(r) < 3:
        return float(r[:, 0].mean()), float(r[:, 1].mean())
    y, x = r[:, 0], r[:, 1]
    x1, y1 = np.roll(x, -1), np.roll(y, -1)
    cr = x * y1 - x1 * y
    area = cr.sum() / 2
    if abs(area) < 1e-12:
        return float(y.mean()), float(x.mean())
    cx = ((x + x1) * cr).sum() / (6 * area)
    cy = ((y + y1) * cr).sum() / (6 * area)
    return float(cy), float(cx)


# ---------------------------------------------------------------- rows
out = open(OUT, "w", encoding="utf-8", errors="surrogateescape")
counts = collections.Counter()


def emit(kind, osm, typ, lat, lon, pop, sls, cap, level, pimp, pflags, names):
    if not names or lat is None:
        counts["skip:" + kind] += 1
        return
    counts[kind] += 1
    out.write("\t".join([kind, osm, typ, f"{lat:.7f}", f"{lon:.7f}", str(pop if pop is not None else ""),
                         str(sls), cap or "", str(level), str(pimp), str(pflags), "\x1f".join(names)]) + "\n")


for oid, (ptype, hidden) in shipped_pn.items():
    if hidden or (ptype not in S4 and ptype not in MINOR):
        continue
    nd = nodes.get(oid)
    nm = place_names.get(oid, [])
    emit("P" if ptype in S4 else "M", f"n{oid}", ptype, nd[0] if nd else None, nd[1] if nd else None,
         parse_pop(nd[3]) if nd else None, sitelinks(nd[4]) if nd else 0, nd[5] if nd else "", "",
         "", "", name_pairs(nm))
log("place nodes", dict(counts))

for key, cl in claims.items():
    if key not in shipped_admin:
        continue
    nlist = sorted(cl.items(), key=lambda kv: (METHOD_RANK.get(kv[1], 9), kv[0]))
    btags, have = [], set()
    for kv in [kv for nid, _m in nlist for kv in place_names.get(nid, [])] + list(admin_names.get(key, [])):
        if kv not in have:
            have.add(kv)
            btags.append(kv)
    first = nlist[0][0]
    nd = nodes.get(first)
    b = bnd.get(key)
    bpop, bwd = (b[6], b[4]) if b else ("", "")
    pop = parse_pop(bpop) or (parse_pop(nd[3]) if nd else None)
    wd = bwd or (nd[4] if nd else "")
    emit("B", f"{key[0]}{key[1]}", nd[2] if nd else "?", nd[0] if nd else None, nd[1] if nd else None,
         pop, sitelinks(wd), nd[5] if nd else "", shipped_admin[key], "", "", name_pairs(btags))
log("boundary places", dict(counts))

for key, lvl in shipped_admin.items():
    if key in claims or lvl < 2 or lvl > 8:
        continue
    an = admin_names.get(key)
    if not an:
        continue
    b = bnd.get(key)
    lat, lon = ring_centroid(best_ring[key][1])
    emit("A", f"{key[0]}{key[1]}", f"admin{lvl}", lat, lon, parse_pop(b[6]) if b else None,
         sitelinks(b[4]) if b else 0, "", lvl, "", "", name_pairs(an))
log("unlinked admin", dict(counts))

for key, i in poi_best.items():
    r = maj[i]
    emit("O", f"{key[0]}{key[1]}", cat_name.get(int(r["cat"]), str(int(r["cat"]))), float(r["lat"]),
         float(r["lng"]), None, sitelinks(poi_wd.get(key)), "", "", int(r["imp"]), int(r["flags"]),
         name_pairs(poi_names.get(key, [])))
out.close()
log("done", dict(counts))
