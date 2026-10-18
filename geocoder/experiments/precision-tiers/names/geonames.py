#!/usr/bin/env python3
"""Step "names": GeoNames cities500 vs our OSM place names.

For every cities500 place with at least one alternate name (alternateNamesV2
rows with a real language code or none, after dropping link/post/wkdt/codes)
that differs from its primary and ASCII name, find the matching OSM search
place (places.pkl from places.py): candidates within 10 km (30 km for
population >= 100k) or anywhere within 50 km with the same Wikidata QID;
a match needs the same QID or a shared normalised name (NFKC, casefold,
whitespace collapsed); best = same QID, then primary-name match, then any
shared name, then distance. Unmatched places retry against every other named
OSM place node ("loose"). For matched places: how many GeoNames alternate
names exist in the OSM place's name tags, per name set and name bucket.
Reports a fixed random sample of 2,000 (seed 20261014) and the full set.
"""
import collections
import math
import multiprocessing
import pickle
import random
import sys
import time
import unicodedata

import numpy as np

N = "/ssd/precision/names/"
G = N + "geonames/"
OUT = N + "out/"
T0 = time.time()
SAMPLE, SEED = 2000, 20261014
NON_NAME = {"wkdt", "link", "post", "unlc", "lauc", "iata", "icao", "uicn", "faac", "geoid", "nuts", "tcid", "phon"}


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, file=sys.stderr, flush=True)


def norm(s):
    return " ".join(unicodedata.normalize("NFKC", s).casefold().split())


def fold(s):
    """Accent and punctuation folded: NFKD, combining marks and non-alphanumerics dropped."""
    d = unicodedata.normalize("NFKD", s.casefold())
    return "".join(ch for ch in d if ch.isalnum() and not unicodedata.combining(ch))


d = pickle.load(open(N + "places.pkl", "rb"))
cand, top10 = d["cand"], d["top10"]
TOP10 = set(top10)
log("candidates", len(cand), "top10", top10)

# ---------------------------------------------------------------- GeoNames
gn = {}
for line in open(G + "cities500.txt", encoding="utf-8"):
    p = line.rstrip("\n").split("\t")
    gn[int(p[0])] = dict(name=p[1], ascii=p[2], altcol=[x for x in p[3].split(",") if x], lat=float(p[4]),
                         lon=float(p[5]), fcode=p[7], cc=p[8], pop=int(p[14] or 0), alts=[], qids=set())
log("cities500", len(gn))
for line in open(G + "alternateNamesV2.txt", encoding="utf-8"):
    p = line.rstrip("\n").split("\t")
    gid = int(p[1])
    g = gn.get(gid)
    if g is None:
        continue
    lang = p[2]
    if lang == "wkdt":
        g["qids"].add(p[3])
        continue
    if lang in NON_NAME:
        continue
    g["alts"].append((lang, p[3], p[4] == "1", p[5] == "1", p[6] == "1", p[7] == "1"))
log("alternate names loaded")


def bucket_of(lang):
    base = lang.split("-")[0].split("_")[0]
    if lang == "":
        return "no language"
    if lang == "abbr":
        return "abbr"
    if base == "en":
        return "en"
    if base in TOP10:
        return "top-10 language"
    return "other language"


for gid, g in gn.items():
    prim = {norm(g["name"]), norm(g["ascii"])}
    g["prim"] = prim
    A = collections.defaultdict(set)  # normalised alt string -> buckets
    for lang, s, pref, short, coll, hist in g["alts"]:
        n = norm(s)
        if not n or n in prim:
            continue
        b = A[n]
        b.add(bucket_of(lang))
        if hist:
            b.add("historic")
        if coll:
            b.add("colloquial")
        if short:
            b.add("short")
        if pref:
            b.add("preferred")
    g["A"] = A
    g["A2"] = {norm(s) for s in g["altcol"]} - prim - {""}
    g["allnames"] = prim | set(A) | g["A2"]

eligible = sorted(gid for gid, g in gn.items() if g["A"])
sample = set(random.Random(SEED).sample(eligible, SAMPLE))
log("eligible (>=1 alternate name differing from primary)", len(eligible))

# ---------------------------------------------------------------- OSM index
CELL = 0.1
grid = {"places": collections.defaultdict(list), "loose": collections.defaultdict(list)}
by_qid = {"places": collections.defaultdict(list), "loose": collections.defaultdict(list)}
lat_a = np.array([c[5] for c in cand])
lon_a = np.array([c[6] for c in cand])
for i, c in enumerate(cand):
    grid[c[0]][(math.floor(c[5] / CELL), math.floor(c[6] / CELL))].append(i)
    if c[7]:
        by_qid[c[0]][c[7]].append(i)
log("grid built")
_norm_cache = {}


def cnorm(i):
    v = _norm_cache.get(i)
    if v is None:
        c = cand[i]
        v = ({norm(x) for x in c[9]}, {norm(x) for x in c[10]}, {norm(x) for x in c[11]}, {norm(x) for x in c[12]})
        _norm_cache[i] = v
    return v


def km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(a)))


def match(g, scope):
    R = 30.0 if g["pop"] >= 100000 else 10.0
    dl = int(math.ceil(R / 111.0 / CELL))
    dlon = int(math.ceil(R / (111.0 * max(0.05, math.cos(math.radians(g["lat"])))) / CELL))
    ci, cj = math.floor(g["lat"] / CELL), math.floor(g["lon"] / CELL)
    idx = set()
    gs = grid[scope]
    for a in range(ci - dl, ci + dl + 1):
        for b in range(cj - dlon, cj + dlon + 1):
            idx.update(gs.get((a, b), ()))
    for q in g["qids"]:
        idx.update(by_qid[scope].get(q, ()))
    if not idx:
        return None
    ia = np.fromiter(idx, dtype=np.int64, count=len(idx))
    p1, p2 = math.radians(g["lat"]), np.radians(lat_a[ia])
    a = np.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * np.cos(p2) * np.sin(np.radians(lon_a[ia] - g["lon"]) / 2) ** 2
    dist = 2 * 6371.0 * np.arcsin(np.minimum(1.0, np.sqrt(a)))
    keep = dist <= 50.0
    best = None
    for i, dd in zip(ia[keep].tolist(), dist[keep].tolist()):
        c = cand[i]
        wd = bool(c[7]) and c[7] in g["qids"]
        if not wd and dd > R:
            continue
        anyn = cnorm(i)[3]
        prim = bool(g["prim"] & anyn)
        alt = prim or bool(g["allnames"] & anyn)
        if not (wd or alt):
            continue
        key = (not wd, not prim, dd)
        if best is None or key < best[0]:
            best = (key, i, "wikidata" if wd else "primary name" if prim else "alternate name only", dd)
    return best


def match_chunk(ids):
    out = []
    for gid in ids:
        g = gn[gid]
        m = match(g, "places")
        scope = "places"
        if m is None:
            m = match(g, "loose")
            scope = "loose"
        out.append((gid, scope, m))
    return out


# ---------------------------------------------------------------- run
SETS = ("base", "base+top10", "all languages", "any name tag", "any name tag, folded")
res = {}
chunks = [eligible[k:k + 2000] for k in range(0, len(eligible), 2000)]
with multiprocessing.get_context("fork").Pool(24) as pool:
    for part in pool.imap_unordered(match_chunk, chunks):
        for gid, scope, m in part:
            res[gid] = (scope, m)
log("matching done")


def summarise(ids, label, out):
    outcome = collections.Counter()
    wd_both = wd_same = 0
    tot = collections.Counter()
    found = collections.defaultdict(collections.Counter)
    per_place = collections.defaultdict(list)
    tot2 = 0
    found2 = collections.Counter()
    prim_found = 0
    osm_extra, osm_total = 0, 0
    matched_places = 0
    popb = collections.defaultdict(lambda: [0, 0, 0, 0])  # places, matched, alt total, alt found(any)
    for gid in ids:
        g = gn[gid]
        scope, m = res[gid]
        pb = ">=100k" if g["pop"] >= 100000 else "10k-100k" if g["pop"] >= 10000 else "1k-10k" if g["pop"] >= 1000 else "<1k"
        popb[pb][0] += 1
        if m is None:
            outcome["no match"] += 1
            continue
        if scope == "loose":
            outcome["only a non-search OSM place node (%s)" % cand[m[1]][4]] += 1
            continue
        outcome[m[2]] += 1
        i = m[1]
        c = cand[i]
        if g["qids"] and c[7]:
            wd_both += 1
            wd_same += c[7] in g["qids"]
        matched_places += 1
        popb[pb][1] += 1
        sets = cnorm(i)
        base, t10, alln, anyn = sets
        osets = {"base": base, "base+top10": t10, "all languages": alln, "any name tag": anyn}
        anyf = {fold(x) for x in anyn}
        prim_found += bool(g["prim"] & anyn)
        n_any = 0
        for s, bks in g["A"].items():
            sf = fold(s) in anyf
            for bk in list(bks) + ["ALL"]:
                tot[bk] += 1
                for sn, so in osets.items():
                    if s in so:
                        found[bk][sn] += 1
                found[bk]["any name tag, folded"] += sf
            n_any += s in anyn
        popb[pb][2] += len(g["A"])
        popb[pb][3] += n_any
        for sn, so in osets.items():
            per_place[sn].append(sum(1 for s in g["A"] if s in so) / len(g["A"]))
        per_place["any name tag, folded"].append(sum(1 for s in g["A"] if fold(s) in anyf) / len(g["A"]))
        tot2 += len(g["A2"])
        for sn, so in osets.items():
            found2[sn] += sum(1 for s in g["A2"] if s in so)
        found2["any name tag, folded"] += sum(1 for s in g["A2"] if fold(s) in anyf)
        gall = g["allnames"]
        osm_total += len(alln)
        osm_extra += sum(1 for s in alln if s not in gall)
    with open(out, "w") as f:
        f.write(f"# {label}: {len(ids)} GeoNames places\n")
        f.write("outcome\tplaces\tshare%\n")
        for k, v in outcome.most_common():
            f.write(f"{k}\t{v}\t{100.0 * v / len(ids):.1f}\n")
        f.write(f"matched to a search place\t{matched_places}\t{100.0 * matched_places / len(ids):.1f}\n")
        f.write(f"wikidata on both sides\t{wd_both}\t\n")
        f.write(f"wikidata equal (of both)\t{wd_same}\t{100.0 * wd_same / max(1, wd_both):.1f}\n")
        f.write(f"GeoNames primary/ascii name in OSM names\t{prim_found}\t{100.0 * prim_found / max(1, matched_places):.1f}\n")
        f.write("\nbucket\talternate names\t" + "\t".join(f"found in {s}%" for s in SETS) + "\n")
        for bk in ["ALL", "en", "top-10 language", "other language", "no language", "abbr", "preferred", "short",
                   "colloquial", "historic"]:
            if tot[bk]:
                f.write(f"{bk}\t{tot[bk]}\t" + "\t".join(f"{100.0 * found[bk][s] / tot[bk]:.1f}" for s in SETS) + "\n")
        f.write(f"cities500 alternatenames column (incl. ASCII transliterations)\t{tot2}\t" +
                "\t".join(f"{100.0 * found2[s] / max(1, tot2):.1f}" for s in SETS) + "\n")
        f.write("\nper matched place: mean share of its alternate names found\t\t" +
                "\t".join(f"{100.0 * np.mean(per_place[s]):.1f}" for s in SETS) + "\n")
        f.write("per matched place: share with every alternate name found\t\t" +
                "\t".join(f"{100.0 * np.mean([x == 1.0 for x in per_place[s]]):.1f}" for s in SETS) + "\n")
        f.write("per matched place: share with none found\t\t" +
                "\t".join(f"{100.0 * np.mean([x == 0.0 for x in per_place[s]]):.1f}" for s in SETS) + "\n")
        f.write(f"\nOSM all-languages strings of matched places\t{osm_total}\n")
        f.write(f"of those absent from GeoNames (primary, ascii, V2 names, alternatenames column)\t{osm_extra}\t"
                f"{100.0 * osm_extra / max(1, osm_total):.1f}%\n")
        f.write(f"mean GeoNames alternate names per matched place\t{tot['ALL'] / max(1, matched_places):.2f}\n")
        f.write(f"mean OSM all-languages strings per matched place\t{osm_total / max(1, matched_places):.2f}\n")
        f.write("\npopulation\tplaces\tmatched%\talternate names\tfound in any name tag%\n")
        for pb in (">=100k", "10k-100k", "1k-10k", "<1k"):
            a, b_, c_, d_ = popb[pb]
            if a:
                f.write(f"{pb}\t{a}\t{100.0 * b_ / a:.1f}\t{c_}\t{100.0 * d_ / max(1, c_):.1f}\n")


summarise(sorted(sample), "random sample (seed 20261014)", OUT + "geonames_sample.tsv")
summarise(eligible, "every eligible cities500 place", OUT + "geonames_full.tsv")

# Unmatched sample places, and examples of missed alternate names, for a sanity check.
with open(OUT + "geonames_examples.tsv", "w") as f:
    f.write("kind\tgeonameid\tname\tcc\tfcode\tpop\tdetail\n")
    k = 0
    for gid in sorted(sample):
        scope, m = res[gid]
        g = gn[gid]
        if m is None or scope == "loose":
            f.write(f"unmatched\t{gid}\t{g['name']}\t{g['cc']}\t{g['fcode']}\t{g['pop']}\t"
                    f"{'loose:' + cand[m[1]][4] if m else 'none'}\n")
            k += 1
            if k >= 25:
                break
    k = 0
    for gid in sorted(sample):
        scope, m = res[gid]
        if m is None or scope == "loose":
            continue
        g = gn[gid]
        anyn = cnorm(m[1])[3]
        miss = [(s, sorted(b)) for s, b in g["A"].items() if s not in anyn]
        if miss:
            f.write(f"missed\t{gid}\t{g['name']}\t{g['cc']}\t{g['fcode']}\t{g['pop']}\t"
                    f"osm={cand[m[1]][2]}{cand[m[1]][3]} missing={miss[:6]}\n")
            k += 1
            if k >= 25:
                break

# GeoNames as a population source for our places (full run, reverse view).
hit = collections.Counter()
seen = set()
for gid in eligible:
    scope, m = res[gid]
    if m is None or scope != "places" or m[1] in seen:
        continue
    seen.add(m[1])
    hit[(cand[m[1]][1], cand[m[1]][4])] += gn[gid]["pop"] > 0
tot = collections.Counter((c[1], c[4]) for c in cand if c[0] == "places")
with open(OUT + "geonames_reverse.tsv", "w") as f:
    f.write("kind\tplace\tsearch places\tmatched to a cities500 place with population\tshare%\n")
    for (kd, pt), n in tot.most_common(25):
        f.write(f"{kd}\t{pt}\t{n}\t{hit[(kd, pt)]}\t{100.0 * hit[(kd, pt)] / n:.2f}\n")
log("done")
