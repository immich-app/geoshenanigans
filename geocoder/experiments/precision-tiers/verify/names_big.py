import collections, pickle, random, sys
sys.argv = ["x", "none"]
exec(open("names_check.py").read())
d = pickle.load(open(N + "places.pkl", "rb"))
pl = [c for c in d["cand"] if c[0] == "places"]
rnd = random.Random(777)
s = rnd.sample(pl, 50000)
want_a = {(c[2], c[3]) for c in s if c[1] == "bnd"}
claims = collections.defaultdict(dict)
for line in open(N + "run-links/links.tsv"):
    m, t, bid, nid, _ = line.rstrip("\n").split("\t")
    if (t, int(bid)) in want_a:
        claims[(t, int(bid))].setdefault(int(nid), m)
want_n = {c[3] for c in s if c[1] == "node"} | {n for v in claims.values() for n in v}
pn, an = {}, {}
with open("/ssd/precision/build/names/names.tsv", encoding="utf-8", errors="surrogateescape") as f:
    for line in f:
        if line.startswith("poi\t"):
            continue
        p = line.rstrip("\n").split("\t")
        cls, t, oid = p[0], p[1], int(p[2])
        if (cls == "place" and oid in want_n) or (cls != "place" and (t, oid) in want_a):
            kv = p[5:]
            tags = [(kv[i], unescape_tsv(kv[i + 1])) for i in range(0, len(kv) - 1, 2)]
            (pn if cls == "place" else an).setdefault(oid if cls == "place" else (t, oid), tags)
agg = collections.Counter(); nb = na = 0
for c in s:
    if c[1] == "node":
        tl = pn.get(c[3], [])
    else:
        tl = merged(an.get((c[2], c[3]), []), claims.get((c[2], c[3]), {}), lambda n: pn.get(n, []))
    f = flags_of(tl)
    for i, k in enumerate(("name:en", "int_name", "alt_name", "lang_not_en", "langs>=2")):
        agg[k] += f[i]
    agg["bnd"] += c[1] == "bnd"
    nb += len(c[9]); na += len(c[11])
n = len(s)
print(f"uniform n={n}: " + ", ".join(f"{k} {100*v/n:.2f}%" for k, v in agg.items()) + f"; mean distinct base {nb/n:.3f} all {na/n:.3f}")
# full population means from the pkl name lists
print("full population (pkl): places", len(pl), "boundaries", sum(1 for c in pl if c[1]=="bnd"),
      "mean base %.3f all %.3f" % (sum(len(c[9]) for c in pl)/len(pl), sum(len(c[11]) for c in pl)/len(pl)))
