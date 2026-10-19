#!/usr/bin/env python3
"""score.py <name> [truth name, default unc] : answer changes of a candidate vs the truth (uncapped
geometry with holes, same reader) on every pool, both sets, and its enclave grid score.

Immich fields as mapGeocoderAddress (geocoding.repository.ts): city = city ?? town ?? village ??
hamlet ?? municipality, state = state ?? province, country = upper(country_code) or address.country.
Pools: p200k + places (photo-like), L2 / L4 / L8 (+-200 m of borders), uniform100k, random5k,
fresh20k (the tiers2 verifier's fresh sample: 10k border points with their level, 10k photo-like).
Enclaves: grids.txt against /ssd/bugs/grids/truth.json (osmium-area truth), as the verifier's vscore.py.
-> /ssd/arcs/cmp/<name>.score.json, summary lines on stdout.
"""
import json
import sys
from collections import Counter, defaultdict

sys.path.insert(0, "/ssd/bugs/probe")
from score_grids_lib import FIELDS, LEAKS, in_enclave  # noqa: E402

A = "/ssd/arcs"
POOLS = ["p200k", "places", "L2", "L4", "L8", "uniform100k", "random5k", "fresh20k"]
SETS = ["smallest", "recommended"]
GRIDS8 = ["vatican", "campione", "busingen", "baarle", "llivia", "jungholz", "san_marino", "lesotho"]
fresh_slices = [f"border_L{l.split()[2]}" for l in open(f"{A}/pts/border10k.meta")] + \
               [f"photo_{l.split()[2]}" for l in open(f"{A}/pts/photo10k.meta")]


def immich(js):
    a = json.loads(js).get("address", {})
    cc = a.get("country_code")
    country = cc.upper() if cc else a.get("country")

    def co(*ks):
        return next((a[k] for k in ks if a.get(k) is not None), None)
    return co("city", "town", "village", "hamlet", "municipality"), co("state", "province"), country


def load(path):
    with open(path) as f:
        return [line.rstrip("\n").split(" ", 2) for line in f]


def accuracy(name, truth, st, pool):
    t, o = load(f"{A}/q/{truth}_{st}/{pool}.txt"), load(f"{A}/q/{name}_{st}/{pool}.txt")
    assert len(t) == len(o)
    res = defaultdict(Counter)
    for k, ((la, ln, tj), (lb, lnb, oj)) in enumerate(zip(t, o)):
        assert (la, ln) == (lb, lnb)
        groups = [pool]
        if pool == "fresh20k":
            groups += [fresh_slices[k], fresh_slices[k].split("_")[0]]
        for g in groups:
            res[g]["n"] += 1
        if tj == oj:
            continue
        for g in groups:
            res[g]["json"] += 1
        ti, oi = immich(tj), immich(oj)
        if ti == oi:
            continue
        for g in groups:
            r = res[g]
            r["any"] += 1
            r["city"] += ti[0] != oi[0]
            r["state"] += ti[1] != oi[1]
            r["country"] += ti[2] != oi[2]
            if ti[2] != oi[2]:
                r["cc_wrong" if (ti[2] and oi[2]) else "cc_none"] += 1
    return res


truth_g = json.load(open("/ssd/bugs/grids/truth.json"))
gpts = [line.split() for line in open("/ssd/bugs/grids/all.txt")]


def grids(name, st):
    ans = load(f"{A}/q/{name}_{st}/grids.txt")
    assert len(ans) == len(gpts)
    out = {}
    for (g, la, ln), (qa, qn, js) in zip(gpts, ans):
        a = json.loads(js).get("address", {})
        t = truth_g[f"{g} {la} {ln}"]
        l2 = [e for e in t.get("2", []) if e[1]]
        cc = l2[0][1] if l2 else None
        s = out.setdefault(g, Counter())
        got = (a.get("country_code") or "").upper()
        if cc:
            s["n_cc"] += 1
            s["cc_right"] += got == cc
        if g in LEAKS and in_enclave(t, LEAKS[g][0]):
            s["n_enc"] += 1
            s["enc_right"] += got == (cc or "")
            s["leak"] += any(a.get(f) in LEAKS[g][1] for f in FIELDS)
    out["sum8"] = sum((out[g] for g in GRIDS8 if g in out), Counter())
    return {g: dict(c) for g, c in out.items()}


def pct(r, f):
    return 100 * r.get(f, 0) / r["n"] if r and r.get("n") else float("nan")


if __name__ == "__main__":
    name = sys.argv[1]
    truth = sys.argv[2] if len(sys.argv) > 2 else "unc"
    R = {}
    for st in SETS:
        acc = {}
        for p in POOLS:
            for g, c in accuracy(name, truth, st, p).items():
                acc[g] = dict(c)
        R[st] = {"acc": acc, "grids": grids(name, st)}
        a = acc
        photo = Counter(a["p200k"]) + Counter(a["places"])
        g8 = R[st]["grids"]["sum8"]
        print(f"SCORE {name} {st}: photo(p200k+places) any {pct(photo, 'any'):.3f} city {pct(photo, 'city'):.3f} "
              f"state {pct(photo, 'state'):.3f} country {pct(photo, 'country'):.3f} | L2 any {pct(a['L2'], 'any'):.2f} "
              f"country {pct(a['L2'], 'country'):.2f} | L4 any {pct(a['L4'], 'any'):.2f} state {pct(a['L4'], 'state'):.2f} | "
              f"L8 any {pct(a['L8'], 'any'):.2f} city {pct(a['L8'], 'city'):.2f} | fresh border any {pct(a['border'], 'any'):.2f} "
              f"L8 city {pct(a['border_L8'], 'city'):.2f} L7 city {pct(a['border_L7'], 'city'):.2f} | fresh photo any "
              f"{pct(a['photo'], 'any'):.3f} | uniform any {pct(a['uniform100k'], 'any'):.3f} | enclaves {g8.get('enc_right', 0)}/"
              f"{g8.get('n_enc', 0)} leaks {g8.get('leak', 0)} cc {g8.get('cc_right', 0)}/{g8.get('n_cc', 0)}", flush=True)
    json.dump(R, open(f"{A}/cmp/{name}.score.json", "w"), indent=1)
