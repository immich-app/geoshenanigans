#!/usr/bin/env python3
"""score3.py [--truth unc] [--grids /ssd/bugs/grids] <label>... : Immich-field accuracy of each label's
answers (q/<label>_<set>/<pool>.txt) against the truth label's, per set (smallest, recommended), as
the tiers2 verify scorer (vscore.py) does: Immich fields as mapGeocoderAddress (immich-src
geocoding.repository.ts): city = city ?? town ?? village ?? hamlet ?? municipality, state = state ??
province, country = upper(country_code) or address.country.
Pools: every q/<truth>_<set>/*.txt except grids/l2c; vfresh also split by the verify meta (border level
L2..L8 and photo). Enclaves: grids.txt against <grids>/truth.json (osmium-area truth, score_grids_lib).
Writes cmp/score_<label>.json and prints one line per label and set."""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--truth", default="unc")
ap.add_argument("--grids", default="/ssd/bugs/grids")
ap.add_argument("--meta", default="/ssd/tiers2-verify/pts")
ap.add_argument("--sets", default="smallest,recommended")
ap.add_argument("labels", nargs="+")
a = ap.parse_args()
T = "/ssd/tiers3"
sys.path.insert(0, "/ssd/bugs/probe")
from score_grids_lib import FIELDS, LEAKS, in_enclave  # noqa: E402

GRIDS8 = ["vatican", "campione", "busingen", "baarle", "llivia", "jungholz", "san_marino", "lesotho"]
vmeta = None
if os.path.exists(f"{a.meta}/border10k.meta"):
    vmeta = [f"vborder_L{l.split()[2]}" for l in open(f"{a.meta}/border10k.meta")] + \
            [f"vphoto_{l.split()[2]}" for l in open(f"{a.meta}/photo10k.meta")]


def immich(js):
    ad = json.loads(js).get("address", {})
    cc = ad.get("country_code")
    country = cc.upper() if cc else ad.get("country")

    def co(*ks):
        return next((ad[k] for k in ks if ad.get(k) is not None), None)
    return co("city", "town", "village", "hamlet", "municipality"), co("state", "province"), country


def load(path):
    with open(path) as f:
        return [line.rstrip("\n").split(" ", 2) for line in f]


def accuracy(truth_dir, var_dir, pool):
    t, o = load(f"{truth_dir}/{pool}.txt"), load(f"{var_dir}/{pool}.txt")
    assert len(t) == len(o), (pool, len(t), len(o))
    res = defaultdict(Counter)
    for i, ((la, ln, tj), (lb, lnb, oj)) in enumerate(zip(t, o)):
        assert (la, ln) == (lb, lnb)
        groups = [pool]
        if pool == "vfresh" and vmeta:
            groups += [vmeta[i], vmeta[i].split("_")[0]]
        for g in groups:
            res[g]["n"] += 1
        if tj == oj:
            continue
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


gtruth = json.load(open(f"{a.grids}/truth.json"))
gpts = [line.split() for line in open(f"{a.grids}/all.txt")]


def grids(var_dir):
    ans = load(f"{var_dir}/grids.txt")
    assert len(ans) == len(gpts)
    out = {}
    for (g, la, ln), (qa, qn, js) in zip(gpts, ans):
        assert abs(float(la) - float(qa)) < 1e-6 and abs(float(ln) - float(qn)) < 1e-6
        ad = json.loads(js).get("address", {})
        t = gtruth[f"{g} {la} {ln}"]
        l2 = [e for e in t.get("2", []) if e[1]]
        cc = l2[0][1] if l2 else None
        s = out.setdefault(g, Counter())
        got = (ad.get("country_code") or "").upper()
        if cc:
            s["n_cc"] += 1
            s["cc_right"] += got == cc
        if g in LEAKS and in_enclave(t, LEAKS[g][0]):
            s["n_enc"] += 1
            s["enc_right"] += got == (cc or "")
            s["leak"] += any(ad.get(f) in LEAKS[g][1] for f in FIELDS)
    out["sum8"] = sum((out[g] for g in GRIDS8 if g in out), Counter())
    return {g: dict(c) for g, c in out.items()}


def pct(r, f):
    return 100 * r.get(f, 0) / r["n"] if r and r.get("n") else float("nan")


for label in a.labels:
    R = {}
    for st in a.sets.split(","):
        td, vd = f"{T}/q/{a.truth}_{st}", f"{T}/q/{label}_{st}"
        if not os.path.isdir(vd):
            continue
        pools = sorted(p[:-4] for p in os.listdir(td) if p.endswith(".txt") and p[:-4] not in ("grids", "l2c"))
        acc = {}
        for p in pools:
            if os.path.exists(f"{vd}/{p}.txt"):
                for g, c in accuracy(td, vd, p).items():
                    acc[g] = dict(c)
        gr = grids(vd) if os.path.exists(f"{vd}/grids.txt") else None
        R[st] = {"acc": acc, "grids": gr}
        s8 = gr["sum8"] if gr else {}
        parts = [f"{label:<22}{st:<12}"]
        for p, f in (("photo", "any"), ("photo", "city"), ("B2", "country"), ("B4", "state"), ("B7", "city"), ("B8", "city"),
                     ("vborder_L2", "country"), ("vborder_L4", "state"), ("vborder_L7", "city"), ("vborder_L8", "city"), ("vphoto", "any")):
            parts.append(f"{p}:{f} {pct(acc.get(p), f):.2f}")
        parts.append(f"enc {s8.get('enc_right', 0)}/{s8.get('n_enc', 0)} leak {s8.get('leak', 0)}")
        print("  ".join(parts))
    json.dump(R, open(f"{T}/cmp/score_{label}.json", "w"), indent=1)
