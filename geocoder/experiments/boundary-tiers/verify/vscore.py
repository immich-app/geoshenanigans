#!/usr/bin/env python3
"""vscore.py : verify-track accuracy and enclave scores, written independently of tiers2/score.py.

Accuracy: q/<scheme>_<set>/fresh20k.txt against q/unc_<set>/fresh20k.txt (truth = uncapped geometry
with holes, same reader). Immich fields as mapGeocoderAddress (immich-src geocoding.repository.ts):
city = city ?? town ?? village ?? hamlet ?? municipality, state = state ?? province,
country = upper(country_code) or address.country. Slices: border (all, and per admin level of the
sampled border), photo (all, and per kind).
Enclaves: q/<scheme>_<set>/grids.txt against /ssd/bugs/grids/truth.json (osmium-area truth): per grid,
points whose answer country code equals the truth level-2 code / points with a truth code; enclave
points (score_grids_lib.in_enclave) with the right country; leaks (surrounding place names on enclave points).
-> cmp/vscore.json and a text summary on stdout."""
import json
import sys
from collections import Counter, defaultdict

sys.path.insert(0, "/ssd/bugs/probe")
from score_grids_lib import FIELDS, LEAKS, in_enclave  # noqa: E402

V = "/ssd/tiers2-verify"
SCHEMES = sys.argv[1:] or ["unc", "q25", "cap10", "rec", "small"]
SETS = ["smallest", "recommended"]
GRIDS8 = ["vatican", "campione", "busingen", "baarle", "llivia", "jungholz", "san_marino", "lesotho"]

slices = [f"border_L{l.split()[2]}" for l in open(f"{V}/pts/border10k.meta")] + \
         [f"photo_{l.split()[2]}" for l in open(f"{V}/pts/photo10k.meta")]


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


def accuracy(scheme, st):
    t, o = load(f"{V}/q/unc_{st}/fresh20k.txt"), load(f"{V}/q/{scheme}_{st}/fresh20k.txt")
    assert len(t) == len(o) == len(slices)
    res = defaultdict(Counter)
    for (la, ln, tj), (lb, lnb, oj), sl in zip(t, o, slices):
        assert (la, ln) == (lb, lnb)
        groups = [sl, sl.split("_")[0], "all"]
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
    return {g: dict(c) for g, c in res.items()}


truth = json.load(open("/ssd/bugs/grids/truth.json"))
gpts = [line.split() for line in open("/ssd/bugs/grids/all.txt")]


def grids(scheme, st):
    ans = load(f"{V}/q/{scheme}_{st}/grids.txt")
    assert len(ans) == len(gpts)
    out = {}
    for (g, la, ln), (qa, qn, js) in zip(gpts, ans):
        assert abs(float(la) - float(qa)) < 1e-6 and abs(float(ln) - float(qn)) < 1e-6
        a = json.loads(js).get("address", {})
        t = truth[f"{g} {la} {ln}"]
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
    out["sum8"] = sum((out[g] for g in GRIDS8), Counter())
    return {g: dict(c) for g, c in out.items()}


R = {}
for s in SCHEMES:
    for st in SETS:
        k = f"{s}_{st}"
        R[k] = {"acc": accuracy(s, st) if s != "unc" else None, "grids": grids(s, st)}
json.dump(R, open(f"{V}/cmp/vscore.json", "w"), indent=1)


def pct(r, f):
    return f"{100 * r.get(f, 0) / r['n']:.2f}" if r and r.get("n") else "-"


for st in SETS:
    print(f"== {st}")
    print("scheme | all any | border any/city/state/country(cc_wrong) | photo any/city/state/country | L2 any | L4 any | L8 any/city | grids sum8 cc_right/n_cc | enc_right/n_enc | leaks")
    for s in SCHEMES:
        k = f"{s}_{st}"
        a = R[k]["acc"]
        g = R[k]["grids"]["sum8"]
        if a:
            b, p = a["border"], a["photo"]
            line = (f"{s} | {pct(a['all'], 'any')} | {pct(b, 'any')}/{pct(b, 'city')}/{pct(b, 'state')}/{pct(b, 'country')}({pct(b, 'cc_wrong')}) | "
                    f"{pct(p, 'any')}/{pct(p, 'city')}/{pct(p, 'state')}/{pct(p, 'country')} | {pct(a['border_L2'], 'any')} | "
                    f"{pct(a['border_L4'], 'any')} | {pct(a['border_L8'], 'any')}/{pct(a['border_L8'], 'city')}")
        else:
            line = f"{s} | truth"
        print(f"{line} | {g['cc_right']}/{g['n_cc']} | {g.get('enc_right', 0)}/{g.get('n_enc', 0)} | {g.get('leak', 0)}")
    print("per grid cc_right:")
    for s in SCHEMES:
        gr = R[f"{s}_{st}"]["grids"]
        print(f"  {s}: " + " ".join(f"{g} {gr[g]['cc_right']}/{gr[g]['n_cc']}" for g in GRIDS8))
