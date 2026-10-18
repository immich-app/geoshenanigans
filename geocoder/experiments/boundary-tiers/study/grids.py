#!/usr/bin/env python3
"""Enclave grid score of one candidate against the osmium-area truth of the enclaves track
(/ssd/bugs/grids/truth.json: every grid point's containing OSM admin areas from osmium-built
areas, levels 2..10; /ssd/bugs/grids/all.txt names the grid of each point of pts/grids.txt).
Per grid: country right / points with a truth country, and leaks (an answer naming the surrounding
place, score_grids_lib.LEAKS, on a point the truth puts in the enclave) / enclave points.
usage: grids.py <label> ...   -> /ssd/tiers2/cmp/<label>.grids.json + one line per label
"""
import json
import sys

sys.path.insert(0, "/ssd/bugs/probe")
from score_grids_lib import FIELDS, LEAKS, in_enclave  # noqa: E402

truth = json.load(open("/ssd/bugs/grids/truth.json"))
pts = [line.split() for line in open("/ssd/bugs/grids/all.txt")]
GRIDS = ["vatican", "campione", "busingen", "baarle", "llivia", "jungholz", "san_marino", "lesotho", "macau"]


def truth_cc(t):
    l2 = [e for e in t.get("2", []) if e[1]]
    return l2[0][1] if l2 else None


for label in sys.argv[1:]:
    ans = [json.loads(line.split(" ", 2)[2]).get("address", {}) for line in open(f"/ssd/tiers2/q/{label}/grids.txt")]
    assert len(ans) == len(pts)
    st = {}
    for (g, la, ln), a in zip(pts, ans):
        t = truth[f"{g} {la} {ln}"]
        s = st.setdefault(g, {"n_cc": 0, "cc": 0, "n_enc": 0, "leak": 0, "enc_cc": 0})
        cc = truth_cc(t)
        if cc:
            s["n_cc"] += 1
            s["cc"] += (a.get("country_code") or "").upper() == cc
        if g in LEAKS and in_enclave(t, LEAKS[g][0]):
            s["n_enc"] += 1
            s["leak"] += any(a.get(f) in LEAKS[g][1] for f in FIELDS)
            s["enc_cc"] += (a.get("country_code") or "").upper() == (cc or "")
    json.dump(st, open(f"/ssd/tiers2/cmp/{label}.grids.json", "w"), indent=1)
    print(label + ": " + "  ".join(
        f"{g} cc {st[g]['cc']}/{st[g]['n_cc']} enc_cc {st[g]['enc_cc']}/{st[g]['n_enc']} leak {st[g]['leak']}" for g in GRIDS if g in st))
