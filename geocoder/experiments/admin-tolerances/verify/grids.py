#!/usr/bin/env python3
"""grids.py <grids dir> <label>... : enclave grid scores as tiers3 score3.py grids() (bugs2 osmium-area
truth, score_grids_lib LEAKS / in_enclave): per grid, points inside the enclave whose country code is
right, summed over the 8 tiers2 grids (the "/3439" figure), plus country-right over every grid."""
import json
import sys
from collections import Counter

sys.path.insert(0, "/ssd/bugs/probe")
from score_grids_lib import FIELDS, LEAKS, in_enclave  # noqa: E402

G = sys.argv[1]
GRIDS8 = ["vatican", "campione", "busingen", "baarle", "llivia", "jungholz", "san_marino", "lesotho"]
truth = json.load(open(f"{G}/truth.json"))
pts = [l.split() for l in open(f"{G}/all.txt")]
for lab in sys.argv[2:]:
    ans = [l.rstrip("\n").split(" ", 2) for l in open(f"/ssd/verify5/q/{lab}/grids.txt")]
    assert len(ans) == len(pts)
    out = {}
    for (g, la, ln), (qa, qn, js) in zip(pts, ans):
        assert abs(float(la) - float(qa)) < 1e-6
        ad = json.loads(js).get("address", {})
        t = truth[f"{g} {la} {ln}"]
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
    s8 = sum((out[g] for g in GRIDS8 if g in out), Counter())
    allg = sum(out.values(), Counter())
    per = " ".join(f"{g}:{c['enc_right']}/{c['n_enc']}L{c['leak']}" for g, c in sorted(out.items()) if c.get("n_enc"))
    print(f"{lab}: sum8 enclave {s8['enc_right']}/{s8['n_enc']} leak {s8['leak']} | all grids cc {allg['cc_right']}/{allg['n_cc']} | {per}")
