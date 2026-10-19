"""r16_vs_api.py <api.jsonl> <label=grid_query:set dir> [...] : per server, how many points have the
same city / town / village fields as Nominatim's answer, and which differ."""
import json
import subprocess
import sys

pts = [json.loads(l) for l in open(sys.argv[1], encoding="utf-8")]
inp = "".join(f"{p['lat']} {p['lng']}\n" for p in pts)
R16 = ("city", "town", "village")
for spec in sys.argv[2:]:
    label, rest = spec.split("=", 1)
    gq, d = rest.split(":", 1)
    out = subprocess.run([gq, d], input=inp, capture_output=True, text=True, check=True).stdout.splitlines()
    bad = []
    for p, line in zip(pts, out):
        a = json.loads(line.split(" ", 2)[2]).get("address") or {}
        n = p["r"].get("address", {})
        if any(a.get(k) != n.get(k) for k in R16):
            bad.append(f"{p['name']} ours={ {k: a[k] for k in R16 if k in a} } nominatim={ {k: n[k] for k in R16 if k in n} }")
    print(f"{label}: rank-16 fields equal to Nominatim on {len(pts) - len(bad)}/{len(pts)}")
    for b in bad:
        print("   ", b)
