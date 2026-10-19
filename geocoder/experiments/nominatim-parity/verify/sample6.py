"""sample6.py : verify6's API sample of par30k changes (seed 6), from /ssd/verify6/cmp/steps_<preset>.json.
recommended: 5 per step (D, S1..S5, T, where a step has that many) + 8 of the net B->T changes;
smallest: 6 builder-step changes that recommended's builder step does not share (253eb9a's effect);
max: 4 of the S5 (509845b) changes. Writes "name lat lng" lines to /ssd/verify6/api/sample_pts.txt,
name = <preset>_<step>_<line>."""
import json
import random

V = "/ssd/verify6/"
rng = random.Random(6)
pts = open(V + "pts/par30k.txt").read().splitlines()
st = {p: json.load(open(f"{V}cmp/steps_{p}.json")) for p in ("smallest", "recommended", "max")}
pick = []
r = st["recommended"]
for s in ("D", "S1", "S2", "S3", "S4", "S5", "T"):
    pick += [("recommended", s, i) for i in rng.sample(r[s], min(5, len(r[s])))]
pick += [("recommended", "net", i) for i in rng.sample(r["changed_BT"], 8)]
only_small = sorted(set(st["smallest"]["D"]) - set(r["D"]))
pick += [("smallest", "D", i) for i in rng.sample(only_small, min(6, len(only_small)))]
pick += [("max", "S5", i) for i in rng.sample(st["max"]["S5"], min(4, len(st["max"]["S5"])))]
seen = set()
with open(V + "api/sample_pts.txt", "w") as f:
    for p, s, i in pick:
        if i in seen:
            continue
        seen.add(i)
        f.write(f"{p}_{s}_{i + 1} {pts[i]}\n")
print(len(seen), "points")
