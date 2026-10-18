#!/bin/bash
# Re-query every check point (and otherCityThan point) on the three planet sets with grid_query,
# pick a seeded sample of 100 checks, evaluate with the independent rules, diff raw answers vs the report's.
set -eu
V=/ssd/transport-verify/l2c
P=/ssd/shrink/venv/bin/python
GQ=/ssd/shrink/target/release/examples/grid_query
cd $V
$P -I - <<'EOF'
import json, random
c = json.load(open("checks_v.json"))
pts = []
for x in c:
    for p in [x["at"]] + ([x["otherCityThan"]] if x["otherCityThan"] else []):
        if p not in pts:
            pts.append(p)
open("points_v.txt", "w").write("".join(f"{p[0]} {p[1]}\n" for p in pts))
random.seed(20261017)
json.dump(random.sample([x["id"] for x in c], 100), open("sample100.json", "w"))
print(len(pts), "points")
EOF
for s in smallest recommended max; do
  /usr/bin/time -f "$s %e s %M KB" $GQ /ssd/shrink/idx/$s < points_v.txt > ans_$s.txt 2> ans_$s.err
  tail -1 ans_$s.err
done
# raw diff vs the report's answers (same points, possibly different order)
for s in smallest recommended max; do
  a=$(sort ans_$s.txt | sha256sum | cut -c1-16); b=$(sort /ssd/loc2city/out/answers_$s.txt | sha256sum | cut -c1-16)
  echo "$s raw answers: mine $a report $b $([ $a = $b ] && echo IDENTICAL || echo DIFFERENT) ($(wc -l < ans_$s.txt) lines)"
done
$P -I /ssd/transport-verify/scripts/l2c_eval.py checks_v.json /ssd/loc2city/ne/ne_10m_admin_0_countries.geojson . sample100.json eval_v.json > eval_v.log
echo L2C DONE
