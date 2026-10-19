#!/bin/bash
# l2c.sh <name> ... : loc-to-city evaluation (transport-verify's l2c_eval.py, 421 checks) of
# q/<name>_{smallest,recommended}/l2c.txt (max: the enclaves track's fix_max answers) -> l2c/<name>/eval.json
set -euo pipefail
for lab in "$@"; do
  E=/ssd/arcs/l2c/$lab
  mkdir -p "$E"
  for s in smallest recommended; do ln -sf /ssd/arcs/q/${lab}_$s/l2c.txt "$E/ans_$s.txt"; done
  ln -sf /ssd/bugs/q/fix_max/l2c.txt "$E/ans_max.txt"
  /ssd/shrink/venv/bin/python -I /ssd/transport-verify/scripts/l2c_eval.py /ssd/transport-verify/l2c/checks_v.json \
    /ssd/loc2city/ne/ne_10m_admin_0_countries.geojson "$E" /ssd/transport-verify/l2c/sample100.json "$E/eval.json" > "$E/eval.log"
  /ssd/shrink/venv/bin/python -I -c "
import json
j=json.load(open('$E/eval.json'))['summary']
print('L2C $lab', ' | '.join(f\"{s}: all_pass {j[s]['all_pass']}\" for s in ('smallest','recommended')))"
done
