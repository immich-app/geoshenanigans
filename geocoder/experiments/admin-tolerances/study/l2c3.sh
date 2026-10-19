#!/bin/bash
# l2c3.sh <label> [max answers] : loc-to-city evaluation (transport-verify l2c_eval.py, mapper B, 421
# checks) of /ssd/tiers3/q/<label>_{smallest,recommended}/l2c.txt (max: answers no tier scheme changes,
# default the enclaves track's fix_max for planet-261006) -> /ssd/tiers3/l2c/<label>/eval.json.
set -euo pipefail
lab=$1
max=${2:-/ssd/bugs/q/fix_max/l2c.txt}
E=/ssd/tiers3/l2c/$lab
mkdir -p "$E"
for s in smallest recommended; do ln -sf /ssd/tiers3/q/${lab}_$s/l2c.txt "$E/ans_$s.txt"; done
ln -sf "$max" "$E/ans_max.txt"
/ssd/shrink/venv/bin/python -I /ssd/transport-verify/scripts/l2c_eval.py /ssd/transport-verify/l2c/checks_v.json \
  /ssd/loc2city/ne/ne_10m_admin_0_countries.geojson "$E" /ssd/transport-verify/l2c/sample100.json "$E/eval.json" > "$E/eval.log"
/ssd/shrink/venv/bin/python -I -c "
import json
j=json.load(open('$E/eval.json'))['summary']
print('$lab', ' | '.join(f\"{s}: all_pass {j[s]['all_pass']} {j[s]['per_tier']}\" for s in ('smallest','recommended')))"
