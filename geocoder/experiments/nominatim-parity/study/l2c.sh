#!/bin/bash
# l2c.sh <label> : loc-to-city evaluation (transport-verify l2c_eval.py, as /ssd/bugs2/l2c.sh) of
# /ssd/parity/q/<label>_{smallest,recommended,max}/l2c.txt -> /ssd/parity/l2c/<label>/eval.json
set -euo pipefail
pre=$1
E=/ssd/parity/l2c/$pre
mkdir -p "$E"
for s in smallest recommended max; do ln -sf /ssd/parity/q/${pre}_$s/l2c.txt "$E/ans_$s.txt"; done
/ssd/shrink/venv/bin/python -I /ssd/transport-verify/scripts/l2c_eval.py /ssd/transport-verify/l2c/checks_v.json \
  /ssd/loc2city/ne/ne_10m_admin_0_countries.geojson "$E" /ssd/transport-verify/l2c/sample100.json "$E/eval.json" > "$E/eval.log"
/ssd/shrink/venv/bin/python -I -c "
import json; d=json.load(open('$E/eval.json'))
print('$pre', ' '.join(f\"{s}={d[s]['all_pass']}\" for s in ('smallest','recommended','max')))
"
