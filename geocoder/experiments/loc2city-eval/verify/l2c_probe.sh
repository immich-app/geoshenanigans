#!/bin/bash
# Re-query the random-point probe (their pts.txt, 20,000 points) on recommended, plus fresh Kolkata 1x1 deg
# (200 random points, own seed) and Vatican interior grid (10x9) points on recommended and max; then l2c_dep.py.
set -eu
L=/ssd/transport-verify/l2c
GQ=/ssd/shrink/target/release/examples/grid_query
P=/ssd/shrink/venv/bin/python
cd $L
$P -I - <<'PY'
import random
random.seed(424242)
open("kolkata_pts.txt","w").write("".join(f"{22.5726+random.uniform(-.5,.5):.6f} {88.3639+random.uniform(-.5,.5):.6f}\n" for _ in range(200)))
open("vatican_pts.txt","w").write("".join(f"{41.9015+i*0.0004444:.6f} {12.4490+j*0.000722:.6f}\n" for i in range(10) for j in range(9)))
PY
$GQ /ssd/shrink/idx/recommended < /ssd/loc2city/probe/pts.txt > probe_recommended.txt 2> /dev/null
for s in recommended max; do
  $GQ /ssd/shrink/idx/$s < kolkata_pts.txt > kolkata_$s.txt 2> /dev/null
  $GQ /ssd/shrink/idx/$s < vatican_pts.txt > vatican_$s.txt 2> /dev/null
done
$P -I /ssd/transport-verify/scripts/l2c_dep.py
