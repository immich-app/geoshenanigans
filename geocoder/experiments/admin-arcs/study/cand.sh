#!/bin/bash
# cand.sh <name> <pp|arc> <min scheme> <quality scheme> : build (mkvar.sh), query both sets,
# score against the truth (unc) and run the loc-to-city checks. Log logs/<name>.log.
set -euo pipefail
A=/ssd/arcs
n=$1
cd $A; mkdir -p logs
{
  ./mkvar.sh "$@"
  for s in smallest recommended; do ./runq.sh ${n}_$s sets/$n/$s; done
  if [ "$n" != unc ]; then /ssd/shrink/venv/bin/python -I score.py $n; ./l2c.sh $n; fi
} > logs/$n.log 2>&1
echo "CAND_DONE $n"
