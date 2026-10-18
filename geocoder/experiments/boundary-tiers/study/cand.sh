#!/bin/bash
# cand.sh <name> <scheme> [quality scheme] : build one candidate (mkvar.sh), query both sets with
# the standard reader (q/<name>_{smallest,recommended}) and also with the chain
# reader (q/<name>g_{smallest,recommended}) when WITHCHAIN=1; compare against the truth (unc_*, standard reader)
# and score the enclave grids. Logs to logs/<name>.log.
set -euo pipefail
T=/ssd/tiers2
name=$1
cd $T
mkdir -p logs
{
  ./mkvar.sh "$@"
  for s in smallest recommended; do
    CHAIN=0 ./runq.sh ${name}_$s sets/$name/$s
    /ssd/shrink/venv/bin/python -I compare.py unc_$s ${name}_$s
    labels="${name}_$s"
    if [ "${WITHCHAIN:-0}" = 1 ]; then
      CHAIN=1 ./runq.sh ${name}g_$s sets/$name/$s
      /ssd/shrink/venv/bin/python -I compare.py unc_$s ${name}g_$s
      labels="$labels ${name}g_$s"
    fi
    /ssd/shrink/venv/bin/python -I grids.py $labels
  done
} > logs/$name.log 2>&1
echo "CAND_DONE $name"
