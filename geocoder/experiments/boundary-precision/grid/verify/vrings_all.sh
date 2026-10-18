#!/bin/bash
# all ring checks, 2 at a time -> logs/rings.jsonl
cd /ssd/shrink/grid/vfy
G=/ssd/shrink/grid; M=/ssd/shrink/idx/max; PY=/ssd/shrink/venv/bin/python
export PY
printf '%s\n' "q25_g4 /ssd/shrink/idx/withaddr $G/var/q25_g4 1e-4" "unc_g4 $M $G/var/unc_g4 1e-4" \
  "q1_g5e5 $G/dl/q1 $G/var/q1_g5e5 5e-5" "q05_g5 $G/dl/q0.5 $G/var/q05_g5 1e-5" \
  "unc_dp11_g4 $M $G/var/unc_dp11_g4 1e-4" "unc_dp22_g4 $M $G/var/unc_dp22_g4 1e-4" "unc_dp37_g4 $M $G/var/unc_dp37_g4 1e-4" \
  "unc_dp11 $M $G/var/unc_dp11 0" \
 | xargs -P 2 -L 1 bash -c '"$PY" vrings.py "$@" >> logs/rings.jsonl 2>> logs/rings.err' _
echo DONE >> logs/rings.err
