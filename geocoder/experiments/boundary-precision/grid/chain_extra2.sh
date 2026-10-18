#!/bin/bash
cd /ssd/shrink/grid
export TMPDIR=/ssd/shrink/tmp
until grep -q "^unc_dp37_g4:" logs/runq_extra2.log 2>/dev/null; do sleep 15; done
JOBS=3 /ssd/shrink/venv/bin/python sizes.py unc_dp22_g4=var/unc_dp22_g4 unc_dp37_g4=var/unc_dp37_g4 > logs/sizes_extra2.log 2>&1
