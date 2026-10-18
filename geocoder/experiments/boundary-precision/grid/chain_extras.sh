#!/bin/bash
# After the main runs: queries + sizes for the extra configurations.
cd /ssd/shrink/grid
export TMPDIR=/ssd/shrink/tmp
until grep -q "^unc_dp11_g4:" logs/runq_main.log 2>/dev/null; do sleep 15; done
JOBS=5 ./runq.sh q2=sets/q2 q15=sets/q15 q02=sets/q02 unc_dp11=sets/unc_dp11 > logs/runq_extra.log 2>&1 &
until grep -q "^unc_dp11_g4 " logs/sizes_main.log 2>/dev/null; do sleep 15; done
JOBS=3 /ssd/shrink/venv/bin/python sizes.py q2=dl/q2 q15=dl/q1.5 q02=dl/q0.2 unc_dp11=var/unc_dp11 > logs/sizes_extra.log 2>&1
wait
