#!/bin/bash
# Run the addresses and index re-runs one after the other once the streets re-run has finished
# (keeps the verifier's disk footprint to one track's intermediates at a time).
cd /ssd/shrink/max/verify
until grep -q "^DONE" logs/j_streets.log 2>/dev/null; do sleep 20; done
scripts/j_addr.sh > logs/j_addr.log 2>&1
scripts/j_index.sh > logs/j_index.log 2>&1
echo CHAIN_DONE >> logs/j_index.log
