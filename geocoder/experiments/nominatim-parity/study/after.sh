#!/bin/bash
# after.sh : once bench.sh is done (nothing of this track runs beside its timing), every query,
# the archive builds of the server commits, then the analysis.
P=/ssd/parity
until grep -q BENCH_DONE $P/logs/bench.log 2>/dev/null; do sleep 10; done
echo "START $(date -Is)"
$P/queries.sh > $P/logs/queries.log 2>&1
echo "QUERIES $(date -Is) $(tail -1 $P/logs/queries.log)"
$P/server_revs.sh > $P/logs/server_revs.log 2>&1
echo "SERVER_REVS $(date -Is)"
$P/analyze.sh > $P/logs/analyze.log 2>&1
echo "AFTER_DONE $(date -Is)"
