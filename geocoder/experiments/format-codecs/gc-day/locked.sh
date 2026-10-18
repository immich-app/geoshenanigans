#!/bin/bash
# locked.sh LABEL CMD... : run CMD while holding /ssd/shrink/sortpatch/TIMING.lock (used for the
# IO-heavy non-timing work too: page-cache warming and whole-day cmp, so they never disturb another
# step's timing run).
LOCK=/ssd/shrink/sortpatch/TIMING.lock
LABEL=$1; shift
while ! ( set -o noclobber; echo "codecs $$ $(date +%T) $LABEL" > $LOCK ) 2> /dev/null; do
  sleep 1
done
trap "rm -f $LOCK" EXIT
"$@"
