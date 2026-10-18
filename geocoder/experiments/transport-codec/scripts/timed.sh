#!/bin/bash
# timed.sh LABEL MODE OUT CMD... : one timing run of CMD writing into OUT, under the shared
# /ssd/shrink/sortpatch/TIMING.lock (never overlapping another step's timing run).
#   warm : $PREWARM dirs are read into the page cache first; 1 core (63)
#   small: sync + drop caches, then the small-device profile: 1 core (63), MemoryMax=1G (page
#          cache included), no swap, md4 read and write 100 MB/s, read 150 IOPS
# Results: logs/time_LABEL.{res,log,time}; the res line also goes to logs/timings.tsv.
set -u
T=/ssd/transport
LABEL=$1 MODE=$2 OUT=$3; shift 3
LOCK=/ssd/shrink/sortpatch/TIMING.lock
RES=$T/logs/time_$LABEL
while ! ( set -o noclobber; echo "transport $$ $(date +%T) $LABEL" > $LOCK ) 2> /dev/null; do
  sleep 1
done
trap "rm -f $LOCK" EXIT
props=(-p MemorySwapMax=0)
if [ $MODE = small ]; then
  props+=(-p MemoryMax=1G -p "IOReadBandwidthMax=/dev/md4 100M" -p "IOWriteBandwidthMax=/dev/md4 100M" -p "IOReadIOPSMax=/dev/md4 150")
fi
if [ $MODE = warm ]; then
  [ -n "${PREWARM:-}" ] && find $PREWARM -type f -print0 | xargs -0 -P 8 -n 4 cat > /dev/null
else
  sync; echo 3 > /proc/sys/vm/drop_caches
fi
echo "$(date +%T) start $LABEL ($MODE)"
r=$(systemd-run --scope --quiet "${props[@]}" bash $T/scripts/scope_run.sh $RES 63 $OUT "$@" | tail -1)
echo "$r" > $RES.res
printf "%s\t%s\t%s\n" "$LABEL" "$MODE" "$r" >> $T/logs/timings.tsv
echo "$(date +%T) done  $LABEL: $r"
