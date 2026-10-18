#!/bin/bash
# timed.sh LOG CORES CMD... : cold-cache timing run under the shared TIMING.lock.
# Prints wall/user/sys/maxrss from /usr/bin/time -v and the peak RssAnon
# (anonymous memory; maxrss also counts the mmapped input pages).
set -u
LOG=$1; CORES=$2; shift 2
LOCK=/ssd/shrink/sortpatch/TIMING.lock
while ! ( set -o noclobber; echo "sort $$ $(date +%T)" > $LOCK ) 2> /dev/null; do
  echo "waiting for $LOCK: $(cat $LOCK 2>/dev/null)"; sleep 20
done
trap "rm -f $LOCK" EXIT
sync; echo 3 > /proc/sys/vm/drop_caches
/usr/bin/time -v -o $LOG.time taskset -c $CORES "$@" > $LOG 2>&1 &
tpid=$!
peak=0
while kill -0 $tpid 2> /dev/null; do
  for p in $(pgrep -P $tpid) $(pgrep -P $(pgrep -P $tpid | head -1) 2>/dev/null); do
    a=$(awk "/^RssAnon/ {print \$2}" /proc/$p/status 2> /dev/null)
    [ -n "$a" ] && [ "$a" -gt "$peak" ] && peak=$a
  done
  sleep 0.25
done
wait $tpid; rc=$?
echo "rc=$rc anon_peak_MiB=$((peak / 1024))" >> $LOG.time
grep -E "Elapsed|User time|System time|Maximum resident|Exit status|File system" $LOG.time
tail -1 $LOG.time
