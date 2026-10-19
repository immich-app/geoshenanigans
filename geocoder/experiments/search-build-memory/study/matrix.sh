#!/bin/bash
# matrix.sh <log> <spec>...: under TIMING.lock, run each spec "bin:cfg:mode:memmb" one after
# another on core 40 (runb.sh), evicting the set's files from the page cache before each run.
set -u
S=/ssd/searchbuild
LOCK=/ssd/shrink/sortpatch/TIMING.lock
log=$1; shift
until (set -o noclobber; echo "searchbuild timing $(date -Iseconds)" > $LOCK) 2>/dev/null; do sleep 30; done
echo "LOCK taken $(date -Iseconds)" >> $log
trap 'rm -f $LOCK; echo "LOCK released $(date -Iseconds)" >> $log' EXIT
for spec in "$@"; do
  IFS=: read -r bin cfg mode mb <<< "$spec"
  case $cfg in
    min) d=minimal ;; rec|recx) d=recommended ;; max|maxn) d=max ;;
  esac
  python3 -I $S/evict.py --no-links ${SETS:-$S/sets}/$d $S/extras/$d >> $log 2>&1
  tag=$bin; [ "$mb" != 16 ] && tag=$bin-m$mb
  # runb.sh names the run dir <bin>-<cfg>-<mode>; keep one dir per memory budget
  SETS=${SETS:-$S/sets} $S/runb.sh $bin $cfg $mode 40 -- --mem-mb $mb >> $log 2>&1
  if [ "$mb" != 16 ]; then
    U=${RUNS:-$S/runs}; rm -rf $U/$tag-$cfg-$mode; mv $U/$bin-$cfg-$mode $U/$tag-$cfg-$mode
  fi
  echo "DONE $spec $(date -Iseconds)" >> $log
done
