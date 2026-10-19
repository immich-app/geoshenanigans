#!/bin/bash
# pipeline3.sh (parity resume) : the branch tip f7096b0 (3726c0e admin files for a region without
# admin polygons + f7096b0 = rank-0 place areas, was 9fe43d0) built from a git archive, one cold
# planet build (r6) under TIMING.lock, then: r6 against r5 file by file (only the Antarctica admin
# files may differ, as added files), the client sets of Antarctica for r5 and r6, a few queries there.
set -u
P=/ssd/parity
L=/ssd/shrink/sortpatch/TIMING.lock
PY="/ssd/shrink/venv/bin/python -I"
export TMPDIR=$P/tmp
log() { echo "$(date -Is) $*"; }
rev=f7096b0
d=$P/src-$rev; rm -rf $d; mkdir -p $d
git -C $P/repo archive $rev geocoder/builder | tar -x -C $d
mkdir -p $d/geocoder/server
rm -rf $P/bbuild-r6
$P/builder_build.sh r6 $d
until ( set -o noclobber; echo "parity $(date -Is)" > $L ) 2>/dev/null; do sleep 2; done
log LOCKED
$P/run_planet.sh r6 $P/bin/build-index.r6
rm -f $L
log "BUILD_DONE $(tail -1 $P/build/r6/run.txt)"
grep -E "Maximum resident" $P/build/r6/build.log
diff <(cut -f2 $P/build/r5/files.tsv) <(cut -f2 $P/build/r6/files.tsv) > $P/build/r5_vs_r6.files.diff
log "FILES r5 $(grep -c . $P/build/r5/files.tsv) r6 $(grep -c . $P/build/r6/files.tsv); only in r5: $(grep -c '^<' $P/build/r5_vs_r6.files.diff), only in r6: $(grep -c '^>' $P/build/r5_vs_r6.files.diff)"
grep '^[<>]' $P/build/r5_vs_r6.files.diff
n=0
while read -r sz f; do
  [ -f "$P/build/out-r6/$f" ] || continue
  cmp -s "$P/build/out-r5/$f" "$P/build/out-r6/$f" || { n=$((n+1)); echo "DIFF $f"; }
done < $P/build/r5/files.tsv > $P/build/r5_vs_r6.cmp
log "CMP common files differing: $n"
for b in r5 r6; do
  $PY $P/probe/assemble_region.py $P/build/out-$b antarctica $P/sets/ant-$b > $P/logs/ant_$b.log 2>&1
  log "ASSEMBLE antarctica $b rc=$? $(tail -1 $P/logs/ant_$b.log)"
done
for s in smallest recommended max; do
  [ -d $P/sets/ant-r6/$s ] || continue
  printf "%s\n" "-77.846323 166.668235" "-90.0 0.0" "-64.7743 -64.0531" "-62.1959 -58.9692" "-75.1 123.33" \
    | $P/bin/grid_query.s7 $P/sets/ant-r6/$s > $P/tmp/ant_$s.txt 2> $P/tmp/ant_$s.err
  log "QUERY antarctica r6 $s rc=$? lines $(grep -c . $P/tmp/ant_$s.txt)"
  cut -c1-220 $P/tmp/ant_$s.txt
done
echo PIPELINE3_DONE
