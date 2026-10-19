#!/bin/bash
# The parity track's heavy work. Under TIMING.lock: three cold planet builds of planet-261018b
# (base = bc2e758, r1 = fff988c relation type gate, r4 = tip 0094397), builders from git archive.
# After the lock: base output vs /ssd/bugs2/build/out-new, flat sets, the relation census.
set -u
L=/ssd/shrink/sortpatch/TIMING.lock
P=/ssd/parity
export TMPDIR=$P/tmp
log() { echo "$(date -Is) $*"; }
# take the lock atomically (noclobber), polling every 2 s
until ( set -o noclobber; echo "parity $(date -Is)" > $L ) 2>/dev/null; do sleep 2; done
log LOCKED
for n in base r1 r4; do $P/run_planet.sh $n $P/bin/build-index.$n; done
rm -f $L
log BUILDS_DONE
# base must reproduce the enclaves2 "new" output
diff <(cut -f2- $P/build/base/files.tsv) <(cd /ssd/bugs2/build/out-new && find . -type f -printf "%s\t%P\n" | sort -k2 | cut -f2-) > $P/build/base_vs_bugs2.files.diff && echo "file list same"
n_diff=0
while read -r sz f; do cmp -s "$P/build/out-base/$f" "/ssd/bugs2/build/out-new/$f" || { n_diff=$((n_diff+1)); echo "DIFF $f"; }; done < $P/build/base/files.tsv > $P/build/base_vs_bugs2.cmp
log "BASE_CMP differing files: $(grep -c DIFF $P/build/base_vs_bugs2.cmp)"
for n in base r1 r4; do /ssd/shrink/venv/bin/python -I $P/probe/assemble_sets.py $P/build/out-$n $P/sets/$n; done
log SETS_DONE
echo PIPELINE_DONE
