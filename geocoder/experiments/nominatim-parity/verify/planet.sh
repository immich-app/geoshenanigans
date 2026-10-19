#!/bin/bash
# planet.sh : verify6's two cold planet builds of planet-261018b with nightly arguments and inputs
# (as /ssd/parity/run_planet.sh: 96 GiB scope without swap, all cores, GC_MEMORY_RESERVE_MIB=12288),
# from verify6's own build-index binaries (git archive): base = bc2e758, tip = f7096b0. Holds
# TIMING.lock for the builds only. Then: each output file by file against the parity track's
# (out-base, out-r6) and bugs2's out-new, and the three client presets assembled as symlinks.
set -u
V=/ssd/verify6
L=/ssd/shrink/sortpatch/TIMING.lock
export TMPDIR=$V/tmp
PBF=/ssd/bugs2/planet/planet-261018b.osm.pbf
EXTRA="--tiger-data /ssd/work/tiger-data.tar.gz --wikidata-sitelinks /ssd/work/qid_sitelinks.bin --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz"
log() { echo "$(date -Is) $*"; }
build() {  # name rev
  local name=$1 bin=$V/bin/build-index.$2 out=$V/build/out-$1 d=$V/build/$1
  rm -rf "$out" "$d"; mkdir -p "$d"
  cat > "$d/inner.sh" << EOS
#!/bin/bash
/usr/bin/time -v $bin $out $PBF --continents --multi-output --multi-quality $EXTRA > $d/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n "s/^0:://p" /proc/self/cgroup)
cat \$own/memory.peak > $d/memory.peak
cat \$own/memory.events > $d/memory.events
exit \$rc
EOS
  chmod +x "$d/inner.sh"
  sync; echo 3 > /proc/sys/vm/drop_caches
  local t0 t1; t0=$(date +%s.%N)
  systemd-run --quiet --scope --slice=verify6.slice --unit=verify6-$name-$$ -p MemoryMax=96G -p MemorySwapMax=0 \
    env GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR "$d/inner.sh"
  local rc=$?; t1=$(date +%s.%N)
  echo "rc=$rc wall=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")" > "$d/run.txt"
  (cd "$out" && find . -type f -printf "%s\t%P\n" | sort -k2) > "$d/files.tsv"
  log "BUILD $name $(cat $d/run.txt) $(grep -E 'Maximum resident' $d/build.log) peak=$(cat $d/memory.peak) files=$(wc -l < $d/files.tsv)"
}
cmpdir() {  # mine theirs label
  local a=$1 b=$2 lab=$3 same=0 diff=0 onlya=0 onlyb=0
  while read -r f; do
    if [ ! -f "$b/$f" ]; then onlya=$((onlya+1)); echo "ONLY_MINE $f"
    elif cmp -s "$a/$f" "$b/$f"; then same=$((same+1))
    else diff=$((diff+1)); echo "DIFF $f"; fi
  done < <(cd $a && find . -type f -printf "%P\n" | sort) > $V/build/cmp_$lab.txt
  onlyb=$(comm -13 <(cd $a && find . -type f -printf "%P\n" | sort) <(cd $b && find . -type f -printf "%P\n" | sort) | tee -a $V/build/cmp_$lab.txt | wc -l)
  log "CMP $lab same=$same differ=$diff only_mine=$onlya only_theirs=$onlyb"
}
until ( set -o noclobber; echo "verify6 planet builds $(date -Is)" > $L ) 2>/dev/null; do sleep 10; done
log LOCKED
build base bc2e758
build tip f7096b0
rm -f $L
log UNLOCKED
cmpdir $V/build/out-tip /ssd/parity/build/out-r6 tip_vs_parity_r6 &
cmpdir $V/build/out-base /ssd/parity/build/out-base base_vs_parity_base &
cmpdir $V/build/out-base /ssd/bugs2/build/out-new base_vs_bugs2_new &
wait
for n in base tip; do /ssd/shrink/venv/bin/python -I $V/scripts/assemble_sets.py $V/build/out-$n $V/sets/$n; done
log PLANET_DONE
