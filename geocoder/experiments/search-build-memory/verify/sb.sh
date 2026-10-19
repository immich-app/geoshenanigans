#!/bin/bash
# sb.sh : verify6 re-measurement of the searchbuild track. Private copies of the set files the
# index build reads (psets), then under TIMING.lock, page cache of the set + extras evicted before
# each run, one pinned core (40), --threads 1, default arguments otherwise:
#   ref   74a4a88 unconstrained         (the reference output)
#   cg    d54f85d in a 32 MiB scope (MemoryMax=32M, MemorySwapMax=0)
#   free  d54f85d unconstrained
#   bcg   74a4a88 in the same 32 MiB scope
# for minimal, recommended, max. memwatch6.py samples RssAnon / VmHWM every 20 ms; the scope's
# memory.peak and memory.events are read by its own shell after the build exits.
# Every output file of cg / free / bcg is compared with ref (and ref with the searchbuild track's
# /ssd/searchbuild/ref/idx/<set>-1c).
set -u
V=/ssd/verify6
S=$V/sb
L=/ssd/shrink/sortpatch/TIMING.lock
EX=/ssd/search-prod/extras
log() { echo "$(date -Is) $*"; }
mkdir -p $S/psets $S/runs
copy_max="place_nodes.bin admin_polygons.bin admin_vertices.bin admin_cells.bin admin_entries.bin place_cells.bin place_entries.bin poi_records.bin poi_meta.json strings_layout.json strings_core.bin strings_poi.bin strings_street.bin strings_addr.bin strings_postcode.bin"
for s in minimal recommended max; do
  mkdir -p $S/psets/$s
  for f in /ssd/search-prod/sets/$s/*; do
    b=$(basename $f); real=$(readlink -f $f)
    [ -e $S/psets/$s/$b ] && continue
    if [ $s != max ] || [[ " $copy_max " == *" $b "* ]]; then cp $real $S/psets/$s/$b; else ln -s $real $S/psets/$s/$b; fi
  done
done
log "PSETS $(du -sh $S/psets | cut -f1)"
evict() {  # set
  /ssd/shrink/venv/bin/python -I -c '
import os, sys
for d in sys.argv[1:]:
    for f in sorted(os.listdir(d)):
        p = os.path.join(d, f)
        if os.path.islink(p) or not os.path.isfile(p):
            continue
        fd = os.open(p, os.O_RDONLY); os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED); os.close(fd)
' $S/psets/$1 $EX/$1
}
run() {  # label rev set mode(free|cg32)
  local lab=$1 rev=$2 set=$3 mode=$4 R=$S/runs/$1-$3
  rm -rf $R; mkdir -p $R/idx $R/tmp
  cat > $R/inner.sh << EOF
#!/bin/bash
taskset -c 40 $V/bin/place-search.$rev build $S/psets/$set $R/idx --threads 1 --tmp $R/tmp --imp $EX/$set > $R/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n "s/^0:://p" /proc/self/cgroup)
echo "rc=\$rc peak=\$(cat \$own/memory.peak 2>/dev/null) \$(tr '\n' ' ' < \$own/memory.events 2>/dev/null)" > $R/inner.out
exit \$rc
EOF
  chmod +x $R/inner.sh
  local cmd=($R/inner.sh)
  [ $mode = cg32 ] && cmd=(systemd-run --scope -q -p MemoryMax=32M -p MemorySwapMax=0 $R/inner.sh)
  sync; evict $set; sleep 1
  /ssd/shrink/venv/bin/python -I $V/scripts/memwatch6.py $R/mem.json $R/tmp -- "${cmd[@]}"
  log "RUN $lab $set $mode exit=$? $(cat $R/inner.out) $(/ssd/shrink/venv/bin/python -I -c "import json,sys;d=json.load(open(sys.argv[1]));print('wall_s',d['wall_s'],'RssAnon_kB',d['peak_kB'].get('RssAnon'),'VmHWM_kB',d['peak_kB'].get('VmHWM'),'tmp_MiB',d['tmp_peak_MiB'])" $R/mem.json)"
}
cmpidx() {  # a b label
  local same=0 diff=0 miss=0 lst=""
  for f in $(ls $2); do
    if [ ! -e $1/$f ]; then miss=$((miss+1)); lst="$lst MISSING:$f"
    elif cmp -s $1/$f $2/$f; then same=$((same+1))
    else diff=$((diff+1)); lst="$lst DIFF:$f"; fi
  done
  log "CMP $3 same=$same differ=$diff missing=$miss$lst"
}
until ( set -o noclobber; echo "verify6 searchbuild $(date -Is)" > $L ) 2>/dev/null; do sleep 10; done
log LOCKED
for s in minimal recommended max; do
  run ref 74a4a88 $s free
  run cg d54f85d $s cg32
  run free d54f85d $s free
  run bcg 74a4a88 $s cg32
done
rm -f $L
log UNLOCKED
declare -A OLD=([minimal]=min-1c [recommended]=rec-1c [max]=max-1c)
for s in minimal recommended max; do
  for lab in cg free bcg; do cmpidx $S/runs/$lab-$s/idx $S/runs/ref-$s/idx "${lab}-${s}_vs_ref"; done
  cmpidx $S/runs/ref-$s/idx /ssd/searchbuild/ref/idx/${OLD[$s]} "ref-${s}_vs_searchbuild_ref"
done
log SB_DONE
