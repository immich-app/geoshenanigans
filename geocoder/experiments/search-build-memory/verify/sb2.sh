#!/bin/bash
# sb2.sh : the searchbuild track's baseline configuration: 74a4a88 with --mem-mb 16 (its default
# sort budget is 64 MiB), in the 32 MiB scope and unconstrained, minimal / recommended / max, same
# method as sb.sh (psets, evicted cache, core 40, memwatch6). Under TIMING.lock.
set -u
V=/ssd/verify6
S=$V/sb
L=/ssd/shrink/sortpatch/TIMING.lock
EX=/ssd/search-prod/extras
log() { echo "$(date -Is) $*"; }
evict() {
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
run() {  # label rev set mode
  local lab=$1 rev=$2 set=$3 mode=$4 R=$S/runs/$1-$3
  rm -rf $R; mkdir -p $R/idx $R/tmp
  cat > $R/inner.sh << EOF
#!/bin/bash
taskset -c 40 $V/bin/place-search.$rev build $S/psets/$set $R/idx --threads 1 --tmp $R/tmp --imp $EX/$set --mem-mb 16 > $R/build.log 2>&1
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
  log "RUN $lab $set $mode exit=$? $(cat $R/inner.out) files=$(ls $R/idx | wc -l) $(/ssd/shrink/venv/bin/python -I -c "import json,sys;d=json.load(open(sys.argv[1]));print('wall_s',d['wall_s'],'RssAnon_kB',d['peak_kB'].get('RssAnon'),'VmHWM_kB',d['peak_kB'].get('VmHWM'),'tmp_MiB',d['tmp_peak_MiB'])" $R/mem.json)"
}
until ( set -o noclobber; echo "verify6 searchbuild2 $(date -Is)" > $L ) 2>/dev/null; do sleep 10; done
log LOCKED
for s in minimal recommended max; do
  run b16cg 74a4a88 $s cg32
  run b16free 74a4a88 $s free
done
rm -f $L
log UNLOCKED
for s in minimal recommended max; do
  same=0; diff=0; miss=0
  for f in $(ls $S/runs/ref-$s/idx); do
    if [ ! -e $S/runs/b16free-$s/idx/$f ]; then miss=$((miss+1)); elif cmp -s $S/runs/b16free-$s/idx/$f $S/runs/ref-$s/idx/$f; then same=$((same+1)); else diff=$((diff+1)); echo "  DIFF $s $f"; fi
  done
  log "CMP b16free-$s vs ref same=$same differ=$diff missing=$miss"
done
log SB2_DONE
