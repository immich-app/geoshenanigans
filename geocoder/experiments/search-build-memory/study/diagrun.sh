#!/bin/bash
# diagrun.sh <bin tag> <cfg> <mode> [-- extra build args]: like runb.sh (one pinned core 40, private
# sets, free or a cgNN systemd scope) but sampled by diag.py (sampler on core 41) instead of
# memwatch3.py; run dir $RUNS/diag-<tag>-<cfg>-<mode>[-$LABEL], output files compared with the reference.
# ENVS="K=V ..." sets environment variables for the build.
set -u
S=/ssd/searchbuild
tag=$1; cfg=$2; mode=$3; shift 3
extra=()
if [ "${1:-}" = "--" ]; then shift; extra=("$@"); fi
case $cfg in
  min) set_=minimal; ref=min-1c ;;
  rec) set_=recommended; ref=rec-1c ;;
  max) set_=max; ref=max-1c ;;
esac
R=${RUNS:-$S/runs3}/diag-$tag-$cfg-$mode${LABEL:+-$LABEL}
rm -rf $R; mkdir -p $R/idx $R/tmp
cat > $R/inner.sh <<EOF
#!/bin/bash
env ${ENVS:-} taskset -c 40 $S/bin/place-search.$tag build ${SETS:-$S/psets}/$set_ $R/idx --threads 1 --tmp $R/tmp --imp $S/extras/$set_ ${extra[@]} > $R/build.log 2>&1
EOF
chmod +x $R/inner.sh
if [ $mode = free ]; then run=($R/inner.sh); else run=(systemd-run --scope -q -p MemoryMax=${mode#cg}M -p MemorySwapMax=0 $R/inner.sh); fi
taskset -c 41 python3 -I $S/diag.py $R/diag.jsonl $R/tmp $R/idx -- "${run[@]}" > $R/rc.json
same=0; bad=0
for f in $(ls $S/ref/idx/$ref); do
  [ $f = search_meta.json ] && continue
  if cmp -s $R/idx/$f $S/ref/idx/$ref/$f; then same=$((same+1)); else bad=$((bad+1)); fi
done
echo "diag $tag $cfg $mode ${LABEL:-} ${ENVS:-}: same=$same bad=$bad $(cat $R/rc.json)"
