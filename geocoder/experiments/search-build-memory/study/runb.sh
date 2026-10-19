#!/bin/bash
# runb.sh <bin tag> <cfg> <mode> [core] [-- extra build args]
#   cfg : min | rec | max | maxn | recx  (the search-prod index configs min-1c, rec-1c, max-1c, ...)
#   mode: free (no limit) | cgNN (systemd scope, MemoryMax=NNM, no swap)
# Builds the index with /ssd/searchbuild/bin/place-search.<tag> on one pinned core, samples memory /
# temporary disk with memwatch3.py, then compares every output file with the reference index.
set -u
S=/ssd/searchbuild
tag=$1; cfg=$2; mode=$3; core=${4:-40}
shift 3; [ $# -gt 0 ] && shift
extra=()
if [ "${1:-}" = "--" ]; then shift; extra=("$@"); fi
case $cfg in
  min)  set_=minimal;     ref=min-1c;   args=(--imp $S/extras/minimal) ;;
  rec)  set_=recommended; ref=rec-1c;   args=(--imp $S/extras/recommended) ;;
  max)  set_=max;         ref=max-1c;   args=(--imp $S/extras/max) ;;
  maxn) set_=max;         ref=maxn-1c;  args=(--imp $S/extras/max --poi-max-tier 2) ;;
  recx) set_=recommended; ref=rec-x-1c; args=(--imp $S/extras/recommended --extras $S/extras/recommended/extra_names.bin) ;;
esac
R=${RUNS:-$S/runs}/$tag-$cfg-$mode
# keep an earlier run of the same binary/config/limit (another sort budget) next to this one
[ -e $R/mem.json ] && mv $R $R.prev-$(date +%s)
rm -rf $R; mkdir -p $R/idx $R/tmp
inner=$R/inner.sh
cat > $inner <<EOF
#!/bin/bash
taskset -c $core $S/bin/place-search.$tag build ${SETS:-$S/sets}/$set_ $R/idx --threads 1 --tmp $R/tmp ${args[@]} ${extra[@]} > $R/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n "s/^0:://p" /proc/self/cgroup)
echo "rc=\$rc peak=\$(cat \$own/memory.peak 2>/dev/null) \$(tr "\n" " " < \$own/memory.events 2>/dev/null)" > $R/inner.out
exit \$rc
EOF
chmod +x $inner
if [ $mode = free ]; then
  run=($inner)
else
  mb=${mode#cg}
  run=(systemd-run --scope -q -p MemoryMax=${mb}M -p MemorySwapMax=0 $inner)
fi
python3 -I $S/memwatch3.py $R/mem.json $R/tmp -- "${run[@]}"
echo "exit=$?" > $R/rc
same=0; diff=0; missing=0
for f in $(ls $S/ref/idx/$ref); do
  [ $f = search_meta.json ] && continue
  if [ ! -e $R/idx/$f ]; then missing=$((missing+1)); echo "MISSING $f" >> $R/cmp.txt
  elif cmp -s $R/idx/$f $S/ref/idx/$ref/$f; then same=$((same+1))
  else diff=$((diff+1)); echo "DIFF $f" >> $R/cmp.txt; fi
done
echo "$tag $cfg $mode: same=$same diff=$diff missing=$missing $(cat $R/rc) $(python3 -c "import json;d=json.load(open('$R/mem.json'));print('wall_s',d['wall_s'],'anon_kB',d['peak_kB'].get('RssAnon'),'hwm_kB',d['peak_kB'].get('VmHWM'),'tmp_MiB',round(d['tmp_peak_bytes']/2**20,1),'cg',d['cgroup'])")" | tee $R/summary.txt
