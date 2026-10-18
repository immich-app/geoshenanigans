#!/bin/bash
# vrun.sh <name> <build-index> [extra builder args...]
# Independent re-run of a planet build under a 96 GiB / no-swap scope with
# GC_MEMORY_RESERVE_MIB=12288. Own 0.25 s sampler: cgroup anon/file/current,
# builder VmRSS/RssAnon/VmHWM. Caller holds /ssd/precision/BUILD.lock.
set -u
P=/ssd/precision
V=$P/verify
name=$1; bin=$2; shift 2
out=$V/out-$name
d=$V/$name
export TMPDIR=$P/tmp
rm -rf "$out" "$d"; mkdir -p "$d" "$TMPDIR"
unit=verify-$name
cg=/sys/fs/cgroup/precision.slice/$unit.scope
cat > "$d/inner.sh" << EOF
#!/bin/bash
/usr/bin/time -v $bin $out /ssd/work/planet-261006.osm.pbf --continents --multi-output --multi-quality --tiger-data /ssd/work/tiger-data.tar.gz --wikidata-sitelinks /ssd/work/qid_sitelinks.bin --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz $* > $d/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n 's/^0:://p' /proc/self/cgroup)
cat \$own/memory.peak > $d/memory.peak
cat \$own/memory.events > $d/memory.events
exit \$rc
EOF
chmod +x "$d/inner.sh"
sync; echo 3 > /proc/sys/vm/drop_caches
(
  until [ -d $cg ]; do sleep 0.05; done
  printf 't\tcurrent\tanon\tfile\tvmrss\trssanon\tvmhwm\n'
  while [ -d $cg ]; do
    cur=$(cat $cg/memory.current 2> /dev/null)
    st=$(awk '$1=="anon"{a=$2} $1=="file"{f=$2} END{printf "%d\t%d",a,f}' $cg/memory.stat 2> /dev/null)
    pid=$(pgrep -f "^$bin " | head -1)
    pr=$( [ -n "$pid" ] && awk '$1=="VmRSS:"{r=$2} $1=="RssAnon:"{a=$2} $1=="VmHWM:"{h=$2} END{printf "%d\t%d\t%d",r*1024,a*1024,h*1024}' /proc/$pid/status 2> /dev/null)
    printf '%s\t%s\t%s\t%s\n' "$(date +%s.%N | cut -c1-14)" "${cur:-0}" "${st:-0	0}" "${pr:-0	0	0}"
    sleep 0.25
  done
) > "$d/mem.tsv" &
sampler=$!
start=$(date +%s.%N)
systemd-run --quiet --scope --slice=precision.slice --unit=$unit -p MemoryMax=96G -p MemorySwapMax=0 \
  env GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR "$d/inner.sh"
rc=$?
end=$(date +%s.%N)
wait $sampler
echo "rc=$rc wall=$(awk "BEGIN{printf \"%.1f\", $end-$start}")" > "$d/run.txt"
awk -F'\t' 'NR>1{if($3>a)a=$3; if($6>r)r=$6; if($7>h)h=$7} END{printf "peak_anon=%.2fGiB peak_rssanon=%.2fGiB vmhwm=%.2fGiB\n",a/2^30,r/2^30,h/2^30}' "$d/mem.tsv" >> "$d/run.txt"
(cd "$out" && find . -type f -printf '%s\t%P\n' | sort -k2) > "$d/files.tsv"
awk -F'\t' '{t+=$1} END{print "total_bytes=" t}' "$d/files.tsv" >> "$d/run.txt"
echo "VRUN_DONE $name rc=$rc"
