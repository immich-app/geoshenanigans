#!/bin/bash
# phase2: (A) independent numpy decode of the 3 largest planet files from the re-made .tt.zst,
# (B) full round trip of all 38 files with the verify build (records+vertices via dec2, .tt.zst only),
# (C) 1-core warm decode timing (core 63, under the shared TIMING.lock): plain zstd -d vs tt2 with
#     the report's binary vs tt2 with the verify build; every output checked against the manifest.
set -u
V=/ssd/transport-verify
P=/ssd/shrink/venv/bin/python
Z=/ssd/shrink/zstd157/bin/zstd
B=$V/planet/best_4m
S=/ssd/shrink/idx/max
export TMPDIR=$V/tmp
cd $V
sha() { $P -I -c "import json,sys;F=json.load(open('/ssd/shrink/data/configurations-2026-10-13.json'))['files'];print([v['sha256'] for k,v in F.items() if k.endswith('/'+sys.argv[1]) and k.startswith('planet/')][0])" $1; }
echo "$(date +%T) A+B start"
for n in geo_cells.bin addr_points.bin street_nodes.bin; do
  s=$(sha $n)
  ( $Z -dc -q $B/$n.tt.zst | $P -I $V/scripts/npdec.py $S/$n $s > logs/npdec_$n.log 2>&1 ) &
done
rm -rf planet/dec_mine planet/dec_mine.dec2.log
bash scripts/dec_set.sh tt2 $V/bin/tt $B planet/dec_mine
$P -I scripts/rt_check.py planet/dec_mine $S "round trip, verify build (parallel phase)" | tee logs/rt_mine.log
cat planet/dec_mine.dec2.log
rm -rf planet/dec_mine
wait
for n in geo_cells.bin addr_points.bin street_nodes.bin; do echo "npdec $n: $(cat logs/npdec_$n.log)"; done
echo "$(date +%T) A+B done"

LOCK=/ssd/shrink/sortpatch/TIMING.lock
while ! ( set -o noclobber; echo "transport-verify $$ $(date +%T)" > $LOCK ) 2> /dev/null; do sleep 2; done
trap "rm -f $LOCK" EXIT
timed() {  # label mode tt indir
  local label=$1 mode=$2 tt=$3 in=$4
  rm -rf planet/dec; sync
  find $in -type f -print0 | xargs -0 -P 8 -n 4 cat > /dev/null
  echo "$(date +%T) load: $(cut -d' ' -f1-3 /proc/loadavg)"
  /usr/bin/time -o logs/time_$label.time -v taskset -c 63 bash scripts/dec_set.sh $mode $tt $in planet/dec
  local wall=$(grep "Elapsed" logs/time_$label.time | awk '{print $NF}')
  local usr=$(grep "User time" logs/time_$label.time | awk '{print $NF}')
  local sys=$(grep "System time" logs/time_$label.time | awk '{print $NF}')
  local rss=$(grep "Maximum resident" logs/time_$label.time | awk '{print $NF}')
  echo "$label wall=$wall user=$usr sys=$sys maxrss_kb=$rss" | tee -a logs/timings.tsv
  $P -I scripts/rt_check.py planet/dec $S "$label output" | tee -a logs/timings.tsv
  rm -rf planet/dec planet/dec.dec2.log; sync
}
timed warm_plain plain - $V/planet/plain_zst
timed warm_tt2_reportbin tt2 /ssd/transport/bin/tt $B
timed warm_tt2_verifybin tt2 $V/bin/tt $B
echo "$(date +%T) PHASE2 DONE"
