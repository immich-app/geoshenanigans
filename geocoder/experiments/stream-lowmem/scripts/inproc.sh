#!/bin/bash
# inproc.sh STYLE CORES OLDENC PDIR NEWENC WORK RES   (memory track; inside the scope mrun.sh makes)
# The low-memory client update: one geocoder-patch process per variant reads the encoded installed
# set, decodes and encodes in process and writes the new encoded set (no FIFOs, no today's-format
# family file on disk). STYLE seq: variants one after another; par: all three at once.
# Prints STAGE lines (wall, CPU, read / written, peak extra disk), PROC lines and a TOTAL line.
set -u
STYLE=$1 CORES=$2 OLDENC=$3 PDIR=$4 NEWENC=$5 WORK=$6 RES=$7
PATCHX=${PATCHX:-/ssd/memory/bin/geocoder-patch-m}
VARIANTS=${SVARIANTS:-"planet/full planet/quality/uncapped planet/poi/all"}
export PATH=/ssd/shrink/zstd157/bin:$PATH
mkdir -p "$WORK/tmp" "$NEWENC" "$RES"
export TMPDIR=$WORK/tmp
cg=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
md=$(cat /sys/block/md4/dev)
iostat() { awk -v d=$md '$1 == d {for (i = 2; i <= NF; i++) {split($i, kv, "="); v[kv[1]] = kv[2]}} END {print v["rbytes"] + 0, v["wbytes"] + 0}' $cg/io.stat 2> /dev/null || echo "0 0"; }
base_kb=$(du -sk "$OLDENC" | awk '{print $1}')
extra_kb() { du -skc "$OLDENC" "$WORK" "$NEWENC" 2> /dev/null | tail -1 | awk -v b=$base_kb '{print $1 - b}'; }
safe() { echo "${1//\//_}"; }

one_variant() {  # variant
  local v=$1 sv=$(safe $1)
  mkdir -p "$NEWENC/$v"
  /usr/bin/time -f "%e %U %S %M" -o "$RES/patch.$sv.time" "$PATCHX" "$OLDENC/$v" "$PDIR/$sv/patch.gcpatch" -o "$NEWENC/$v/" \
    2> "$RES/patch.$sv.log"
}
export -f one_variant safe
export OLDENC PDIR NEWENC RES PATCHX

TOT_W=0 PEAK_DISK=0
declare -A SC
run_stage() {  # label CMD...
  local label=$1; shift
  read r0 w0 < <(iostat)
  local t0=$(date +%s.%N)
  /usr/bin/time -v -o "$RES/time.$label" taskset -c "$CORES" "$@" &
  local tpid=$! disk=0 d
  while kill -0 $tpid 2> /dev/null; do
    d=$(extra_kb); [ -n "$d" ] && [ "$d" -gt $disk ] && disk=$d
    sleep 0.5
  done
  wait $tpid; local rc=$?
  local t1=$(date +%s.%N)
  d=$(extra_kb); [ "$d" -gt $disk ] && disk=$d
  read r1 w1 < <(iostat)
  local u=$(awk -F': ' '/User time/ {print $2}' "$RES/time.$label") s=$(awk -F': ' '/System time/ {print $2}' "$RES/time.$label")
  local wall=$(awk -v a=$t0 -v b=$t1 'BEGIN {printf "%.2f", b - a}') cpu=$(awk -v u=$u -v s=$s 'BEGIN {printf "%.2f", u + s}')
  printf "STAGE %-28s rc=%d wall_s=%s cpu_s=%s user_s=%s sys_s=%s read_gb=%.3f written_gb=%.3f peak_extra_disk_gb=%.2f\n" \
    "$label" $rc "$wall" "$cpu" "$u" "$s" \
    "$(awk -v b=$((r1 - r0)) 'BEGIN {print b / 1e9}')" "$(awk -v b=$((w1 - w0)) 'BEGIN {print b / 1e9}')" \
    "$(awk -v k=$disk 'BEGIN {print k * 1024 / 1e9}')" | tee -a "$RES/stages.txt"
  SC[$label]=$cpu
  [ $disk -gt $PEAK_DISK ] && PEAK_DISK=$disk
  [ $rc -eq 0 ] || { echo "STAGE $label FAILED rc=$rc"; tail -5 "$RES"/patch.*.log; exit 1; }
}

T0=$(date +%s.%N)
read R0 W0 < <(iostat)
if [ "$STYLE" = seq ]; then
  for v in $VARIANTS; do run_stage "patch.$(safe $v)" bash -c "one_variant $v"; done
else
  run_stage patch.all bash -c "rc=0; for v in $VARIANTS; do one_variant \$v & done; for j in \$(jobs -p); do wait \$j || rc=1; done; exit \$rc"
fi
run_stage fsync bash -c "find '$NEWENC' -type f -print0 | xargs -0 -r sync"
T1=$(date +%s.%N)
read R1 W1 < <(iostat)
for t in "$RES"/patch.*.time; do
  read w u s m < <(tail -1 "$t")
  echo "PROC $(basename "$t" .time) wall_s=$w user_s=$u sys_s=$s maxrss_mib=$((m / 1024))"
done | tee -a "$RES/stages.txt"
cpu_total=0; for k in "${!SC[@]}"; do cpu_total=$(awk -v a=$cpu_total -v b=${SC[$k]} 'BEGIN {print a + b}'); done
printf "TOTAL wall_s=%.2f cpu_s=%.2f cgroup_mem_peak_mib=%d oom_kills=%s read_gb=%.3f written_gb=%.3f peak_extra_disk_gb=%.2f new_set_gb=%.3f\n" \
  "$(awk -v a=$T0 -v b=$T1 'BEGIN {print b - a}')" "$cpu_total" \
  $(( $(cat $cg/memory.peak 2> /dev/null || echo 0) / 1048576 )) "$(awk '/^oom_kill / {print $2}' $cg/memory.events 2> /dev/null)" \
  "$(awk -v b=$((R1 - R0)) 'BEGIN {print b / 1e9}')" "$(awk -v b=$((W1 - W0)) 'BEGIN {print b / 1e9}')" \
  "$(awk -v k=$PEAK_DISK 'BEGIN {print k * 1024 / 1e9}')" "$(du -sk --apparent-size "$NEWENC" | awk '{print $1 * 1024 / 1e9}')" | tee -a "$RES/stages.txt"
