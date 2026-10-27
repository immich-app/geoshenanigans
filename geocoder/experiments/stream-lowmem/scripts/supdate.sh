#!/bin/bash
# supdate.sh STYLE CORES OLDENC PDIR NEWENC WORK RES
# The STREAMING client update of the encoded installed set, inside one systemd scope (run_update2.sh
# makes it): no today's-format family file is ever written to disk. Per variant:
#   WORK/old/<v>: a FIFO for every today's file of the installed encoded families, hardlinks for the
#                 rest; WORK/new/<v>: a FIFO for every today's file of those families.
#   decoders  gcx decode, one process per old FIFO (gxc: one process for its four), writing them
#   patcher   geocoder-patch (e2e build) reading the old FIFOs, writing the new FIFOs and the
#             unencoded files (regular files, then hardlinked into NEWENC)
#   encoders  gcx encode, one process per new FIFO (gxc: one for four; streets / interps / addr /
#             poi: pass 1 on the first file the patcher writes, then pass 2 on the second)
# STYLE stream: variants one after another; streampar: all three at once.
# Prints STAGE lines (one per variant, or one for all) and a TOTAL line like update.sh, plus PROC
# lines: wall / CPU / max RSS of every decoder, encoder and patcher process (CPU by role).
set -u
source /ssd/shrink/sortpatch/e2e/common.sh
STYLE=$1 CORES=$2 OLDENC=$3 PDIR=$4 NEWENC=$5 WORK=$6 RES=$7
GCX=$E/bin/gcx
PATCHX=$E/build-x/geocoder-patch
mkdir -p "$WORK/old" "$WORK/new" "$WORK/tmp" "$WORK/man" "$NEWENC" "$RES"
export TMPDIR=$WORK/tmp
cg=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
md=$(cat /sys/block/md4/dev)
iostat() { awk -v d=$md '$1 == d {for (i = 2; i <= NF; i++) {split($i, kv, "="); v[kv[1]] = kv[2]}} END {print v["rbytes"] + 0, v["wbytes"] + 0}' $cg/io.stat 2> /dev/null || echo "0 0"; }
base_kb=$(du -sk "$OLDENC" | awk '{print $1}')
extra_kb() { du -skc "$OLDENC" "$WORK" "$NEWENC" 2> /dev/null | tail -1 | awk -v b=$base_kb '{print $1 - b}'; }

fam_today() {
  case $1 in
    streets) echo street_ways.bin street_nodes.bin ;;
    interps) echo interp_ways.bin interp_nodes.bin ;;
    way_parents) echo way_parents.bin ;;
    way_postcodes|interp_postcodes) echo $1.bin ;;
    gxc) echo geo_cells.bin street_entries.bin addr_entries.bin interp_entries.bin ;;
    addr) echo addr_points.bin addr_vertices.bin ;;
    admin) echo admin_vertices.bin ;;
    poi) echo poi_records.bin poi_vertices.bin ;;
  esac
}
fam_enc() {
  case $1 in
    streets) echo street.groups street.nodes2 street.cnt street.code street.name ;;
    interps) echo interp.groups interp.nodes2 interp.cntcode interp.street interp.start interp.span interp.interp ;;
    way_parents) echo way_parents.u24 ;;
    way_postcodes|interp_postcodes) echo $1.u16 $1.dict ;;
    gxc) echo gxc_top.bin gxc_rows.bin gxc_street.bin gxc_interp.bin ;;
    addr) echo addr_points.col addr_vertices.abs2 ;;
    admin) echo admin_vertices.B ;;
    poi) echo poi_records.r3 poi_vertices.bopen ;;
  esac
}
FAM_ORDER="streets interps way_parents way_postcodes interp_postcodes gxc addr admin poi"
fams_of_variant() { tr ',' ' ' <<< "$(families_of "$OLDENC/$1")"; }

# tm LOGBASE CMD...: runs CMD (4 h cap) with /usr/bin/time; LOGBASE.{log,time,rc}
tm() {
  local b=$1; shift
  /usr/bin/time -f "%e %U %S %M" -o "$b.time" timeout 4h "$@" 2> "$b.log"
  local rc=$?
  echo $rc > "$b.rc"
  return $rc
}

stream_variant() {  # variant
  local v=$1 sv=$(safe $1) O=$WORK/old/$1 N=$WORK/new/$1 P=$PDIR/$(safe $1)/patch.gcpatch L=$RES/p.$(safe $1)
  local fams f x files sizes pids=()
  mkdir -p "$O" "$N" "$NEWENC/$v" "$L"
  fams=$(fams_of_variant $v)
  local skip=" gc.manifest "
  for f in $fams; do skip+="$(fam_enc $f) "; done
  for x in "$OLDENC/$v"/*; do
    case "$skip" in *" $(basename "$x") "*) continue ;; esac
    ln "$x" "$O/"
  done
  for f in $fams; do for x in $(fam_today $f); do mkfifo "$O/$x" "$N/$x"; done; done
  python3 $E/patch_sizes.py "$P" > "$L/sizes" || return 1
  sz() { awk -v n=$1 '$1 == n {print "--size " n "=" $2}' "$L/sizes"; }
  # decoders
  for f in $fams; do
    if [ $f = gxc ]; then
      tm "$L/dec.gxc" $GCX decode "$OLDENC/$v" "$O" --only gxc --no-link --files geo_cells.bin,street_entries.bin,addr_entries.bin,interp_entries.bin &
      pids+=($!)
    else
      for x in $(fam_today $f); do
        tm "$L/dec.$f.$x" $GCX decode "$OLDENC/$v" "$O" --only $f --no-link --files $x &
        pids+=($!)
      done
    fi
  done
  # encoders
  for f in $fams; do
    files=($(fam_today $f))
    if [ $f = gxc ] || [ ${#files[@]} -eq 1 ]; then
      sizes=$(for x in "${files[@]}"; do sz $x; done)
      tm "$L/enc.$f" $GCX encode "$N" "$NEWENC/$v" --only $f --no-link --files "$(IFS=,; echo "${files[*]}")" $sizes \
        --manifest-out "$WORK/man/$sv.$f.1" &
      pids+=($!)
    else
      ( tm "$L/enc.$f.1" $GCX encode "$N" "$NEWENC/$v" --only $f --no-link --files ${files[0]} $(sz ${files[0]}) \
          --manifest-out "$WORK/man/$sv.$f.1" &&
        tm "$L/enc.$f.2" $GCX encode "$N" "$NEWENC/$v" --only $f --no-link --files ${files[1]} $(sz ${files[1]}) \
          --manifest-out "$WORK/man/$sv.$f.2" ) &
      pids+=($!)
    fi
  done
  # the patcher
  tm "$L/patch" $PATCHX "$O" "$P" -o "$N/"
  local prc=$?
  if [ $prc -ne 0 ]; then
    echo "patcher failed rc=$prc ($v)"; tail -5 "$L/patch.log"
    for p in "${pids[@]}"; do pkill -P $p 2> /dev/null; kill $p 2> /dev/null; done
    return 1
  fi
  local bad=0
  for p in "${pids[@]}"; do wait $p || bad=1; done
  [ $bad -eq 0 ] || { echo "a decoder or encoder failed ($v)"; grep -l -v '^0$' "$L"/*.rc; return 1; }
  # the unencoded new files, the merged manifest
  for x in "$N"/*; do [ -f "$x" ] && ln "$x" "$NEWENC/$v/"; done
  local fl=""
  for f in $FAM_ORDER; do case " $fams " in *" $f "*) fl+="${fl:+,}$f" ;; esac; done
  { grep -hv '^gc\.' "$WORK/man/$sv".*; echo "gc.families $fl"; echo "gc.version 1"; } | LC_ALL=C sort > "$NEWENC/$v/gc.manifest"
  rm -f "$WORK/man/$sv".*
  rm -rf "$O" "$N"
}
export -f stream_variant tm fam_today fam_enc fams_of_variant safe families_of
export WORK OLDENC NEWENC PDIR RES GCX PATCHX FAM_ORDER E

TOT_W=0 PEAK_ANON=0 PEAK_DISK=0
declare -A SC
run_stage() {  # label CMD...
  local label=$1; shift
  read r0 w0 < <(iostat)
  local t0=$(date +%s.%N)
  /usr/bin/time -v -o "$RES/time.$label" taskset -c "$CORES" "$@" &
  local tpid=$! anon=0 disk=0 n=0 a d
  while kill -0 $tpid 2> /dev/null; do
    a=0
    for p in $(cat $cg/cgroup.procs 2> /dev/null); do
      case "$(cat /proc/$p/comm 2> /dev/null)" in
        gcx|geocoder-patch|zstd) a=$((a + $(awk '/^RssAnon/ {print $2}' /proc/$p/status 2> /dev/null || echo 0))) ;;
      esac
    done
    [ $a -gt $anon ] && anon=$a
    if [ $((n % 2)) -eq 0 ]; then d=$(extra_kb); [ -n "$d" ] && [ "$d" -gt $disk ] && disk=$d; fi
    n=$((n + 1)); sleep 0.25
  done
  wait $tpid; local rc=$?
  local t1=$(date +%s.%N)
  d=$(extra_kb); [ "$d" -gt $disk ] && disk=$d
  read r1 w1 < <(iostat)
  local u=$(awk -F': ' '/User time/ {print $2}' "$RES/time.$label") s=$(awk -F': ' '/System time/ {print $2}' "$RES/time.$label")
  local wall=$(awk -v a=$t0 -v b=$t1 'BEGIN {printf "%.2f", b - a}') cpu=$(awk -v u=$u -v s=$s 'BEGIN {printf "%.2f", u + s}')
  printf "STAGE %-28s rc=%d wall_s=%s cpu_s=%s user_s=%s sys_s=%s anon_peak_mib=%d read_gb=%.3f written_gb=%.3f peak_extra_disk_gb=%.2f\n" \
    "$label" $rc "$wall" "$cpu" "$u" "$s" $((anon / 1024)) \
    "$(awk -v b=$((r1 - r0)) 'BEGIN {print b / 1e9}')" "$(awk -v b=$((w1 - w0)) 'BEGIN {print b / 1e9}')" \
    "$(awk -v k=$disk 'BEGIN {print k * 1024 / 1e9}')" | tee -a "$RES/stages.txt"
  SC[$label]=$cpu
  [ $anon -gt $PEAK_ANON ] && PEAK_ANON=$anon
  [ $disk -gt $PEAK_DISK ] && PEAK_DISK=$disk
  [ $rc -eq 0 ] || { echo "STAGE $label FAILED rc=$rc"; exit 1; }
}

T0=$(date +%s.%N)
read R0 W0 < <(iostat)
if [ "$STYLE" = stream ]; then
  for v in ${SVARIANTS:-planet/full planet/quality/uncapped planet/poi/all}; do
    run_stage "stream.$(safe $v)" bash -c "stream_variant $v"
  done
else
  run_stage stream.all bash -c "rc=0; for v in $VARIANTS; do stream_variant \$v & done; for j in \$(jobs -p); do wait \$j || rc=1; done; exit \$rc"
fi
run_stage fsync bash -c "find '$NEWENC' -type f -print0 | xargs -0 -r sync"
T1=$(date +%s.%N)
read R1 W1 < <(iostat)
rmdir "$WORK/old/planet/quality" "$WORK/old/planet/poi" "$WORK/old/planet" "$WORK/new/planet/quality" "$WORK/new/planet/poi" "$WORK/new/planet" 2> /dev/null
# per process: wall user sys maxrss_kb, summed by role
for t in "$RES"/p.*/*.time; do
  role=$(basename "$t" .time); role=${role%%.*}
  read w u s m < <(tail -1 "$t")
  echo "PROC $(basename $(dirname "$t"))/$(basename "$t" .time) role=$role wall_s=$w user_s=$u sys_s=$s maxrss_mib=$((m / 1024))"
done | tee -a "$RES/stages.txt" | awk '{split($3, r, "="); split($5, u, "="); split($6, s, "="); c[r[2]] += u[2] + s[2]} END {for (k in c) printf "ROLE %-7s cpu_s=%.2f\n", k, c[k]}' | tee -a "$RES/stages.txt"
cpu_total=0; for k in "${!SC[@]}"; do cpu_total=$(awk -v a=$cpu_total -v b=${SC[$k]} 'BEGIN {print a + b}'); done
printf "TOTAL wall_s=%.2f cpu_s=%.2f anon_peak_mib=%d cgroup_mem_peak_mib=%d oom_kills=%s read_gb=%.3f written_gb=%.3f peak_extra_disk_gb=%.2f new_set_gb=%.3f\n" \
  "$(awk -v a=$T0 -v b=$T1 'BEGIN {print b - a}')" "$cpu_total" \
  $((PEAK_ANON / 1024)) $(( $(cat $cg/memory.peak 2> /dev/null || echo 0) / 1048576 )) "$(awk '/^oom_kill / {print $2}' $cg/memory.events 2> /dev/null)" \
  "$(awk -v b=$((R1 - R0)) 'BEGIN {print b / 1e9}')" "$(awk -v b=$((W1 - W0)) 'BEGIN {print b / 1e9}')" \
  "$(awk -v k=$PEAK_DISK 'BEGIN {print k * 1024 / 1e9}')" "$(du -sk --apparent-size "$NEWENC" | awk '{print $1 * 1024 / 1e9}')" | tee -a "$RES/stages.txt"
