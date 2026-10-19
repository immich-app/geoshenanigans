#!/bin/bash
# post1.sh : after the lock window. (1) tiervar3-n reproduces the real builds byte for byte (D, D1 at
# q2.5+cap; D1_d2 and D1_s5i at their schemes, all warm slots); (2) daily patches D -> D1 per scheme
# (patch3.sh); (3) d2 on planet-261006 end to end (cand6.sh) for the final tables.
set -u
T=/ssd/tiers3
cd $T
export OMP_NUM_THREADS=16
Q=";t9=42;t10=46;t11=47;t15=47"
D2="t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=44.5;cap=0.1;iso=1"
S5="t2=82;t3=275;t4=82;t5=165;t6=165;t7=82;t8=66;cap=0.1;iso=1"
check() {  # check <label> <dump> <build out planet> <minimal scheme> <quality scheme>
  local w=$T/tmp/chk-$1; rm -rf $w; mkdir -p $w/min $w/q
  TIERVAR_MIN_SLOTS=$3/admin-minimal/admin_polygons.bin $T/bin/tiervar3-n minimal $2 $3/admin $w/min "$4" > /dev/null
  $T/bin/tiervar3-n quality $2 $w/q "$5" > /dev/null
  local same=0 diff=0 f
  for f in admin_cells admin_entries admin_polygons admin_vertices; do
    if cmp -s $w/min/$f.bin $3/admin-minimal/$f.bin; then same=$((same+1)); else diff=$((diff+1)); echo "DIFF $1 admin-minimal/$f.bin"; fi
  done
  for f in admin_polygons admin_vertices postal_polygons postal_vertices; do
    if cmp -s $w/q/$f.bin $3/quality/q2.5/$f.bin; then same=$((same+1)); else diff=$((diff+1)); echo "DIFF $1 quality/q2.5/$f.bin"; fi
  done
  echo "CHECK $1 identical $same/8"
  rm -rf $w
}
{
  check D $T/dumpD $T/build/out-D/planet "scale=2.5;cap=0.1" "scale=2.5;cap=0.1"
  check D1 $T/dumpD1 $T/build/out-D1/planet "scale=2.5;cap=0.1" "scale=2.5;cap=0.1"
  check D1_d2 $T/dumpD1 $T/build/out-D1_d2/planet "$D2" "$D2$Q"
  check D1_s5i $T/dumpD1 $T/build/out-D1_s5i/planet "$S5" "$S5$Q"
} > cmp/check_builds.txt 2>&1
./patch3.sh q25 "scale=2.5" "scale=2.5" > /dev/null
./patch3.sh cap10 "scale=2.5;cap=0.1" "scale=2.5;cap=0.1" > /dev/null
./patch3.sh f50 "uni=75;t8=50;cap=0.1" > /dev/null
./patch3.sh f75 "uni=75;cap=0.1" > /dev/null
./patch3.sh c5 "t2=75;t3=250;t4=75;t5=150;t6=125;t7=50;t8=37.5;cap=0.1" > /dev/null
./patch3.sh d2 "$D2" "$D2$Q" > /dev/null
./patch3.sh s5i "$S5" "$S5$Q" > /dev/null
JOBS=12 ./cand6.sh d2 "$D2" "$D2$Q"
echo POST1_DONE
