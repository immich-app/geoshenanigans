#!/bin/bash
# analyze2.sh (parity resume) : after analyze.sh, the analysis the resume adds:
#  - the new tip 64183ef (T7 = r4 data x grid_query.s7): totals vs B0, mechanism checks per preset,
#    grids, Nominatim point sets, Singapore city counts, loc-to-city diff;
#  - mechanism checks for the quarter (984e668) and half (7e8e9bb) steps (mech2.py);
#  - second / third passes over the builder rows explain_builder left unexplained (label, POI);
#  - archive build + tests of 64183ef, md5 against bin/grid_query.s7.
set -u
P=/ssd/parity
PY="/ssd/shrink/venv/bin/python -I"
C=$P/cmp
q() { echo $P/q/$1_$2/$3.txt; }
for s in smallest recommended max; do
  echo "## $s"
  $PY $P/probe/mech2.py $(q D4 $s p200k) $(q T2a $s p200k) quarter $P/sets/r4/$s $P/bin/parity_probe.base $P/bin/parity_probe.s2a $C/mech_quarter.$s.json
  $PY $P/probe/mech2.py $(q T2a $s p200k) $(q T2b $s p200k) half $P/sets/r4/$s $P/bin/parity_probe.s2a $P/bin/parity_probe.s2b $C/mech_half.$s.json
  $PY $P/probe/mech_s7.py $(q T6 $s p200k) $(q T7 $s p200k) $P/sets/r4/$s $P/bin/parity_probe.s7 $P/bin/admin_probe.s7 $C/mech_s7.$s.json
  $PY $P/probe/explain_label.py $s base r1 $C/builder_type.$s.json $C/builder_type_label.$s.json
  $PY $P/probe/explain_poi.py $s base r1 $C/builder_type_label.$s.json $P/census/base_r1.json $P/census/objs.opl $C/builder_type_poi.$s.json
  $PY $P/probe/explain_label.py $s r1 r4 $C/builder_postal_minimal.$s.json $C/builder_postal_minimal_label.$s.json
  $PY $P/probe/adiff.py $(q B0 $s p200k) $(q T7 $s p200k) 0 | sed -n '1,3p;/by field/,/examples/p' > $C/total_T7.$s.txt
  $PY $P/probe/adiff.py $(q T6 $s p200k) $(q T7 $s p200k) 0 | sed -n '1,3p;/by field/,/examples/p' > $C/step_s7.$s.txt
done > $C/explain2.txt 2>&1
for s in smallest recommended max; do
  echo "## grids $s"; $PY /ssd/bugs2/probe/score2.py /ssd/bugs2/grids/truth_c.json /ssd/bugs2/grids/all.txt $P/q $s B0 D1 D4 T6 T7
done > $C/grids_T7.md 2>&1
for f in fixed24 extra trace l2c6 l2c7 s7; do
  for s in smallest recommended max; do
    echo "## $f $s"
    $PY $P/probe/score.py $P/nominatim/${f}_api.jsonl place B0=$P/bin/grid_query.base:$P/sets/base/$s D4=$P/bin/grid_query.base:$P/sets/r4/$s T6=$P/bin/grid_query.s6:$P/sets/r4/$s T7=$P/bin/grid_query.s7:$P/sets/r4/$s | tail -4
  done
done > $C/nominatim_summary_T7.txt 2>&1
for f in fixed24 extra trace; do
  for s in smallest recommended max; do
    $PY $P/probe/score.py $P/nominatim/${f}_api.jsonl place B0=$P/bin/grid_query.base:$P/sets/base/$s T7=$P/bin/grid_query.s7:$P/sets/r4/$s > $C/nominatim_${f}_${s}_T7.md
    $PY $P/probe/score.py $P/nominatim/${f}_api.jsonl all B0=$P/bin/grid_query.base:$P/sets/base/$s T7=$P/bin/grid_query.s7:$P/sets/r4/$s > $C/nominatim_${f}_${s}_T7_all.md
  done
done
$PY $P/probe/sg_city.py $P/q p200k,v4photo,v4land,v4cpu,v4changed B0,T6,T7 > $C/sg_city.md 2>&1
$PY $P/probe/l2c_diff.py $P/l2c/B0/eval.json $P/l2c/T7/eval.json $P/q B0 T7 > $C/l2c_B0_T7.txt 2>&1
# archive build of 64183ef
export CARGO_HOME=$P/cargo TMPDIR=$P/tmp PATH=$HOME/.cargo/bin:$PATH
rev=64183ef; d=$P/srcs-$rev; rm -rf $d; mkdir -p $d
git -C $P/repo archive $rev geocoder/server geocoder/builder/data geocoder/builder/configurations.template.json geocoder/test-portal | tar -x -C $d
cd $d/geocoder/server
CARGO_TARGET_DIR=$P/target-arch-$rev nice -n 19 cargo test --release -j12 > $P/logs/arch_test.$rev.log 2>&1
echo "CARGO_TEST s7 $rev rc=$? $(grep -E 'test result' $P/logs/arch_test.$rev.log | tr '\n' ' ')" > $C/arch_s7.txt
CARGO_TARGET_DIR=$P/target-arch-$rev nice -n 19 cargo build --release -j12 --example grid_query > $P/logs/arch_build.$rev.log 2>&1
echo "CARGO_BUILD s7 $rev rc=$? $(md5sum < $P/target-arch-$rev/release/examples/grid_query | cut -c1-32) vs $(md5sum < $P/bin/grid_query.s7 | cut -c1-32)" >> $C/arch_s7.txt
echo ANALYZE2_DONE
