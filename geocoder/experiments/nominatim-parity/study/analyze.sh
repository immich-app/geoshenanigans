#!/bin/bash
# analyze.sh : after queries.sh. Every changed p200k answer explained per step, grids, Singapore,
# the API point sets, sizes. Outputs under /ssd/parity/cmp.
set -u
P=/ssd/parity
PY="/ssd/shrink/venv/bin/python -I"
C=$P/cmp
mkdir -p $C
q() { echo $P/q/$1_$2/$3.txt; }
for s in smallest recommended max; do
  echo "## $s"
  $PY $P/probe/explain_builder.py $s base r1 $(q B0 $s p200k) $(q D1 $s p200k) $C/builder_type.$s.json
  $PY $P/probe/explain_builder.py $s r1 r4 $(q D1 $s p200k) $(q D4 $s p200k) $C/builder_postal_minimal.$s.json
  $PY $P/probe/explain_server.py $(q D4 $s p200k) $(q T2a $s p200k) quarter $C/server_quarter.$s.json
  $PY $P/probe/explain_server.py $(q T2a $s p200k) $(q T2b $s p200k) half $C/server_half.$s.json
  $PY $P/probe/explain_server.py $(q T2b $s p200k) $(q T3a $s p200k) subcity $C/server_subcity.$s.json
  $PY $P/probe/explain_server.py $(q T3a $s p200k) $(q T3b $s p200k) rank16 $C/server_rank16.$s.json
  $PY $P/probe/explain_server.py $(q T3b $s p200k) $(q T6 $s p200k) coarse $C/server_coarse.$s.json
  $PY $P/probe/mech_server.py $(q T2b $s p200k) $(q T3a $s p200k) subcity $P/sets/r4/$s $C/mech_subcity.$s.json $P/bin/parity_probe.s2b $P/bin/parity_probe.s3a
  $PY $P/probe/mech_server.py $(q T3a $s p200k) $(q T3b $s p200k) rank16 $P/sets/r4/$s $C/mech_rank16.$s.json $P/bin/parity_probe.s3a $P/bin/parity_probe.s3b
  $PY $P/probe/adiff.py $(q B0 $s p200k) $(q T6 $s p200k) 0 | sed -n '1,3p;/by field/,/examples/p' > $C/total.$s.txt
done > $C/explain.txt 2>&1
for s in smallest recommended max; do
  echo "## grids $s"; $PY /ssd/bugs2/probe/score2.py /ssd/bugs2/grids/truth_c.json /ssd/bugs2/grids/all.txt $P/q $s B0 D1 D4 T6
done > $C/grids.md 2>&1
for f in fixed24 extra s2a s2b s3a s3b s6; do
  for s in smallest recommended max; do
    echo "## $f $s"
    $PY $P/probe/score.py $P/nominatim/${f}_api.jsonl place B0=$P/bin/grid_query.base:$P/sets/base/$s D4=$P/bin/grid_query.base:$P/sets/r4/$s T6=$P/bin/grid_query.s6:$P/sets/r4/$s | tail -3
  done
done > $C/nominatim_summary.txt 2>&1
for f in fixed24 extra; do
  for s in recommended max; do
    $PY $P/probe/score.py $P/nominatim/${f}_api.jsonl place B0=$P/bin/grid_query.base:$P/sets/base/$s T6=$P/bin/grid_query.s6:$P/sets/r4/$s > $C/nominatim_${f}_$s.md
    $PY $P/probe/score.py $P/nominatim/${f}_api.jsonl all B0=$P/bin/grid_query.base:$P/sets/base/$s T6=$P/bin/grid_query.s6:$P/sets/r4/$s > $C/nominatim_${f}_${s}_all.md
  done
done
$P/sizes.sh $P/sets/base $P/sets/r4 > $C/sizes_base_r4.tsv 2> $C/sizes_base_r4.err
echo ANALYZE_DONE
