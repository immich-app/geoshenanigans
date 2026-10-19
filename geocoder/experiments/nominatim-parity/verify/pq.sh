#!/bin/bash
# pq.sh : parity answers, all from verify6's own builds. Data: /ssd/verify6/sets/{base,tip}/<preset>
# (bc2e758 / f7096b0 planet-261018b builds). Servers: grid_query.<rev>. Labels walk base -> tip one
# step at a time, so each changed answer is attributed to the step that changed it:
#   B  base data, server bc2e758        D  tip data, server bc2e758 (all builder commits)
#   S1 tip data, 984e668   S2 7e8e9bb   S3 c99516b   S4 9171a8e   S5 509845b   T 64183ef (tip)
# Pools: par30k (verify6 fresh), l2c (loc-to-city points), grids (the 20 grids).
# Out /ssd/verify6/q/<label>_<preset>/<pool>.txt; chunks of 2,500 lines, 40 jobs, niced.
set -u
V=/ssd/verify6
export TMPDIR=$V/tmp
declare -A DATA=([B]=base [D]=tip [S1]=tip [S2]=tip [S3]=tip [S4]=tip [S5]=tip [T]=tip)
declare -A SRV=([B]=bc2e758 [D]=bc2e758 [S1]=984e668 [S2]=7e8e9bb [S3]=c99516b [S4]=9171a8e [S5]=509845b [T]=64183ef)
LABELS=${LABELS:-"B D S1 S2 S3 S4 S5 T"}
POOLS=${POOLS:-"par30k l2c grids"}
C=$V/q/chunks; mkdir -p $C
cp /ssd/parity/pts/l2c.txt /ssd/parity/pts/grids.txt $V/pts/ 2>/dev/null
for p in $POOLS; do
  [ -f $C/$p.done ] || { split -d -l 2500 -a 3 $V/pts/$p.txt $C/$p.; touch $C/$p.done; }
done
for l in $LABELS; do
  for s in smallest recommended max; do
    for p in $POOLS; do
      for c in $(ls $C | grep -E "^$p\.[0-9]{3}$"); do
        echo "$l $s $p $c ${DATA[$l]} ${SRV[$l]}"
      done
    done
  done
done | xargs -P ${JOBS:-40} -L 1 bash -c '
  l=$0 s=$1 p=$2 c=$3 data=$4 srv=$5
  W=/ssd/verify6/q/parts/${l}_$s; mkdir -p $W
  nice -n 10 /ssd/verify6/bin/grid_query.$srv /ssd/verify6/sets/$data/$s < /ssd/verify6/q/chunks/$c > $W/$c.part 2> $W/$c.err || echo "FAIL $l $s $c"'
for l in $LABELS; do
  for s in smallest recommended max; do
    Q=$V/q/${l}_$s; mkdir -p $Q
    for p in $POOLS; do
      cat $(ls $V/q/parts/${l}_$s/$p.[0-9][0-9][0-9].part | sort) > $Q/$p.txt
      [ "$(wc -l < $Q/$p.txt)" = "$(wc -l < $V/pts/$p.txt)" ] || echo "SHORT $l $s $p"
    done
  done
  E=$V/l2c/$l; mkdir -p $E
  for s in smallest recommended max; do ln -sf $V/q/${l}_$s/l2c.txt $E/ans_$s.txt; done
  /ssd/shrink/venv/bin/python -I /ssd/transport-verify/scripts/l2c_eval.py /ssd/transport-verify/l2c/checks_v.json \
    /ssd/loc2city/ne/ne_10m_admin_0_countries.geojson $E /ssd/transport-verify/l2c/sample100.json $E/eval.json > $E/eval.log 2>&1
  /ssd/shrink/venv/bin/python -I -c "
import json; d=json.load(open('$E/eval.json'))
print('L2C $l', ' '.join(f\"{s}={d['summary'][s]['all_pass']}\" for s in ('smallest','recommended','max')))"
done
echo PQ_DONE
