#!/bin/bash
# mkvar.sh <name> <pp|arc> <min scheme> <quality scheme> : one candidate.
#   pp : tiervar (per-polygon, today's format) on the d1006 dump -> var/<name>/{min,q}
#   arc: arcs (shared arcs) on the d1006 dump + arcdump -> var/<name>/{min,q}/{arc,dec}; the sets
#        use dec/ (the arc format decoded back into today's files, f32 vertices)
# sets/<name>/{smallest,recommended} = the base sets with the variant's admin files.
# BUILD=<name> uses another planet build's dump (default d1006); SKIPSETS=1 builds files only.
set -euo pipefail
A=/ssd/arcs
b=${BUILD:-d1006}
D=$A/build/$b
B=$A/build/out-$b/planet
n=$1; kind=$2; ms=$3; qs=$4
V=$A/var/$n
export OMP_NUM_THREADS=${THREADS:-32}
rm -rf $V; mkdir -p $V/min $V/q
if [ $kind = pp ]; then
  /usr/bin/time -f "%e s %M KB" -o $V/min.time $A/tiervar minimal $D/dump $B/admin $V/min "$ms" > $V/min.json
  /usr/bin/time -f "%e s %M KB" -o $V/q.time $A/tiervar quality $D/dump $V/q "$qs" > $V/q.json
  mf=$V/min; qf=$V/q
else
  /usr/bin/time -f "%e s %M KB" -o $V/min.time $A/arcs minimal $D/dump $D/arcdump $B/admin $V/min "$ms" > $V/min.json
  /usr/bin/time -f "%e s %M KB" -o $V/q.time $A/arcs quality $D/dump $D/arcdump - $V/q "$qs" > $V/q.json
  mf=$V/min/dec; qf=$V/q/dec
fi
[ -n "${SKIPSETS:-}" ] && { echo "MKVAR_DONE $n"; exit 0; }
for s in smallest recommended; do
  d=$A/sets/$n/$s; rm -rf $d; mkdir -p $d
  for f in $A/sets/base/$s/*; do ln -s "$(readlink -f "$f")" "$d/$(basename "$f")"; done
done
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin admin_parents.bin; do ln -sfn $mf/$f $A/sets/$n/smallest/$f; done
for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $qf/$f $A/sets/$n/recommended/$f; done
if [ $kind = arc ]; then
  # sets/<name>a: the same sets read through the arc format (arc reader: target-r grid_query)
  for s in smallest recommended; do
    part=$([ $s = smallest ] && echo min || echo q)
    d=$A/sets/${n}a/$s; rm -rf $d; mkdir -p $d
    for f in $A/sets/$n/$s/*; do ln -s "$(readlink -f "$f")" "$d/$(basename "$f")"; done
    for f in arc_vertices.bin arc_index.bin poly_arcs.bin arc_meta.json admin_polygons.bin; do ln -sfn $V/$part/arc/$f $d/$f; done
  done
fi
echo "MKVAR_DONE $n"
