#!/bin/bash
# Build the boundary-tier variants (today's format) and the query set dirs.
#   var/<name>/admin_polygons.bin + admin_vertices.bin   (gridvar var ...)
#   sets/<name>/  symlink farm: every idx/max file, except the 4 quality files, which come
#                 from the variant (admin_*) and from the variant's base quality (postal_*)
# Logs one JSON stats line per variant to logs/gridvar.jsonl.
set -euo pipefail
cd /ssd/shrink/grid
export OMP_NUM_THREADS=8 TMPDIR=/ssd/shrink/tmp
MAX=/ssd/shrink/idx/max
WA=/ssd/shrink/idx/withaddr
# name | admin source dir | postal source dir | dp metres | grid deg
VARS=(
  "q25_g4    $WA       $WA       0  1e-4"
  "unc_g4    $MAX      $MAX      0  1e-4"
  "q1_g5     dl/q1     dl/q1     0  1e-5"
  "q1_g5e5   dl/q1     dl/q1     0  5e-5"
  "q05_g5    dl/q0.5   dl/q0.5   0  1e-5"
  "unc_dp11_g4 $MAX    $MAX      11 1e-4"
  "unc_dp11  $MAX      $MAX      11 0"
)
# plain quality dirs (no variant): name | quality dir
PLAIN=("q1 dl/q1" "q05 dl/q0.5" "q2 dl/q2" "q15 dl/q1.5" "q02 dl/q0.2")

farm() {  # farm <name> <admin dir> <postal dir>
  local s=sets/$1
  rm -rf "$s"; mkdir -p "$s"
  for f in "$MAX"/*; do
    b=$(basename "$f")
    case $b in
      admin_polygons.bin|admin_vertices.bin) ln -s "$(realpath "$2/$b")" "$s/$b" ;;
      postal_polygons.bin|postal_vertices.bin) ln -s "$(realpath "$3/$b")" "$s/$b" ;;
      *) ln -s "$f" "$s/$b" ;;
    esac
  done
}

for v in "${VARS[@]}"; do
  read -r name src postal dp g <<< "$v"
  [ -n "${ONLY:-}" ] && [[ " $ONLY " != *" $name "* ]] && continue
  mkdir -p var/$name
  /usr/bin/time -f "$name gridvar %e s %M KB" ./gridvar var "$src" var/$name "$dp" "$g" | tee -a logs/gridvar.jsonl
  farm "$name" var/$name "$postal"
done
for v in "${PLAIN[@]}"; do
  read -r name src <<< "$v"
  farm "$name" "$src" "$src"
done
ls sets
