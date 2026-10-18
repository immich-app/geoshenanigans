#!/bin/bash
# Answer every pts_*.txt with the real reader (grid_query) against the
# original minimal set and every var/* dir. Output: q/<variant>.<pool>.jsonl
cd "$(dirname "$0")"
GQ=/ssd/shrink/target/release/examples/grid_query
mkdir -p q
run() {
  v=$1; p=$2
  if [ "$v" = orig ]; then d=/ssd/shrink/idx/smallest; else d=var/$v; fi
  "$GQ" "$d" < "pts_$p.txt" > "q/$v.$p.jsonl" 2> "q/$v.$p.err"
}
export -f run; export GQ
for v in orig ${VARIANTS:-identity floor_75 floor_150 scale_2 snap_1e-6 snap_1e-5 snap_1e-4}; do
  for p in random places L2 L4 L8 all; do echo "$v $p"; done
done | xargs -P ${JOBS:-16} -L 1 bash -c 'run "$@"' _
