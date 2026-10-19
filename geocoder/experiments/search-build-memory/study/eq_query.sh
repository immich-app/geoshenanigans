#!/bin/bash
# eq_query.sh <run dir> <cfg>: run the evaluation queries (the 1,311 of queries/*.tsv plus
# verify4's fresh held-out 283) against the reference index and the index in <run dir>/idx with the
# same reader (place-search.base), and count result lines that differ (timing field "us" dropped).
set -u
S=/ssd/searchbuild
R=$1; cfg=$2
case $cfg in
  min)  set_=minimal;     ref=min-1c;   extra=() ;;
  rec)  set_=recommended; ref=rec-1c;   extra=() ;;
  max)  set_=max;         ref=max-1c;   extra=() ;;
  maxn) set_=max;         ref=maxn-1c;  extra=() ;;
  recx) set_=recommended; ref=rec-x-1c; extra=(--extras $S/extras/recommended/extra_names.bin) ;;
esac
Q=$S/eval
mkdir -p $Q
if [ ! -s $Q/queries.txt ]; then
  for f in $S/repo/geocoder/search/queries/queries_*.tsv /ssd/verify4/srch/acc/h4fresh.tsv; do
    grep -v '^#' $f | cut -f2
  done | grep -v '^$' > $Q/queries.txt
fi
strip() { python3 -I -c 'import json,sys
for l in sys.stdin:
    d=json.loads(l); d.pop("us",None); print(json.dumps(d,sort_keys=True))'; }
for which in ref new; do
  idx=$S/ref/idx/$ref; [ $which = new ] && idx=$R/idx
  taskset -c 43 $S/bin/place-search.base query $S/sets/$set_ $idx --iso $S/extras/$set_/iso_codes.tsv "${extra[@]}" < $Q/queries.txt 2> $R/q.$which.err | strip > $R/q.$which.jsonl
done
n=$(wc -l < $R/q.ref.jsonl)
d=$(diff $R/q.ref.jsonl $R/q.new.jsonl | grep -c '^>')
echo "$cfg queries=$n differing=$d" | tee $R/eq_query.txt
