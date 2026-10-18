#!/bin/bash
# Re-runs every timing result whose SMT siblings (24-31) were >10% busy
# (another step's untimed work), keeping the first result as *.sibbusy.
# nohup bash rerun_busy.sh > logs/rerun_busy.log 2>&1 < /dev/null &
set -u
B=/ssd/shrink/sortpatch/patch
cd $B
pair_of() { case $1 in 2026-10-13) echo "2026-10-12 2026-10-13" ;; 2026-10-12) echo "2026-10-11 2026-10-12" ;; esac; }
var_of() { case $1 in planet_full) echo planet/full ;; planet_quality_uncapped) echo planet/quality/uncapped ;; planet_poi_all) echo planet/poi/all ;; esac; }
for r in out/*/*/*/diff.result out/*/*/*/apply-*.result; do
  [ -f "$r" ] || continue
  busy=$(grep -o 'sib_busy=[0-9]*' "$r" | cut -d= -f2)
  [ -n "$busy" ] && [ "$busy" -gt 10 ] || continue
  IFS=/ read -r _ tool new safe file <<< "$r"
  v=$(var_of $safe); set -- $(pair_of $new)
  echo "--- rerun $r (sib_busy=$busy%) $(date +%T)"
  mv "$r" "$r.sibbusy"
  case $file in
    diff.result) cp out/$tool/$new/$safe/patch.gcpatch out/$tool/$new/$safe/patch.gcpatch.first
                 bash $B/diff_one.sh $tool $1 $2 $v
                 cmp -s out/$tool/$new/$safe/patch.gcpatch out/$tool/$new/$safe/patch.gcpatch.first && echo "same patch bytes" || echo "PATCH CHANGED"
                 rm -f out/$tool/$new/$safe/patch.gcpatch.first ;;
    apply-*.result) prof=${file#apply-}; prof=${prof%.result}
                    bash $B/apply_one.sh $tool $1 $2 $v $prof ;;
  esac
done
echo "=== RERUN DONE $(date -u +%FT%TZ)"
