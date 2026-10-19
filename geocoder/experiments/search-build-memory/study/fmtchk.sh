#!/bin/bash
# fmtchk.sh <rev>...: rustfmt --check diff hunks per search source file at each rev (no rustfmt.toml).
export PATH=$HOME/.cargo/bin:$PATH
S=/ssd/searchbuild
for rev in "$@"; do
  d=$S/src/fmtchk/$rev; rm -rf $d; mkdir -p $d
  (cd $S/repo && git archive $rev geocoder/search/src) | tar -x -C $d
  printf '%s:' $rev
  for f in $d/geocoder/search/src/*.rs; do
    n=$(rustfmt --edition 2021 --check $f 2>/dev/null | grep -c '^Diff in')
    printf ' %s=%s' $(basename $f .rs) $n
  done
  echo
done
