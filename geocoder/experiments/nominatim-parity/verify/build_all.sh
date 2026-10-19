#!/bin/bash
# build_all.sh : every binary verify6 needs, each from a git archive of a committed revision in
# verify6's own clones of the three tracks' repos (/ssd/verify6/repo-{parity,reader,searchbuild}).
#   builder  build-index + geocoder-tests at bc2e758 (fix/admin-nesting) and f7096b0 (parity tip)
#   server   grid_query at bc2e758 and each parity server commit; cargo test at bc2e758 and 64183ef
#   reader   grid_query, admin_dump, rg_bench at main f48ddc4, 66a5ad0, tip 9b16065; cargo test at tip
#   search   place-search at 74a4a88 and d54f85d; cargo test at d54f85d
# Out: /ssd/verify6/bin/<name>.<rev>, logs /ssd/verify6/logs/build_*.log. Niced.
set -u
V=/ssd/verify6
export TMPDIR=$V/tmp CARGO_HOME=$V/cargo PATH=$HOME/.cargo/bin:$PATH
log() { echo "$(date -Is) $*"; }
mkdir -p $V/src $V/bin $V/logs
bbuild() {
  local rev=$1 s=$V/src/b-$1 b=$V/bb-$1
  rm -rf $s $b; mkdir -p $s/geocoder/server $b
  git -C $V/repo-parity archive $rev geocoder/builder | tar -x -C $s
  cmake -S $s/geocoder/builder -B $b -DCMAKE_BUILD_TYPE=Release -DGEOCODER_S2_PREFIX=/ssd/precision/s2-prefix > $b/cmake.log 2>&1 || { log "CMAKE_FAIL $rev"; return 1; }
  nice -n 19 make -C $b -j16 build-index geocoder-tests > $b/make.log 2>&1 || { log "MAKE_FAIL $rev"; return 1; }
  (cd $b && ./geocoder-tests > tests.log 2>&1; echo "rc=$?" >> tests.log)
  cp $b/build-index $V/bin/build-index.$rev
  log "BUILDER $rev $(tail -3 $b/tests.log | tr '\n' ' ')"
}
cbuild() {  # repo rev examples(comma) test(0/1)
  local repo=$1 rev=$2 ex=$3 t=$4 s=$V/src/s-$1-$2
  rm -rf $s; mkdir -p $s
  git -C $V/repo-$repo archive $rev geocoder/server geocoder/builder/data | tar -x -C $s
  cp $V/scripts/rg_bench.rs $V/scripts/admin_dump.rs $s/geocoder/server/examples/
  local args=()
  for e in ${ex//,/ }; do args+=(--example $e); done
  (cd $s/geocoder/server && CARGO_TARGET_DIR=$V/target-$repo-$rev nice -n 19 cargo build --release -j8 "${args[@]}") > $V/logs/build_$repo-$rev.log 2>&1 || { log "CARGO_FAIL $repo $rev"; return 1; }
  for e in ${ex//,/ }; do cp $V/target-$repo-$rev/release/examples/$e $V/bin/$e.$rev; done
  if [ "$t" = 1 ]; then
    rm -f $s/geocoder/server/examples/rg_bench.rs $s/geocoder/server/examples/admin_dump.rs
    (cd $s/geocoder/server && CARGO_TARGET_DIR=$V/target-$repo-$rev nice -n 19 cargo test --release -j8) > $V/logs/test_$repo-$rev.log 2>&1
    log "TEST $repo $rev rc=$? $(grep -E '^test result' $V/logs/test_$repo-$rev.log | tr '\n' ' ')"
  fi
  log "CARGO $repo $rev done"
}
sbuild() {  # rev test(0/1)
  local rev=$1 t=$2 s=$V/src/p-$1
  rm -rf $s; mkdir -p $s
  git -C $V/repo-searchbuild archive $rev geocoder | tar -x -C $s
  (cd $s/geocoder/search && CARGO_TARGET_DIR=$V/target-search-$rev nice -n 19 cargo build --release -j8) > $V/logs/build_search-$rev.log 2>&1 || { log "CARGO_FAIL search $rev"; return 1; }
  cp $V/target-search-$rev/release/place-search $V/bin/place-search.$rev
  if [ "$t" = 1 ]; then
    (cd $s/geocoder/search && CARGO_TARGET_DIR=$V/target-search-$rev nice -n 19 cargo test --release -j8) > $V/logs/test_search-$rev.log 2>&1
    log "TEST search $rev rc=$? $(grep -E '^test result' $V/logs/test_search-$rev.log | tr '\n' ' ')"
  fi
  log "SEARCH $rev done"
}
bbuild bc2e758 & bbuild f7096b0 &
cbuild parity bc2e758 grid_query,admin_dump 1 &
cbuild parity 64183ef grid_query,admin_dump 1 &
cbuild reader f48ddc4 grid_query,admin_dump,rg_bench 0 &
cbuild reader 9b16065 grid_query,admin_dump,rg_bench 1 &
wait
cbuild reader 66a5ad0 grid_query,admin_dump,rg_bench 0 &
for r in 984e668 7e8e9bb c99516b 9171a8e 509845b; do cbuild parity $r grid_query 0 & done
sbuild 74a4a88 0 & sbuild d54f85d 1 &
wait
md5sum $V/bin/* | sort -k2
echo BUILD_ALL_DONE
