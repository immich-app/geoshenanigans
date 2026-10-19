#!/bin/bash
# build_bins.sh : verify5's own binaries from the committed sources (nice 10, 16 jobs).
#   builder  : bq = 4f87ab6 (q2.5 + ring cap), br1 = bdac8cd (feat/admin-tier-tolerances, r1)
#              + geocoder-tests of br1, geocoder-diff / geocoder-patch from br1
#   readers  : main = f48ddc4, psp = 74a4a88, pspr = psp with 66a5ad0 reverted,
#              nest = bc2e758 (fix/admin-nesting), nest3 = nest + 20e4f4f f2f6d52 8dbb58d (cherry-picked here),
#              arcs = arcs/arcs-reader e566648 (r5 arc reader prototype)
set -u
V=/ssd/verify5
export CARGO_HOME=$V/cargo TMPDIR=$V/tmp PATH=$HOME/.cargo/bin:$PATH
[ -d $V/cargo ] || cp -a /ssd/tiers3/cargo $V/cargo
cd $V/repo
git config user.name Zack; git config user.email zack@futo.org
wt() { [ -d $V/wt-$1 ] || git worktree add -q --detach $V/wt-$1 $2; }
wt bq 4f87ab6
wt br1 bdac8cd
wt main f48ddc4
wt psp 74a4a88
wt nest bc2e758
wt arcs e566648
if [ ! -d $V/wt-pspr ]; then
  git worktree add -q -b verify5/pspr $V/wt-pspr 74a4a88
  (cd $V/wt-pspr && git revert --no-edit 66a5ad0 > /dev/null && git log --oneline -1)
fi
if [ ! -d $V/wt-nest3 ]; then
  git worktree add -q -b verify5/nest3 $V/wt-nest3 bc2e758
  (cd $V/wt-nest3 && git cherry-pick 20e4f4f f2f6d52 8dbb58d > /dev/null && git log --oneline -4)
fi
for b in bq br1; do
  mkdir -p $V/bin-$b
  (cd $V/bin-$b && cmake -DCMAKE_BUILD_TYPE=Release $V/wt-$b/geocoder/builder > cmake.log 2>&1 && nice -n 10 make -j 16 > make.log 2>&1)
  echo "MAKE $b rc=$?"
done
(cd $V/bin-br1 && ./geocoder-tests > tests.log 2>&1; echo "TESTS br1 rc=$?"; tail -2 tests.log)
for r in main psp pspr nest nest3 arcs; do
  (cd $V/wt-$r/geocoder/server && CARGO_TARGET_DIR=$V/target-$r nice -n 10 cargo build -j 16 --release --example grid_query > $V/logs/cargo_$r.log 2>&1)
  echo "CARGO $r rc=$?"
  cp $V/target-$r/release/examples/grid_query $V/bin/grid_query.$r
done
ls $V/wt-arcs/geocoder/builder/tools/arcs_study
echo BINS_DONE
