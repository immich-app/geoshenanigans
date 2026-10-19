# Search build memory: the place-search index in a 32 MiB cgroup

Can the place-search index (`geocoder/search`, `place-search build`) be
built on a small device, inside a 32 MiB memory cgroup with no swap, and
still write byte-identical index files? At `feat/place-search-prod`
74a4a88 the recommended and max builds were OOM-killed at 32 MiB. This
track cut the build's anonymous memory commit by commit, measured each step
with a 20 ms sampler under the box's timing lock, and diagnosed where the
remaining 32 MiB slowdown comes from. Run 2026-10-19 on the box
(`/ssd/searchbuild`, two agents: the second resumed after a cutoff),
re-checked the same day by an independent verifier (`/ssd/verify6`,
`verify/`) that rebuilt both binaries from committed source.

The code is branch `perf/search-build-memory` (8 commits 84210e7..d54f85d
on 74a4a88, box repo `/ssd/searchbuild/repo`, not yet pushed), plus
`perf/search-build-memory-fst` (9c17e1d, a vendored fst crate). Neither is
in this research branch.

Tags: **M** = measured on the box (by this track's agents or the
verifier), **C** = computed from measured values, **E** = estimated. Every
run: one pinned core, `--threads 1`, private copies of the set files, page
cache evicted before the run; 32 MiB = `systemd-run --scope -p
MemoryMax=32M -p MemorySwapMax=0`; peak anonymous memory sampled every
20 ms; temp disk = peak size of the temporary directory.

## Results (verified 2026-10-19)

- **Fits 32 MiB, byte-identical (M):** the final binary d54f85d with
  default arguments builds minimal, recommended and max inside 32 MiB with
  0 OOM kills, and every index file equals the unconstrained 74a4a88
  build's (5/5, 9/9, 9/9). Only `search_meta.json` differs between builds,
  and only in its statistics fields (memory, run counts, times).
- **Queries identical (M):** all 1,594 evaluation queries (the 1,311 in
  `queries/*.tsv` plus 283 held out by an earlier verifier) give identical
  results on the d54f85d indexes for all three sets; the `recx` and `maxn`
  configurations matched too (on 957ef7f). Reading with the new binary
  instead of 74a4a88's: 0 differences on recommended.
- **Baseline OOM confirmed (M):** 74a4a88 with `--mem-mb 16` in 32 MiB is
  killed on recommended at 29.0 MiB anon with 5 of 9 files written, and on
  max at 26.6 MiB with 3 files (the track said 1 file); both kills are in
  the kernel log. Minimal fits (22.7 MiB, 52.5 s).
- **Cost in 32 MiB (M):** max takes 496.4 s against 135.5 s unconstrained
  (3.66×, C); recommended 44.2 against 21.4 s, minimal 19.0 against 10.4 s.
- **Temp disk grows (C):** recommended +33%, max +11% against 74a4a88
  (97.8 / 168.7 / 1,488 MiB). Both sub-indexes now sort their keys before
  either merges, so both sets of sort runs sit on disk together, and the
  extra merge passes rewrite some of them.
- **Tests and lint (M):** 20/20 tests pass (base had 12). Clippy reports the
  same 4 findings at base and head, one of them the deny-level error at
  `query.rs:91` that predates the branch, so `cargo clippy` fails on base
  as well. The crate was not rustfmt-clean at base either (no
  `rustfmt.toml`).

### d54f85d, default arguments (M)

Verifier (`/ssd/verify6/sb`):

| set | in 32 MiB: anon MiB / wall s | unconstrained wall s | 74a4a88 `--mem-mb 16` in 32 MiB | files equal to unconstrained 74a4a88 |
|---|---|---|---|---|
| minimal | 13.3 / 19.0 | 10.4 | ok, 22.7 MiB / 52.5 s | 5/5 |
| recommended | 14.6 / 44.2 | 21.4 | OOM-killed | 9/9 |
| max | 23.5 / 496.4 | 135.5 | OOM-killed | 9/9 |

Track (`/ssd/searchbuild/runs3`):

| set | mode | result | files equal to 74a4a88 | peak anon MiB | wall s | key / merge s | peak temp MiB |
|---|---|---|---:|---:|---:|---:|---:|
| minimal | 32 MiB | ok | 5/5 | 13.3 | 19.2 | 15.9 / 3.2 | 100 |
| recommended | 32 MiB | ok | 9/9 | 14.6 | 42.7 | 36.6 / 6.0 | 169 |
| max | 32 MiB | ok | 9/9 | 23.5 | 502.1 | 434.8 / 67.2 | 1486 |
| minimal | unconstrained | ok | 5/5 | 13.4 | 10.4 | 7.3 / 3.1 | 98 |
| recommended | unconstrained | ok | 9/9 | 14.6 | 21.4 | 15.8 / 5.5 | 167 |
| max | unconstrained | ok | 9/9 | 23.5 | 137.9 | 85.7 / 52.1 | 1485 |

Max in 32 MiB measured 510.5 s (957ef7f), 502.1 and 510.9 s; unconstrained
138.2 / 137.9 / 137.4 s.

### Effect of each commit, unconstrained peak anon MiB (M, first agent's runs; shared set files, so times not comparable)

| commit | minimal | recommended | max |
|---|---:|---:|---:|
| 74a4a88 (instrumented) | 22.7 | 30.8 | 51.8 |
| 84210e7 boundary slots on disk | 15.3 | 30.7 | 51.8 |
| 159d180 buffers in their own mappings | 15.3 | 27.3 | 34.9 |
| c38ce96 merge reads at most 16 runs at once | 13.4 | 27.4 | 30.8 |
| b130ba7 cap trailing-word postings while merging | 13.4 | 27.4 | 30.4 |
| 09ef864 all key phases before the merges | 13.3 | 14.6 | 23.4 |
| 957ef7f country looked up only to transliterate | 13.3 | 14.6 | 23.5 |

d8853ae adds the tests; d54f85d lowers the default sort budget from 64 to
16 MiB (at 64 MiB the build itself needs 43–58 MiB, so it could not fit
32 MiB without `--mem-mb 16`). 957ef7f is mainly the speed fix for minimal
in 32 MiB: 46.8 s before it, 20.1 s after (M).

### Why max is 3.6× slower in 32 MiB (M, per-phase sampler)

| POI key phase | wall s | CPU s | major faults | disk read MiB |
|---|---:|---:|---:|---:|
| 32 MiB | 398.7 | 124.4 | 4,139,452 | 16,195 |
| unconstrained | 67.0 | 61.1 | 257,257 | 1,028 |

- **Cause:** the POI names in `strings_poi` are not stored in the order of
  the POI records. A simulation over all 24.17M indexed POIs
  (`namecache.py`, C) predicts 3.62M random page misses with a 4–8 MiB
  cache, which matches the measured fault count.
- **A name cache would not help (C):** there are 15.4M distinct names, and a
  64k-entry name cache removes only 0.4% of the misses.
- **Readahead makes it worse (M):** replacing the reader's no-readahead
  advice (records sequential, names normal readahead; `mk_adv.py`,
  `advexp.sh`) took max in 32 MiB to 689.2 s, the POI phase reading
  270,607 MiB. Not committed.
- The verifier did not re-measure this diagnosis; the slowdown itself it
  measured (3.66×).

### Vendored fst (M, measured on 6196c84 before the rebase)

With the patched fst 0.4.7 builder registry, max peaks at 18.0 MiB at the
16 MiB budget, and minimal / recommended / max at 8.8 / 8.9 / 10.8 MiB at an
8 MiB budget. It does not change time in 32 MiB (505.7 against 510.5 s).
The rebased tip 9c17e1d builds and gives identical minimal output (M); its
recommended and max numbers are unverified (the only difference is the
changed default).

## Decision status

The verifier found all 8 commits of `perf/search-build-memory` safe to push
as they are (output byte-identical, tests pass); it did not check the fst
branch. Open:
1. Vendor the patched fst 0.4.7, or try to get the fix into fst upstream?
   (a dependency decision)
2. Fix max's wall time in 32 MiB? Two ways: the builder writes
   `strings_poi` in POI record order (a builder change), or the index
   build reads POIs in name order (two passes, about 300 MB more temp disk,
   E). Recommended shows the same pattern (42.7 against 21.4 s), not
   diagnosed.
3. Is +11% (max) and +33% (recommended) temp disk acceptable on devices?
4. Fix the pre-existing clippy error at `query.rs:91` as its own change.

Not done, by decision: an importance byte in the POI record padding (the
36-byte record has no spare byte, so `imp_poi.u8` and `--imp` stay; for
places it would only replace two sequential reads and needs a builder
format change).

## Method

- **Runs:** `runb.sh` (one build: binary tag, config `min` / `rec` /
  `max` / `maxn` / `recx`, mode `free` or `cgNN`; pinned core,
  `memwatch3.py` sampler, every output file compared with the reference
  index), `matrix.sh` (a list of runs under the timing lock, cache evicted
  before each), `queue2.sh`, `final.sh` (the final matrix plus `diagrun.sh`
  runs of max), `evict.py` (`POSIX_FADV_DONTNEED` on the set's files),
  `mk_psets.sh` (private copies of the files the build reads, so no other
  track's page cache is charged outside the cgroup), `table.py` (run
  table).
- **Builds:** `cbuild.sh` (a revision from `git archive` into
  `bin/place-search.<tag>`), `check.sh` (cargo test and clippy of a
  revision), `fmtchk.sh` (rustfmt diff per file).
- **Equivalence:** `eq_query.sh` (the 1,594 evaluation queries against the
  reference index with the same reader; timing field dropped).
- **Where memory and time go:** `tbuild.sh` + `mk_trace.py` + `memtrace.rs`
  (an experiment binary that prints live heap and RssAnon per build phase;
  not committed), `fstmem/` (a small crate that rebuilds an fst key by key
  and reports the builder registry's heap), `rsswatch.py` (are mapped file
  pages charged to the build's cgroup), `diag.py` / `diagrun.sh` /
  `diagsum.py` (per-phase CPU, faults, I/O and cgroup pressure every
  100 ms), `locality.py` and `namecache.py` (page locality of the build's
  name reads, and what a name cache would save), `advexp.sh` + `mk_adv.py`
  (the readahead experiment).
- **Verifier** (`verify/`): `sb.sh` (74a4a88 reference unconstrained,
  d54f85d in 32 MiB and unconstrained, 74a4a88 at default arguments in
  32 MiB, every file compared), `sb2.sh` (74a4a88 at `--mem-mb 16`, the
  track's baseline configuration), `bcg_cap.sh` (stops a thrashing 74a4a88
  default run after 180 s), `memwatch6.py` (its own sampler), `clippy6.sh`.
  Binaries from `../nominatim-parity/verify/build_all.sh`.

## Layout

- `study/`: the track's scripts (both agents; `/ssd/searchbuild` on the
  box) and `fstmem/` (Cargo.toml and source of the fst heap experiment).
- `verify/`: the searchbuild part of the verifier's `/ssd/verify6/scripts`.

Left out: the edit scripts and test file whose results are commits on the
branches (`edit_phases.py` = 09ef864, `edit_lazycc.py` = 957ef7f,
`build_tests.rs` = d8853ae's tests, `fst_patch.py` and `fst_test_patch.py`
= the vendored fst of 9c17e1d), `sync.sh` (copied a local working tree to
the box), and the local source snapshots of the search crate (`work/`,
`src-orig/`, `head957/`: copies of the branch).

The scripts hard-code the box paths (`/ssd/searchbuild`, `/ssd/verify6`,
and the sets and extras of `/ssd/search-prod`) and use
`/ssd/shrink/sortpatch/TIMING.lock`.

## Rebuild and run

```sh
X=$PWD/geocoder/experiments/search-build-memory
S=/ssd/searchbuild
mkdir -p $S/bin $S/logs && cp -r $X/study/* $S/
# $S/repo: a clone with perf/search-build-memory; $S/sets, $S/extras -> /ssd/search-prod
cd $S
./cbuild.sh base 74a4a88 && ./cbuild.sh s8 d54f85d
./mk_psets.sh
./final.sh logs/final.log            # takes the timing lock: min / rec / max in 32 MiB and free, diag runs
./eq_query.sh runs3/s8-max-cg32 max
python3 -I table.py runs3
./check.sh s8 d54f85d

# verifier (binaries from ../nominatim-parity/verify/build_all.sh)
/ssd/verify6/scripts/sb.sh && /ssd/verify6/scripts/sb2.sh
```

## Box paths

- `/ssd/searchbuild`: final and diagnostic runs `runs3/` (`summary.txt`,
  `mem.json`, `build.log`, `diag.jsonl`), the first agent's matrix
  `runs2/` (logs only), `runs/`, `runs-trace/`, logs `logs/` (`final.log`,
  `advexp.log`, `namecache_max.txt`), binaries
  `bin/place-search.s8` (d54f85d) and `place-search.s9f` (fst branch),
  private set copies `psets/`, reference index `ref/`.
- Repo `/ssd/searchbuild/repo`: `perf/search-build-memory` (d54f85d),
  `perf/search-build-memory-fst` (9c17e1d).
- Verifier: `/ssd/verify6/sb/runs/`.

## Data and tool versions

- Sets: the search-prod client sets (minimal, recommended, max) and their
  extras (`/ssd/search-prod/sets`, `/ssd/search-prod/extras`).
- `place-search` at 74a4a88 (`feat/place-search-prod`) and d54f85d; fst
  0.4.7 (vendored and patched on the fst branch).
