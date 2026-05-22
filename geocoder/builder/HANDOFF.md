# Geocoder patch-size reduction — handoff

This is a session handoff for the patch-reduction work being done in
`geocoder/builder/tools/geocoder_diff.cpp` + `geocoder_patch.cpp`.
Picking this up from another machine: read this, then jump to the
"Next concrete step" section at the bottom.

## Goal

Reduce the daily incremental patch files emitted by step 10
("Generate patches and verify") of `.github/workflows/geocoder-build.yml`.
The headline number is **planet/full compressed gcpatch size**.

| Snapshot | planet/full compressed | Notes |
| --- | --- | --- |
| Before this work series | ~3,890 MiB | Many FULL_REPLACE files, byte-key secondary matching |
| **Current baseline (HEAD = `e2f3c0e`)** | **~3,798 MiB** | Sparse-delta for 2 of 5 small files, otherwise unchanged |
| Last failed attempt | 4,164 MiB grew / 6 variants FAIL verify | See "What was tried and reverted" |

The original session target was ~5 MiB for major patches. We are nowhere near
that — the structural cost is dominated by `addr_vertices` (3,391 MiB
FULL_REPLACE) and `addr_points` (2,566 MiB merge), neither of which we have
yet touched.

## Repository state

- Branch: `feat/geocoder`
- HEAD: `e2f3c0e` (Revert "perf(geocoder-patch): delta-encode ENTRY_CORRECTION")
- All work has been pushed.
- Working tree carries one untracked + one modified file unrelated to this
  session — leave them alone:
  - `M geocoder/builder-ui/static/configurations.json` (modified before this
    session began; appears to be a stale local build artifact)
  - `?? geocoder/HANDOFF.md` (different project — TS/WASM port refactor)

## Where things stand

The reverted commits are preserved in git history for re-use. Commit chain
on `feat/geocoder` (newest first):

```
e2f3c0e Revert delta-encode ENTRY_CORRECTION       ← HEAD (clean baseline)
7dc21d3 Revert sidecar secondary-match key
8b15bfe fix(geocoder-diff): osm_id sidecar as secondary-match key   ← cherry-pick of 5453880
948cdfb perf(geocoder-patch): delta-encode ENTRY_CORRECTION
bc83158 Revert sidecar secondary-match key (first attempt)
5453880 fix(geocoder-diff): osm_id sidecar as secondary-match key   ← original implementation
8c1e39d chore(geocoder-diff): log sparse-delta run-length stats
aa54f52 fix(geocoder-diff): keep str_remap alive until after emit_sparse_delta
55ca34f perf(geocoder-diff,patch): sparse position-delta for FULL_REPLACE files
```

The two reverted experiments still apply cleanly on top of HEAD if you want
to retry them after fixing the underlying bugs.

## What's in the working baseline (HEAD)

planet/full uncompressed section breakdown from CI run **26194306798**
(2026-05-01 build with HEAD's code path):

| Section | Size | Notes |
| --- | --- | --- |
| addr_vertices (FULL_REPLACE) | 3,391 MiB | Untouched — biggest remaining target |
| addr_points (merge stride=28) | 2,566 MiB | secondary key is byte-derived |
| street_nodes (merge stride=8) | 764 MiB | |
| STRINGS_or_SECONDARY | 687 MiB | tier diff + secondary remap pairs |
| addr_postcodes (FULL_REPLACE) | 666 MiB | sparse-delta fell back |
| POI_PARENT_REMAP | 424 MiB | ~50M admin/street/postcode old→new id pairs |
| way_parents (FULL_REPLACE) | 208 MiB | sparse-delta fell back |
| interp_nodes (merge) | 177 MiB | |
| street_ways (merge w/ 49M fixups) | 158 MiB | |
| ENTRY_CORRECTION | 135 MiB | Full new-list per changed cell |
| way_postcodes [SPARSE] | 110 MiB | sparse-delta WIN (47% saving) |
| interp_ways (merge) | 77 MiB | |
| admin_parents (FULL_REPLACE) | 4 MiB | tiny, fallback doesn't matter |
| postcode_centroids [SPARSE] | 0.03 MiB | sparse-delta WIN (99.95% saving) |
| (total uncompressed) | 9,387 MiB | compresses to ~3,798 MiB |

## What was tried and reverted (do NOT just re-apply blindly)

### Attempt A: osm_id sidecar as secondary-match key (#119)

**Theory:** The diff's secondary-match fallback used byte-derived composite
keys like `(name_id<<8) | node_count` for `street_ways` or `(housenumber_id,
street_id)` for `addr_points`. These keys all embed a string-pool offset that
re-tiers daily, so the fallback can't recover the obvious "same osm_id,
different bytes" case → ~50M records per planet build get classified as
DELETE+INSERT, blowing POI_PARENT_REMAP to 424 MiB. Strategy-2 sidecars
(`.osm_ids`) already carry stable per-slot osm_ids; use those as the
secondary key instead.

**Implementation:** commit `5453880` / cherry-pick `8b15bfe`. Adds
`load_sidecar_keys()` + `secondary_match_by_idx()` + a `try_load_sidecar()`
fallback path lambda, converts the 6 call sites
(street_ways/addr_points/interp_ways/admin_polygons/poi_records/place_nodes)
plus the poi-only street_ways "cheap fallback" path.

**Result (alone, CI run 26221015149):** All 126 variants PASSED verify, but
planet/full compressed GREW 3,798 → 4,164 MiB (+366 MiB). Section breakdown
showed POI_PARENT_REMAP shrunk only modestly (424 → 387 MiB) and STRINGS
shrunk (687 → 459 MiB), but **ENTRY_CORRECTION exploded 135 → 1,594 MiB**
and addr_points merge grew 2,566 → 2,911 MiB. Reverted in `bc83158`.

**Diagnosis:** ENTRY_CORRECTION's full-new-list-per-cell encoding amplifies
with denser id_remaps. Each cell that trips the differs-check after sort
pays the cost of writing its entire entry list (not the delta).

### Attempt B: delta-encode ENTRY_CORRECTION (#120)

**Theory:** Replace the full-list emission with set-diff encoding. Cap the
per-cell cost at the count of records that actually shifted in/out, not the
cell's total entry count. Should help the baseline AND unblock attempt A.

**Implementation:** commit `948cdfb`. New marker `ENTRY_CORRECTION_DELTA_MARKER
= 0xFFFFFFF1` (legacy `0xFFFFFFF8` still handled for backward compat). Per
cell: `cell_id(8) + n_removed(2) + n_added(2) + removed_ids[] + added_ids[]`.
Patch side derives `remapped_old` (same parse+remap as the no-correction
path) then applies `(remapped_old MINUS removed) UNION added` to produce
the new list. All 4 emit sites updated (street/addr/interp via
`streaming_corrections`, plus admin/poi/place).

**Local validation:** oceania 29→30, 26/26 file MATCH on verify.

### Combined A+B: failure mode

Cherry-picked Attempt A on top of Attempt B (HEAD `8b15bfe` over `948cdfb`).
Local oceania 29→30 verify: 26/26 PASS (both alone, and combined).

**CI run 26249158568 result:** 120/126 PASS, **6 FAIL on entries files only**:

| Variant | Failed file | first_diff offset |
| --- | --- | --- |
| africa/full | interp_entries.bin | 53 |
| asia/full | interp_entries.bin + street_entries.bin (old/new size differs 457,556,752 vs 457,599,226) | 6,521 / 262,005,910 |
| asia/no-addresses | street_entries.bin | 262,005,910 |
| north-america/full | interp_entries.bin | 4,262,883 |
| oceania/full | interp_entries.bin | 243 |
| planet/full | interp_entries.bin | 4,447,659 |

**europe/* and south-america/* all passed**, including their /full variants.
verify_size always equals new_size — files are the right LENGTH but wrong
CONTENT. The mismatches are concentrated in `interp_entries` (5 of 6) and
`street_entries` (2 of 6), never `addr_entries`. Both reverted in `7dc21d3`
+ `e2f3c0e`.

**Why this is suspicious:** Either attempt alone passes locally. Combined,
they pass locally on oceania 29→30 but fail in CI on bigger / differently
shaped data. My best guess for the bug is a subtle divergence between the
diff's `id_rm` and the patch's reconstructed `rm` for `interp_ways`/
`street_ways` when sidecar soft matches make those remaps denser, which the
delta-encoded ENTRY_CORRECTION then propagates as silent content corruption
(because the legacy format embedded the full new list and never relied on
patch-side remap equality). Both sides should produce identical sorted IDs
by construction (`derive_id_remap_from_merge` + soft matches mirrored via
`SECONDARY_REMAP_MARKER`'s `+1` encoding), so the divergence is non-obvious.

## Open tasks

(Mirror of `TaskList` — keep in sync if you edit there.)

- **#101 pending** — Strategy 2: simplify diff/patch — drop fixup_*_offsets
  and PARENT_REMAP_MARKER once strategy-2 is fully bootstrapped.
- **#105 pending** — Investigate residual POI parent-link non-determinism.
- **#115 pending** — Rebuild `*_vertices` files in record-order after
  strategy-2 reorder (admin_vertices, poi_vertices, addr_vertices,
  postal_vertices). `addr_vertices` 3,391 MiB FULL_REPLACE is the single
  biggest remaining target.
- **#117 pending** — Improve `addr_points` secondary match key (2.5 GiB
  merge). Essentially: re-apply Attempt A *just* for addr_points once the
  delta-correctness issue is resolved.
- **#119 pending (reverted)** — Use osm_id from sidecar as diff
  secondary-match key. Implementation in commits `5453880` / `8b15bfe`.
- **#120 pending (reverted)** — Delta-encode ENTRY_CORRECTION. Implementation
  in commit `948cdfb`.

## Useful artefacts

- Walker for decompressed `.gcpatch` files: `/tmp/walk-patch.py` — reports
  per-section sizes. NOTE: its `FILE_NAMES` table maps `PatchFileId` →
  filename; if `patch_format.h` changes the enum, fix the table or you'll
  mis-label sections (see commit history for an example).
- `geocoder-diff` stderr is `2>/dev/null`'d at line 605 of
  `.github/workflows/geocoder-build.yml`. To see per-variant diff stats in
  CI logs, un-redirect that line. Otherwise, fetch the patch from
  `https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/<DATE>/<region>/<variant>/patch.gcpatch`,
  zstd-decompress, and run walk-patch.
- Sidecar format (12 bytes/slot): magic `0xD0510EAD`, version `1`, count,
  then `{u8 object_type, u8 flags, u16 reserved, u64 stable_id}` per slot.
  Tombstones = `flags & 0x01` or `object_type == 0` (ObjectType::NONE).
  See `src/id_allocator.h` line 60+.
- Strategy-2 logs from step 9 (`Build geocoder index`) — `strategy2 streets:
  N live, M tombstones, total slots (no shifts)`. `(no shifts)` appears only
  when the identity check passes. `streets`/`admins`/`postcodes` always
  report it; `addrs`/`places`/`pois`/`interps` reshuffle (this is fine —
  positions still end up stable post-reshuffle).

## Memory entries worth knowing

The auto-memory under
`/home/zack/.claude/projects/-home-zack-Source-immich-geoshenanigans/memory/`
already has these (relevant to this work):

- `project_entry_correction_amplifies.md` — full record of the ENTRY_CORRECTION
  blowup observed with attempt A alone. The diagnosis there is correct.
- `reference_diff_stderr_redirected.md` — the `2>/dev/null` gotcha.
- `feedback_keep_iterating_on_build.md`, `feedback_ship_dont_pause.md`,
  `feedback_github_actions_build.md` — process feedback that shaped this
  session's cadence.

## Next concrete step

Pick **one** of these:

1. **Debug the delta+sidecar combo (highest leverage).** Get the asia/full
   or planet/full failing data locally, reproduce the verify failure, and
   trace where diff's `d_ids` vs patch's `remapped_old` diverge for an
   `interp_entries` cell. The diff and patch side ID maps should be
   identical by construction — finding what's actually different will
   unlock both #119 and #120. Start by un-redirecting the CI stderr or
   adding a temporary log dump inside `streaming_corrections` (diff) and
   `process_cell::do_entry`'s delta-application branch (patch) for one
   specific cell_id from the failing offset.
2. **Tackle `addr_vertices` (#115).** 3,391 MiB FULL_REPLACE every day is
   the single biggest section. Strategy-2 reshuffles addr_points but
   `addr_vertices.bin` is byte-offset-keyed, not slot-keyed. Rebuilding it
   in record-order after the strategy-2 reshuffle would make it byte-block
   diffable like `admin_vertices` / `poi_vertices` already are.
3. **Accept the baseline and ship.** 3.8 GiB compressed daily for an
   11-billion-coordinate global geocoder isn't unreasonable. Close #117 /
   #119 / #120 as "won't pursue further" if the cost-benefit doesn't
   justify another debug cycle.

If picking option 1 and you successfully isolate the bug, the cleanest
re-introduction order is: fix the delta-correctness issue → land #120 alone
→ verify CI passes + measure → land #119 on top → verify CI passes +
measure.

## Don't do

- Don't trust local oceania 29→30 PASS as evidence the change works at
  planet scale. It didn't catch the combo bug.
- Don't push another change without local oceania PASS AND a clear
  prediction of the planet/full section breakdown. Two cycles of "should
  shrink" → "actually grew" cost ~6 hours of CI.
- Don't commit `geocoder/builder-ui/static/configurations.json` — it's
  modified from a stale local build and unrelated to this work.
- Don't touch `geocoder/HANDOFF.md` (root) — it's for a different project
  (TS/WASM port refactor), not the patch tools.
